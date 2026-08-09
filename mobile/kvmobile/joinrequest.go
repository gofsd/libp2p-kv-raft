package kvmobile

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"

	"github.com/gofsd/libp2p-kv-raft/pkg/daemon"
	"github.com/gofsd/libp2p-kv-raft/pkg/registry"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmclient"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// pendingDirName is startPending's fixed cluster subdirectory -- unlike
// startAgainst's clusterDir (named after the leader's own peer id, since
// which cluster this identity joins is already known), a pending node has
// no cluster yet to name a directory after.
const pendingDirName = "pending"

// StartPending brings up the follower daemon in-process under dataDir
// (an Android app-private directory, same as Start) but -- unlike Start,
// which immediately joins the build-time-configured leader -- never sends
// the bootstrap-or-join EventAdd at all: it's left running (host + IPC
// alive) with no raft instance, the kvmobile counterpart to desktop's
// kvctl.AddPending. This is the prerequisite for the reverse-invite
// "join-request" flow (see CreateJoinRequest/RecruitPeer on the desktop
// side, redeemed here): some other cluster's voter can push a join invite
// directly at this device (pkg/daemon's RecruitProtocolID,
// handleRecruitStream), which admits this device with no further action on
// its side beyond having minted the ticket -- but only because this device
// has no existing raft instance to conflict with; see that protocol's doc
// comment for why this feature doesn't support an already-joined device.
//
// After a successful recruit-triggered join, ListClusters' ClusterID field
// stays "" (kvmobile keeps no persistent multi-cluster registry the way
// desktop does -- see that function's doc comment) until the next
// Stop/Start cycle; Submit/Get/ListClusterMembers are unaffected, since
// those read the device's actual live raft state, not that bookkeeping
// field.
func StartPending(dataDir string) (string, error) {
	return startPending(dataDir, 0, ensureIdentity)
}

// StartPendingWithKey is like StartPending but provisions dataDir's
// identity from keyHex (see StartWithKey) instead of always falling back
// to ensureIdentity's persisted-or-generated-or-build-seeded key.
func StartPendingWithKey(dataDir, keyHex string) (string, error) {
	return startPending(dataDir, 0, func(dataDir string) (keyPath, peerID string, err error) {
		return importIdentity(dataDir, keyHex)
	})
}

// StartPendingWithKeyAndPort is StartPendingWithKey, additionally pinning
// the libp2p listen port instead of leaving it ephemeral (port == 0
// behaves exactly like StartPendingWithKey) -- see
// StartSoloWithKeyAndPort's doc comment for why pkg/e2erun/android_pair.go's
// two-emulator Join/RecruitPeer scenario specifically needs this: an
// address captured in one `adb shell am instrument` invocation must still
// be valid when dialed from a *different* device's own, later, separate
// invocation, which an ephemeral port (freshly, randomly re-chosen every
// time this device's own daemon is resumed in a fresh process) can't
// guarantee.
func StartPendingWithKeyAndPort(dataDir, keyHex string, port int) (string, error) {
	return startPending(dataDir, port, func(dataDir string) (keyPath, peerID string, err error) {
		return importIdentity(dataDir, keyHex)
	})
}

func startPending(dataDirRoot string, port int, resolveIdentity func(dataDir string) (keyPath, peerID string, err error)) (string, error) {
	mu.Lock()
	defer mu.Unlock()
	if started {
		return peerID, nil
	}

	keyPath, id, err := resolveIdentity(dataDirRoot)
	if err != nil {
		return "", err
	}

	pendingDir := filepath.Join(dataDirRoot, pendingDirName)

	// See startAgainst's identical comment: a stale ready file from an
	// earlier Start/Stop cycle against this same directory would let
	// waitForReady below return early.
	_ = os.Remove(filepath.Join(pendingDir, daemon.ReadyFileName))

	ctx, cancel := context.WithCancel(context.Background())
	errC := make(chan error, 1)
	go func() {
		errC <- daemon.Run(ctx, daemon.Config{
			DataDir:            pendingDir,
			KeyPath:            keyPath,
			ListenPort:         port,
			RelayPeers:         relayPeers(),
			HeartbeatTimeout:   raftHeartbeatTimeout,
			ElectionTimeout:    raftElectionTimeout,
			CommitTimeout:      raftCommitTimeout,
			LeaderLeaseTimeout: raftLeaderLeaseTimeout,
		})
	}()

	if err := waitForReady(pendingDir, errC, callTimeout); err != nil {
		cancel()
		return "", fmt.Errorf("kvmobile: start pending: %w", err)
	}

	// Best-effort: register id -> pendingDir in the local registry, the
	// same reasoning startAgainst's identical block gives -- needed only
	// for this package's own test suite (which exercises the desktop
	// ipc.go transport as a stand-in), never on a real device (Android's
	// ipc_android.go transport has no token/registry concept at all, so
	// registry.Open failing there must never block startup).
	if reg, regErr := registry.Open(); regErr == nil {
		_ = reg.Put(registry.NodeInfo{PeerID: id, DataDir: pendingDir})
	}

	sessCtx, sessCancel := context.WithTimeout(ctx, callTimeout)
	defer sessCancel()
	sess, err := shmclient.Open(sessCtx, id)
	if err != nil {
		cancel()
		return "", fmt.Errorf("kvmobile: fetch signing key: %w", err)
	}

	session = sess
	peerID = id
	curDataDir = pendingDir
	curDataDirRoot = dataDirRoot
	curClusterID = ""
	runErrC = errC
	cancelRun = cancel
	started = true
	return peerID, nil
}

// GetOwnAddr returns this device's own current best-advertised multiaddr
// (public first, then a relay reservation, then anything else, loopback
// last -- see pkg/daemon's advertisedAddrs) -- the address to combine with
// a CreateJoinRequest token as "<ownAddr>#<tokenHex>". Queried live, never
// cached: relayMultiaddr's circuit reservation completes asynchronously in
// the background after Start/StartPending returns, so call this again a
// moment later if an earlier call returned a private/loopback address
// instead of the relayed one.
func GetOwnAddr() (string, error) {
	sess, err := currentSession()
	if err != nil {
		return "", err
	}

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	addr, err := sess.GetOwnAddr(ctx)
	if err != nil {
		return "", fmt.Errorf("kvmobile: get own addr: %w", err)
	}
	return addr, nil
}

// CreateJoinRequest mints a fresh one-time join-request ticket on this
// device (see shmevent.EventJoinRequestCreate) and returns it hex-encoded
// -- combine with this device's own advertised address (GetOwnAddr) as
// "<ownAddr>#<tokenHex>" into a barcode for some other cluster's operator
// to scan and redeem via desktop's `mage recruitpeer`/`kvctl-cli
// recruitpeer`.
func CreateJoinRequest() (string, error) {
	sess, err := currentSession()
	if err != nil {
		return "", err
	}

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	token, err := sess.CreateJoinRequest(ctx)
	if err != nil {
		return "", fmt.Errorf("kvmobile: create join request: %w", err)
	}
	return hex.EncodeToString(token), nil
}

// CancelJoinRequest clears this device's pending join-request ticket
// before it's ever redeemed (a no-op if tokenHex no longer matches --
// already consumed or superseded by a later CreateJoinRequest).
func CancelJoinRequest(tokenHex string) error {
	sess, err := currentSession()
	if err != nil {
		return err
	}
	token, err := hex.DecodeString(tokenHex)
	if err != nil {
		return fmt.Errorf("kvmobile: invalid join request token %q: %w", tokenHex, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	if err := sess.CancelJoinRequest(ctx, token); err != nil {
		return fmt.Errorf("kvmobile: cancel join request: %w", err)
	}
	return nil
}

// RecruitPeer asks this device (an existing raft voter) to mint a normal
// join invite on its own cluster and hand-deliver it directly to the
// device named in ticket ("<device's own multiaddr>#<tokenHex>", from that
// device's own CreateJoinRequest) -- see shmevent.EventRecruit's doc
// comment. suffrage is "voter" or "learner", the kvmobile string-enum
// convention (see permitKindFromName). Returns the recruited device's own
// join result ("<peerID> ok"/"<peerID> pending") on success.
func RecruitPeer(ticket, suffrage string) (string, error) {
	sess, err := currentSession()
	if err != nil {
		return "", err
	}
	var sf byte
	switch suffrage {
	case "voter":
		sf = shmevent.SuffrageVoter
	case "learner":
		sf = shmevent.SuffrageLearner
	default:
		return "", fmt.Errorf("kvmobile: unknown suffrage %q (want \"voter\" or \"learner\")", suffrage)
	}

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	result, err := sess.Recruit(ctx, ticket, sf)
	if err != nil {
		return "", fmt.Errorf("kvmobile: recruit peer: %w", err)
	}
	return result, nil
}

// CreateJoinRequestTicket is CreateJoinRequest's signed-ticket
// counterpart -- the mobile equivalent of desktop's
// pkg/kvctl.CreateJoinRequestTicket. It does everything CreateJoinRequest
// does (mints a one-time join-request token on this device), then wraps
// this device's own current address and that token into a single signed,
// self-contained ticket, base64-encoded, for an app to render as a Data
// Matrix directly. Unlike CreateJoinInviteTicket, this ticket is signed
// by the *requesting* device, not an already-established voter -- see
// shmevent.EventJoinRequestTicket's doc comment for why the direction of
// proof is reversed here.
func CreateJoinRequestTicket() (string, error) {
	sess, err := currentSession()
	if err != nil {
		return "", err
	}

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	token, err := sess.CreateJoinRequest(ctx)
	if err != nil {
		return "", fmt.Errorf("kvmobile: create join request ticket: %w", err)
	}

	addr, err := sess.GetOwnAddr(ctx)
	if err != nil {
		return "", fmt.Errorf("kvmobile: create join request ticket: get own addr: %w", err)
	}

	m, err := shmevent.NewJoinRequestTicket(addr, token)
	if err != nil {
		return "", fmt.Errorf("kvmobile: create join request ticket: %w", err)
	}
	priv, err := shmclient.GetPrivateKey(ctx, PeerID())
	if err != nil {
		return "", fmt.Errorf("kvmobile: create join request ticket: fetch signing key: %w", err)
	}
	wire, err := shmevent.Encode(m, priv)
	if err != nil {
		return "", fmt.Errorf("kvmobile: create join request ticket: sign: %w", err)
	}
	return base64.StdEncoding.EncodeToString(wire), nil
}

// RedeemJoinRequestTicket is RecruitPeer's signed-ticket counterpart --
// the mobile equivalent of desktop's pkg/kvctl.RedeemJoinRequestTicket.
// Decodes ticketB64, extracts the requesting device's peer id from its
// own embedded address (self-certifying, via relayNodePeerID) and
// verifies the signature against that peer id's own public key,
// rejecting the ticket outright if it doesn't check out -- only then
// calls RecruitPeer exactly like it always has.
func RedeemJoinRequestTicket(ticketB64, suffrage string) (string, error) {
	var sf byte
	switch suffrage {
	case "voter":
		sf = shmevent.SuffrageVoter
	case "learner":
		sf = shmevent.SuffrageLearner
	default:
		return "", fmt.Errorf("kvmobile: unknown suffrage %q (want \"voter\" or \"learner\")", suffrage)
	}

	wire, err := base64.StdEncoding.DecodeString(ticketB64)
	if err != nil {
		return "", fmt.Errorf("kvmobile: invalid join request ticket: %w", err)
	}
	m, crc, sig, err := shmevent.Decode(wire)
	if err != nil {
		return "", fmt.Errorf("kvmobile: decode join request ticket: %w", err)
	}
	if m.Which() != shmevent.Event_Which_joinRequestTicket {
		return "", fmt.Errorf("kvmobile: not a join request ticket (event %s)", shmevent.EventName(m.Which()))
	}
	grp := m.JoinRequestTicket()
	addr, err := grp.SourceAddr()
	if err != nil {
		return "", fmt.Errorf("kvmobile: decode join request ticket: %w", err)
	}
	token, err := grp.Token()
	if err != nil {
		return "", fmt.Errorf("kvmobile: decode join request ticket: %w", err)
	}

	requesterID, err := relayNodePeerID(addr)
	if err != nil {
		return "", fmt.Errorf("kvmobile: join request ticket: %w", err)
	}
	requesterPub, err := requesterID.ExtractPublicKey()
	if err != nil {
		return "", fmt.Errorf("kvmobile: join request ticket: extract requester public key: %w", err)
	}
	requesterPubRaw, err := requesterPub.Raw()
	if err != nil {
		return "", fmt.Errorf("kvmobile: join request ticket: requester public key: %w", err)
	}
	if err := shmevent.Verify(requesterPubRaw, m, crc, sig); err != nil {
		return "", fmt.Errorf("kvmobile: join request ticket: %w", err)
	}

	sess, err := currentSession()
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	result, err := sess.Recruit(ctx, addr+"#"+hex.EncodeToString(token), sf)
	if err != nil {
		return "", fmt.Errorf("kvmobile: recruit peer: %w", err)
	}
	return result, nil
}
