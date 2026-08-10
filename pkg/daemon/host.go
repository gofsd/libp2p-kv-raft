package daemon

import (
	"fmt"
	"sort"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	lp2phost "github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/host/autorelay"
	"github.com/libp2p/go-libp2p/p2p/net/connmgr"
	v2relay "github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/relay"
	"github.com/multiformats/go-multiaddr"

	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
	"github.com/gofsd/libp2p-kv-raft/pkg/store"
)

// streamRequestTimeoutMultiplier scales streamRequestTimeout off the same
// n.electionTimeout every raft-facing wait in this package already scales
// off (5x/10x in forward.go/daemon.go) -- hashicorp/raft's default
// ElectionTimeout is 1s, so 30x preserves today's fixed 30s default exactly
// while still giving a WAN deployment with a longer configured election
// timeout proportionally more slack for its forward/join/recruit/
// exec-invite streams too.
const streamRequestTimeoutMultiplier = 30

// streamRequestTimeout bounds how long any stream protocol handler
// registered below waits for its peer to finish sending its request
// before giving up. Every handler reads its request via some blocking
// call (bufio.Scanner.Scan, readFramed, io.ReadAll) with no timeout of
// its own -- a peer that opens the stream and then stalls or dies before
// finishing its request previously left that read blocked, and its
// goroutine leaked, forever. Confirmed as the most likely single cause of
// a real production leak: a node with 14 days of uptime found running at
// 4.95GB RSS against only ~265MB of real on-disk data (raft log/
// snapshots/sqlite) -- collapsing to ~30MB immediately after a clean
// restart with the identical data, and hit by a shared, heavily-reused
// e2e test pipeline whose peers routinely get killed or time out
// mid-request. Generous relative to a same-machine/LAN round trip, short
// relative to "forever": long enough for a legitimate slow/relayed peer,
// short enough that a truly abandoned stream's goroutine exits within
// seconds, not indefinitely.
//
// n.electionTimeout is only set once initRaft runs, so a stream handled
// before that (a narrow window right after newHost registers these
// handlers, before start finishes bootstrapping raft) falls back to the
// same fixed 30s instead of a degenerate zero-timeout close.
//
// handleChannelStream/dispatchChannelOpen are the one exception: each
// clears this deadline itself (SetDeadline(time.Time{})) right after its
// initial handshake succeeds, before handing the stream off to
// pumpChannelReads' intentionally long-lived read loop -- a channel is
// meant to sit idle between chunks for arbitrary periods (bounded instead
// by channelIdleTimeout via channelTable.reap), not by this one-shot
// per-request budget.
func (n *Node) streamRequestTimeout() time.Duration {
	if n.electionTimeout <= 0 {
		return 30 * time.Second
	}
	return streamRequestTimeoutMultiplier * n.electionTimeout
}

// withStreamRequestDeadline wraps handler so its stream has
// n.streamRequestTimeout() to complete before whatever blocking read it
// does can hang forever on a peer that opened the stream and never
// finished sending anything -- see streamRequestTimeout's doc comment.
func (n *Node) withStreamRequestDeadline(handler network.StreamHandler) network.StreamHandler {
	return func(s network.Stream) {
		_ = s.SetDeadline(time.Now().Add(n.streamRequestTimeout()))
		handler(s)
	}
}

// newHost builds this node's libp2p host. Every node gets relay-client and
// hole-punching capability unconditionally, so it can be dialed through
// (or dial through) a circuit relay when a direct connection isn't
// possible -- the "worst case" NAT fallback. A node only advertises itself
// as a relay *for others* (RelayService) and forces public reachability
// when the caller knows it actually has one, e.g. the leader on a public
// VPS; the resource limits mirror the standalone relay in
// pkg/raft/node.go's StartRelayNode. st is consulted unconditionally
// whenever RelayService is on (see relayACL); it's threaded in here, ahead
// of any *Node existing, because the ACL closure needs to read confirmed
// PeerGroup records live -- one already-open *store.Store, not a snapshot
// taken at host-construction time.
// relayLimits resolves cfg's relay resource fields, substituting
// shmevent.DefaultRelay* for whichever were left at their zero value --
// mirrors the same zero-means-default pattern this Config already uses for
// its raft timing/snapshot fields. Shared by newHost (what go-libp2p
// actually enforces) and handleShmEvent's EventPermitRequest case (what
// gets stamped onto a new KindPermitPeer record), so both always agree on
// what "this node's default relay allotment" currently is.
// relayQuotaLimits/channelQuotaLimits resolve cfg's Quota* fields exactly
// the way relayLimits resolves RelayLimits just below: substituting this
// package's Default* constants (quota.go) for whichever field was left at
// its zero value. Returned in newQuotaTracker's own (peerPerSec,
// peerBurst, ipPerSec, ipBurst) parameter order so callers can pass the
// result straight through (newQuotaTracker(relayQuotaLimits(cfg))).
func relayQuotaLimits(cfg Config) (peerPerSec float64, peerBurst int, ipPerSec float64, ipBurst int) {
	peerPerSec, peerBurst, ipPerSec, ipBurst = cfg.QuotaRelayEventsPerPeerPerSec, cfg.QuotaRelayBurstPerPeer, cfg.QuotaRelayEventsPerIPPerSec, cfg.QuotaRelayBurstPerIP
	if peerPerSec == 0 {
		peerPerSec = DefaultQuotaRelayEventsPerPeerPerSec
	}
	if peerBurst == 0 {
		peerBurst = DefaultQuotaRelayBurstPerPeer
	}
	if ipPerSec == 0 {
		ipPerSec = DefaultQuotaRelayEventsPerIPPerSec
	}
	if ipBurst == 0 {
		ipBurst = DefaultQuotaRelayBurstPerIP
	}
	return peerPerSec, peerBurst, ipPerSec, ipBurst
}

func channelQuotaLimits(cfg Config) (peerPerSec float64, peerBurst int, ipPerSec float64, ipBurst int) {
	peerPerSec, peerBurst, ipPerSec, ipBurst = cfg.QuotaChannelBytesPerPeerPerSec, cfg.QuotaChannelBurstPerPeer, cfg.QuotaChannelBytesPerIPPerSec, cfg.QuotaChannelBurstPerIP
	if peerPerSec == 0 {
		peerPerSec = DefaultQuotaChannelBytesPerPeerPerSec
	}
	if peerBurst == 0 {
		peerBurst = DefaultQuotaChannelBurstPerPeer
	}
	if ipPerSec == 0 {
		ipPerSec = DefaultQuotaChannelBytesPerIPPerSec
	}
	if ipBurst == 0 {
		ipBurst = DefaultQuotaChannelBurstPerIP
	}
	return peerPerSec, peerBurst, ipPerSec, ipBurst
}

func relayLimits(cfg Config) shmevent.RelayLimits {
	limits := shmevent.DefaultRelayLimits()
	if cfg.RelayMaxCircuitsPerPeer != 0 {
		limits.MaxCircuitsPerPeer = int32(cfg.RelayMaxCircuitsPerPeer)
	}
	if cfg.RelayLimitData != 0 {
		limits.LimitData = cfg.RelayLimitData
	}
	if cfg.RelayLimitDuration != 0 {
		limits.LimitDuration = cfg.RelayLimitDuration
	}
	if cfg.RelayMaxReservationsPerIP != 0 {
		limits.MaxReservationsPerIP = int32(cfg.RelayMaxReservationsPerIP)
	}
	if cfg.RelayMaxReservationsPerPeer != 0 {
		limits.MaxReservationsPerPeer = int32(cfg.RelayMaxReservationsPerPeer)
	}
	return limits
}

// connManagerLowWater/connManagerHighWater bound this host's simultaneous
// open connections: once above high, go-libp2p's connection manager trims
// the least-useful connections back down toward low (respecting
// connManagerGracePeriod for anything newer than that). Previously
// unbounded -- no libp2p.ConnectionManager option at all -- alongside
// forgetTransientPeer, this is the other confirmed root cause of a real
// production node found running at 4.95GB RSS against ~265MB of real
// on-disk data after 14 days of uptime, hit by a shared, heavily-reused
// e2e test pipeline whose peers connect and disconnect constantly.
// Generous for this project's actual cluster sizes (single digits to low
// tens of members plus occasional relayed clients), not a hard cap on
// legitimate cluster size.
const (
	connManagerLowWater    = 100
	connManagerHighWater   = 400
	connManagerGracePeriod = 30 * time.Second
)

// relayReserveBackoff is how long AutoRelay waits before re-attempting a
// reservation with a relay that just refused it, and how often it re-reads
// its candidate list -- see newHost's own comment on why both of
// AutoRelay's defaults (1h and 30s) are wrong for a project whose relays
// gate every reservation behind standing the device itself has to go ask
// for after startup. Short enough that "request access, then pair" works
// as one uninterrupted sequence, long enough that a device with no
// standing at all isn't hammering a relay that keeps saying no.
const relayReserveBackoff = 10 * time.Second

func newHost(priv crypto.PrivKey, cfg Config, st *store.Store, selfPeerID string, relayQuota *quotaTracker) (lp2phost.Host, error) {
	cm, err := connmgr.NewConnManager(connManagerLowWater, connManagerHighWater, connmgr.WithGracePeriod(connManagerGracePeriod))
	if err != nil {
		return nil, fmt.Errorf("daemon: create connection manager: %w", err)
	}

	opts := []libp2p.Option{
		libp2p.Identity(priv),
		libp2p.ConnectionManager(cm),
		libp2p.ListenAddrStrings(
			fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", cfg.ListenPort),
			fmt.Sprintf("/ip4/0.0.0.0/udp/%d/quic-v1", cfg.ListenPort),
			// Shares the quic-v1 UDP port above (WebTransport is a session
			// layered on the same QUIC socket, not a separate listener).
			// The webtransport transport module itself is already part of
			// go-libp2p's default transport set (this Config never calls
			// libp2p.Transport, so DefaultTransports applies); only the
			// listen address was missing, which is why every other node
			// this project has run so far -- none of them reachable from a
			// browser -- never noticed. n.host.Addrs() will report the
			// resulting address with its /certhash component appended
			// automatically, so advertisedAddrs()/ready.json need no
			// change to start including it. See web-app/ for the browser
			// client (js-libp2p, since go-libp2p itself has no usable
			// browser-sandbox transport) that dials this.
			fmt.Sprintf("/ip4/0.0.0.0/udp/%d/quic-v1/webtransport", cfg.ListenPort),
		),
		libp2p.EnableRelay(),
		libp2p.EnableHolePunching(),
	}

	if cfg.RelayService {
		limits := relayLimits(cfg)
		rc := v2relay.DefaultResources()
		rc.Limit = &v2relay.RelayLimit{
			Duration: limits.LimitDuration,
			Data:     limits.LimitData,
		}
		rc.ReservationTTL = time.Hour
		rc.MaxReservations = 256
		rc.MaxCircuits = int(limits.MaxCircuitsPerPeer)
		rc.BufferSize = 4096
		rc.MaxReservationsPerIP = int(limits.MaxReservationsPerIP)
		// rc.MaxReservationsPerPeer is deliberately not set: go-libp2p
		// v0.48.0 deprecated it ("we only need 1 reservation per peer")
		// and its relay implementation no longer reads it at all, so
		// cfg.RelayMaxReservationsPerPeer/-relay-max-reservations-per-peer
		// (see that flag's own doc comment in cmd/kvnode) has been a
		// no-op since that upgrade regardless of what value an operator
		// passes.

		relayOpts := []v2relay.Option{
			v2relay.WithResources(rc),
			v2relay.WithACL(relayACL{store: st, selfPeerID: selfPeerID, quota: relayQuota}),
		}
		opts = append(opts,
			libp2p.EnableRelayService(relayOpts...),
			libp2p.ForceReachabilityPublic(),
		)
	}

	// !cfg.RelayService guards this whole branch because ForceReachabilityPublic
	// (just above, when RelayService is on) and ForceReachabilityPrivate (this
	// branch) both just overwrite the same libp2p.Config.AutoNATConfig.
	// ForceReachability field with no conflict detection -- go-libp2p silently
	// keeps whichever was appended last. relayCandidates(cfg, st) merges every
	// confirmed KindBootstrapNode record in this node's own store unconditionally,
	// regardless of RelayService, so a relay-service node (by definition one of
	// the stable, directly-dialable hosts per Config.RelayPeers' doc comment, not
	// one that needs a relay itself) could still end up with a non-empty
	// candidate list and have this branch silently override its own forced
	// Public reachability with Private -- exactly backwards for a node whose
	// whole job is being reliably dialable. A node can't simultaneously force
	// both, so RelayService wins: it already stands in for "this node doesn't
	// need a relay."
	if candidates := relayCandidates(cfg, st); !cfg.RelayService && len(candidates) > 0 {
		// AutoRelay only actively reserves a relay slot once it believes
		// this host is privately reachable, a judgment it otherwise leaves
		// to AutoNAT -- which can be slow, or simply wrong on a network
		// (like this project's own test environment) that looks publicly
		// dialable but isn't actually reachable by the specific peer that
		// matters (the raft leader). RelayPeers is only ever set (or a
		// KindBootstrapNode record only ever confirmed) by a caller who
		// already knows this node needs a relay to be reached at all (see
		// Config.RelayPeers' doc comment), so force that judgment instead
		// of leaving the reservation -- and therefore the /p2p-circuit
		// address join()'s awaitRelayAddr waits for -- contingent on
		// AutoNAT.
		//
		// The first two autorelay options are what make a *fresh* device
		// able to get a reservation at all without being restarted. Every
		// relay in this project gates reservations unconditionally
		// (relayACL), and a device that has never asked for standing yet
		// has none -- so AutoRelay's very first reservation attempt, fired
		// within milliseconds of this host coming up, is refused with
		// PERMISSION_DENIED, long before its owner can run the
		// EventPublicAccess self-service escalation that would grant it
		// (see dialAndSubmitPublicAccess). AutoRelay's own defaults then
		// back that peer off for a full *hour* (autorelay's defaultConfig:
		// backoff 1h, minInterval 30s), so the standing the device just
		// obtained has no effect until the process is restarted -- which is
		// why pkg/e2erun/android_pair.go had to restart the app between
		// asking for access and reading its own address, and why a device
		// that skipped that restart only ever advertised loopback. Shrinking
		// both intervals makes the retry land ~10s after the grant instead,
		// so requesting access and then pairing works in one session.
		//
		// WithBootDelay closes a second, independent gap in the same
		// family, caught writing TestRelayFailoverToSecondCandidateWhenFirstIsDown:
		// WithStaticRelays sets AutoRelay's minCandidates to len(candidates),
		// but a candidate only counts once it actually answers a live
		// connect-and-probe (relay_finder.go's handleNewNode/tryNode) --  a
		// down or unreachable entry (the exact case this multi-candidate
		// list exists to tolerate, see Config.RelayPeers' doc comment on
		// failover) never clears that bar, so the real candidate count
		// permanently falls short of minCandidates whenever any one entry
		// is down. AutoRelay's default response to "fewer real candidates
		// than minCandidates" is to wait out bootDelay -- 3 minutes,
		// unmodified by anything above -- before trying the reservations it
		// already has anyway. A device with a dead first relay and a
		// perfectly good second one therefore got zero relay connectivity
		// for a full 3 minutes on startup. Matching relayReserveBackoff
		// here means it tries what it's already found instead of waiting
		// out that window.
		opts = append(opts,
			libp2p.ForceReachabilityPrivate(),
			libp2p.EnableAutoRelayWithStaticRelays(candidates,
				autorelay.WithBackoff(relayReserveBackoff),
				autorelay.WithMinInterval(relayReserveBackoff),
				autorelay.WithBootDelay(relayReserveBackoff),
			),
		)
	}

	return libp2p.New(opts...)
}

// relayCandidates builds newHost's full ordered relay candidate list --
// Config.RelayPeers (the seed list a caller already knows about, e.g.
// cmd/kvnode's -relay-peer flag or mobile/kvmobile's build-time
// relayMultiaddr, tried first and in the order given) followed by every
// currently-confirmed shmevent.KindBootstrapNode record already replicated
// into this node's own local store (see pkg/kvctl's AddRelayNode/
// ConfirmRelayNode/ListRelayNodes), sorted by ascending priority (lower
// tried first -- see EncodeBootstrapNodeMetadata). This is a plain local
// store read, no raft/leader round trip needed, the same same-machine
// trust boundary rangescan/Get already rely on -- so it works even before
// this node has (re)joined a cluster, as long as st already holds
// KindBootstrapNode records from a prior session.
//
// libp2p's EnableAutoRelayWithStaticRelays already accepts (and rotates
// reservations across) more than one peer.AddrInfo, so handing it every
// known-good relay here -- rather than just one -- is what gives a node
// failover if its first-choice relay goes down, without any extra
// dial/retry logic of this package's own. A malformed or unparseable
// entry (RelayPeers or KindBootstrapNode alike) is skipped rather than
// failing node startup outright -- one bad relay address shouldn't take
// down every other candidate still worth trying.
func relayCandidates(cfg Config, st *store.Store) []peer.AddrInfo {
	seen := make(map[peer.ID]bool)
	var candidates []peer.AddrInfo
	addCandidate := func(addr string) {
		maddr, err := multiaddr.NewMultiaddr(addr)
		if err != nil {
			return
		}
		info, err := peer.AddrInfoFromP2pAddr(maddr)
		if err != nil || seen[info.ID] {
			return
		}
		seen[info.ID] = true
		candidates = append(candidates, *info)
	}

	for _, addr := range cfg.RelayPeers {
		if addr != "" {
			addCandidate(addr)
		}
	}

	if st != nil {
		lo, hi := shmevent.BootstrapNodeKeyBounds()
		matches, err := st.ScanRange(lo, hi, 0)
		if err == nil {
			type bootstrapEntry struct {
				addr     string
				priority uint8
			}
			entries := make([]bootstrapEntry, 0, len(matches))
			for _, kv := range matches {
				addr, priority, err := shmevent.DecodeBootstrapNodeMetadata(string(kv.Value))
				if err != nil {
					continue
				}
				entries = append(entries, bootstrapEntry{addr: addr, priority: priority})
			}
			sort.SliceStable(entries, func(i, j int) bool { return entries[i].priority < entries[j].priority })
			for _, e := range entries {
				addCandidate(e.addr)
			}
		}
	}

	return candidates
}
