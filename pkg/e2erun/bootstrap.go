package e2erun

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/daemon"
	"github.com/gofsd/libp2p-kv-raft/pkg/e2edata"
	"github.com/gofsd/libp2p-kv-raft/pkg/registry"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// BootstrapHost is the one SSH server this project currently deploys the
// shared e2e bootstrap/leader node to.
const BootstrapHost = "root@63.250.47.155"

// BootstrapRemoteDir is deliberately its own tree, not /root/kvstore --
// that directory already holds a separately, manually managed live kvnode
// leader from earlier work on this same server; sharing a data dir or port
// with it would either corrupt its raft state or fail to bind. Isolating
// the e2e pipeline's own bootstrap node here means `mage e2e:bootstrap` can
// never disturb it.
const BootstrapRemoteDir = "/root/kvstore-e2e"

// BootstrapRemotePort is fixed (not ephemeral) so the deployment is
// idempotent across runs and the leader's multiaddr is stable enough for a
// human to hardcode in a phone/browser client between e2e runs, matching
// the existing leader's own fixed -listen-port 4001 convention.
const BootstrapRemotePort = 4101

// BootstrapToken is the sentinel EventAdd rows use in place of a literal
// leader multiaddr -- see ResolveBootstrapPlaceholder. The bootstrap
// server's address is fixed but its multiaddr embeds a QUIC/WebTransport
// certhash that isn't predictable ahead of a real deploy, so rows are
// authored against this token instead of a frozen (and possibly stale)
// address.
const BootstrapToken = "BOOTSTRAP"

// EnsureBootstrap makes sure f has a bootstrap node identity (provisioning
// one and mutating f if it doesn't yet), cross-compiles and deploys kvnode
// + kvctl-cli to BootstrapHost:BootstrapRemoteDir if not already present,
// and starts (or confirms already running) the daemon there. It returns
// the bootstrap's live, dialable multiaddr (read back from its own
// ready.json over ssh) and its peer id.
//
// multiaddr is address [0] of advertisedAddrs()'s own best-first ordering
// (see pkg/daemon.advertisedAddrs's doc comment) -- a plain TCP/QUIC
// address every Go-based platform (desktop, remote, Android's go-libp2p)
// can dial directly. webTransportAddr is separately the
// /quic-v1/webtransport/... address specifically, since a browser sandbox
// can *only* dial WebTransport, never raw TCP/QUIC -- using multiaddr for
// a web row's "BOOTSTRAP" placeholder would silently try to connect a
// browser tab to an address it can never reach.
//
// Deployment is idempotent: re-running this after the daemon is already up
// just confirms it's alive and returns its current addresses -- safe to
// call at the start of every e2e:current/e2e:all invocation. path is saved
// to immediately after provisioning the PlatformRemote identity (if none
// existed yet), before any remote action is taken -- a newly generated
// identity that dies with this process before ever reaching disk would
// otherwise get silently regenerated (and so *changed*) on the next
// attempt, even though the first attempt may have already deployed the
// original identity's key file remotely; caught by hitting exactly that
// after a first deploy attempt failed partway through for an unrelated
// reason.
func EnsureBootstrap(repoRoot, path string, f *e2edata.File) (multiaddr, webTransportAddr, peerID string, err error) {
	_, node, ok := f.RemoteNode()
	if !ok {
		var err error
		_, node, err = f.AddNode(e2edata.PlatformRemote)
		if err != nil {
			return "", "", "", fmt.Errorf("e2erun: provision bootstrap identity: %w", err)
		}
		if err := f.Save(path); err != nil {
			return "", "", "", fmt.Errorf("e2erun: save provisioned bootstrap identity: %w", err)
		}
	}

	binDir, err := buildLinuxAmd64Binaries(repoRoot)
	if err != nil {
		return "", "", "", err
	}

	redeployed, err := deployIfMissing(binDir)
	if err != nil {
		return "", "", "", err
	}
	alreadyBootstrapped, err := deployKeyIfMissing(node)
	if err != nil {
		return "", "", "", err
	}
	if redeployed {
		// A new binary alone doesn't make an already-running daemon pick
		// it up -- force a restart so it does. Safe: raft's own on-disk
		// state means the restarted process loses nothing (see
		// stopIfRunning's doc comment).
		if err := stopIfRunning(); err != nil {
			return "", "", "", err
		}
	}
	justStarted, err := startIfNotRunning()
	if err != nil {
		return "", "", "", err
	}

	ready, err := waitRemoteReady(BootstrapRemoteDir+"/data", readyTimeout)
	if err != nil {
		return "", "", "", fmt.Errorf("e2erun: bootstrap not ready: %w", err)
	}
	if ready.PeerID != node.PeerID {
		return "", "", "", fmt.Errorf("e2erun: bootstrap peer id mismatch: deployed identity is %s but running daemon reports %s (stale /root/kvstore-e2e data from a different identity?)", node.PeerID, ready.PeerID)
	}
	if len(ready.ListenAddrs) == 0 {
		return "", "", "", fmt.Errorf("e2erun: bootstrap reported no listen addresses")
	}

	if err := deployRegistryEntry(node, ready.ListenAddrs); err != nil {
		return "", "", "", fmt.Errorf("e2erun: deploy bootstrap registry entry: %w", err)
	}

	if justStarted && !alreadyBootstrapped {
		// A genuinely first-ever-started kvnode has no raft configuration
		// at all -- EventAdd with SourceID 0 and an empty Value is what
		// pkg/daemon.handleAdd reads as "bootstrap as the cluster's sole
		// leader" (see pkg/shmevent.EventAdd's doc comment). Without this,
		// the daemon just sits there correctly rejecting every join
		// attempt with "not leader" -- caught by a real join row failing
		// with exactly that error against a freshly deployed bootstrap.
		//
		// Gated on !alreadyBootstrapped too, not just justStarted: a
		// restart forced by redeploying a new binary (see the `redeployed`
		// branch above) also makes justStarted true, but that daemon
		// already has persisted raft state and resumes it on its own (see
		// pkg/daemon.Run's hasState check) -- sending it another
		// self-leader EventAdd would hit BootstrapCluster's own refusal to
		// re-bootstrap a non-empty log and fail this whole call.
		status, errMsg := sendEventRemote(node.PeerID, e2edata.NewEvent(shmevent.EventAdd, 0, 0, nil, 0))
		if status != e2edata.StatusPass {
			return "", "", "", fmt.Errorf("e2erun: bootstrap self-leader EventAdd: %s", errMsg)
		}
	}

	wt := findWebTransportAddr(ready.ListenAddrs)
	return ready.ListenAddrs[0], wt, node.PeerID, nil
}

// GrantRelayAccess grants targetPeerID standing in
// shmevent.ReservedGroupRelay on the bootstrap cluster, submitted as
// bootstrapPeerID itself over the same sendEventRemote path
// EnsureBootstrap's own self-leader EventAdd already uses -- the bootstrap
// is always the sole voter of its own cluster, so it's always authorized to
// author this PeerGroup record for anyone.
//
// This exists because an Android e2e row can only ever reach the bootstrap
// through a real circuit-relay v2 reservation once relayMultiaddr is baked
// in (see buildAndroidAAR's own ldflags) -- an emulator with no port
// forwarding is unreachable without one, the same reason a browser sandbox
// needs one too (see p2p.rs's do_reserve) -- and relayACL unconditionally
// gates that reservation on this exact group (see pkg/daemon.relayACL's
// doc comment). A brand-new bootstrap identity (freshly provisioned after
// `mage e2e:destroyall`/`e2e:release` wiped the previous one's whole
// store, group memberships included) starts with none of that standing
// granted to anyone, so an Android row's very first "add" would otherwise
// hang out its own AutoRelay reservation retry loop and fail -- caught by
// exactly that happening against a freshly redeployed bootstrap.
//
// Web/desktop no longer need this: both now request their own standing
// self-service instead (runWebNode's synthetic row 0, runRow's
// PlatformDesktop branch), the way a real, standing-less device is meant
// to -- see run.go's own call site for why Android's regular rows can't
// do the same without restructuring E2ETest.kt's setup. Called
// unconditionally for every known Android node identity on every Run, not
// just after a fresh bootstrap: PutPeerGroup is a plain idempotent
// set-membership write, so re-granting standing that's already there is
// harmless, and this also naturally covers a node identity that itself
// got reprovisioned (new peer id) against an *existing* bootstrap.
func GrantRelayAccess(bootstrapPeerID, targetPeerID string) error {
	payload, err := shmevent.EncodePeerGroupPayload([]byte(targetPeerID), []byte(shmevent.ReservedGroupRelay))
	if err != nil {
		return fmt.Errorf("e2erun: encode peer-group payload: %w", err)
	}
	status, errMsg := sendEventRemote(bootstrapPeerID, e2edata.NewEvent(shmevent.EventPeerGroupPut, 0, 0, payload, 0))
	if status != e2edata.StatusPass {
		return fmt.Errorf("e2erun: grant %s relay access: %s", targetPeerID, errMsg)
	}
	return nil
}

// findWebTransportAddr returns the first /webtransport/ address in addrs,
// or "" if none is present (e.g. an older deployment's ready.json, or a
// node that somehow never advertised one) -- callers driving a web row
// should treat that as "no browser-reachable address available" rather
// than silently falling back to a plain TCP/QUIC address a browser can
// never dial.
func findWebTransportAddr(addrs []string) string {
	for _, a := range addrs {
		if strings.Contains(a, "/webtransport/") {
			return a
		}
	}
	return ""
}

// ResolveBootstrapPlaceholder replaces BootstrapToken with the live
// bootstrap multiaddr inside an EventAdd row's value, leaving any other
// value untouched. Rows are authored against the token (see BootstrapToken)
// since the real address isn't known until deploy time.
func ResolveBootstrapPlaceholder(value, bootstrapMultiaddr string) string {
	if value == BootstrapToken {
		return bootstrapMultiaddr
	}
	return value
}

func buildLinuxAmd64Binaries(repoRoot string) (string, error) {
	binDir := filepath.Join(repoRoot, ".e2e-build", "linux_amd64")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		return "", err
	}
	for _, pkg := range []string{"./cmd/kvnode", "./cmd/kvctl-cli"} {
		name := filepath.Base(pkg)
		out := filepath.Join(binDir, name)
		// -s -w strips the debug/symbol info this binary never needs on a
		// deploy target, cutting the upload ~30% smaller -- meaningfully
		// lowers the odds of the scp transfer below getting cut short
		// partway through (see deployIfMissing's doc comment).
		cmd := exec.Command("go", "build", "-ldflags=-s -w", "-o", out, pkg)
		cmd.Dir = repoRoot
		cmd.Env = append(os.Environ(), "GOOS=linux", "GOARCH=amd64")
		cmd.Stdout = os.Stderr
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			return "", fmt.Errorf("e2erun: cross-compile %s for linux/amd64: %w", pkg, err)
		}
	}
	return binDir, nil
}

// deployIfMissing scp's kvnode/kvctl-cli to the bootstrap host only if
// they're not already there with a matching size -- avoids re-uploading on
// every single e2e run while still self-healing a partial/missing deploy.
//
// It uploads to a ".upload" sibling path and `mv`s it into place rather
// than scp-ing straight over remote, because kvnode is very likely still
// running (using the very binary being replaced) whenever a deploy picks
// up code changes -- overwriting a running executable in place fails with
// ETXTBSY ("scp: dest open ... Failure", opaque enough that it took a real
// deploy against a live bootstrap to surface), while `mv`'s rename leaves
// the running process's already-open inode untouched and just repoints
// the path for whatever starts next.
// deployIfMissing uploads kvnode/kvctl-cli if the remote binary differs in
// size from the freshly-built local one. deployed reports whether anything
// was actually re-uploaded -- EnsureBootstrap uses this to force a restart
// of an already-running daemon, since deploying a new binary alone doesn't
// make a live process pick it up.
func deployIfMissing(binDir string) (deployed bool, err error) {
	if err := sshRun(BootstrapHost, fmt.Sprintf("mkdir -p %s/bin %s/data", BootstrapRemoteDir, BootstrapRemoteDir)); err != nil {
		return false, err
	}
	for _, name := range []string{"kvnode", "kvctl-cli"} {
		local := filepath.Join(binDir, name)
		remote := BootstrapRemoteDir + "/bin/" + name
		localInfo, err := os.Stat(local)
		if err != nil {
			return deployed, err
		}
		out, err := sshOutput(BootstrapHost, fmt.Sprintf("stat -c%%s %s 2>/dev/null || true", remote))
		if err == nil && fmt.Sprintf("%d\n", localInfo.Size()) == out {
			continue // already deployed, same size
		}
		if err := uploadWithRetry(local, BootstrapHost, remote, localInfo.Size()); err != nil {
			return deployed, err
		}
		deployed = true
	}
	return deployed, nil
}

// uploadRetries bounds how many times uploadWithRetry re-sends a binary
// whose transfer landed short.
const uploadRetries = 3

// uploadWithRetry scp's local to a ".upload" sibling of remote and verifies
// the resulting remote file size matches localSize before `mv`-ing it into
// place, retrying the whole upload (not just the size check) up to
// uploadRetries times on a mismatch. This exists because scp can return a
// zero exit status for a transfer the connection dropped partway through
// -- observed directly against this project's own deploy target: a 37 MB
// binary repeatedly landed as an 8-11 MB ".upload" file with scp itself
// reporting success, silently deploying a truncated (unexecutable)
// binary -- so "scp didn't error" is not sufficient evidence the transfer
// actually completed.
func uploadWithRetry(local, host, remote string, localSize int64) error {
	uploadPath := remote + ".upload"
	var lastErr error
	for attempt := 1; attempt <= uploadRetries; attempt++ {
		if err := scp(local, host, uploadPath); err != nil {
			lastErr = err
			continue
		}
		out, err := sshOutput(host, fmt.Sprintf("stat -c%%s %s 2>/dev/null || true", uploadPath))
		if err != nil {
			lastErr = err
			continue
		}
		if strings.TrimSpace(out) != fmt.Sprintf("%d", localSize) {
			lastErr = fmt.Errorf("upload of %s landed as %s bytes, want %d (attempt %d/%d)", filepath.Base(local), strings.TrimSpace(out), localSize, attempt, uploadRetries)
			continue
		}
		return sshRun(host, fmt.Sprintf("chmod +x %s && mv -f %s %s", uploadPath, uploadPath, remote))
	}
	return fmt.Errorf("e2erun: upload %s: %w", local, lastErr)
}

// deployKeyIfMissing writes node's identity key remotely if it isn't there
// yet. alreadyDeployed reports whether it already was -- true means this
// node has been bootstrapped before (on some earlier EnsureBootstrap call,
// possibly a much earlier process lifetime than whatever's running now), so
// it already has its own persisted raft state and must never be sent
// another self-leader EventAdd bootstrap, regardless of whether the current
// process happens to have been freshly (re)started -- unlike justStarted,
// this stays true across a restart triggered by a binary redeploy (see
// EnsureBootstrap's use of this return value).
func deployKeyIfMissing(node e2edata.Node) (alreadyDeployed bool, err error) {
	remoteKey := BootstrapRemoteDir + "/data/identity.key"
	out, _ := sshOutput(BootstrapHost, "cat "+remoteKey+" 2>/dev/null || true")
	if out != "" {
		return true, nil // already deployed
	}
	tmp, err := os.CreateTemp("", "kvstore-e2e-bootstrap-key-*")
	if err != nil {
		return false, err
	}
	defer os.Remove(tmp.Name())
	tmp.Close()
	if err := e2edata.WriteDesktopKeyFile(node, tmp.Name()); err != nil {
		return false, err
	}
	return false, scp(tmp.Name(), BootstrapHost, remoteKey)
}

// deployRegistryEntry writes (or refreshes) node's pkg/registry entry at
// BootstrapRemoteDir/registry.json -- built locally via that package's own
// Registry.Put (so this can never drift out of sync with whatever shape
// registry.json actually needs, the same reasoning deployKeyIfMissing's use
// of e2edata.WriteDesktopKeyFile already follows for identity.key) into a
// throwaway local temp directory, then scp'd into place the same way
// identity.key already is.
//
// This exists because pkg/ipc.tokenForPeer (added by the local IPC token
// work) refuses to hand out an auth token for any peer id with no
// pkg/registry entry -- and EnsureBootstrap's own deploy path never had
// one, since it starts kvnode directly over ssh (mkdir/scp/setsid) rather
// than through pkg/kvctl.AddNodeWithBinary, the only other code path that
// already calls Registry.Put. Without this, every sendEventRemote call
// (starting with EnsureBootstrap's own self-leader EventAdd immediately
// below) fails outright against a freshly deployed bootstrap with "ipc: no
// registered node for peer id ..." -- caught by an actual `mage e2e:all`
// run against a brand new BootstrapRemoteDir, not by inspection.
//
// Always overwrites (Registry.Put upserts) rather than skipping when
// already present, unlike deployKeyIfMissing: unlike key material, a
// refreshed ListenAddrs is exactly what should happen on every call, and
// re-writing DataDir/KeyPath/PID to the same values they already hold is
// harmless. sendEventRemote reads this same file back by setting
// KVSTORE_HOME=BootstrapRemoteDir on the remote kvctl-cli invocation --
// deliberately not the ssh user's shared default (~/.libp2p-kv-raft),
// consistent with BootstrapRemoteDir's own doc comment on keeping this
// pipeline's state fully isolated from anything else on the host.
func deployRegistryEntry(node e2edata.Node, listenAddrs []string) error {
	tmpDir, err := os.MkdirTemp("", "kvstore-e2e-bootstrap-registry-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpDir)

	reg := &registry.Registry{Dir: tmpDir}
	if err := reg.Put(registry.NodeInfo{
		PeerID:      node.PeerID,
		Role:        registry.RoleLeader,
		DataDir:     BootstrapRemoteDir + "/data",
		KeyPath:     BootstrapRemoteDir + "/data/identity.key",
		ListenAddrs: listenAddrs,
		// Without this the entry says pid 0, and every kvctl-cli command
		// that first checks whether the node is up (listnodes, use, and so
		// on -- see pkg/kvctl's "does not appear to be running" gate)
		// refuses to talk to a daemon that is running perfectly well.
		// That leaves the one node in this project nobody can reach
		// directly also the one nobody can inspect with the CLI sitting
		// next to it -- found while trying to read this very cluster's
		// membership during an e2e failure. The pid is already recorded
		// remotely by startIfNotRunning; this carries it into the registry
		// the CLI actually reads.
		PID: remoteBootstrapPID(),
	}); err != nil {
		return fmt.Errorf("e2erun: build local registry.json: %w", err)
	}

	return scp(filepath.Join(tmpDir, "registry.json"), BootstrapHost, BootstrapRemoteDir+"/registry.json")
}

// remoteBootstrapPID reads the pid startIfNotRunning recorded on the
// bootstrap host, for deployRegistryEntry to store. Zero (what the entry
// carried before this existed) whenever it cannot be read -- this is
// bookkeeping that makes the remote CLI usable, never something worth
// failing a deploy over.
func remoteBootstrapPID() int {
	out, err := sshOutput(BootstrapHost, fmt.Sprintf("cat %s 2>/dev/null || true", bootstrapPidFile))
	if err != nil {
		return 0
	}
	pid, err := strconv.Atoi(strings.TrimSpace(out))
	if err != nil {
		return 0
	}
	return pid
}

// bootstrapPidFile is where startIfNotRunning records the daemon's pid, so
// a later run can check liveness precisely (`kill -0 <pid>`) instead of a
// `pgrep -f`-style command-line pattern match -- which, run over ssh as
// `ssh host "pgrep -f '<pattern containing the remote dir path>'"`, would
// false-positive-match the very shell process ssh itself spawns to run
// that command (its argv literally contains the search pattern), since
// pgrep only excludes itself, not its parent -- caught by testing this
// against the real server before trusting it.
const bootstrapPidFile = BootstrapRemoteDir + "/data/e2e.pid"

// bootstrapDaemonAlive reports whether bootstrapPidFile names a still-live
// remote pid (over ssh) -- distinct from this package's local-process
// isAlive(pid int) helper.
func bootstrapDaemonAlive() bool {
	out, _ := sshOutput(BootstrapHost, fmt.Sprintf("kill -0 \"$(cat %s 2>/dev/null)\" 2>/dev/null && echo alive || true", bootstrapPidFile))
	return strings.TrimSpace(out) == "alive"
}

// startIfNotRunning starts the bootstrap kvnode over ssh unless
// bootstrapPidFile names a still-live pid, in which case it's a no-op.
// justStarted reports which happened -- EnsureBootstrap only needs to send
// the self-leader EventAdd bootstrap request the first time a daemon is
// actually started, not on every idempotent "already running" check.
func startIfNotRunning() (justStarted bool, err error) {
	if bootstrapDaemonAlive() {
		return false, nil // already running
	}
	// setsid fully detaches the daemon into its own session, immune to
	// the SIGHUP the ssh server sends the command's process group when
	// the channel closes -- verified directly against this server that
	// the more common `nohup cmd & disown` idiom does *not* survive here:
	// disown isn't a dash builtin (this host's /bin/sh), so it silently
	// no-ops/errors and the backgrounded job dies with the ssh session
	// anyway. `< /dev/null` matters too: leaving stdin attached to the
	// ssh channel is enough on its own to keep the session from being
	// considered closed.
	//
	// -raft-snapshot-threshold/-raft-snapshot-interval/-raft-trailing-logs
	// are set well below hashicorp/raft's own defaults (8192 entries /
	// 120s / 10240 entries) specifically because this leader is never torn
	// down between e2e runs (see this file's doc comment) -- its log only
	// ever grows, and a freshly-joined non-voter must replay everything
	// from index 1 up to the last snapshot. Confirmed directly: without
	// this, a browser tab joining after ~1470 accumulated log entries
	// needed 20-30+ seconds just to catch up before it could read back its
	// own just-written value. All three matter together: a lowered
	// threshold alone still snapshots without compacting anything, since a
	// log under the (still-default) TrailingLogs entries has nothing
	// eligible for removal regardless of how often it snapshots.
	//
	// -raft-heartbeat-timeout/-raft-election-timeout/-raft-commit-timeout/
	// -raft-leader-lease-timeout match mobile/kvmobile's own widened
	// raftHeartbeatTimeout/raftElectionTimeout/raftCommitTimeout/
	// raftLeaderLeaseTimeout constants exactly (4s/4s/200ms/2500ms) --
	// without this, the leader ran on hashicorp/raft's LAN-tuned 1s/1s/
	// 500ms defaults while every real voter/learner joining it (desktop's
	// own widened -raft-*-timeout flags, mobile's identically-named
	// constants, this file's own already-widened snapshot settings above)
	// assumed a real cross-continental link -- confirmed directly: this
	// asymmetry caused repeated spurious "leadership lost while committing
	// log"/"not leader and no leader known" failures across desktop/
	// Android/web e2e rows alike on a real 3+-node cluster spanning
	// Ukraine and this US-hosted VPS, even though every individual link
	// was otherwise healthy. The leader must tolerate at least as much
	// latency as its most latency-exposed voter/learner, not the LAN
	// default every other node here had already been widened past.
	remoteCmd := fmt.Sprintf(
		"setsid %[1]s/bin/kvnode -data-dir %[1]s/data -key-path %[1]s/data/identity.key -listen-port %[2]d -relay-service -raft-snapshot-threshold 256 -raft-snapshot-interval 30s -raft-trailing-logs 256 -raft-heartbeat-timeout 4s -raft-election-timeout 4s -raft-commit-timeout 200ms -raft-leader-lease-timeout 2500ms >%[1]s/daemon.log 2>&1 </dev/null & echo $! >%[3]s",
		BootstrapRemoteDir, BootstrapRemotePort, bootstrapPidFile,
	)
	if err := sshRun(BootstrapHost, remoteCmd); err != nil {
		return false, err
	}
	return true, nil
}

// stopIfRunning kills the bootstrap daemon by the exact pid recorded in
// bootstrapPidFile (never by a `pkill`-style pattern match, same reasoning
// as bootstrapPidFile's own doc comment) and waits for it to actually exit,
// so a subsequent startIfNotRunning reliably launches the new binary rather
// than racing the old process's shutdown. A no-op if nothing is running.
// Raft's own on-disk log/snapshot state (BoltDB + the file snapshot store)
// makes this safe: the restarted process rebuilds its raft.Raft from
// exactly what was already persisted, losing nothing.
func stopIfRunning() error {
	if !bootstrapDaemonAlive() {
		return nil // not running
	}
	waitCmd := fmt.Sprintf(
		"pid=\"$(cat %s)\"; kill \"$pid\" 2>/dev/null; for i in $(seq 1 50); do kill -0 \"$pid\" 2>/dev/null || exit 0; sleep 0.2; done; kill -9 \"$pid\" 2>/dev/null || true",
		bootstrapPidFile,
	)
	return sshRun(BootstrapHost, waitCmd)
}

const readyTimeout = 30 * time.Second

// waitRemoteReady polls for remoteDataDir/ready.json over ssh, returning a
// real timeout error if it never appears -- earlier versions folded "cat
// succeeded but the file doesn't exist yet" (via `|| true`, needed so a
// merely-not-ready-yet poll doesn't count as an ssh failure) into a nil
// lastErr, so a daemon that never started at all was reported as "ready"
// with a zero-value ReadyInfo instead of a clear timeout -- caught by
// actually hitting it against the real server, not by inspection.
func waitRemoteReady(remoteDataDir string, timeout time.Duration) (daemon.ReadyInfo, error) {
	deadline := time.Now().Add(timeout)
	lastErr := fmt.Errorf("no attempt made")
	for time.Now().Before(deadline) {
		out, err := sshOutput(BootstrapHost, "cat "+remoteDataDir+"/"+daemon.ReadyFileName+" 2>/dev/null || true")
		if err != nil {
			lastErr = err
		} else if out == "" {
			lastErr = fmt.Errorf("ready file not written yet")
		} else {
			var info daemon.ReadyInfo
			if jsonErr := json.Unmarshal([]byte(out), &info); jsonErr == nil {
				return info, nil
			} else {
				lastErr = fmt.Errorf("parse ready file: %s: %w", out, jsonErr)
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	return daemon.ReadyInfo{}, fmt.Errorf("timed out after %s waiting for %s/%s: %w", timeout, remoteDataDir, daemon.ReadyFileName, lastErr)
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}
