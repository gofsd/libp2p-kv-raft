//go:build !android

package ipc

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/gofsd/libp2p-kv-raft/pkg/registry"
)

// tokenFileName is the file, alongside identity.key/kv.db in a node's own
// DataDir, holding that node's local-IPC secret -- see loadOrGenerateToken.
const tokenFileName = "ipc.token"

// tokenSize is the token's raw byte length before hex encoding (32 bytes =
// 256 bits, comfortably more than this needs: the threat this defends
// against is a co-resident process guessing/enumerating it, not an
// offline attacker with compute to spare).
const tokenSize = 32

// loadOrGenerateToken returns dataDir's persisted local-IPC secret,
// generating and persisting a fresh random one (hex-encoded, 0600 --
// mirrors identity.key's own format and permissions, see
// pkg/raft.LoadOrGenerateKey) the first time it's asked for. Folding this
// secret into reqChannel/respChannel's own segment names (see ipc.go) is
// what closes the gap a bare peerID-derived name left open: shmring's
// POSIX backend creates its shared-memory segments with owner+group
// read/write (see github.com/hidez8891/shm), so any process running as
// the same user *or group* could previously attach to a node's IPC
// channel just by knowing (or guessing) its public peer id. A 0600 token
// file is unreadable by anyone but the user that owns it, regardless of
// group membership or the containing DataDir's own (0755, listable)
// permissions -- the same "tight file, loose directory" split
// identity.key already relies on.
//
// Whichever side -- the daemon starting up (Serve, given its own
// cfg.DataDir directly) or the first client to dial in (Call/CallRaw, via
// tokenForPeer's registry lookup) -- reaches this first for a given
// dataDir creates the file; every other caller, on either side, just
// reads the same bytes back off disk afterward. pkg/kvctl's bootUp
// already has the daemon write ready.json (Config.ReadyFileName) before
// any client's waitForReady returns and issues its first call, so in
// practice the daemon wins the race -- but Serve and a racing
// Call/CallRaw can still both reach a cold dataDir at nearly the same
// time (as in this package's own unit tests, which start Serve and issue
// a call without waiting on any readiness signal), so creation itself
// still has to be race-safe rather than relying on that ordering: two
// concurrent first-callers generating and persisting *different* random
// tokens independently would derive different reqChannel/respChannel
// names (see ipc.go) from them and never rendezvous. Publishing via a
// temp file + os.Link (Link fails with an already-exists error rather
// than silently overwriting, unlike a second plain WriteFile) makes
// exactly one caller's generated token win and guarantees every other
// caller reads back that same, fully-written value instead of racing.
func loadOrGenerateToken(dataDir string) (string, error) {
	path := filepath.Join(dataDir, tokenFileName)
	if b, err := os.ReadFile(path); err == nil {
		tok := strings.TrimSpace(string(b))
		if tok == "" {
			return "", fmt.Errorf("ipc: token file %s is empty", path)
		}
		return tok, nil
	} else if !os.IsNotExist(err) {
		return "", fmt.Errorf("ipc: read token file %s: %w", path, err)
	}

	raw := make([]byte, tokenSize)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("ipc: generate token: %w", err)
	}
	tok := hex.EncodeToString(raw)

	tmp, err := os.CreateTemp(dataDir, tokenFileName+".tmp-*")
	if err != nil {
		return "", fmt.Errorf("ipc: create temp token file: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if _, err := tmp.Write([]byte(tok)); err != nil {
		tmp.Close()
		return "", fmt.Errorf("ipc: write temp token file %s: %w", tmpPath, err)
	}
	if err := tmp.Close(); err != nil {
		return "", fmt.Errorf("ipc: close temp token file %s: %w", tmpPath, err)
	}

	if err := os.Link(tmpPath, path); err != nil {
		if !os.IsExist(err) {
			return "", fmt.Errorf("ipc: publish token file %s: %w", path, err)
		}
		// Lost the creation race to a concurrent caller -- read back its
		// already-fully-written token instead of using our own.
		b, rerr := os.ReadFile(path)
		if rerr != nil {
			return "", fmt.Errorf("ipc: read token file %s after losing creation race: %w", path, rerr)
		}
		tok = strings.TrimSpace(string(b))
		if tok == "" {
			return "", fmt.Errorf("ipc: token file %s is empty", path)
		}
	}
	return tok, nil
}

// tokenForPeer is Call/CallRaw's side of loadOrGenerateToken: it resolves
// peerID to its own DataDir via the local registry (the same lookup
// pkg/kvctl/mobile/kvmobile already do to find a node by peer id) and
// loads/generates that node's token from there. A peerID with no
// registry entry at all is refused outright, deliberately fail-closed
// rather than falling back to the old, tokenless segment name: every real
// caller in this codebase (pkg/shmclient, pkg/kvctl, cmd/kvctl-cli,
// mobile/kvmobile) only ever calls Call/CallRaw with a peerID it already
// read out of the registry in the first place, so this can only fail for
// a genuinely unregistered peer id -- either a caller's bug, or a
// registry that changed underfoot -- neither of which should silently
// downgrade to an unauthenticated channel.
func tokenForPeer(peerID string) (string, error) {
	reg, err := registry.Open()
	if err != nil {
		return "", fmt.Errorf("ipc: open registry: %w", err)
	}
	info, ok, err := reg.Get(peerID)
	if err != nil {
		return "", fmt.Errorf("ipc: look up peer %s: %w", peerID, err)
	}
	if !ok || info.DataDir == "" {
		return "", fmt.Errorf("ipc: no registered node for peer id %s", peerID)
	}
	return loadOrGenerateToken(info.DataDir)
}
