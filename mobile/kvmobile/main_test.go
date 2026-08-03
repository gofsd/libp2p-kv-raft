package kvmobile

import (
	"os"
	"testing"

	"github.com/gofsd/libp2p-kv-raft/pkg/registry"
)

// TestMain points registry.EnvHome at a fresh temp directory for this
// test binary's entire run, before any individual test can touch the
// real environment variable -- see pkg/daemon's identical TestMain for
// the full reasoning. This package's own tests (built without the
// android tag, so ipc.Call/Serve resolve to ipc.go's desktop transport,
// standing in for ipc_android.go's real Android one -- see that file's
// doc comment) now depend on pkg/registry two ways: spawnTestLeader
// (sendevent_test.go) registers the leader nodes it spins up directly,
// and startAgainst (kvmobile.go) best-effort registers this package's
// own in-process follower the same way, both so pkg/ipc's tokenForPeer
// (pkg/ipc/token.go) can resolve a peer id to its local-IPC token.
// Without this, both would silently read/write the real operator's own
// ~/.libp2p-kv-raft/registry.json during `go test`.
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "kvstore-kvmobile-test-registry-*")
	if err != nil {
		panic(err)
	}
	os.Setenv(registry.EnvHome, dir)
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}
