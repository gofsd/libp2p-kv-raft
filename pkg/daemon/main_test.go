package daemon

import (
	"os"
	"testing"

	"github.com/gofsd/libp2p-kv-raft/pkg/registry"
)

// TestMain points registry.EnvHome at a fresh temp directory for this test
// binary's entire run, before any individual test -- parallel or not --
// can touch the real environment variable. pkg/ipc's tokenForPeer (see
// pkg/ipc/token.go) now resolves a peer id to its own DataDir through
// pkg/registry on every Call/CallRaw a test drives through a real
// shmclient.Session (channel_dataplane_test.go/
// channel_dataplane_bench_test.go's startChannelDataplaneTestNode
// registers each node it starts) -- without this, those tests would
// silently read/write the real operator's own
// ~/.libp2p-kv-raft/registry.json during `go test` instead of an isolated
// one, and t.Setenv per-test would race across parallel siblings that
// each want a different registry root (env vars are process-global, not
// per-goroutine).
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "kvstore-daemon-test-registry-*")
	if err != nil {
		panic(err)
	}
	os.Setenv(registry.EnvHome, dir)
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}
