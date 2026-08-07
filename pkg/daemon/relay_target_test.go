package daemon

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	p2praft "github.com/gofsd/libp2p-kv-raft/pkg/raft"
)

// EnvRealRelayAddr, when set to a real, already-running relay's own
// multiaddr (e.g. the deployed VPS bootstrap host's own advertised
// address -- see configs/bootstrap-nodes.json and CLAUDE.md's Node
// connectivity policy), redirects TestJoinThroughRelay/
// TestAddLearnerThroughRelay/TestPairThroughRelayCluster from each
// spinning up its own hermetic local relay onto that real one instead --
// the same real network path the Android e2e pairing scenario runs over
// (63.250.47.155:4101 in this project's own deploy), without needing an
// emulator or the SSH-deployed mage e2e:* pipeline to exercise it. Unset
// (the default) keeps these tests exactly as fast and hermetic as they've
// always been -- this is purely additive, opt-in real-infra coverage
// layered onto the same test bodies, not a replacement for them.
const EnvRealRelayAddr = "E2E_REAL_RELAY_ADDR"

// testRelayAddr resolves the one relay address TestJoinThroughRelay and
// TestAddLearnerThroughRelay each need -- EnvRealRelayAddr's value if set,
// or (the default) a fresh hermetic p2praft.RelayNode started for this
// test alone. This is the merge point behind both: previously each spun
// up its own identical local relay inline, unable to ever be pointed at
// anything else; now the exact same test bodies exercise a real relay
// when asked to, with no duplicated test logic between a hermetic and a
// "real" version of each.
//
// TestPairThroughRelayCluster needs its own equivalent inline (see that
// test) rather than calling this: its hermetic fallback is a full
// bootstrapped daemon.Node cluster (RelayService:true, its own
// ensureDefaultPublicCommand seed), not a bare p2praft.RelayNode, since it
// specifically needs the EventPublicAccess self-service escalation path
// -- a real deployed bootstrap already satisfies that same precondition
// in production, so real mode there also just needs the address.
func testRelayAddr(t *testing.T, ctx context.Context, tmpDir string) string {
	t.Helper()
	if addr := os.Getenv(EnvRealRelayAddr); addr != "" {
		t.Logf("using real relay from %s: %s", EnvRealRelayAddr, addr)
		return addr
	}
	relay, err := p2praft.StartRelayNode(ctx, filepath.Join(tmpDir, "relay.key"), 0)
	if err != nil {
		t.Fatalf("start relay: %v", err)
	}
	t.Cleanup(func() { relay.Host.Close() })
	if len(relay.Addrs) == 0 {
		t.Fatal("relay has no addresses")
	}
	return relay.Addrs[0]
}
