package e2erun

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/gofsd/libp2p-kv-raft/pkg/e2edata"
)

// TestDeleteLocalDesktopNode exercises the real filesystem/process-liveness
// logic (a real short-lived process, guaranteed exited by the time this
// runs, and a real data directory), isolated from any real operator state
// via EnvE2EHome. The remote (ssh) deletion path isn't covered here --
// like the rest of this package's ssh-dependent code, it needs a live
// server to exercise meaningfully; see pkg/e2erun's use from magefile.go's
// E2E namespace for how it's been verified against the real one.
func TestDeleteLocalDesktopNode(t *testing.T) {
	t.Setenv(EnvE2EHome, t.TempDir())

	_, priv, err := e2edata.GenerateIdentity()
	if err != nil {
		t.Fatalf("GenerateIdentity: %v", err)
	}
	peerID, err := e2edata.PeerIDFromPrivateKey(priv)
	if err != nil {
		t.Fatalf("PeerIDFromPrivateKey: %v", err)
	}
	node := e2edata.Node{Platform: e2edata.PlatformDesktop, PeerID: peerID}

	e2eHome, err := localE2EHome()
	if err != nil {
		t.Fatalf("localE2EHome: %v", err)
	}
	dataDir := desktopNodeDataDir(e2eHome, node)
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dataDir, "some-node-file"), []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}

	// A real process guaranteed to have already exited by the time
	// deleteLocalDesktopNode runs, so its pid is a genuine (not merely
	// simulated) "not alive" case -- isAlive has to actually work here,
	// not just parse a pidfile.
	cmd := exec.Command("true")
	if err := cmd.Run(); err != nil {
		t.Fatalf("run `true`: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dataDir, "e2e.pid"), []byte(strconv.Itoa(cmd.Process.Pid)), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := deleteLocalDesktopNode(node); err != nil {
		t.Fatalf("deleteLocalDesktopNode: %v", err)
	}
	if _, err := os.Stat(dataDir); !os.IsNotExist(err) {
		t.Fatalf("data dir %s still exists after deleteLocalDesktopNode", dataDir)
	}
}
