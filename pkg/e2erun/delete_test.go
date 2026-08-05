package e2erun

import (
	"bytes"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
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

// captureFD temporarily redirects *target (os.Stdout or os.Stderr) to a
// pipe for the duration of fn, returning everything written to it --
// DeleteNode/DeleteAllNodes report their real behavior (the affected-rows
// warning, the per-node "destroyed" confirmation) via fmt.Fprintf/Println
// rather than a return value, so this is the only way to actually verify
// those code paths run rather than just their side effects on disk.
func captureFD(t *testing.T, target **os.File, fn func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	orig := *target
	*target = w
	defer func() { *target = orig }()

	fn()

	if err := w.Close(); err != nil {
		t.Fatalf("close pipe writer: %v", err)
	}
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		t.Fatalf("read captured output: %v", err)
	}
	return buf.String()
}

// newTestDesktopNode generates a real, distinct identity and a real
// (empty) data directory for it under e2eHome -- the fixture
// TestDeleteNodeRemovesNodeAndWarnsAboutAffectedRows/
// TestDeleteAllNodesDestroysEveryNode build multiple of, same as
// TestDeleteLocalDesktopNode's single-node setup above.
func newTestDesktopNode(t *testing.T, e2eHome string) (e2edata.Node, string) {
	t.Helper()
	_, priv, err := e2edata.GenerateIdentity()
	if err != nil {
		t.Fatalf("GenerateIdentity: %v", err)
	}
	peerID, err := e2edata.PeerIDFromPrivateKey(priv)
	if err != nil {
		t.Fatalf("PeerIDFromPrivateKey: %v", err)
	}
	node := e2edata.Node{Platform: e2edata.PlatformDesktop, PeerID: peerID}
	dataDir := desktopNodeDataDir(e2eHome, node)
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		t.Fatal(err)
	}
	return node, dataDir
}

// TestDeleteNodeRemovesNodeAndWarnsAboutAffectedRows exercises DeleteNode's
// own path end to end -- previously entirely uncovered (only the
// lower-level deleteLocalDesktopNode it calls had a test, above): it must
// tear down the node's real data, remove it from f.Nodes, and -- since a
// row here still references the deleted node id -- print the documented
// warning to stderr instead of silently dropping that information.
func TestDeleteNodeRemovesNodeAndWarnsAboutAffectedRows(t *testing.T) {
	t.Setenv(EnvE2EHome, t.TempDir())

	e2eHome, err := localE2EHome()
	if err != nil {
		t.Fatalf("localE2EHome: %v", err)
	}
	node, dataDir := newTestDesktopNode(t, e2eHome)

	f := &e2edata.File{
		Nodes: map[int]e2edata.Node{1: node},
		Rows: []e2edata.Row{
			{Version: 1, Node: 1, Platform: e2edata.PlatformDesktop},
		},
	}

	var deleteErr error
	stderr := captureFD(t, &os.Stderr, func() {
		deleteErr = DeleteNode(f, 1)
	})
	if deleteErr != nil {
		t.Fatalf("DeleteNode: %v", deleteErr)
	}

	if _, ok := f.Nodes[1]; ok {
		t.Fatal("node 1 still present in f.Nodes after DeleteNode")
	}
	if _, err := os.Stat(dataDir); !os.IsNotExist(err) {
		t.Fatalf("data dir %s still exists after DeleteNode", dataDir)
	}
	if !strings.Contains(stderr, "1 row(s) still reference deleted node 1") {
		t.Fatalf("stderr = %q, want it to warn about the still-referencing row", stderr)
	}
}

// TestDeleteAllNodesDestroysEveryNode exercises DeleteAllNodes' real,
// multi-node teardown path -- previously entirely uncovered, the
// mage e2e:destroyall implementation this task set out to add coverage
// for. Builds three real desktop nodes (inserted out of id order, to
// actually prove the ascending-order claim rather than assume map
// iteration happened to match it) and checks all three are torn down,
// removed from f.Nodes, and reported "destroyed" in ascending node-id
// order, matching this function's own doc comment.
//
// The "continues past one node's failure" branch isn't covered here:
// forcing deleteLocalDesktopNode's only real failure mode (a genuine
// os.RemoveAll error) portably and deterministically, without either a
// permission check that's meaningless running as root or touching the
// real ssh-deployed bootstrap host DeleteNode's PlatformRemote case
// targets, isn't practical in this test environment -- the same class of
// gap TestDeleteLocalDesktopNode's own doc comment already accepts for
// the remote deletion path.
func TestDeleteAllNodesDestroysEveryNode(t *testing.T) {
	t.Setenv(EnvE2EHome, t.TempDir())

	e2eHome, err := localE2EHome()
	if err != nil {
		t.Fatalf("localE2EHome: %v", err)
	}

	f := &e2edata.File{Nodes: map[int]e2edata.Node{}}
	dataDirs := map[int]string{}
	for _, id := range []int{3, 1, 2} {
		node, dataDir := newTestDesktopNode(t, e2eHome)
		f.Nodes[id] = node
		dataDirs[id] = dataDir
	}

	var destroyErr error
	stdout := captureFD(t, &os.Stdout, func() {
		destroyErr = DeleteAllNodes(f)
	})
	if destroyErr != nil {
		t.Fatalf("DeleteAllNodes: %v", destroyErr)
	}

	if len(f.Nodes) != 0 {
		t.Fatalf("f.Nodes still has %d node(s) after DeleteAllNodes: %v", len(f.Nodes), f.Nodes)
	}
	for id, dir := range dataDirs {
		if _, err := os.Stat(dir); !os.IsNotExist(err) {
			t.Fatalf("node %d's data dir %s still exists after DeleteAllNodes", id, dir)
		}
	}

	wantLines := []string{"✅ node 1 destroyed", "✅ node 2 destroyed", "✅ node 3 destroyed"}
	gotLines := strings.Split(strings.TrimSpace(stdout), "\n")
	if len(gotLines) != len(wantLines) {
		t.Fatalf("stdout = %q, want exactly the lines %v in order", stdout, wantLines)
	}
	for i, want := range wantLines {
		if gotLines[i] != want {
			t.Fatalf("stdout line %d = %q, want %q (ascending node-id order)", i, gotLines[i], want)
		}
	}
}
