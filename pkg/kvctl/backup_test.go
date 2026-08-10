package kvctl

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/registry"
)

// writeFile is a small test helper: creates path (and any missing parent
// directories) with the given contents.
func writeFile(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// TestArchiveExtractRoundTrip checks archiveDir/extractArchive together
// preserve a nested directory tree exactly -- file contents, relative
// paths, and directory structure -- the property BackupNode/RestoreNode
// both depend on.
func TestArchiveExtractRoundTrip(t *testing.T) {
	src := t.TempDir()
	writeFile(t, filepath.Join(src, "identity.key"), "the-key")
	writeFile(t, filepath.Join(src, "kv.db"), "sqlite-bytes")
	writeFile(t, filepath.Join(src, "raft", "snapshots", "snap-1", "meta.json"), `{"a":1}`)
	writeFile(t, filepath.Join(src, "raft", "log.db"), "boltdb-bytes")

	archivePath := filepath.Join(t.TempDir(), "backup.tar.gz")
	if err := archiveDir(src, archivePath); err != nil {
		t.Fatalf("archiveDir: %v", err)
	}

	dest := filepath.Join(t.TempDir(), "restored")
	if err := extractArchive(archivePath, dest); err != nil {
		t.Fatalf("extractArchive: %v", err)
	}

	wantFiles := map[string]string{
		"identity.key": "the-key",
		"kv.db":        "sqlite-bytes",
		filepath.Join("raft", "snapshots", "snap-1", "meta.json"): `{"a":1}`,
		filepath.Join("raft", "log.db"):                           "boltdb-bytes",
	}
	for rel, want := range wantFiles {
		got, err := os.ReadFile(filepath.Join(dest, rel))
		if err != nil {
			t.Fatalf("read restored %s: %v", rel, err)
		}
		if string(got) != want {
			t.Fatalf("restored %s = %q, want %q", rel, got, want)
		}
	}
}

// buildMaliciousArchive writes a gzip-compressed tar containing one
// regular-file entry named by rawName, with no corresponding directory
// entries -- exactly the shape a hand-crafted (not archiveDir-produced)
// archive attempting a path-traversal escape would have.
func buildMaliciousArchive(t *testing.T, rawName, contents string) string {
	t.Helper()
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gw)
	hdr := &tar.Header{
		Name:     rawName,
		Typeflag: tar.TypeReg,
		Mode:     0o644,
		Size:     int64(len(contents)),
	}
	if err := tw.WriteHeader(hdr); err != nil {
		t.Fatalf("write header: %v", err)
	}
	if _, err := tw.Write([]byte(contents)); err != nil {
		t.Fatalf("write body: %v", err)
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("close tar writer: %v", err)
	}
	if err := gw.Close(); err != nil {
		t.Fatalf("close gzip writer: %v", err)
	}

	path := filepath.Join(t.TempDir(), "malicious.tar.gz")
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		t.Fatalf("write archive: %v", err)
	}
	return path
}

// TestExtractArchiveRejectsPathTraversal is the security-relevant case
// this package's own doc comment calls out: extractArchive must refuse
// any entry whose path would resolve outside destDir, rather than
// silently following it out -- the difference between a corrupted/hostile
// backup archive just failing to restore versus overwriting arbitrary
// files elsewhere on disk under whatever permissions the restoring
// process has.
func TestExtractArchiveRejectsPathTraversal(t *testing.T) {
	tests := []struct {
		name string
		raw  string
	}{
		{name: "relative parent traversal", raw: "../../etc/passwd"},
		{name: "traversal buried after a legitimate-looking prefix", raw: "raft/../../outside.txt"},
		{name: "absolute path", raw: "/etc/passwd"},
		{name: "bare parent reference", raw: ".."},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			archivePath := buildMaliciousArchive(t, tt.raw, "pwned")
			dest := filepath.Join(t.TempDir(), "restored")

			err := extractArchive(archivePath, dest)
			if err == nil {
				t.Fatalf("extractArchive accepted entry %q, want it rejected", tt.raw)
			}

			// Nothing from this entry should have landed anywhere on disk,
			// inside dest or out -- confirm the specific escape target this
			// case names was never created.
			escapeTarget := filepath.Join(filepath.Dir(dest), "..", "etc", "passwd")
			if _, statErr := os.Stat(escapeTarget); statErr == nil {
				t.Fatalf("entry %q actually wrote outside dest", tt.raw)
			}
		})
	}
}

// TestExtractArchiveAcceptsOrdinaryNestedEntry is
// TestExtractArchiveRejectsPathTraversal's control case: a legitimately
// nested (but not archiveDir-produced) entry must still extract normally,
// so the traversal guard is confirmed to be checking the resolved path
// rather than rejecting every non-flat entry on sight.
func TestExtractArchiveAcceptsOrdinaryNestedEntry(t *testing.T) {
	archivePath := buildMaliciousArchive(t, "raft/snapshots/meta.json", `{"ok":true}`)
	dest := filepath.Join(t.TempDir(), "restored")

	if err := extractArchive(archivePath, dest); err != nil {
		t.Fatalf("extractArchive rejected an ordinary nested entry: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(dest, "raft", "snapshots", "meta.json"))
	if err != nil {
		t.Fatalf("read extracted file: %v", err)
	}
	if string(got) != `{"ok":true}` {
		t.Fatalf("got %q, want %q", got, `{"ok":true}`)
	}
}

// TestRestoreNodeRefusesNonEmptyDestDir checks RestoreNode's own guard
// against interleaving a backup's files with whatever already exists at
// destDir -- including, worst case, a live node's own data -- rather than
// silently merging the two.
func TestRestoreNodeRefusesNonEmptyDestDir(t *testing.T) {
	src := t.TempDir()
	writeFile(t, filepath.Join(src, "identity.key"), "k")
	archivePath := filepath.Join(t.TempDir(), "backup.tar.gz")
	if err := archiveDir(src, archivePath); err != nil {
		t.Fatalf("archiveDir: %v", err)
	}

	dest := t.TempDir()
	writeFile(t, filepath.Join(dest, "already-here.txt"), "pre-existing")

	if err := RestoreNode(archivePath, dest); err == nil {
		t.Fatal("RestoreNode into a non-empty destDir succeeded, want refusal")
	}

	// The pre-existing file must be untouched, and nothing from the
	// archive should have been extracted alongside it.
	got, err := os.ReadFile(filepath.Join(dest, "already-here.txt"))
	if err != nil {
		t.Fatalf("pre-existing file missing after refused restore: %v", err)
	}
	if string(got) != "pre-existing" {
		t.Fatalf("pre-existing file modified: got %q", got)
	}
	if _, err := os.Stat(filepath.Join(dest, "identity.key")); err == nil {
		t.Fatal("RestoreNode extracted archive contents despite refusing the non-empty destDir")
	}
}

// TestRestoreNodeIntoEmptyDir is RestoreNode's happy path, isolated from
// BackupNode's registry/liveness plumbing.
func TestRestoreNodeIntoEmptyDir(t *testing.T) {
	src := t.TempDir()
	writeFile(t, filepath.Join(src, "identity.key"), "k")
	writeFile(t, filepath.Join(src, "kv.db"), "v")
	archivePath := filepath.Join(t.TempDir(), "backup.tar.gz")
	if err := archiveDir(src, archivePath); err != nil {
		t.Fatalf("archiveDir: %v", err)
	}

	dest := filepath.Join(t.TempDir(), "restored")
	if err := RestoreNode(archivePath, dest); err != nil {
		t.Fatalf("RestoreNode: %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(dest, "identity.key")); err != nil || string(got) != "k" {
		t.Fatalf("identity.key = %q, err %v, want %q", got, err, "k")
	}

	// Restoring again into the same (now non-empty) dest must be refused,
	// not silently re-extracted on top.
	if err := RestoreNode(archivePath, dest); err == nil {
		t.Fatal("second RestoreNode into the now-populated dest succeeded, want refusal")
	}
}

// registerBackupTestNode isolates the registry the same way pkg/ipc's own
// tests do (t.Setenv, never the real operator registry) and registers
// peerID against dataDir with the given pid -- 0 for "not running,"
// os.Getpid() to simulate "still running" without needing a real second
// process.
func registerBackupTestNode(t *testing.T, peerID, dataDir string, pid int) {
	t.Helper()
	t.Setenv(registry.EnvHome, t.TempDir())
	reg, err := registry.Open()
	if err != nil {
		t.Fatalf("registry.Open: %v", err)
	}
	if err := reg.Put(registry.NodeInfo{PeerID: peerID, DataDir: dataDir, PID: pid}); err != nil {
		t.Fatalf("registry.Put: %v", err)
	}
}

// TestBackupNodeRefusesUnknownPeer checks BackupNode fails closed on a
// peer id this machine's registry has never heard of, rather than trying
// to guess a data directory for it.
func TestBackupNodeRefusesUnknownPeer(t *testing.T) {
	t.Setenv(registry.EnvHome, t.TempDir())
	if err := BackupNode("no-such-peer", filepath.Join(t.TempDir(), "out.tar.gz")); err == nil {
		t.Fatal("BackupNode succeeded for an unregistered peer id, want an error")
	}
}

// TestBackupNodeRefusesRunningNode checks the liveness guard BackupNode's
// own doc comment explains: archiving a live node's files risks a torn,
// unusable backup, so BackupNode must refuse rather than proceed. Uses
// this test process's own pid, which isAlive can genuinely observe as
// alive without needing to spawn a real second process.
func TestBackupNodeRefusesRunningNode(t *testing.T) {
	peerID := fmt.Sprintf("backup-running-test-%d", time.Now().UnixNano())
	dataDir := t.TempDir()
	writeFile(t, filepath.Join(dataDir, "identity.key"), "k")
	registerBackupTestNode(t, peerID, dataDir, os.Getpid())

	err := BackupNode(peerID, filepath.Join(t.TempDir(), "out.tar.gz"))
	if err == nil {
		t.Fatal("BackupNode succeeded against a node registered as still running, want refusal")
	}
}

// TestBackupNodeRestoreNodeRoundTrip is the full, registry-driven happy
// path: a not-running registered node's data directory round-trips
// through BackupNode then RestoreNode with its contents intact.
func TestBackupNodeRestoreNodeRoundTrip(t *testing.T) {
	peerID := fmt.Sprintf("backup-roundtrip-test-%d", time.Now().UnixNano())
	dataDir := t.TempDir()
	writeFile(t, filepath.Join(dataDir, "identity.key"), "the-identity-key")
	writeFile(t, filepath.Join(dataDir, "kv.db"), "the-store")
	registerBackupTestNode(t, peerID, dataDir, 0) // pid 0: never running

	archivePath := filepath.Join(t.TempDir(), "out.tar.gz")
	if err := BackupNode(peerID, archivePath); err != nil {
		t.Fatalf("BackupNode: %v", err)
	}

	restoreDir := filepath.Join(t.TempDir(), "restored")
	if err := RestoreNode(archivePath, restoreDir); err != nil {
		t.Fatalf("RestoreNode: %v", err)
	}

	for name, want := range map[string]string{
		"identity.key": "the-identity-key",
		"kv.db":        "the-store",
	} {
		got, err := os.ReadFile(filepath.Join(restoreDir, name))
		if err != nil {
			t.Fatalf("read restored %s: %v", name, err)
		}
		if string(got) != want {
			t.Fatalf("restored %s = %q, want %q", name, got, want)
		}
	}
}
