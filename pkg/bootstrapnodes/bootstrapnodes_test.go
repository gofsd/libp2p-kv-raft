package bootstrapnodes

import (
	"os"
	"path/filepath"
	"testing"
)

func writeCatalog(t *testing.T, dir, contents string) string {
	t.Helper()
	path := filepath.Join(dir, "bootstrap-nodes.json")
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("write catalog: %v", err)
	}
	return path
}

func TestLoadParsesRealCatalog(t *testing.T) {
	// The actual repo file, not a synthetic one -- so a future edit to
	// configs/bootstrap-nodes.json that breaks its own schema fails a test
	// instead of silently breaking cmd/relaytool's startup.
	root, err := FindRepoRoot("")
	if err != nil {
		t.Fatalf("FindRepoRoot: %v", err)
	}
	f, err := Load(filepath.Join(root, RelativePath))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(f.BootstrapNodes) == 0 {
		t.Fatal("expected at least one bootstrap node in the real catalog")
	}
	addr, err := f.PrimaryRelayAddr()
	if err != nil {
		t.Fatalf("PrimaryRelayAddr: %v", err)
	}
	if addr == "" {
		t.Fatal("expected a non-empty primary relay addr")
	}
}

func TestPrimaryRelayAddrPrefersTCPOverQUIC(t *testing.T) {
	dir := t.TempDir()
	path := writeCatalog(t, dir, `{
		"bootstrap_nodes": [
			{
				"name": "primary",
				"relay_service": true,
				"peer_id": "12D3KooWExample",
				"listen_addrs": [
					"/ip4/1.2.3.4/udp/4001/quic-v1/p2p/12D3KooWExample",
					"/ip4/1.2.3.4/tcp/4001/p2p/12D3KooWExample"
				]
			}
		]
	}`)
	f, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	addr, err := f.PrimaryRelayAddr()
	if err != nil {
		t.Fatalf("PrimaryRelayAddr: %v", err)
	}
	want := "/ip4/1.2.3.4/tcp/4001/p2p/12D3KooWExample"
	if addr != want {
		t.Fatalf("got %q, want %q (TCP entry, even though it's listed second)", addr, want)
	}
}

func TestPrimaryRelayAddrSkipsNonRelayEntries(t *testing.T) {
	dir := t.TempDir()
	path := writeCatalog(t, dir, `{
		"bootstrap_nodes": [
			{"name": "not-a-relay", "relay_service": false, "listen_addrs": ["/ip4/9.9.9.9/tcp/1/p2p/x"]},
			{"name": "primary", "relay_service": true, "listen_addrs": ["/ip4/1.2.3.4/tcp/4001/p2p/12D3KooWExample"]}
		]
	}`)
	f, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	addr, err := f.PrimaryRelayAddr()
	if err != nil {
		t.Fatalf("PrimaryRelayAddr: %v", err)
	}
	if addr != "/ip4/1.2.3.4/tcp/4001/p2p/12D3KooWExample" {
		t.Fatalf("got %q, expected the relay_service entry's address, not the non-relay one", addr)
	}
}

func TestPrimaryRelayAddrErrorsWithNoRelayEntry(t *testing.T) {
	dir := t.TempDir()
	path := writeCatalog(t, dir, `{"bootstrap_nodes": [{"name": "not-a-relay", "relay_service": false}]}`)
	f, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if _, err := f.PrimaryRelayAddr(); err == nil {
		t.Fatal("expected an error when no entry has relay_service=true")
	}
}

func TestPrimaryRelayAddrErrorsWithNoListenAddrs(t *testing.T) {
	dir := t.TempDir()
	path := writeCatalog(t, dir, `{"bootstrap_nodes": [{"name": "primary", "relay_service": true, "listen_addrs": []}]}`)
	f, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if _, err := f.PrimaryRelayAddr(); err == nil {
		t.Fatal("expected an error when the relay entry has no listen_addrs")
	}
}

func TestLoadErrorsOnMissingFile(t *testing.T) {
	if _, err := Load(filepath.Join(t.TempDir(), "does-not-exist.json")); err == nil {
		t.Fatal("expected an error reading a nonexistent catalog file")
	}
}

func TestLoadErrorsOnInvalidJSON(t *testing.T) {
	dir := t.TempDir()
	path := writeCatalog(t, dir, `{not valid json`)
	if _, err := Load(path); err == nil {
		t.Fatal("expected an error parsing invalid JSON")
	}
}

func TestFindRepoRootLocatesGoMod(t *testing.T) {
	root, err := FindRepoRoot("")
	if err != nil {
		t.Fatalf("FindRepoRoot: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("resolved root %s has no go.mod: %v", root, err)
	}
	if _, err := os.Stat(filepath.Join(root, RelativePath)); err != nil {
		t.Fatalf("resolved root %s has no %s: %v", root, RelativePath, err)
	}
}

func TestFindRepoRootFromSubdirectory(t *testing.T) {
	root, err := FindRepoRoot("")
	if err != nil {
		t.Fatalf("FindRepoRoot(\"\"): %v", err)
	}
	sub := filepath.Join(root, "pkg", "bootstrapnodes")
	fromSub, err := FindRepoRoot(sub)
	if err != nil {
		t.Fatalf("FindRepoRoot(%q): %v", sub, err)
	}
	if fromSub != root {
		t.Fatalf("got %q, want %q", fromSub, root)
	}
}

func TestFindRepoRootErrorsOutsideAnyModule(t *testing.T) {
	// A fresh temp dir has no go.mod anywhere above it (t.TempDir() lives
	// under the OS temp root, never inside this module's own tree).
	if _, err := FindRepoRoot(t.TempDir()); err == nil {
		t.Fatal("expected an error when no go.mod exists above dir")
	}
}
