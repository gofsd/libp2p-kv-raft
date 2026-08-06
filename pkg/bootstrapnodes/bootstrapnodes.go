// Package bootstrapnodes reads configs/bootstrap-nodes.json, the
// human-maintained catalog of already-deployed -relay-service nodes (see
// that file's own entries and CLAUDE.md's "Node connectivity policy").
// magefile.go's own BootstrapNodes target (mage bootstrapnodes) parses the
// same file independently, since it lives under a `mage`-only build tag
// and can't import a binary-only caller's package graph; this package
// exists so an ordinary binary -- cmd/relaytool, so far -- can load the
// catalog too, instead of hardcoding a copy of one entry that only stays
// correct until the catalog's next edit.
package bootstrapnodes

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// RelativePath is configs/bootstrap-nodes.json's path relative to the repo
// root.
const RelativePath = "configs/bootstrap-nodes.json"

// Node is one configs/bootstrap-nodes.json entry.
type Node struct {
	Name         string   `json:"name"`
	SSHHost      string   `json:"ssh_host"`
	InstallDir   string   `json:"install_dir"`
	ListenPort   int      `json:"listen_port"`
	RelayService bool     `json:"relay_service"`
	PeerID       string   `json:"peer_id"`
	DataDir      string   `json:"data_dir"`
	KeyPath      string   `json:"key_path"`
	ListenAddrs  []string `json:"listen_addrs"`
}

// File is configs/bootstrap-nodes.json's top-level shape.
type File struct {
	BootstrapNodes []Node `json:"bootstrap_nodes"`
}

// Load reads and parses the catalog at path.
func Load(path string) (File, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return File{}, fmt.Errorf("bootstrapnodes: read %s: %w", path, err)
	}
	var f File
	if err := json.Unmarshal(data, &f); err != nil {
		return File{}, fmt.Errorf("bootstrapnodes: parse %s: %w", path, err)
	}
	return f, nil
}

// FindRepoRoot walks upward from dir (empty means the current working
// directory) looking for go.mod, the same way the go tool itself locates a
// module boundary. Robust to being invoked via `go run` from any
// subdirectory of the repo -- unlike locating the root from the calling
// file's own compile-time path (as magefile.go's repoRoot does, relying on
// runtime.Caller), this doesn't depend on debug info surviving into
// whatever binary ends up calling it.
func FindRepoRoot(dir string) (string, error) {
	if dir == "" {
		wd, err := os.Getwd()
		if err != nil {
			return "", fmt.Errorf("bootstrapnodes: getwd: %w", err)
		}
		dir = wd
	}
	dir, err := filepath.Abs(dir)
	if err != nil {
		return "", fmt.Errorf("bootstrapnodes: resolve %s: %w", dir, err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("bootstrapnodes: no go.mod found above %s", dir)
		}
		dir = parent
	}
}

// PrimaryRelayAddr returns the first RelayService entry's best dialable
// multiaddr -- the plain TCP one where present, since that's what every
// Go-based platform (desktop, remote, Android) can dial directly, unlike a
// QUIC/WebTransport entry only a browser client needs (see
// pkg/daemon.advertisedAddrs's own doc comment on the same TCP-first
// preference). Returns an error if no entry has RelayService set, or that
// entry has no listen_addrs at all.
func (f File) PrimaryRelayAddr() (string, error) {
	for _, n := range f.BootstrapNodes {
		if !n.RelayService {
			continue
		}
		if len(n.ListenAddrs) == 0 {
			return "", fmt.Errorf("bootstrapnodes: %s has relay_service=true but no listen_addrs", n.Name)
		}
		for _, addr := range n.ListenAddrs {
			if strings.Contains(addr, "/tcp/") {
				return addr, nil
			}
		}
		return n.ListenAddrs[0], nil
	}
	return "", fmt.Errorf("bootstrapnodes: no entry has relay_service=true")
}
