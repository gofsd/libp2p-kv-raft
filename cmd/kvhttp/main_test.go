package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/gofsd/libp2p-kv-raft/pkg/registry"
)

// TestResolveTLSFilesDefaultsToRegistryDirAndRequiresBothFiles covers
// resolveTLSFiles' three real branches: the default pair actually existing
// (success), one file missing from that default pair (fails closed, no
// silent plain-HTTP fallback), and an explicit pair overriding the
// default.
func TestResolveTLSFilesDefaultsToRegistryDirAndRequiresBothFiles(t *testing.T) {
	regDir := t.TempDir()

	t.Run("default pair missing entirely", func(t *testing.T) {
		if _, _, err := resolveTLSFiles("", "", regDir); err == nil {
			t.Fatal("expected an error when neither the default cert nor key exists")
		}
	})

	t.Run("default pair present", func(t *testing.T) {
		dir := defaultTLSDir(regDir)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("MkdirAll: %v", err)
		}
		certPath := filepath.Join(dir, "cert.pem")
		keyPath := filepath.Join(dir, "key.pem")
		if err := os.WriteFile(certPath, []byte("cert"), 0o644); err != nil {
			t.Fatalf("write cert: %v", err)
		}
		if err := os.WriteFile(keyPath, []byte("key"), 0o600); err != nil {
			t.Fatalf("write key: %v", err)
		}

		gotCert, gotKey, err := resolveTLSFiles("", "", regDir)
		if err != nil {
			t.Fatalf("resolveTLSFiles: %v", err)
		}
		if gotCert != certPath || gotKey != keyPath {
			t.Fatalf("resolveTLSFiles = (%q, %q), want (%q, %q)", gotCert, gotKey, certPath, keyPath)
		}
	})

	t.Run("only one of -tls-cert/-tls-key set", func(t *testing.T) {
		if _, _, err := resolveTLSFiles("/some/cert.pem", "", regDir); err == nil {
			t.Fatal("expected an error when only -tls-cert is set")
		}
		if _, _, err := resolveTLSFiles("", "/some/key.pem", regDir); err == nil {
			t.Fatal("expected an error when only -tls-key is set")
		}
	})

	t.Run("explicit pair missing on disk", func(t *testing.T) {
		if _, _, err := resolveTLSFiles(filepath.Join(t.TempDir(), "cert.pem"), filepath.Join(t.TempDir(), "key.pem"), regDir); err == nil {
			t.Fatal("expected an error when the explicitly-passed files don't exist")
		}
	})
}

func TestBearerToken(t *testing.T) {
	cases := []struct {
		name   string
		header string
		want   string
	}{
		{"well formed", "Bearer abc123", "abc123"},
		{"extra whitespace", "Bearer   abc123  ", "abc123"},
		{"missing header", "", ""},
		{"wrong scheme", "Basic abc123", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, "/command", nil)
			if tc.header != "" {
				r.Header.Set("Authorization", tc.header)
			}
			if got := bearerToken(r); got != tc.want {
				t.Fatalf("bearerToken(%q) = %q, want %q", tc.header, got, tc.want)
			}
		})
	}
}

// testHandler registers one node (peerA, backed by a real hex key file so
// registry.AccessTokenForKeyFile can derive its token) into an isolated
// registry, and returns the handler plus that node's own token.
func testHandler(t *testing.T) (h *handler, peerA, tokenA string) {
	t.Helper()
	t.Setenv(registry.EnvHome, t.TempDir())
	reg, err := registry.Open()
	if err != nil {
		t.Fatalf("registry.Open: %v", err)
	}

	keyPath := filepath.Join(t.TempDir(), "identity.key")
	if err := os.WriteFile(keyPath, []byte("aabbccdd"), 0o600); err != nil {
		t.Fatalf("write key file: %v", err)
	}
	peerA = "peer-a"
	if err := reg.Put(registry.NodeInfo{PeerID: peerA, KeyPath: keyPath}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	tokenA, err = registry.AccessTokenForKeyFile(keyPath)
	if err != nil {
		t.Fatalf("AccessTokenForKeyFile: %v", err)
	}
	return &handler{reg: reg}, peerA, tokenA
}

// TestResolvePeerFromTokenMatches checks the success path: a registered
// node's own token resolves back to that node's peer id.
func TestResolvePeerFromTokenMatches(t *testing.T) {
	h, peerA, tokenA := testHandler(t)

	got, err := h.resolvePeerFromToken(tokenA)
	if err != nil {
		t.Fatalf("resolvePeerFromToken: %v", err)
	}
	if got != peerA {
		t.Fatalf("resolvePeerFromToken = %q, want %q", got, peerA)
	}
}

// TestResolvePeerFromTokenRejectsUnknown checks that a token belonging to
// no registered node -- the "wrong/forged token" case -- is rejected,
// never silently falling back to some default node.
func TestResolvePeerFromTokenRejectsUnknown(t *testing.T) {
	h, _, _ := testHandler(t)

	if _, err := h.resolvePeerFromToken("not-a-real-token"); err == nil {
		t.Fatalf("expected an error for an unrecognized token")
	}
}

// TestResolvePeerFromTokenRejectsEmpty checks the "no Authorization header
// at all" case is rejected the same way, not treated as "any node".
func TestResolvePeerFromTokenRejectsEmpty(t *testing.T) {
	h, _, _ := testHandler(t)

	if _, err := h.resolvePeerFromToken(""); err == nil {
		t.Fatalf("expected an error for an empty token")
	}
}

// TestResolvePeerFromTokenScopesToOwnNode checks the actual security
// property this feature exists for: a second node's token must not resolve
// to the first node, and vice versa -- holding one node's token grants
// access only to that node.
func TestResolvePeerFromTokenScopesToOwnNode(t *testing.T) {
	h, peerA, tokenA := testHandler(t)

	keyPathB := filepath.Join(t.TempDir(), "identity.key")
	if err := os.WriteFile(keyPathB, []byte("11223344"), 0o600); err != nil {
		t.Fatalf("write key file: %v", err)
	}
	peerB := "peer-b"
	if err := h.reg.Put(registry.NodeInfo{PeerID: peerB, KeyPath: keyPathB}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	tokenB, err := registry.AccessTokenForKeyFile(keyPathB)
	if err != nil {
		t.Fatalf("AccessTokenForKeyFile: %v", err)
	}

	if got, err := h.resolvePeerFromToken(tokenA); err != nil || got != peerA {
		t.Fatalf("resolvePeerFromToken(tokenA) = %q, %v, want %q, nil", got, err, peerA)
	}
	if got, err := h.resolvePeerFromToken(tokenB); err != nil || got != peerB {
		t.Fatalf("resolvePeerFromToken(tokenB) = %q, %v, want %q, nil", got, err, peerB)
	}
}
