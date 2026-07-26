package registry_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/gofsd/libp2p-kv-raft/pkg/registry"
)

func writeKeyFile(t *testing.T, hexContent string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "identity.key")
	if err := os.WriteFile(path, []byte(hexContent), 0o600); err != nil {
		t.Fatalf("write key file: %v", err)
	}
	return path
}

// TestAccessTokenForKeyFileDeterministic checks the core property the whole
// feature relies on: the same identity.key always derives the same token,
// with no separate storage involved.
func TestAccessTokenForKeyFileDeterministic(t *testing.T) {
	path := writeKeyFile(t, "aabbccdd")

	first, err := registry.AccessTokenForKeyFile(path)
	if err != nil {
		t.Fatalf("AccessTokenForKeyFile: %v", err)
	}
	second, err := registry.AccessTokenForKeyFile(path)
	if err != nil {
		t.Fatalf("AccessTokenForKeyFile (second call): %v", err)
	}
	if first != second {
		t.Fatalf("token not deterministic: %q != %q", first, second)
	}
	if first == "" {
		t.Fatalf("token is empty")
	}
}

// TestAccessTokenForKeyFileDistinctPerKey checks that two different keys
// never collide onto the same token -- otherwise one node's token would
// grant access to another's.
func TestAccessTokenForKeyFileDistinctPerKey(t *testing.T) {
	tokenA, err := registry.AccessTokenForKeyFile(writeKeyFile(t, "aabbccdd"))
	if err != nil {
		t.Fatalf("AccessTokenForKeyFile(a): %v", err)
	}
	tokenB, err := registry.AccessTokenForKeyFile(writeKeyFile(t, "11223344"))
	if err != nil {
		t.Fatalf("AccessTokenForKeyFile(b): %v", err)
	}
	if tokenA == tokenB {
		t.Fatalf("distinct keys derived the same token: %q", tokenA)
	}
}

// TestAccessTokenForKeyFileTrimsWhitespace matches identity.key's own
// reader (importIdentity/loadKey): a trailing newline, as a text editor or
// echo without -n would add, must not change the derived token.
func TestAccessTokenForKeyFileTrimsWhitespace(t *testing.T) {
	bare, err := registry.AccessTokenForKeyFile(writeKeyFile(t, "aabbccdd"))
	if err != nil {
		t.Fatalf("AccessTokenForKeyFile(bare): %v", err)
	}
	padded, err := registry.AccessTokenForKeyFile(writeKeyFile(t, "aabbccdd\n"))
	if err != nil {
		t.Fatalf("AccessTokenForKeyFile(padded): %v", err)
	}
	if bare != padded {
		t.Fatalf("trailing whitespace changed the token: %q != %q", bare, padded)
	}
}

// TestAccessTokenForKeyFileRejectsBadInput checks the two ways a key file
// can be unusable: missing, or not valid hex.
func TestAccessTokenForKeyFileRejectsBadInput(t *testing.T) {
	if _, err := registry.AccessTokenForKeyFile(filepath.Join(t.TempDir(), "missing.key")); err == nil {
		t.Fatalf("expected an error for a missing key file")
	}
	if _, err := registry.AccessTokenForKeyFile(writeKeyFile(t, "not-hex!!")); err == nil {
		t.Fatalf("expected an error for a non-hex key file")
	}
}

// TestRegistryAccessTokenMatchesKeyFile checks Registry.AccessToken's own
// peerID-to-keyPath resolution against the same result
// AccessTokenForKeyFile(info.KeyPath) would give directly.
func TestRegistryAccessTokenMatchesKeyFile(t *testing.T) {
	reg := openTestRegistry(t)
	keyPath := writeKeyFile(t, "deadbeef")
	if err := reg.Put(registry.NodeInfo{PeerID: "peer-a", KeyPath: keyPath}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	want, err := registry.AccessTokenForKeyFile(keyPath)
	if err != nil {
		t.Fatalf("AccessTokenForKeyFile: %v", err)
	}
	got, err := reg.AccessToken("peer-a")
	if err != nil {
		t.Fatalf("reg.AccessToken: %v", err)
	}
	if got != want {
		t.Fatalf("reg.AccessToken = %q, want %q", got, want)
	}
}

// TestRegistryAccessTokenUnknownPeer checks the "not created on this
// machine" case Registry.Get already reports elsewhere.
func TestRegistryAccessTokenUnknownPeer(t *testing.T) {
	reg := openTestRegistry(t)
	if _, err := reg.AccessToken("no-such-peer"); err == nil {
		t.Fatalf("expected an error for an unregistered peer id")
	}
}
