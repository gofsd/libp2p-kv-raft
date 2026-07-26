package registry

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
)

// accessTokenLabel domain-separates AccessTokenForKeyFile's HMAC from any
// other use of the same key material (there is none today, but costs
// nothing to keep the derivation from ever being reusable as some other
// value by construction). Versioned so a future v2 derivation can coexist
// without silently changing everyone's existing token.
const accessTokenLabel = "libp2p-kv-raft/kvhttp-access-token/v1"

// AccessTokenForKeyFile derives cmd/kvhttp's per-node bearer token from the
// Ed25519 identity key stored at keyPath (identity.key's own hex-encoded
// format -- see pkg/kvctl/identity.go's persistIdentity, the only writer of
// this format). It is deterministic and needs no storage of its own: the
// same identity.key always derives the same token, so a node created via
// `mage addnode`/`addnodewithkey` can have its token reprinted any time
// (`mage accesstoken <peerID>`) without kvhttp or the registry having
// persisted a separate secret anywhere.
//
// HMAC-SHA256, keyed by the raw private key bytes, over a fixed label --
// one-way (recovering the private key, or even just the peer id, from the
// token is infeasible) and unique per key, unlike the peer id itself, which
// is derived from the *public* key and so is not a secret an attacker
// couldn't already compute on their own from a leaked peer id.
func AccessTokenForKeyFile(keyPath string) (string, error) {
	hexData, err := os.ReadFile(keyPath)
	if err != nil {
		return "", fmt.Errorf("registry: read key file %s: %w", keyPath, err)
	}
	raw, err := hex.DecodeString(strings.TrimSpace(string(hexData)))
	if err != nil {
		return "", fmt.Errorf("registry: decode key file %s: %w", keyPath, err)
	}
	mac := hmac.New(sha256.New, raw)
	mac.Write([]byte(accessTokenLabel))
	return hex.EncodeToString(mac.Sum(nil)), nil
}

// AccessToken is AccessTokenForKeyFile for a node already known to this
// registry: it resolves peerID's own identity.key path first, then derives
// exactly the same way -- the lookup `mage accesstoken <peerID>` and
// cmd/kvhttp's own token-to-peer matching both use.
func (r *Registry) AccessToken(peerID string) (string, error) {
	info, ok, err := r.Get(peerID)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", fmt.Errorf("registry: unknown peer id %s (not created on this machine)", peerID)
	}
	return AccessTokenForKeyFile(info.KeyPath)
}
