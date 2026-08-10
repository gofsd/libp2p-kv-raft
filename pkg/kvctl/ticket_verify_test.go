package kvctl

import (
	"encoding/base64"
	"fmt"
	"strings"
	"testing"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/gofsd/libp2p-kv-raft/pkg/registry"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// testIdentity is a self-consistent (peer id, address, raw signing key)
// triple -- addr's own embedded /p2p/<id> component really does
// correspond to priv, exactly the property RedeemExecInviteTicket/
// VerifyJoinInviteTicket/RedeemJoinRequestTicket all check before trusting
// a ticket's claimed source.
type testIdentity struct {
	addr string
	priv shmevent.PrivateKey
}

func newTestIdentity(t *testing.T) testIdentity {
	t.Helper()
	priv, pub, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err != nil {
		t.Fatalf("generate key pair: %v", err)
	}
	id, err := peer.IDFromPublicKey(pub)
	if err != nil {
		t.Fatalf("peer id from public key: %v", err)
	}
	raw, err := priv.Raw()
	if err != nil {
		t.Fatalf("raw private key: %v", err)
	}
	return testIdentity{
		addr: fmt.Sprintf("/ip4/127.0.0.1/tcp/4001/p2p/%s", id),
		priv: shmevent.PrivateKey(raw),
	}
}

// buildTicket builds and signs a ticket via build (one of
// shmevent.NewExecTicket/NewJoinTicket/NewJoinRequestTicket), claiming
// sourceAddr, but signs with signWith -- genuine when signWith is the
// identity sourceAddr's own peer id actually corresponds to, forged when
// it's a different identity's key entirely.
func buildTicket(t *testing.T, build func(sourceAddr string, token []byte) (shmevent.Msg, error), sourceAddr string, token []byte, signWith shmevent.PrivateKey) string {
	t.Helper()
	m, err := build(sourceAddr, token)
	if err != nil {
		t.Fatalf("build ticket: %v", err)
	}
	wire, err := shmevent.Encode(m, signWith)
	if err != nil {
		t.Fatalf("sign ticket: %v", err)
	}
	return base64.StdEncoding.EncodeToString(wire)
}

// TestVerifyJoinInviteTicketAcceptsGenuineTicket is VerifyJoinInviteTicket's
// happy path -- it needs no daemon/IPC at all (unlike the exec-invite and
// join-request equivalents), so this is a complete, fully isolated check:
// a ticket whose embedded sourceAddr peer id really did sign it is
// accepted, and the plain "<addr>#<tokenHex>" string it returns is exactly
// what CreateJoinInvite's own callers already hand-assemble.
func TestVerifyJoinInviteTicketAcceptsGenuineTicket(t *testing.T) {
	id := newTestIdentity(t)
	token := []byte("a-real-join-invite-token")

	ticketB64 := buildTicket(t, shmevent.NewJoinTicket, id.addr, token, id.priv)

	got, err := VerifyJoinInviteTicket(ticketB64)
	if err != nil {
		t.Fatalf("VerifyJoinInviteTicket rejected a genuinely self-signed ticket: %v", err)
	}
	want := id.addr + "#" + fmt.Sprintf("%x", token)
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// TestVerifyJoinInviteTicketRejectsForgedSigner is the core security
// property under test across all three ticket verifiers: a ticket
// claiming (via sourceAddr) to come from one peer id, but actually signed
// by a completely different key, must be rejected outright -- this is
// exactly what stops a peer from impersonating another peer's identity in
// a ticket it relays or crafts itself. The forged ticket here is
// otherwise perfectly well-formed (right event type, right field
// encoding, valid CRC/signature bytes) -- the *only* thing wrong with it
// is which key produced the signature, so a pass here would mean the
// self-certifying property this ticket design depends on doesn't
// actually hold.
func TestVerifyJoinInviteTicketRejectsForgedSigner(t *testing.T) {
	victim := newTestIdentity(t)   // sourceAddr claims to be this peer
	attacker := newTestIdentity(t) // but this key actually signed it
	token := []byte("stolen-token")

	forged := buildTicket(t, shmevent.NewJoinTicket, victim.addr, token, attacker.priv)

	if _, err := VerifyJoinInviteTicket(forged); err == nil {
		t.Fatal("VerifyJoinInviteTicket accepted a ticket signed by a key other than the one sourceAddr claims, want rejection")
	}
}

// TestVerifyJoinInviteTicketRejectsMalformedInput covers the parsing
// failures that must happen before any cryptographic check is even
// attempted: invalid base64, base64 that doesn't decode to a valid
// shmevent-encoded message, and a validly-encoded message of the wrong
// event type entirely (a genuinely signed message, just not a join
// ticket).
func TestVerifyJoinInviteTicketRejectsMalformedInput(t *testing.T) {
	t.Run("invalid base64", func(t *testing.T) {
		if _, err := VerifyJoinInviteTicket("not valid base64!!!"); err == nil {
			t.Fatal("want rejection of invalid base64")
		}
	})

	t.Run("valid base64, not a real encoded message", func(t *testing.T) {
		bogus := base64.StdEncoding.EncodeToString([]byte("not an encoded shmevent message"))
		if _, err := VerifyJoinInviteTicket(bogus); err == nil {
			t.Fatal("want rejection of base64 that isn't a real encoded message")
		}
	})

	t.Run("wrong event type", func(t *testing.T) {
		id := newTestIdentity(t)
		// A genuinely signed message, just not a joinTicket -- e.g. an exec
		// ticket instead. NewExecTicket, unlike NewJoinTicket, enforces a
		// fixed ExecInviteTokenSize (16 bytes).
		wrongType := buildTicket(t, shmevent.NewExecTicket, id.addr, make([]byte, shmevent.ExecInviteTokenSize), id.priv)
		_, err := VerifyJoinInviteTicket(wrongType)
		if err == nil {
			t.Fatal("want rejection of a well-signed message of the wrong event type")
		}
		if !strings.Contains(err.Error(), "not a join invite ticket") {
			t.Fatalf("error = %v, want it to name the type mismatch", err)
		}
	})

	t.Run("sourceAddr with no /p2p component", func(t *testing.T) {
		id := newTestIdentity(t)
		noPeerID := buildTicket(t, shmevent.NewJoinTicket, "/ip4/127.0.0.1/tcp/4001", []byte("tok"), id.priv)
		if _, err := VerifyJoinInviteTicket(noPeerID); err == nil {
			t.Fatal("want rejection of a sourceAddr with no embedded peer id")
		}
	})
}

// TestRedeemExecInviteTicketAppliesTheSameVerification checks
// RedeemExecInviteTicket -- which does go on to touch IPC once
// verification passes, unlike VerifyJoinInviteTicket -- applies the exact
// same forged-signer rejection before ever reaching that point, and that
// a genuinely self-signed ticket gets past verification (failing only at
// the registry/session stage in this test, since no daemon is running --
// distinguishable from a verification failure by its error text).
func TestRedeemExecInviteTicketAppliesTheSameVerification(t *testing.T) {
	t.Run("rejects forged signer before touching IPC", func(t *testing.T) {
		victim := newTestIdentity(t)
		attacker := newTestIdentity(t)
		forged := buildTicket(t, shmevent.NewExecTicket, victim.addr, make([]byte, shmevent.ExecInviteTokenSize), attacker.priv)

		if _, err := RedeemExecInviteTicket(forged); err == nil {
			t.Fatal("RedeemExecInviteTicket accepted a forged ticket, want rejection")
		}
	})

	t.Run("genuine ticket passes verification and only fails at the session stage", func(t *testing.T) {
		t.Setenv(registry.EnvHome, t.TempDir())
		id := newTestIdentity(t)
		genuine := buildTicket(t, shmevent.NewExecTicket, id.addr, make([]byte, shmevent.ExecInviteTokenSize), id.priv)

		_, err := RedeemExecInviteTicket(genuine)
		if err == nil {
			t.Fatal("expected an error since no daemon is running, but verification itself should have passed")
		}
		if strings.Contains(err.Error(), "verify") || strings.Contains(err.Error(), "signature") || strings.Contains(err.Error(), "issuer") {
			t.Fatalf("error = %v, looks like a verification failure -- want it to fail past verification, at the session/registry stage instead", err)
		}
	})
}

// TestRedeemJoinRequestTicketAppliesTheSameVerification is
// TestRedeemExecInviteTicketAppliesTheSameVerification's counterpart for
// the third and last of this package's self-certifying ticket verifiers.
func TestRedeemJoinRequestTicketAppliesTheSameVerification(t *testing.T) {
	t.Run("rejects forged signer before touching IPC", func(t *testing.T) {
		victim := newTestIdentity(t)
		attacker := newTestIdentity(t)
		forged := buildTicket(t, shmevent.NewJoinRequestTicket, victim.addr, []byte("tok"), attacker.priv)

		if _, err := RedeemJoinRequestTicket(forged, 1); err == nil {
			t.Fatal("RedeemJoinRequestTicket accepted a forged ticket, want rejection")
		}
	})

	t.Run("genuine ticket passes verification and only fails at the session stage", func(t *testing.T) {
		t.Setenv(registry.EnvHome, t.TempDir())
		id := newTestIdentity(t)
		genuine := buildTicket(t, shmevent.NewJoinRequestTicket, id.addr, []byte("tok"), id.priv)

		_, err := RedeemJoinRequestTicket(genuine, 1)
		if err == nil {
			t.Fatal("expected an error since no daemon is running, but verification itself should have passed")
		}
		if strings.Contains(err.Error(), "verify") || strings.Contains(err.Error(), "signature") || strings.Contains(err.Error(), "issuer") {
			t.Fatalf("error = %v, looks like a verification failure -- want it to fail past verification, at the session/registry stage instead", err)
		}
	})
}
