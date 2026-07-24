//go:build !android

package ipc

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"testing"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// TestCallRawPreservesOriginalSignature is CallRaw's core guarantee: unlike
// Call (which always signs with whatever priv the caller passes), CallRaw
// must deliver encoded's bytes -- and therefore whatever signature was
// baked into them, however long ago and by whichever key -- to the daemon
// completely unchanged. It proves this by building a request with one
// keypair (simulating a ticket signed well in advance) and having the
// serving side verify it against that same original public key, never a
// key CallRaw itself introduced.
func TestCallRawPreservesOriginalSignature(t *testing.T) {
	t.Parallel()

	peerID := fmt.Sprintf("callraw-test-%d", time.Now().UnixNano())

	originalPub, originalPriv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	req := shmevent.Msg{EventType: shmevent.EventGetField, Value: []byte("some-key"), ID: 7}
	encoded, err := shmevent.Encode(req, originalPriv)
	if err != nil {
		t.Fatalf("shmevent.Encode: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	serveErrCh := make(chan error, 1)
	go func() {
		serveErrCh <- Serve(ctx, peerID, originalPriv, func(_ context.Context, m shmevent.Msg, crc uint32, sig []byte) shmevent.Msg {
			// The point of the test: verifying against originalPub must
			// succeed, proving CallRaw never substituted a different
			// signature -- a bug here would mean CallRaw quietly re-signed
			// (or mangled) the caller's already-signed bytes.
			if err := shmevent.Verify(originalPub, m, crc, sig); err != nil {
				return shmevent.Msg{EventType: shmevent.EventError, ID: m.ID, Value: []byte(err.Error())}
			}
			return shmevent.Msg{EventType: shmevent.EventGetField, ID: m.ID, Value: []byte("ok")}
		})
	}()

	resp, err := CallRaw(ctx, peerID, encoded)
	if err != nil {
		t.Fatalf("CallRaw: %v", err)
	}
	if resp.EventType == shmevent.EventError {
		t.Fatalf("server rejected the request's original signature: %s", resp.Value)
	}
	if string(resp.Value) != "ok" {
		t.Fatalf("got response value %q, want %q", resp.Value, "ok")
	}

	cancel()
	<-serveErrCh
}

// TestCallRawRejectsMalformedBytes checks CallRaw fails fast (before ever
// touching shmring) on input that isn't a valid shmevent.Encode output --
// it decodes encoded up front purely to read m.ID back out, and that same
// decode is what catches this case.
func TestCallRawRejectsMalformedBytes(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if _, err := CallRaw(ctx, "irrelevant-peer", []byte("not a real encoded message")); err == nil {
		t.Fatal("CallRaw unexpectedly accepted malformed input")
	}
}
