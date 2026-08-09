//go:build !android

package ipc

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"testing"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/registry"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// registerTestNode points registry.EnvHome at a fresh temp registry (so
// this never touches the real operator's ~/.libp2p-kv-raft) and registers
// peerID against dataDir -- the minimum tokenForPeer needs to resolve a
// peer id to the DataDir its loadOrGenerateToken reads/writes (see
// token.go). Real callers always reach Call/CallRaw with a peerID that
// came from a registry lookup in the first place; this is this package's
// own from-scratch stand-in for that, since pkg/ipc's tests otherwise
// have no dependency on pkg/registry at all. The calling test must not
// (and must never) call t.Parallel() -- Go's testing package forbids
// t.Setenv in any parallel test or one with parallel ancestors,
// regardless of call order.
func registerTestNode(t *testing.T, peerID, dataDir string) {
	t.Helper()
	t.Setenv(registry.EnvHome, t.TempDir())
	reg, err := registry.Open()
	if err != nil {
		t.Fatalf("registry.Open: %v", err)
	}
	if err := reg.Put(registry.NodeInfo{PeerID: peerID, DataDir: dataDir}); err != nil {
		t.Fatalf("registry.Put: %v", err)
	}
}

// TestCallRawPreservesOriginalSignature is CallRaw's core guarantee: unlike
// Call (which always signs with whatever priv the caller passes), CallRaw
// must deliver encoded's bytes -- and therefore whatever signature was
// baked into them, however long ago and by whichever key -- to the daemon
// completely unchanged. It proves this by building a request with one
// keypair (simulating a ticket signed well in advance) and having the
// serving side verify it against that same original public key, never a
// key CallRaw itself introduced.
//
// Not t.Parallel(): registerTestNode calls t.Setenv (to point
// registry.EnvHome at an isolated temp registry, see its own doc
// comment), and Go's testing package forbids Setenv in a parallel test or
// one with parallel ancestors.
func TestCallRawPreservesOriginalSignature(t *testing.T) {
	peerID := fmt.Sprintf("callraw-test-%d", time.Now().UnixNano())
	dataDir := t.TempDir()
	registerTestNode(t, peerID, dataDir)

	originalPub, originalPriv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	req, err := shmevent.NewGetFieldByKey([]byte("some-key"))
	if err != nil {
		t.Fatalf("NewGetFieldByKey: %v", err)
	}
	req.SetId(7)
	encoded, err := shmevent.Encode(req, originalPriv)
	if err != nil {
		t.Fatalf("shmevent.Encode: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	serveErrCh := make(chan error, 1)
	go func() {
		serveErrCh <- Serve(ctx, peerID, dataDir, originalPriv, func(_ context.Context, m shmevent.Msg, crc uint32, sig []byte) shmevent.Msg {
			// The point of the test: verifying against originalPub must
			// succeed, proving CallRaw never substituted a different
			// signature -- a bug here would mean CallRaw quietly re-signed
			// (or mangled) the caller's already-signed bytes.
			if err := shmevent.Verify(originalPub, m, crc, sig); err != nil {
				errResp, encErr := shmevent.NewError(err.Error())
				if encErr != nil {
					// Only possible on a malformed message string; not
					// reachable with the fixed input this test uses.
					panic(encErr)
				}
				errResp.SetId(m.Id())
				return errResp
			}
			okResp, encErr := shmevent.NewGetFieldByKey([]byte("some-key"))
			if encErr == nil {
				encErr = okResp.GetFieldByKey().SetValue([]byte("ok"))
			}
			if encErr != nil {
				panic(encErr)
			}
			okResp.SetId(m.Id())
			return okResp
		})
	}()

	resp, err := CallRaw(ctx, peerID, encoded)
	if err != nil {
		t.Fatalf("CallRaw: %v", err)
	}
	if resp.Which() == shmevent.Event_Which_error {
		msg, _ := resp.Error().Message_()
		t.Fatalf("server rejected the request's original signature: %s", msg)
	}
	respValue, err := resp.GetFieldByKey().Value()
	if err != nil {
		t.Fatalf("resp.GetFieldByKey().Value(): %v", err)
	}
	if string(respValue) != "ok" {
		t.Fatalf("got response value %q, want %q", respValue, "ok")
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
