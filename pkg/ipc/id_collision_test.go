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

// Two *different* requests that happen to draw the same message id must both be answered.
//
// Serve dedups a re-read of the request segment by comparing the decoded id against the last one
// it answered, which is right for the case it was written for -- the client has not torn the
// segment down yet, so the daemon sees the same bytes again. But ids come from shmclient.newID,
// a random uint16: draw the same value twice in a row and a genuinely new request is mistaken for
// that echo, skipped, and never answered. The caller then waits out its whole budget.
//
// **It passes, and it was written expecting it to fail.** The theory was that this explained the
// optical rig's cmd-inventoryDictionary stalls -- a list blocking for 10.001s, exactly kvctl's
// ipcTimeout rather than any amount of work, while the rest of the same sweep cost milliseconds.
// Sequentially it does not reproduce: the client tears its segment down before the next call, and
// Serve gets past the dedup. Kept anyway, because "two requests sharing an id are both answered"
// is a property worth holding on to whatever made it true, and because the next person reading
// that 10.001s should not spend the afternoon re-deriving this theory.
func TestTwoRequestsSharingAnIDAreBothAnswered(t *testing.T) {
	peerID := fmt.Sprintf("idcollision-test-%d", time.Now().UnixNano())
	dataDir := t.TempDir()
	registerTestNode(t, peerID, dataDir)

	_, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	served := make(chan error, 1)
	go func() {
		served <- Serve(ctx, peerID, dataDir, priv, func(_ context.Context, m shmevent.Msg, _ uint32, _ []byte) shmevent.Msg {
			resp, err := shmevent.NewGetFieldByKey([]byte("k"))
			if err != nil {
				panic(err)
			}
			if err := resp.GetFieldByKey().SetValue([]byte("answered")); err != nil {
				panic(err)
			}
			resp.SetId(m.Id())
			return resp
		})
	}()
	defer func() { cancel(); <-served }()

	const sharedID = 4242
	for attempt := 1; attempt <= 2; attempt++ {
		req, err := shmevent.NewGetFieldByKey([]byte("k"))
		if err != nil {
			t.Fatalf("NewGetFieldByKey: %v", err)
		}
		req.SetId(sharedID)

		callCtx, callCancel := context.WithTimeout(ctx, 5*time.Second)
		started := time.Now()
		_, err = Call(callCtx, peerID, req, priv)
		elapsed := time.Since(started)
		callCancel()

		if err != nil {
			t.Fatalf("request %d of 2 with id %d was never answered after %v: %v\n"+
				"a second request that draws the same random uint16 as the one before it is "+
				"mistaken for a re-read and skipped, and its caller waits out the whole budget",
				attempt, sharedID, elapsed, err)
		}
		if elapsed > 2*time.Second {
			t.Errorf("request %d took %v -- answered, but only after the dedup backoff", attempt, elapsed)
		}
	}
}
