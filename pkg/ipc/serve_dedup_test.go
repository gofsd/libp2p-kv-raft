//go:build !android

package ipc

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/gofsd/shmring"

	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// echoHandler builds a Handler that answers a getFieldByKey request with
// "echo:"+key -- enough to tell, from the client side, exactly which
// request a given response actually answers (a stale/replayed response
// would carry a different key's echo).
func echoHandler() Handler {
	return func(_ context.Context, m shmevent.Msg, _ uint32, _ []byte) shmevent.Msg {
		key, err := m.GetFieldByKey().Key()
		if err != nil {
			panic(err)
		}
		resp, err := shmevent.NewGetFieldByKey(key)
		if err != nil {
			panic(err)
		}
		if err := resp.GetFieldByKey().SetValue(append([]byte("echo:"), key...)); err != nil {
			panic(err)
		}
		resp.SetId(m.Id())
		return resp
	}
}

// TestCallTwoSequentialRoundTrips issues two ordinary Calls back to back
// against one Serve loop -- the everyday case the package doc comment's
// "why the response channel is per request, not fixed per node" section
// describes a past silent-request-loss bug for. Serve loops back to
// openReqWithRetry the instant it finishes sending round 1's response,
// almost always before this test has finished reading that response and
// torn down round 1's request segment -- so in practice this already
// drives the dedup ("stale-segment-reread") branch on nearly every run,
// in addition to checking the properties that actually matter to a
// caller: each round gets its own, correctly-matched response.
func TestCallTwoSequentialRoundTrips(t *testing.T) {
	peerID := fmt.Sprintf("sequential-test-%d", time.Now().UnixNano())
	dataDir := t.TempDir()
	registerTestNode(t, peerID, dataDir)

	_, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	serveErrCh := make(chan error, 1)
	go func() {
		serveErrCh <- Serve(ctx, peerID, dataDir, priv, echoHandler())
	}()

	for i, key := range []string{"first", "second"} {
		req, err := shmevent.NewGetFieldByKey([]byte(key))
		if err != nil {
			t.Fatalf("NewGetFieldByKey: %v", err)
		}
		req.SetId(uint16(i + 1))
		resp, err := Call(ctx, peerID, req, priv)
		if err != nil {
			t.Fatalf("Call %d: %v", i, err)
		}
		val, err := resp.GetFieldByKey().Value()
		if err != nil {
			t.Fatalf("resp.Value %d: %v", i, err)
		}
		want := "echo:" + key
		if string(val) != want {
			t.Fatalf("round %d: got %q, want %q -- a stale/replayed response would surface here", i, val, want)
		}
	}

	cancel()
	<-serveErrCh
}

// TestServeDedupsReopenedRequestSegment forces, deterministically, the
// exact scenario the package doc comment describes Serve tolerating by
// design: looping back and reopening a request segment the client hasn't
// torn down yet. It drives the request channel by hand (the same
// unexported primitives Call itself uses) so it can hold the segment open
// across several of Serve's loop iterations before finally removing it --
// something a real Call, which tears the segment down immediately after
// reading its response, can't reliably stall long enough to guarantee.
// The property under test: rereading the same still-live segment must not
// re-invoke the handler (dedup by ID), and the next genuinely new request
// must still be picked up correctly once the stale segment is gone.
func TestServeDedupsReopenedRequestSegment(t *testing.T) {
	peerID := fmt.Sprintf("dedup-test-%d", time.Now().UnixNano())
	dataDir := t.TempDir()
	registerTestNode(t, peerID, dataDir)

	_, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	token, err := tokenForPeer(peerID)
	if err != nil {
		t.Fatalf("tokenForPeer: %v", err)
	}

	var mu sync.Mutex
	var handledIDs []uint16
	countingHandler := func(ctx context.Context, m shmevent.Msg, crc uint32, sig []byte) shmevent.Msg {
		mu.Lock()
		handledIDs = append(handledIDs, m.Id())
		mu.Unlock()
		return echoHandler()(ctx, m, crc, sig)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	serveErrCh := make(chan error, 1)
	go func() {
		serveErrCh <- Serve(ctx, peerID, dataDir, priv, countingHandler)
	}()

	rn := reqChannel(peerID, token)

	// Round 1, built and sent by hand (mirroring Call's own steps) so this
	// test -- not Call -- controls when the request segment finally goes
	// away.
	req1, err := shmevent.NewGetFieldByKey([]byte("round-1"))
	if err != nil {
		t.Fatalf("NewGetFieldByKey: %v", err)
	}
	req1.SetId(1)
	buf1, err := shmevent.Encode(req1, priv)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	w1, err := shmring.CreateShm(rn, capacity, shmring.WithPollInterval(minPoll, maxPoll))
	if err != nil {
		t.Fatalf("CreateShm request 1: %v", err)
	}
	if _, err := w1.WriteContext(ctx, buf1); err != nil {
		t.Fatalf("write request 1: %v", err)
	}
	if err := w1.Close(); err != nil {
		t.Fatalf("close request writer 1: %v", err)
	}

	r1, err := openRespWithRetry(ctx, peerID, token, 1)
	if err != nil {
		t.Fatalf("openRespWithRetry round 1: %v", err)
	}
	resp1Buf, err := readAll(ctx, r1)
	r1.Close()
	if err != nil {
		t.Fatalf("readAll response 1: %v", err)
	}
	resp1, _, _, err := shmevent.Decode(resp1Buf)
	if err != nil {
		t.Fatalf("decode response 1: %v", err)
	}
	if val, err := resp1.GetFieldByKey().Value(); err != nil || string(val) != "echo:round-1" {
		t.Fatalf("round 1 response = %q, err %v, want %q", val, err, "echo:round-1")
	}

	// Deliberately don't CloseStorage w1 yet -- simulate a client that's
	// slow to tear down its request segment after reading its response.
	// Give Serve many chances to loop back, reopen the still-live segment,
	// and rereread the identical bytes.
	time.Sleep(10 * openRetryInterval)

	mu.Lock()
	lingering := append([]uint16(nil), handledIDs...)
	mu.Unlock()
	if len(lingering) != 1 || lingering[0] != 1 {
		t.Fatalf("handler was invoked %v while request 1's segment was still live, want exactly [1] -- dedup should have suppressed every reread", lingering)
	}

	// Now simulate the client finally tearing down round 1 and issuing
	// round 2.
	w1.CloseStorage()

	req2, err := shmevent.NewGetFieldByKey([]byte("round-2"))
	if err != nil {
		t.Fatalf("NewGetFieldByKey: %v", err)
	}
	req2.SetId(2)
	buf2, err := shmevent.Encode(req2, priv)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	w2, err := shmring.CreateShm(rn, capacity, shmring.WithPollInterval(minPoll, maxPoll))
	if err != nil {
		t.Fatalf("CreateShm request 2: %v", err)
	}
	if _, err := w2.WriteContext(ctx, buf2); err != nil {
		t.Fatalf("write request 2: %v", err)
	}
	if err := w2.Close(); err != nil {
		t.Fatalf("close request writer 2: %v", err)
	}

	r2, err := openRespWithRetry(ctx, peerID, token, 2)
	if err != nil {
		t.Fatalf("openRespWithRetry round 2: %v", err)
	}
	resp2Buf, err := readAll(ctx, r2)
	r2.Close()
	w2.CloseStorage()
	if err != nil {
		t.Fatalf("readAll response 2: %v", err)
	}
	resp2, _, _, err := shmevent.Decode(resp2Buf)
	if err != nil {
		t.Fatalf("decode response 2: %v", err)
	}
	if val, err := resp2.GetFieldByKey().Value(); err != nil || string(val) != "echo:round-2" {
		t.Fatalf("round 2 response = %q, err %v, want %q", val, err, "echo:round-2")
	}

	mu.Lock()
	final := append([]uint16(nil), handledIDs...)
	mu.Unlock()
	if len(final) != 2 || final[0] != 1 || final[1] != 2 {
		t.Fatalf("handler was invoked for ids %v across the whole test, want exactly [1 2]", final)
	}

	cancel()
	<-serveErrCh
}

// TestCallSerializesConcurrentCallers exercises callerLock: several
// goroutines Call the same peerID at once from within one process. Call's
// own package doc comment says this must serialize into a safe queue
// rather than race to create/write/tear down the same fixed-name request
// segment -- if the lock were missing or scoped wrong, this test's
// concurrent callers would corrupt each other's requests/responses or
// hang, not just run slowly.
func TestCallSerializesConcurrentCallers(t *testing.T) {
	peerID := fmt.Sprintf("concurrent-test-%d", time.Now().UnixNano())
	dataDir := t.TempDir()
	registerTestNode(t, peerID, dataDir)

	_, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	serveErrCh := make(chan error, 1)
	go func() {
		serveErrCh <- Serve(ctx, peerID, dataDir, priv, echoHandler())
	}()

	const n = 8
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("caller-%d", i)
			req, err := shmevent.NewGetFieldByKey([]byte(key))
			if err != nil {
				errs[i] = err
				return
			}
			req.SetId(uint16(100 + i))
			resp, err := Call(ctx, peerID, req, priv)
			if err != nil {
				errs[i] = err
				return
			}
			val, err := resp.GetFieldByKey().Value()
			if err != nil {
				errs[i] = err
				return
			}
			if want := "echo:" + key; string(val) != want {
				errs[i] = fmt.Errorf("got %q, want %q -- another caller's response was received instead", val, want)
			}
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("caller %d: %v", i, err)
		}
	}

	cancel()
	<-serveErrCh
}
