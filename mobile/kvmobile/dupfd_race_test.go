package kvmobile

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// TestCallsAcrossAFailedJoinNeverSeeADeadSegment is the regression for
// "ipc: open response shm: backend: dup fd N: bad file descriptor", which is what
// an ordinary call gets when the daemon holding its shm segments is gone: the
// call captured a session, something tore that daemon down, and the read then
// found a dead backend. It has been seen twice -- through two concurrent *starts*
// over one data directory (2026-08-19, fixed by startSeqMu) and on a phone whose
// remembered cluster named a destroyed peer, where Join stopped the daemon and
// left nothing running at all (2026-09-30, fixed by join's rollback).
//
// So this drives the shape rather than either cause: ordinary calls in flight
// while a join fails underneath them. What such a call may legitimately get is a
// transient -- a caller-lock timeout while the daemon is swapped, or "Start has
// not completed successfully yet" if it reads the flag mid-swap. What it must
// never get is a dead segment, and the node must be usable when the dust settles.
func TestCallsAcrossAFailedJoinNeverSeeADeadSegment(t *testing.T) {
	prevLeader := leaderMultiaddr
	leaderMultiaddr = ""
	t.Cleanup(func() {
		leaderMultiaddr = prevLeader
		if err := Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})

	dir := t.TempDir()
	soloPeerID, err := StartSolo(dir)
	if err != nil {
		t.Fatalf("StartSolo: %v", err)
	}

	var dupFd, transient, okCalls int64
	var sampleOnce sync.Once
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for j := 0; ; j++ {
				select {
				case <-stop:
					return
				default:
				}
				err := Submit(fmt.Sprintf("k%d-%d", n, j), "v")
				switch {
				case err == nil:
					atomic.AddInt64(&okCalls, 1)
				case strings.Contains(err.Error(), "dup fd"):
					atomic.AddInt64(&dupFd, 1)
					sampleOnce.Do(func() { t.Errorf("a call saw a dead segment: %v", err) })
				default:
					atomic.AddInt64(&transient, 1)
				}
			}
		}(i)
	}

	// One failing join is enough: it is the whole stop-and-start sequence, and the
	// workers above are inside it.
	if _, err := JoinAs(dir, unreachableLeaderAddr(t), "learner"); err == nil {
		t.Fatalf("JoinAs against an unreachable leader: want an error, got none")
	}
	close(stop)
	wg.Wait()
	t.Logf("calls: ok=%d transient=%d dup-fd=%d", okCalls, transient, dupFd)

	if dupFd != 0 {
		t.Fatalf("%d call(s) saw a dead segment", dupFd)
	}
	if got := PeerID(); got != soloPeerID {
		t.Fatalf("PeerID() = %q after the hammering, want %q", got, soloPeerID)
	}
	if err := Submit("after", "everything"); err != nil {
		t.Fatalf("Submit once it has settled: %v", err)
	}
}

// TestAStartDuringAJoinIsSerialised pins what startSeqMu buys on the join path,
// which until 2026-09-30 it did not cover: a start sequence arriving while a join
// is mid-swap waits for it rather than running a second daemon over the same data
// directory. The Android caller does exactly this on every launch -- StartSolo
// from AppContainer.init, and the remembered-cluster rejoin on its own scope
// immediately after.
func TestAStartDuringAJoinIsSerialised(t *testing.T) {
	prevLeader := leaderMultiaddr
	leaderMultiaddr = ""
	t.Cleanup(func() {
		leaderMultiaddr = prevLeader
		if err := Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})

	dir := t.TempDir()
	soloPeerID, err := StartSolo(dir)
	if err != nil {
		t.Fatalf("StartSolo: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Runs while the join below is between its Stop and its start. Serialised,
		// it either waits and finds a node already up or brings the solo one back;
		// unserialised, it starts a second daemon over the same store.
		if _, err := StartSolo(dir); err != nil {
			t.Errorf("StartSolo during a join: %v", err)
		}
	}()

	if _, err := JoinAs(dir, unreachableLeaderAddr(t), "learner"); err == nil {
		t.Fatalf("JoinAs against an unreachable leader: want an error, got none")
	}
	wg.Wait()

	if got := PeerID(); got != soloPeerID {
		t.Fatalf("PeerID() = %q, want the one identity this data directory has, %q", got, soloPeerID)
	}
	if err := Submit("serialised", "yes"); err != nil {
		t.Fatalf("Submit after a start raced a join: %v", err)
	}
}
