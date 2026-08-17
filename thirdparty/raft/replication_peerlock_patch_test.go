// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"sync"
	"testing"
)

// TestUpdateLastAppendedReadsPeerUnderLock is a LOCAL PATCH regression test
// (see this repo's README, "Vendored dependency patch"). followerReplication's
// 'peer' is documented as protected by peerLock, and startStopReplication takes
// the write lock to swap it whenever a follower's address changes -- but
// updateLastAppended used to read s.peer.ID with no lock at all, which is a
// data race, and one that feeds a torn ServerID into commitment.match, the
// call that advances the leader's commit index.
//
// This models exactly that pair: one goroutine swapping the address the way
// startStopReplication does, another calling updateLastAppended. It fails
// under `go test -race` against unpatched upstream (v1.7.3 and main alike) and
// passes with getPeer() in place. Without -race it proves nothing, so it is
// deliberately written to be cheap enough to always leave enabled.
func TestUpdateLastAppendedReadsPeerUnderLock(t *testing.T) {
	const id = ServerID("server-1")

	s := &followerReplication{
		peer:       Server{ID: id, Address: ServerAddress("addr-0")},
		commitment: newCommitment(make(chan struct{}, 1), Configuration{Servers: []Server{{Suffrage: Voter, ID: id, Address: ServerAddress("addr-0")}}}, 1),
		notify:     make(map[*verifyFuture]struct{}),
	}

	req := &AppendEntriesRequest{Entries: []*Log{{Index: 1, Term: 1}}}

	var wg sync.WaitGroup
	wg.Add(2)

	// Mirrors startStopReplication: the address changes, the ID does not.
	go func() {
		defer wg.Done()
		for i := 0; i < 2000; i++ {
			s.peerLock.Lock()
			s.peer = Server{ID: id, Address: ServerAddress("addr-1")}
			s.peerLock.Unlock()
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < 2000; i++ {
			updateLastAppended(s, req)
		}
	}()

	wg.Wait()
}
