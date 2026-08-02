//go:build android

// See ipc_android.go's doc comment for why Android needs an entirely
// different rendezvous than desktop's name-based CreateShm/OpenShm:
// ASharedMemory has no OS-level naming at all, only fd handoff, which only
// works within one process -- exactly this project's Android build, where
// the daemon and its local caller are the same process (see the mobile
// package). This file plays the same role for the data plane that
// ipc_android.go's mailbox plays for request/response IPC, just simpler:
// unlike a Call/Serve round trip, a data-plane ring is created once and
// lives for the whole channel, so the fd only needs to be handed off once,
// not per message -- a small (name -> fd) registry with a poll-until-
// present Open is enough, no ack synchronization required.
package chandata

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/gofsd/shmring"
)

const openRetryInterval = 2 * time.Millisecond

var (
	registryMu sync.Mutex
	registry   = map[string]int{} // ring name -> fd, populated by Create
)

func ringName(peerID, channelID string, dir Dir) string {
	return fmt.Sprintf("kvchan-%s-%s-%s", peerID, channelID, dir)
}

// Create creates the (peerID, channelID, dir) ring over ASharedMemory and
// publishes its fd in the in-process registry for Open to find -- this
// side is that ring's producer for its whole lifetime, and (per
// ChunkWriter.CloseStorage's doc comment) the one responsible for
// eventually releasing its storage.
func Create(peerID, channelID string, dir Dir) (*ChunkWriter, error) {
	name := ringName(peerID, channelID, dir)
	w, fd, err := shmring.CreateAndroidSharedMemory(name, Capacity)
	if err != nil {
		return nil, fmt.Errorf("chandata: create %s ring: %w", dir, err)
	}
	registryMu.Lock()
	registry[name] = fd
	registryMu.Unlock()
	return &ChunkWriter{w: w}, nil
}

// Open opens the (peerID, channelID, dir) ring Create'd by the other side
// (in this same process), retrying until its fd is published (bounded by
// ctx) or ctx is done first.
func Open(ctx context.Context, peerID, channelID string, dir Dir) (*ChunkReader, error) {
	name := ringName(peerID, channelID, dir)
	for {
		registryMu.Lock()
		fd, ok := registry[name]
		registryMu.Unlock()
		if ok {
			r, err := shmring.OpenAndroidSharedMemory(fd, Capacity)
			if err != nil {
				return nil, fmt.Errorf("chandata: open %s ring %s: %w", dir, name, err)
			}
			return &ChunkReader{r: r}, nil
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("chandata: waiting for %s ring %s: %w", dir, name, ctx.Err())
		case <-time.After(openRetryInterval):
		}
	}
}
