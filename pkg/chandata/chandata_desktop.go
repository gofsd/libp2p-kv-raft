//go:build !android

// See ipc.go's identical build-constraint comment for why GOOS=android
// needs excluding explicitly despite inheriting Go's "linux" build tag.
package chandata

import (
	"context"
	"fmt"
	"time"

	"github.com/gofsd/shmring"
)

// openRetryInterval bounds how often Open retries finding a ring Create
// hasn't produced yet. Short: unlike pkg/ipc's own equivalent constant,
// both sides of a data-plane ring set up back to back right after a
// channel's control-plane handshake completes (see shmevent.
// EventChannelDataReady's doc comment), so the normal case is finding it on
// the first or second attempt, not a long wait.
const openRetryInterval = 2 * time.Millisecond

func ringName(peerID, channelID string, dir Dir) string {
	return fmt.Sprintf("kvchan-%s-%s-%s", peerID, channelID, dir)
}

// Create creates the (peerID, channelID, dir) ring and returns the writer
// handle for it -- this side is that ring's producer for its whole
// lifetime, and (per ChunkWriter.CloseStorage's doc comment) the one
// responsible for eventually releasing its storage.
func Create(peerID, channelID string, dir Dir) (*ChunkWriter, error) {
	w, err := shmring.CreateShm(ringName(peerID, channelID, dir), Capacity)
	if err != nil {
		return nil, fmt.Errorf("chandata: create %s ring: %w", dir, err)
	}
	return &ChunkWriter{w: w}, nil
}

// Open opens the (peerID, channelID, dir) ring Create'd by the other side,
// retrying until it appears (bounded by ctx) or ctx is done first --
// Create on the other side may not have run yet the first time Open is
// attempted (see openRetryInterval's doc comment).
func Open(ctx context.Context, peerID, channelID string, dir Dir) (*ChunkReader, error) {
	name := ringName(peerID, channelID, dir)
	for {
		r, err := shmring.OpenShm(name, Capacity)
		if err == nil {
			return &ChunkReader{r: r}, nil
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("chandata: waiting for %s ring %s: %w", dir, name, ctx.Err())
		case <-time.After(openRetryInterval):
		}
	}
}
