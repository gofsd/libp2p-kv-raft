package kvctl

import (
	"context"
	"fmt"

	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// Version implements `mage version`: returns the current node's own
// build/version info (see shmevent.EventGetVersion's doc comment) -- git
// commit, dirty flag, build time, Go version, and the go-libp2p version it
// was built against. Queried live, never cached.
func Version() (shmevent.VersionInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), ipcTimeout)
	defer cancel()
	sess, _, err := openCurrentSession(ctx)
	if err != nil {
		return shmevent.VersionInfo{}, err
	}
	info, err := sess.GetVersion(ctx)
	if err != nil {
		return shmevent.VersionInfo{}, fmt.Errorf("get version: %w", err)
	}
	return info, nil
}
