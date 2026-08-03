package kvmobile

import (
	"context"
	"encoding/json"
	"fmt"
)

// Version returns this device's own build/version info as a JSON string
// (shmevent.VersionInfo -- git commit, dirty flag, build time, Go version,
// go-libp2p version -- see shmevent.EventGetVersion's doc comment), the
// kvmobile counterpart to `mage version`. Queried live, never cached.
func Version() (string, error) {
	sess, err := currentSession()
	if err != nil {
		return "", err
	}

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	info, err := sess.GetVersion(ctx)
	if err != nil {
		return "", fmt.Errorf("kvmobile: get version: %w", err)
	}
	out, err := json.Marshal(info)
	if err != nil {
		return "", fmt.Errorf("kvmobile: encode version: %w", err)
	}
	return string(out), nil
}
