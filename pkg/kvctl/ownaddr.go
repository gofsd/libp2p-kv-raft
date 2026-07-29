package kvctl

import (
	"context"
	"fmt"
)

// GetOwnAddr implements `mage getownaddr`: returns the current node's own
// current best-advertised multiaddr (see shmevent.EventGetOwnAddr's doc
// comment) -- the address to embed in a "<ownAddr>#<tokenHex>" ticket
// (printjoinrequestdatamatrix) or invite (printjoininvitedatamatrix/
// printexecinvitedatamatrix). Queried live, never cached: a node whose
// Config.RelayPeers reservation completed after startup only returns the
// up-to-date circuit address once that reservation has actually landed --
// call it again a moment later if an earlier call returned a private/
// loopback address instead.
func GetOwnAddr() (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), ipcTimeout)
	defer cancel()
	sess, _, err := openCurrentSession(ctx)
	if err != nil {
		return "", err
	}
	addr, err := sess.GetOwnAddr(ctx)
	if err != nil {
		return "", fmt.Errorf("get own addr: %w", err)
	}
	return addr, nil
}
