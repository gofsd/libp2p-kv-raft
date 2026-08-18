package relations

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/gofsd/libp2p-kv-raft/pkg/registry"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmclient"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// node is the real Backend: every write is one shmevent txn applied
// through raft on the local daemon, every read one ascending listRange
// walk of the local replica.
//
// It talks to pkg/shmclient directly rather than to pkg/kvctl, for one
// concrete reason: kvctl.Txn takes its ops as a *space-separated string*
// (`k=v del:k if:k=v`), which cannot express a key with a 0x00 byte in
// it, and every key this package writes is nine raw bytes. kvctl.Set/
// Get/RangeScan would have been fine -- they pass []byte(key) straight
// through, and capnp carries both key and value as Data -- but the
// atomic pair-write Store.Link depends on has no string-free kvctl
// entry point today. See the README's "Improving this" section.
type node struct {
	peerID string

	// sess is opened lazily and reused: shmclient.Open fetches the
	// node's signing key over IPC, and paying that round trip once per
	// Apply/Scan would double the cost of every single relation write.
	mu   sync.Mutex
	sess *shmclient.Session
}

// Node returns a Backend bound to the daemon running as peerID.
func Node(peerID string) Backend { return &node{peerID: peerID} }

// Session returns a Backend over a session somebody else already opened.
//
// This is what a caller with no registry needs: mobile/kvmobile runs its
// daemon in the same process as the app and holds one session for its
// lifetime, so there is nothing to resolve a peer id against and no
// second session worth opening.
func Session(sess *shmclient.Session) Backend { return &node{sess: sess} }

// CurrentNode returns a Backend bound to whichever node `mage use`
// selected -- the same "current node" pkg/kvctl's own Set/Get resolve
// against, so an example driven from a shell behaves the way the rest of
// this repo's tooling does.
func CurrentNode() (Backend, error) {
	reg, err := registry.Open()
	if err != nil {
		return nil, err
	}
	peerID, err := reg.Current()
	if err != nil {
		return nil, err
	}
	return Node(peerID), nil
}

func (n *node) session(ctx context.Context) (*shmclient.Session, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.sess != nil {
		return n.sess, nil
	}
	if n.peerID == "" {
		return nil, fmt.Errorf("relations: this backend has neither a session nor a peer id")
	}
	sess, err := shmclient.Open(ctx, n.peerID)
	if err != nil {
		return nil, fmt.Errorf("relations: open node %s: %w", n.peerID, err)
	}
	n.sess = sess
	return sess, nil
}

func (n *node) Apply(ctx context.Context, ops []Op) error {
	sess, err := n.session(ctx)
	if err != nil {
		return err
	}
	specs := make([]shmevent.TxnOpSpec, 0, len(ops))
	for _, op := range ops {
		var kind byte
		switch op.Kind {
		case OpSet:
			kind = shmevent.TxnOpSet
		case OpDelete:
			kind = shmevent.TxnOpDelete
		case OpCompare:
			kind = shmevent.TxnOpCompare
		case OpCompareAbsent:
			kind = shmevent.TxnOpCompareAbsent
		default:
			return fmt.Errorf("relations: unknown op kind %d", op.Kind)
		}
		specs = append(specs, shmevent.TxnOpSpec{Op: kind, Key: op.Key, Value: op.Value})
	}
	if err := sess.Txn(ctx, specs); err != nil {
		// A failed precondition is the one outcome a caller retries
		// rather than reports, so it gets this package's own sentinel --
		// see Store.Allocate, the only caller that relies on telling the
		// two apart.
		if errors.Is(err, shmclient.ErrCompareFailed) {
			return fmt.Errorf("%w: %s", ErrConflict, err)
		}
		return err
	}
	return nil
}

func (n *node) Scan(ctx context.Context, start, end []byte, limit int) ([]Pair, error) {
	sess, err := n.session(ctx)
	if err != nil {
		return nil, err
	}
	found, err := sess.ScanRange(ctx, start, end, limit, 0, shmclient.RangeOrderAsc)
	if err != nil {
		return nil, err
	}
	pairs := make([]Pair, 0, len(found))
	for _, p := range found {
		pairs = append(pairs, Pair{Key: p.Key, Value: p.Value})
	}
	return pairs, nil
}
