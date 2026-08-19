package croncmd

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/gofsd/libp2p-kv-raft/pkg/registry"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmclient"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// Two Store implementations with identical semantics: memory, which
// excludes goroutines in one process, and node, which excludes every
// scheduler in the cluster because its Claim commits through raft. The
// scheduler's own logic is written against the interface and tested
// against the first, which is what makes the tests need no cluster.

// memoryStore is an in-process Store. It exists so a schedule can be
// worked out, and this package's own logic tested, without spawning a
// node -- not as a storage option anyone should ship: it is neither
// durable nor replicated, so its Claim only serialises schedulers sharing
// one process, which in production is exactly the case that does not
// matter.
type memoryStore struct {
	mu sync.Mutex
	kv map[string]string
}

// Memory returns an in-process Store. See memoryStore's own comment for
// what it is and is not for.
func Memory() Store { return &memoryStore{kv: make(map[string]string)} }

func (m *memoryStore) Get(ctx context.Context, key string) (string, bool, error) {
	if err := ctx.Err(); err != nil {
		return "", false, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	value, ok := m.kv[key]
	return value, ok, nil
}

func (m *memoryStore) Scan(ctx context.Context, prefix string) ([]Pair, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	var keys []string
	for k := range m.kv {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)

	pairs := make([]Pair, 0, len(keys))
	for _, k := range keys {
		pairs = append(pairs, Pair{Key: k, Value: m.kv[k]})
	}
	return pairs, nil
}

func (m *memoryStore) Put(ctx context.Context, key, value string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.kv[key] = value
	return nil
}

func (m *memoryStore) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.kv, key)
	return nil
}

// Claim is create-if-absent under the same lock that serves every other
// operation, which is this backend's whole equivalent of the raft-ordered
// compare the real one gets.
func (m *memoryStore) Claim(ctx context.Context, key, value string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.kv[key]; exists {
		return false, nil
	}
	m.kv[key] = value
	return true, nil
}

// nodeStore is the real Store: every write goes through the local daemon
// over shmring IPC and commits through raft, so a Claim is ordered against
// every other scheduler's Claim in the cluster.
type nodeStore struct {
	peerID string
	// resolve, when set, is asked for a session on every call instead of
	// one being opened and cached. See Sessions.
	resolve func(context.Context) (*shmclient.Session, error)

	// sess is opened lazily and reused: shmclient.Open fetches the node's
	// signing key over IPC, and paying that round trip on every tick would
	// dominate the cost of a scheduler that mostly finds nothing to do.
	mu   sync.Mutex
	sess *shmclient.Session
}

// Node returns a Store bound to the daemon running as peerID.
func Node(peerID string) Store { return &nodeStore{peerID: peerID} }

// Session returns a Store over a session somebody else already opened --
// what a caller with no registry needs, e.g. a process running its own
// in-process daemon (see mobile/kvmobile).
func Session(sess *shmclient.Session) Store { return &nodeStore{sess: sess} }

// Sessions is Session for a caller whose session comes and goes: resolve
// is asked on every call rather than once.
//
// That is the shape an in-process daemon actually has -- mobile/kvmobile's
// Stop tears the session down and a later Start makes a new one -- and a
// long-running scheduler must not be left holding the old one. Nothing
// here treats a resolve failure specially: it surfaces as an ordinary
// error from whichever call needed it, which a Scheduler reports and
// retries on the next tick.
func Sessions(resolve func(context.Context) (*shmclient.Session, error)) Store {
	return &nodeStore{resolve: resolve}
}

// CurrentNode returns a Store bound to whichever node `mage use` selected,
// so a scheduler driven from a shell behaves the way the rest of this
// repo's tooling does.
func CurrentNode() (Store, error) {
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

func (n *nodeStore) session(ctx context.Context) (*shmclient.Session, error) {
	// A resolver owns its session's lifetime, so this must not cache what
	// it returns -- that is the whole reason it is a function.
	if n.resolve != nil {
		return n.resolve(ctx)
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.sess != nil {
		return n.sess, nil
	}
	if n.peerID == "" {
		return nil, fmt.Errorf("croncmd: this store has neither a session nor a peer id")
	}
	sess, err := shmclient.Open(ctx, n.peerID)
	if err != nil {
		return nil, fmt.Errorf("croncmd: open node %s: %w", n.peerID, err)
	}
	n.sess = sess
	return sess, nil
}

// Get reads one key as a one-key range scan rather than through
// Session.Get, because "does this key exist" is the question here and a
// point read answers a missing key with an error that is not usefully
// distinguishable from a real one.
func (n *nodeStore) Get(ctx context.Context, key string) (string, bool, error) {
	sess, err := n.session(ctx)
	if err != nil {
		return "", false, err
	}
	found, err := sess.ScanRange(ctx, []byte(key), []byte(key), 1, 0, shmclient.RangeOrderAsc)
	if err != nil {
		return "", false, fmt.Errorf("croncmd: get %s: %w", key, err)
	}
	if len(found) == 0 {
		return "", false, nil
	}
	return string(found[0].Value), true, nil
}

func (n *nodeStore) Scan(ctx context.Context, prefix string) ([]Pair, error) {
	sess, err := n.session(ctx)
	if err != nil {
		return nil, err
	}
	found, err := sess.ScanRange(ctx, []byte(prefix), prefixEnd(prefix), 0, 0, shmclient.RangeOrderAsc)
	if err != nil {
		return nil, fmt.Errorf("croncmd: scan %s: %w", prefix, err)
	}

	pairs := make([]Pair, 0, len(found))
	for _, p := range found {
		key := string(p.Key)
		// ScanRange's upper bound is inclusive, so the computed end could
		// itself be a real key belonging to a different prefix. Checking
		// the prefix here rather than trusting the bound keeps that
		// impossible instead of merely unlikely.
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		pairs = append(pairs, Pair{Key: key, Value: string(p.Value)})
	}
	return pairs, nil
}

func (n *nodeStore) Put(ctx context.Context, key, value string) error {
	sess, err := n.session(ctx)
	if err != nil {
		return err
	}
	if err := sess.Set(ctx, key, value); err != nil {
		return fmt.Errorf("croncmd: put %s: %w", key, err)
	}
	return nil
}

// Delete goes through a one-op Txn because that is the only string-keyed
// delete this client exposes; there is no Session.Delete.
func (n *nodeStore) Delete(ctx context.Context, key string) error {
	sess, err := n.session(ctx)
	if err != nil {
		return err
	}
	ops := []shmevent.TxnOpSpec{{Op: shmevent.TxnOpDelete, Key: []byte(key)}}
	if err := sess.Txn(ctx, ops); err != nil {
		return fmt.Errorf("croncmd: delete %s: %w", key, err)
	}
	return nil
}

// Claim is the compare-and-swap the whole design rests on: absent=true
// means "write this only if nothing is there", evaluated inside kvfsm's
// Apply where raft has already decided the order. A refused claim comes
// back as (false, nil) -- somebody else won this fire -- not as an error.
func (n *nodeStore) Claim(ctx context.Context, key, value string) (bool, error) {
	sess, err := n.session(ctx)
	if err != nil {
		return false, err
	}
	claimed, err := sess.CompareAndSwap(ctx, key, "", value, true)
	if err != nil {
		return false, fmt.Errorf("croncmd: claim %s: %w", key, err)
	}
	return claimed, nil
}

// prefixEnd returns the last key a prefix scan should read: the prefix
// with its final byte incremented, which is the smallest key greater than
// every key starting with prefix. A prefix of all 0xFF bytes (or an empty
// one) has no such successor, and scanning to 0xFF... is then the honest
// answer.
func prefixEnd(prefix string) []byte {
	end := []byte(prefix)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] != 0xFF {
			end[i]++
			return end[:i+1]
		}
	}
	return []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
}
