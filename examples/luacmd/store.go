package luacmd

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/gofsd/libp2p-kv-raft/pkg/registry"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmclient"
)

// Two Journal implementations with identical semantics: memory, which
// keeps records in one process, and node, which writes them through the
// local daemon so they replicate through raft. The script catalog itself
// (script.go) is written against the interface and tested against the
// first, which is what makes those tests need no cluster -- the same split
// examples/croncmd draws between its own memory and node stores, for the
// same reason.

// Pair is one stored record: the raw pkg/store key pkg/logrecord.BuildKey
// produced for it, and the encoded logrecord.Record it holds. Raw bytes
// rather than a decoded Record, so that a Journal implementation stays a
// dumb byte store and every decision about what a record *means* stays in
// script.go, where it can be tested without one.
type Pair struct {
	Key   []byte
	Value []byte
}

// Journal is the append-only record store scripts live in: pkg/logrecord
// over pkg/store, reached through whatever client the caller already has.
//
// Deliberately two methods. Everything this package does to a script --
// write a revision, read the latest, list them, replay the history -- is
// an append plus an ordered range read, because that is all an append-only
// log offers. There is no update and no delete here for the same reason
// there is none in the journal itself: a superseding revision is how both
// are expressed (see Catalog.Delete).
type Journal interface {
	// Append writes one record. Keys are unique by construction
	// (BuildKey stamps a timestamp and a random tiebreaker), so an
	// implementation never has to decide what an overwrite means.
	Append(ctx context.Context, key, value []byte) error
	// Range returns every pair in [lo, hi] -- both bounds inclusive, the
	// same convention pkg/logrecord.ScanBounds and
	// pkg/shmclient.Session.ScanRange already use -- in ascending key
	// order, which for these keys is chronological order within one
	// script.
	Range(ctx context.Context, lo, hi []byte) ([]Pair, error)
}

// memoryJournal is an in-process Journal. It exists so a script can be
// written and this package's own logic tested without a running node --
// not as a storage option anyone should ship: nothing here is durable or
// replicated, so a script stored in one is invisible to the device that
// would actually run it.
type memoryJournal struct {
	mu    sync.Mutex
	pairs map[string][]byte
}

// Memory returns an in-process Journal. See memoryJournal's own comment
// for what it is and is not for.
func Memory() Journal { return &memoryJournal{pairs: make(map[string][]byte)} }

func (m *memoryJournal) Append(ctx context.Context, key, value []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pairs[string(key)] = append([]byte{}, value...)
	return nil
}

// Range sorts on every call rather than keeping an ordered structure: the
// point of this implementation is to be obviously correct against the real
// one, and these ranges hold a script's revisions, not a workload.
func (m *memoryJournal) Range(ctx context.Context, lo, hi []byte) ([]Pair, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	var keys []string
	for k := range m.pairs {
		key := []byte(k)
		if bytes.Compare(key, lo) >= 0 && bytes.Compare(key, hi) <= 0 {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)

	pairs := make([]Pair, 0, len(keys))
	for _, k := range keys {
		pairs = append(pairs, Pair{
			Key:   []byte(k),
			Value: append([]byte{}, m.pairs[k]...),
		})
	}
	return pairs, nil
}

// nodeJournal is the real Journal: every append goes through the local
// daemon over shmring IPC and commits through raft, so a script written on
// one node is readable on every other one -- which is the entire point,
// since the device that runs a script is generally not the device somebody
// typed it on.
type nodeJournal struct {
	peerID string
	// resolve, when set, is asked for a session on every call instead of
	// one being opened and cached. See Sessions.
	resolve func(context.Context) (*shmclient.Session, error)

	// sess is opened lazily and reused: shmclient.Open fetches the node's
	// signing key over IPC, and paying that round trip per call would
	// dominate the cost of, say, listing scripts.
	mu   sync.Mutex
	sess *shmclient.Session
}

// Node returns a Journal bound to the daemon running as peerID.
func Node(peerID string) Journal { return &nodeJournal{peerID: peerID} }

// Session returns a Journal over a session somebody else already opened --
// what a caller with no registry needs, e.g. a process running its own
// in-process daemon (see mobile/kvmobile).
func Session(sess *shmclient.Session) Journal { return &nodeJournal{sess: sess} }

// Sessions is Session for a caller whose session comes and goes: resolve
// is asked on every call rather than once.
//
// That is the shape an in-process daemon actually has -- mobile/kvmobile's
// Stop tears the session down and a later Start makes a new one -- and a
// long-running runner (see the Phase 3 runner, which holds one of these
// for its whole life) must not be left holding the old one. A resolve
// failure surfaces as an ordinary error from whichever call needed it.
func Sessions(resolve func(context.Context) (*shmclient.Session, error)) Journal {
	return &nodeJournal{resolve: resolve}
}

// CurrentNode returns a Journal bound to whichever node `mage use`
// selected, alongside that node's peer id -- which callers need separately
// as the author of everything they write (see NewCatalog), since a
// shmclient.Session doesn't expose the identity it was opened for.
func CurrentNode() (Journal, string, error) {
	reg, err := registry.Open()
	if err != nil {
		return nil, "", err
	}
	peerID, err := reg.Current()
	if err != nil {
		return nil, "", err
	}
	return Node(peerID), peerID, nil
}

func (n *nodeJournal) session(ctx context.Context) (*shmclient.Session, error) {
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
		return nil, fmt.Errorf("luacmd: this journal has neither a session nor a peer id")
	}
	sess, err := shmclient.Open(ctx, n.peerID)
	if err != nil {
		return nil, fmt.Errorf("luacmd: open node %s: %w", n.peerID, err)
	}
	n.sess = sess
	return sess, nil
}

func (n *nodeJournal) Append(ctx context.Context, key, value []byte) error {
	sess, err := n.session(ctx)
	if err != nil {
		return err
	}
	if err := sess.LogAppend(ctx, key, value); err != nil {
		return fmt.Errorf("luacmd: append record: %w", err)
	}
	return nil
}

// Range reads the whole range, unpaginated. ScanRange is itself a cursor
// walk issuing one round trip per pair (see its doc comment), so this
// costs what the equivalent hand-written ListRange loop elsewhere in this
// repo costs and carries no "the whole answer must fit in one message"
// risk.
func (n *nodeJournal) Range(ctx context.Context, lo, hi []byte) ([]Pair, error) {
	sess, err := n.session(ctx)
	if err != nil {
		return nil, err
	}
	found, err := sess.ScanRange(ctx, lo, hi, 0, 0, shmclient.RangeOrderAsc)
	if err != nil {
		return nil, fmt.Errorf("luacmd: scan records: %w", err)
	}
	pairs := make([]Pair, 0, len(found))
	for _, p := range found {
		pairs = append(pairs, Pair{Key: p.Key, Value: p.Value})
	}
	return pairs, nil
}
