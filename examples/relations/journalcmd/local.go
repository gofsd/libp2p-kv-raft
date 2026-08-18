package journalcmd

import (
	"context"
	"fmt"
	"time"

	"github.com/gofsd/libp2p-kv-raft/examples/relations"
)

// openTimeout bounds the reads and writes opening a journal performs.
const openTimeout = 20 * time.Second

// OpenLocalJournal opens log book `log` on the current node, writing as
// that node's own identity: the actor is the node's peer id, and the key
// is the node's own Ed25519 key, so a line this journal writes is signed
// by the same identity the rest of the cluster knows the node by.
//
// This is what a service running beside a daemon needs, and what
// `mage journalserve` uses. A device that only submits commands needs
// none of it -- it has no journal at all, which is the point.
//
// The one wrinkle is the first open of a fresh log: creating this node's
// actor is a write, and there is no actor yet to author it. That first
// declaration is written under the zero entity and immediately rewritten
// self-authored, which is the ordinary bootstrap any signing identity
// has -- somebody's first signature is always on their own name.
func OpenLocalJournal(log uint8) (*relations.Journal, error) {
	ctx, cancel := context.WithTimeout(context.Background(), openTimeout)
	defer cancel()

	signer, err := LocalSigner()
	if err != nil {
		return nil, err
	}
	pub, err := ed25519KeyOf(signer.PeerID)
	if err != nil {
		return nil, err
	}
	backend, err := relations.CurrentNode()
	if err != nil {
		return nil, err
	}

	store := relations.New(backend, log, relations.Zero, signer.Key)
	journal := relations.NewJournal(store)
	actor, err := journal.Actor(ctx, signer.PeerID, pub)
	if err != nil {
		return nil, err
	}
	store.Author = actor

	decl, found, err := store.Declaration(ctx, actor)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("journalcmd: actor %s went missing while opening the log", actor)
	}
	if decl.Record.Author != actor {
		if err := store.DeclareActor(ctx, actor, signer.PeerID, pub); err != nil {
			return nil, err
		}
	}
	return journal, nil
}
