package journalcmd

import (
	"context"
	"fmt"
	"time"

	"github.com/gofsd/libp2p-kv-raft/examples/relations"
	"github.com/gofsd/libp2p-kv-raft/pkg/registry"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmclient"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// The submitting side of the two operations that are somebody's own
// signature. Everything else in this package asks the log's node to
// write something; these produce the record here, signed with this
// device's own key, and hand over the bytes.
//
// The key never leaves: shmclient.GetPrivateKey reads the local node's
// Ed25519 identity over the same-machine IPC boundary pkg/shmevent's doc
// comment describes, and the signature is made in this process. What
// crosses the wire is a record nobody else could have produced and
// anybody can check.

// keyTimeout bounds fetching the local node's own key.
const keyTimeout = 10 * time.Second

// Signer is a device's own identity for signing: which peer it is, and
// the key it signs with.
type Signer struct {
	PeerID string
	Key    shmevent.PrivateKey
}

// LocalSigner returns the signer for the node this process is pointed at
// -- the same "current node" every other kvctl call resolves against.
func LocalSigner() (Signer, error) {
	reg, err := registry.Open()
	if err != nil {
		return Signer{}, err
	}
	peerID, err := reg.Current()
	if err != nil {
		return Signer{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), keyTimeout)
	defer cancel()
	key, err := shmclient.GetPrivateKey(ctx, peerID)
	if err != nil {
		return Signer{}, fmt.Errorf("journalcmd: read this node's signing key: %w", err)
	}
	return Signer{PeerID: peerID, Key: key}, nil
}

// Countersign endorses a line: it asks the log which actor this device
// signs as, signs the endorsement here, and submits it.
//
// The record that ends up in the log is this device's, signed with this
// device's key -- the node that owns the log checks it and writes it
// down, and could not have produced it. That is the difference between
// an endorsement and a note saying somebody endorsed something.
func (s Signer) Countersign(commandID, line string, timeout time.Duration) error {
	entry, err := relations.ParseEntity(line)
	if err != nil {
		return err
	}
	identity, err := s.identity(commandID, timeout)
	if err != nil {
		return err
	}
	actor, err := relations.ParseEntity(identity.Actor)
	if err != nil {
		return err
	}

	link, err := relations.SignLink(entry, actor, relations.KindCountersign, nil, actor, s.Key, time.Now())
	if err != nil {
		return err
	}
	signed := EncodeSignedLink(link)
	result, err := Do(commandID, Request{Op: OpCountersign, Line: line, Signed: &signed}, timeout)
	if err != nil {
		return err
	}
	if result.Error != "" {
		return fmt.Errorf("journalcmd: %s", result.Error)
	}
	return nil
}

// SignOffPage closes a page under this device's signature.
//
// The signature says how many lines the page held, which is what makes
// it a statement rather than a gesture -- and why a line landing between
// asking and signing invalidates it. The log refuses a stale one; sign
// again against the count it reports.
func (s Signer) SignOffPage(commandID string, page uint8, timeout time.Duration) error {
	identity, err := s.identity(commandID, timeout)
	if err != nil {
		return err
	}
	actor, err := relations.ParseEntity(identity.Actor)
	if err != nil {
		return err
	}
	if page == 0 {
		page = identity.Page
	}
	if page != identity.Page {
		return fmt.Errorf("journalcmd: this log is writing page %d, not page %d", identity.Page, page)
	}

	link, err := relations.SignLink(
		relations.PageEntityOf(identity.Log, page),
		relations.StatusMarkerOf(identity.Log),
		relations.KindPageSignoff,
		[]byte{identity.Lines},
		actor, s.Key, time.Now(),
	)
	if err != nil {
		return err
	}
	signed := EncodeSignedLink(link)
	result, err := Do(commandID, Request{Op: OpSignoff, Page: page, Signed: &signed}, timeout)
	if err != nil {
		return err
	}
	if result.Error != "" {
		return fmt.Errorf("journalcmd: %s", result.Error)
	}
	return nil
}

// Identity asks the log which actor this device signs as, declaring it
// there on first use.
func (s Signer) Identity(commandID string, timeout time.Duration) (Identity, error) {
	return s.identity(commandID, timeout)
}

func (s Signer) identity(commandID string, timeout time.Duration) (Identity, error) {
	result, err := Do(commandID, Request{Op: OpIdentity}, timeout)
	if err != nil {
		return Identity{}, err
	}
	if result.Error != "" {
		return Identity{}, fmt.Errorf("journalcmd: %s", result.Error)
	}
	if result.Identity == nil {
		return Identity{}, fmt.Errorf("journalcmd: %s answered with no identity", commandID)
	}
	if result.Identity.Name != s.PeerID {
		return Identity{}, fmt.Errorf("journalcmd: the log knows this submitter as %s, not %s -- signing as somebody else is not possible",
			result.Identity.Name, s.PeerID)
	}
	return *result.Identity, nil
}
