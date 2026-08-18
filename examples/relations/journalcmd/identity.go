package journalcmd

import (
	"context"
	"crypto/ed25519"
	"fmt"

	"github.com/gofsd/libp2p-kv-raft/examples/relations"
	"github.com/gofsd/libp2p-kv-raft/pkg/kvctl"
	"github.com/libp2p/go-libp2p/core/peer"
)

// Identity is what a submitter needs before it can sign anything: which
// actor entity the log knows it as, and the state a signature would have
// to be made against.
//
// The actor is not something a submitter chooses. It is derived from the
// peer id the request was authored under, whose Ed25519 public key is
// the peer id -- so a device can only ever sign as itself, and the
// binding between "who submitted" and "whose key" is made by the FSM
// authoring the request rather than by anything this service is told.
type Identity struct {
	// Actor is the entity to sign as, and Name the peer id it stands
	// for.
	Actor string `json:"actor"`
	Name  string `json:"name"`
	// Log is which book this is -- enough, with Page, for a submitter to
	// name the exact records the log will check without reading it.
	Log uint8 `json:"log"`
	// Page is the page being written, and Lines how many lines it holds
	// right now: what a sign-off signature has to say about it.
	Page  uint8 `json:"page"`
	Lines uint8 `json:"lines"`
}

// identity answers OpIdentity: it declares the submitter's actor if this
// is the first time the log has seen it, and reports the page state a
// signature would be made against.
func (s *Service) identity(ctx context.Context, req kvctl.CommandRequest) (Identity, error) {
	actor, err := s.actorFor(ctx, req.RequestedBy)
	if err != nil {
		return Identity{}, err
	}
	page, _, err := s.currentPage(ctx)
	if err != nil {
		return Identity{}, err
	}
	lines, err := s.Journal.Store().LastAllocated(ctx, page, relations.TypeEntry)
	if err != nil {
		return Identity{}, err
	}
	return Identity{
		Actor: actor.String(),
		Name:  req.RequestedBy,
		Log:   s.Journal.Store().Log,
		Page:  page,
		Lines: lines,
	}, nil
}

// actorFor resolves the actor a peer signs as, declaring it on first
// use with the key its own peer id carries.
//
// This is the whole of the enrolment story, and it is deliberately not a
// story: nothing is trusted from the request except the peer id, which
// the FSM already established by accepting the submission, and the key
// comes out of that peer id rather than out of anything the submitter
// says. So a device cannot enrol as somebody else, and cannot rebind an
// existing actor to a new key (relations.Actor refuses that outright).
func (s *Service) actorFor(ctx context.Context, peerID string) (relations.Entity, error) {
	if peerID == "" {
		return relations.Zero, fmt.Errorf("journalcmd: this request has no submitter to sign as")
	}
	pub, err := ed25519KeyOf(peerID)
	if err != nil {
		return relations.Zero, err
	}
	return s.Journal.Actor(ctx, peerID, pub)
}

// ed25519KeyOf extracts a peer id's own Ed25519 public key. Every
// identity in this project is Ed25519 (see pkg/shmevent's PrivateKey/
// PublicKey), and such a peer id embeds its key rather than hashing it,
// which is what makes this derivation possible at all.
func ed25519KeyOf(peerID string) (ed25519.PublicKey, error) {
	id, err := peer.Decode(peerID)
	if err != nil {
		return nil, fmt.Errorf("journalcmd: %q is not a peer id: %w", peerID, err)
	}
	pub, err := id.ExtractPublicKey()
	if err != nil {
		return nil, fmt.Errorf("journalcmd: %s carries no public key: %w", peerID, err)
	}
	raw, err := pub.Raw()
	if err != nil {
		return nil, fmt.Errorf("journalcmd: %s: %w", peerID, err)
	}
	if len(raw) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("journalcmd: %s is not an Ed25519 identity", peerID)
	}
	return ed25519.PublicKey(raw), nil
}

// signedLink decodes the record a submitter signed and checks it is
// theirs to submit.
//
// The signature itself is checked by the journal, against the key the
// actor was declared with. What is checked here is narrower and just as
// necessary: that the record is authored by *this* submitter's actor. A
// device may only hand over its own signature -- otherwise one could
// hold somebody else's and produce it at a moment of its own choosing.
func (s *Service) signedLink(ctx context.Context, req kvctl.CommandRequest, parsed Request) (relations.SignedLink, error) {
	if parsed.Signed == nil {
		return relations.SignedLink{}, fmt.Errorf("journalcmd: this operation is your own signature; sign the record and send it in \"signed\"")
	}
	link, err := parsed.Signed.Decode()
	if err != nil {
		return relations.SignedLink{}, err
	}
	actor, err := s.actorFor(ctx, req.RequestedBy)
	if err != nil {
		return relations.SignedLink{}, err
	}
	record, _, err := relations.DecodeRecord(link.Forward)
	if err != nil {
		return relations.SignedLink{}, err
	}
	if record.Author != actor {
		return relations.SignedLink{}, fmt.Errorf("journalcmd: this record is signed by %s, and you submit as %s -- you may only hand over your own signature",
			record.Author, actor)
	}
	return link, nil
}
