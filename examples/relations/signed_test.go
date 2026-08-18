package relations_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"strings"
	"testing"
	"time"

	"github.com/gofsd/libp2p-kv-raft/examples/relations"
)

// device is somebody who signs but does not write: it holds a key and an
// actor entity in a log it has no access to, which is the whole point of
// the signed path.
type device struct {
	actor relations.Entity
	pub   ed25519.PublicKey
	priv  ed25519.PrivateKey
}

func newDevice(t *testing.T, ctx context.Context, j *relations.Journal, name string) device {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	actor, err := j.Actor(ctx, name, pub)
	if err != nil {
		t.Fatalf("Actor(%s): %v", name, err)
	}
	return device{actor: actor, pub: pub, priv: priv}
}

// TestCountersignWithADeviceSignature is the endorsement that means
// something when somebody else does the writing: the record is the
// endorser's, signed with the endorser's key, and the log merely checks
// it and writes it down.
func TestCountersignWithADeviceSignature(t *testing.T) {
	ctx := context.Background()
	st, _, _ := newStore(t)
	j := relations.NewJournal(st)
	entries, _ := writeShiftLog(t, j)
	line := entries[0]

	supervisor := newDevice(t, ctx, j, "Petrov")
	link, err := relations.SignLink(line, supervisor.actor, relations.KindCountersign, nil,
		supervisor.actor, supervisor.priv, time.Now())
	if err != nil {
		t.Fatalf("SignLink: %v", err)
	}
	if err := j.CountersignWith(ctx, line, link); err != nil {
		t.Fatalf("CountersignWith: %v", err)
	}

	// It reads back as the supervisor's endorsement, and verifies against
	// the supervisor's own declared key -- not the log node's.
	signatures, err := j.Countersignatures(ctx, line)
	if err != nil {
		t.Fatalf("Countersignatures: %v", err)
	}
	if len(signatures) != 1 || signatures[0].Actor != supervisor.actor || signatures[0].Name != "Petrov" {
		t.Fatalf("countersignatures = %+v, want one by Petrov", signatures)
	}
	rel, found, err := st.Lookup(ctx, line, supervisor.actor)
	if err != nil || !found {
		t.Fatalf("Lookup = %v, %v", found, err)
	}
	if rel.Record.Author != supervisor.actor {
		t.Fatalf("the endorsement is authored by %s, want the supervisor", rel.Record.Author)
	}
	if err := st.Verify(ctx, rel); err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if _, err := j.VerifyChain(ctx); err != nil {
		t.Fatalf("VerifyChain: %v", err)
	}

	// The rules do not change because the signature came from elsewhere.
	again, err := relations.SignLink(line, supervisor.actor, relations.KindCountersign, nil,
		supervisor.actor, supervisor.priv, time.Now())
	if err != nil {
		t.Fatalf("SignLink: %v", err)
	}
	if err := j.CountersignWith(ctx, line, again); err == nil {
		t.Fatal("a second endorsement by the same actor was accepted")
	}
}

// TestSignedRecordsAreCheckedNotTrusted is the other half: everything a
// submitter could get wrong or lie about is refused.
func TestSignedRecordsAreCheckedNotTrusted(t *testing.T) {
	ctx := context.Background()
	st, _, _ := newStore(t)
	j := relations.NewJournal(st)
	entries, _ := writeShiftLog(t, j)
	line := entries[0]

	supervisor := newDevice(t, ctx, j, "Petrov")
	impostor := newDevice(t, ctx, j, "Nobody")

	t.Run("signed with the wrong key", func(t *testing.T) {
		// Claims to be the supervisor, signs with its own key.
		link, err := relations.SignLink(line, supervisor.actor, relations.KindCountersign, nil,
			supervisor.actor, impostor.priv, time.Now())
		if err != nil {
			t.Fatalf("SignLink: %v", err)
		}
		if err := j.CountersignWith(ctx, line, link); err == nil {
			t.Fatal("an endorsement signed with the wrong key was accepted")
		}
	})

	t.Run("authored by somebody else", func(t *testing.T) {
		// Correctly signed by the impostor, but filed under the
		// supervisor's endorsement key.
		link, err := relations.SignLink(line, supervisor.actor, relations.KindCountersign, nil,
			impostor.actor, impostor.priv, time.Now())
		if err != nil {
			t.Fatalf("SignLink: %v", err)
		}
		if err := j.CountersignWith(ctx, line, link); err == nil {
			t.Fatal("an endorsement authored by one actor and filed under another was accepted")
		}
	})

	t.Run("for a different line", func(t *testing.T) {
		link, err := relations.SignLink(entries[1], supervisor.actor, relations.KindCountersign, nil,
			supervisor.actor, supervisor.priv, time.Now())
		if err != nil {
			t.Fatalf("SignLink: %v", err)
		}
		if err := j.CountersignWith(ctx, line, link); err == nil {
			t.Fatal("an endorsement of a different line was accepted")
		}
	})

	t.Run("the wrong kind of record", func(t *testing.T) {
		link, err := relations.SignLink(line, supervisor.actor, relations.KindCell, nil,
			supervisor.actor, supervisor.priv, time.Now())
		if err != nil {
			t.Fatalf("SignLink: %v", err)
		}
		if err := j.CountersignWith(ctx, line, link); err == nil {
			t.Fatal("a record that is not an endorsement was accepted as one")
		}
	})

	t.Run("halves that disagree", func(t *testing.T) {
		good, err := relations.SignLink(line, supervisor.actor, relations.KindCountersign, nil,
			supervisor.actor, supervisor.priv, time.Now())
		if err != nil {
			t.Fatalf("SignLink: %v", err)
		}
		other, err := relations.SignLink(line, supervisor.actor, relations.KindCountersign, []byte("extra"),
			supervisor.actor, supervisor.priv, time.Now())
		if err != nil {
			t.Fatalf("SignLink: %v", err)
		}
		good.Index = other.Index
		if err := j.CountersignWith(ctx, line, good); err == nil {
			t.Fatal("a link whose two directions are different records was accepted")
		}
	})

	t.Run("an actor nobody declared", func(t *testing.T) {
		pub, priv, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatalf("GenerateKey: %v", err)
		}
		_ = pub
		stranger := relations.Entity{Log: testLog, Page: relations.SchemaPage, Type: relations.TypeActor, ID: 0xFE}
		link, err := relations.SignLink(line, stranger, relations.KindCountersign, nil, stranger, priv, time.Now())
		if err != nil {
			t.Fatalf("SignLink: %v", err)
		}
		if err := j.CountersignWith(ctx, line, link); err == nil {
			t.Fatal("an endorsement by an undeclared actor was accepted")
		}
	})
}

// TestSignOffPageWithADeviceSignature covers the same path for closing a
// page, including the count the signature commits to.
func TestSignOffPageWithADeviceSignature(t *testing.T) {
	ctx := context.Background()
	st, _, _ := newStore(t)
	j := relations.NewJournal(st)
	entries, fields := writeShiftLog(t, j)

	supervisor := newDevice(t, ctx, j, "Petrov")
	sign := func(lines uint8) relations.SignedLink {
		t.Helper()
		link, err := relations.SignLink(
			relations.PageEntityOf(testLog, relations.FirstEntryPage),
			relations.StatusMarkerOf(testLog),
			relations.KindPageSignoff, []byte{lines},
			supervisor.actor, supervisor.priv, time.Now())
		if err != nil {
			t.Fatalf("SignLink: %v", err)
		}
		return link
	}

	// A signature for a page that has since grown is stale, and saying so
	// is the point: it was made about a page that no longer exists.
	if err := j.SignOffPageWith(ctx, relations.FirstEntryPage, sign(uint8(len(entries))-1)); err == nil {
		t.Fatal("a sign-off for the wrong number of lines was accepted")
	} else if !strings.Contains(err.Error(), "sign it again") {
		t.Fatalf("refusal = %v", err)
	}

	if err := j.SignOffPageWith(ctx, relations.FirstEntryPage, sign(uint8(len(entries)))); err != nil {
		t.Fatalf("SignOffPageWith: %v", err)
	}
	signoff, found, err := j.PageStatus(ctx, relations.FirstEntryPage)
	if err != nil || !found {
		t.Fatalf("PageStatus = %v, %v", found, err)
	}
	if signoff.By != supervisor.actor || signoff.Name != "Petrov" {
		t.Fatalf("the page was closed by %s (%q), want the supervisor", signoff.By, signoff.Name)
	}
	if signoff.Lines != uint8(len(entries)) {
		t.Fatalf("the sign-off records %d lines, want %d", signoff.Lines, len(entries))
	}

	// The page really is closed: the next line rolls onto the next one.
	next, err := j.Append(ctx, relations.TermCell(fields[fieldResult], "OK"))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if next.Page != relations.FirstEntryPage+1 {
		t.Fatalf("the line after a signed sign-off landed at %s", next)
	}
	if _, err := j.VerifyChain(ctx); err != nil {
		t.Fatalf("VerifyChain: %v", err)
	}
}

// TestActorBindsANameToAKey checks the registry a signature is checked
// against cannot be quietly rebound.
func TestActorBindsANameToAKey(t *testing.T) {
	ctx := context.Background()
	st, _, _ := newStore(t)
	j := relations.NewJournal(st)

	first := newDevice(t, ctx, j, "Petrov")
	again, err := j.Actor(ctx, "Petrov", first.pub)
	if err != nil {
		t.Fatalf("Actor again: %v", err)
	}
	if again != first.actor {
		t.Fatalf("the same name resolved to %s and %s", first.actor, again)
	}

	other, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	if _, err := j.Actor(ctx, "Petrov", other); err == nil {
		t.Fatal("an actor was rebound to a different key")
	}
	if _, err := j.Actor(ctx, "", first.pub); err == nil {
		t.Fatal("expected an error declaring an actor with no name")
	}
	if _, err := j.Actor(ctx, "Short", []byte("not a key")); err == nil {
		t.Fatal("expected an error declaring an actor with a malformed key")
	}
}
