package kvfsm

import (
	"path/filepath"
	"testing"

	"github.com/hashicorp/raft"

	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
	"github.com/gofsd/libp2p-kv-raft/pkg/store"
)

// Apply once rejected a second Command record naming a peerID some other
// command already targeted. That rule is gone (see the note where it used to
// live, above OpType), and this pins its absence rather than leaving the
// change untested: a cluster has to be able to define more commands than it
// has devices -- twenty operations across five stations is the ordinary case,
// not an edge one -- and under the old rule the catalog could never hold more
// entries than the cluster had peers.
func TestApplyOpSetAllowsManyCommandsPerPeer(t *testing.T) {
	s, err := store.Open(filepath.Join(t.TempDir(), "sqlite"))
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer s.Close()

	f := New(s)
	apply := func(key, value []byte) error {
		res, ok := f.Apply(&raft.Log{Data: EncodeCommand(OpSet, key, value)}).(ApplyResult)
		if !ok {
			t.Fatal("Apply did not return ApplyResult")
		}
		return res.Err
	}
	payload := func(name, peerID string) []byte {
		v, err := shmevent.EncodeCommandPayload(name, []byte(peerID))
		if err != nil {
			t.Fatalf("EncodeCommandPayload: %v", err)
		}
		return v
	}

	// Every command in a realistic catalog targets the same station.
	for _, cmd := range []struct{ id, name string }{
		{"inspect", "Inspect"},
		{"assemble", "Assemble"},
		{"pack", "Pack"},
	} {
		if err := apply(shmevent.CommandKey([]byte(cmd.id)), payload(cmd.name, "peer1")); err != nil {
			t.Fatalf("Apply OpSet %s: %v", cmd.id, err)
		}
	}
	for _, id := range []string{"inspect", "assemble", "pack"} {
		if _, err := s.Get(shmevent.CommandKey([]byte(id))); err != nil {
			t.Fatalf("command %s was not written: %v", id, err)
		}
	}

	// A command with no target peer at all is also allowed: a definition can
	// exist before anyone decides which station runs it. Dispatch rejects it
	// at submit time (see pkg/kvctl's SubmitCommand), which is the layer that
	// knows the difference between "not bound yet" and "broken".
	if err := apply(shmevent.CommandKey([]byte("unbound")), payload("Unbound", "")); err != nil {
		t.Fatalf("Apply OpSet unbound: %v", err)
	}

	// A plain, non-Command key (e.g. a Group record) is unaffected either way.
	grpKey := shmevent.GroupKey([]byte("grp-1"))
	if err := apply(grpKey, shmevent.EncodeGroupPayload("peer1", false)); err != nil {
		t.Fatalf("Apply OpSet grp-1 sharing a name with a peerID: %v", err)
	}
}

// A command's spec rides in the record's own value (see
// shmevent.EncodeCommandPayloadWithSpec) and has to survive a round trip
// through Apply untouched -- the FSM stores it opaquely and must not parse,
// normalize or truncate what a client will later render a form from.
func TestApplyOpSetPreservesCommandSpec(t *testing.T) {
	s, err := store.Open(filepath.Join(t.TempDir(), "sqlite"))
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer s.Close()

	f := New(s)
	spec := []byte(`{"fields":[{"name":"serial","kind":"scan","required":true}]}`)
	value, err := shmevent.EncodeCommandPayloadWithSpec("Inspect", []byte("peer1"), spec)
	if err != nil {
		t.Fatalf("EncodeCommandPayloadWithSpec: %v", err)
	}
	key := shmevent.CommandKey([]byte("inspect"))
	res, _ := f.Apply(&raft.Log{Data: EncodeCommand(OpSet, key, value)}).(ApplyResult)
	if res.Err != nil {
		t.Fatalf("Apply: %v", res.Err)
	}

	stored, err := s.Get(key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	name, peerID, gotSpec, err := shmevent.DecodeCommandPayloadFull(stored)
	if err != nil {
		t.Fatalf("DecodeCommandPayloadFull: %v", err)
	}
	if name != "Inspect" || string(peerID) != "peer1" {
		t.Fatalf("name=%q peerID=%q", name, peerID)
	}
	if string(gotSpec) != string(spec) {
		t.Fatalf("spec = %q, want %q", gotSpec, spec)
	}
}

// A command's spec is written once and its name/target edited often, so a
// Put that carries no spec must leave the stored one alone. Without this,
// renaming a command -- or an app re-registering its catalog on startup, as
// object-history-app's AppBootstrap does on *every launch* -- silently
// deleted the form definition, with no error on either side.
func TestApplyOpSetPreservesSpecAcrossASpeclessPut(t *testing.T) {
	s, err := store.Open(filepath.Join(t.TempDir(), "sqlite"))
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer s.Close()

	f := New(s)
	key := shmevent.CommandKey([]byte("measure"))
	apply := func(value []byte) {
		t.Helper()
		res, _ := f.Apply(&raft.Log{Data: EncodeCommand(OpSet, key, value)}).(ApplyResult)
		if res.Err != nil {
			t.Fatalf("Apply: %v", res.Err)
		}
	}
	specOf := func() string {
		t.Helper()
		stored, err := s.Get(key)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		_, _, spec, err := shmevent.DecodeCommandPayloadFull(stored)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		return string(spec)
	}

	spec := `{"fields":[{"name":"serial"}]}`
	withSpec, err := shmevent.EncodeCommandPayloadWithSpec("Measure", []byte("peer1"), []byte(spec))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	apply(withSpec)

	// A plain rename, carrying no spec at all.
	renamed, err := shmevent.EncodeCommandPayload("Measure v2", []byte("peer1"))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	apply(renamed)

	name, peerID, err := shmevent.DecodeCommandPayload(mustGet(t, s, key))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if name != "Measure v2" || string(peerID) != "peer1" {
		t.Fatalf("rename did not take: %q/%q", name, peerID)
	}
	if got := specOf(); got != spec {
		t.Fatalf("spec after rename = %q, want it preserved as %q", got, spec)
	}

	// An explicitly emptied spec still clears it -- otherwise a spec could
	// never be removed once set.
	cleared, err := shmevent.EncodeCommandPayloadClearingSpec("Measure v2", []byte("peer1"))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	apply(cleared)
	if got := specOf(); got != "" {
		t.Fatalf("spec after explicit clear = %q, want empty", got)
	}

	// And a spec-less put over a record that never had one stays v1.
	apply(renamed)
	if got := specOf(); got != "" {
		t.Fatalf("spec = %q, want empty", got)
	}
}

func mustGet(t *testing.T, s *store.Store, key []byte) []byte {
	t.Helper()
	v, err := s.Get(key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	return v
}
