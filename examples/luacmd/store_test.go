package luacmd_test

import (
	"context"
	"testing"

	"github.com/gofsd/libp2p-kv-raft/examples/luacmd"
)

// The memory journal exists to stand in for the real one, so what these
// check is the handful of properties script.go actually leans on: both
// bounds inclusive, ascending key order, and returned bytes that are the
// caller's own. A memory journal that got any of those wrong would make
// every script_test.go case pass against semantics the node journal does
// not have.

func TestMemoryJournalRangeIsInclusiveOfBothBounds(t *testing.T) {
	ctx := context.Background()
	j := luacmd.Memory()
	for _, k := range []string{"a", "b", "c", "d"} {
		if err := j.Append(ctx, []byte(k), []byte("v-"+k)); err != nil {
			t.Fatalf("Append(%s): %v", k, err)
		}
	}

	pairs, err := j.Range(ctx, []byte("b"), []byte("c"))
	if err != nil {
		t.Fatalf("Range: %v", err)
	}
	if got := keysOf(pairs); len(got) != 2 || got[0] != "b" || got[1] != "c" {
		t.Errorf("Range(b, c) = %v, want both bounds included and nothing else", got)
	}
}

func TestMemoryJournalRangeIsAscending(t *testing.T) {
	ctx := context.Background()
	j := luacmd.Memory()
	// Appended out of order on purpose: the ordering must come from the
	// keys, not from insertion, because that is what the real store's
	// byte-wise scan does.
	for _, k := range []string{"k3", "k1", "k2"} {
		if err := j.Append(ctx, []byte(k), []byte(k)); err != nil {
			t.Fatalf("Append(%s): %v", k, err)
		}
	}

	pairs, err := j.Range(ctx, []byte("k0"), []byte("k9"))
	if err != nil {
		t.Fatalf("Range: %v", err)
	}
	got := keysOf(pairs)
	for i, want := range []string{"k1", "k2", "k3"} {
		if i >= len(got) || got[i] != want {
			t.Fatalf("Range returned %v, want ascending k1,k2,k3", got)
		}
	}
}

// A caller that mutates what Range handed back must not be able to change
// what the journal holds -- the node journal returns bytes decoded off the
// wire, so a memory one sharing its own storage would be strictly more
// permissive than the thing it stands in for.
func TestMemoryJournalDoesNotShareItsStorage(t *testing.T) {
	ctx := context.Background()
	j := luacmd.Memory()
	if err := j.Append(ctx, []byte("key"), []byte("original")); err != nil {
		t.Fatalf("Append: %v", err)
	}

	pairs, err := j.Range(ctx, []byte("key"), []byte("key"))
	if err != nil {
		t.Fatalf("Range: %v", err)
	}
	copy(pairs[0].Value, "TAMPERED")

	pairs, err = j.Range(ctx, []byte("key"), []byte("key"))
	if err != nil {
		t.Fatalf("Range: %v", err)
	}
	if got := string(pairs[0].Value); got != "original" {
		t.Errorf("journal now holds %q; a caller mutated its storage through a returned pair", got)
	}
}

func TestMemoryJournalHonorsAControlledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	j := luacmd.Memory()

	if err := j.Append(ctx, []byte("k"), []byte("v")); err == nil {
		t.Error("Append ignored a cancelled context")
	}
	if _, err := j.Range(ctx, []byte("a"), []byte("z")); err == nil {
		t.Error("Range ignored a cancelled context")
	}
}

func keysOf(pairs []luacmd.Pair) []string {
	keys := make([]string, 0, len(pairs))
	for _, p := range pairs {
		keys = append(keys, string(p.Key))
	}
	return keys
}
