package store_test

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"

	"github.com/gofsd/libp2p-kv-raft/pkg/store"
)

func TestSetGetDelete(t *testing.T) {
	s, err := store.Open(filepath.Join(t.TempDir(), "sqlite"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	if _, err := s.Get([]byte("missing")); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("Get missing key: got %v, want ErrNotFound", err)
	}

	if err := s.Set([]byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	v, err := s.Get([]byte("k1"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(v, []byte("v1")) {
		t.Fatalf("Get: got %q, want %q", v, "v1")
	}

	// Set again should overwrite, not error or duplicate.
	if err := s.Set([]byte("k1"), []byte("v2")); err != nil {
		t.Fatalf("Set overwrite: %v", err)
	}
	v, err = s.Get([]byte("k1"))
	if err != nil {
		t.Fatalf("Get after overwrite: %v", err)
	}
	if !bytes.Equal(v, []byte("v2")) {
		t.Fatalf("Get after overwrite: got %q, want %q", v, "v2")
	}

	if err := s.Delete([]byte("k1")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := s.Get([]byte("k1")); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("Get after delete: got %v, want ErrNotFound", err)
	}
}

func TestSetNilValue(t *testing.T) {
	s, err := store.Open(filepath.Join(t.TempDir(), "sqlite"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	if err := s.Set([]byte("k1"), nil); err != nil {
		t.Fatalf("Set with nil value: %v", err)
	}
	v, err := s.Get([]byte("k1"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if len(v) != 0 {
		t.Fatalf("Get after Set(nil): got %q, want empty", v)
	}

	// Re-Set that same (possibly nil, per Get's own doc comment) value
	// under a different key -- the exact read-then-write pattern
	// kvfsm's OpConfirm performs, and the one that originally surfaced
	// this as a NOT NULL constraint failure.
	if err := s.Set([]byte("k2"), v); err != nil {
		t.Fatalf("Set(k2, <value read back from k1>): %v", err)
	}
}

func TestDumpAllLoadAllRoundTrip(t *testing.T) {
	src, err := store.Open(filepath.Join(t.TempDir(), "sqlite"))
	if err != nil {
		t.Fatalf("Open src: %v", err)
	}
	defer src.Close()

	want := map[string]string{
		"a":   "1",
		"bb":  "22",
		"ccc": "333",
		"":    "empty-key",
	}
	for k, v := range want {
		if err := src.Set([]byte(k), []byte(v)); err != nil {
			t.Fatalf("Set(%q): %v", k, err)
		}
	}

	var buf bytes.Buffer
	if err := src.DumpAll(&buf); err != nil {
		t.Fatalf("DumpAll: %v", err)
	}

	dst, err := store.Open(filepath.Join(t.TempDir(), "sqlite"))
	if err != nil {
		t.Fatalf("Open dst: %v", err)
	}
	defer dst.Close()

	// Pre-populate dst with a key that must not survive LoadAll, mirroring
	// how raft's Restore calls it against a store that may already have
	// (stale) content.
	if err := dst.Set([]byte("stale"), []byte("x")); err != nil {
		t.Fatalf("Set stale: %v", err)
	}

	if err := dst.LoadAll(&buf); err != nil {
		t.Fatalf("LoadAll: %v", err)
	}

	if _, err := dst.Get([]byte("stale")); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("stale key survived LoadAll: err=%v", err)
	}
	for k, wantV := range want {
		gotV, err := dst.Get([]byte(k))
		if err != nil {
			t.Fatalf("Get(%q) after LoadAll: %v", k, err)
		}
		if string(gotV) != wantV {
			t.Fatalf("Get(%q) after LoadAll: got %q, want %q", k, gotV, wantV)
		}
	}
}

func TestCountPrefix(t *testing.T) {
	s, err := store.Open(filepath.Join(t.TempDir(), "sqlite"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	entries := map[string]string{
		string([]byte{0x00, 0x01, 0x02, 'a'}): "1",
		string([]byte{0x00, 0x01, 0x02, 'b'}): "2",
		string([]byte{0x00, 0x01, 0x02, 'c'}): "3",
		// Same kind byte (0x01), different status byte (0x03) -- must not
		// be counted under the 0x00,0x01,0x02 prefix.
		string([]byte{0x00, 0x01, 0x03, 'a'}): "4",
		// Entirely different top-level key, no shared prefix at all.
		string([]byte{0xFF}): "5",
	}
	for k, v := range entries {
		if err := s.Set([]byte(k), []byte(v)); err != nil {
			t.Fatalf("Set(%x): %v", k, err)
		}
	}

	prefix := []byte{0x00, 0x01, 0x02}
	count, err := s.CountPrefix(prefix)
	if err != nil {
		t.Fatalf("CountPrefix(%x): %v", prefix, err)
	}
	if count != 3 {
		t.Fatalf("CountPrefix(%x) = %d, want 3", prefix, count)
	}

	differentStatus := []byte{0x00, 0x01, 0x03}
	count, err = s.CountPrefix(differentStatus)
	if err != nil {
		t.Fatalf("CountPrefix(%x): %v", differentStatus, err)
	}
	if count != 1 {
		t.Fatalf("CountPrefix(%x) = %d, want 1", differentStatus, count)
	}

	noMatch := []byte{0x00, 0x02, 0x02}
	count, err = s.CountPrefix(noMatch)
	if err != nil {
		t.Fatalf("CountPrefix(%x): %v", noMatch, err)
	}
	if count != 0 {
		t.Fatalf("CountPrefix(%x) = %d, want 0", noMatch, count)
	}

	// A prefix ending in 0xFF exercises prefixUpperBound's carry logic
	// (the byte before it must be incremented instead).
	if err := s.Set([]byte{0x00, 0xFF, 'x'}, []byte("6")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	carryPrefix := []byte{0x00, 0xFF}
	count, err = s.CountPrefix(carryPrefix)
	if err != nil {
		t.Fatalf("CountPrefix(%x): %v", carryPrefix, err)
	}
	if count != 1 {
		t.Fatalf("CountPrefix(%x) = %d, want 1 (must not also match the unrelated 0xFF top-level key)", carryPrefix, count)
	}
}

func TestScanRange(t *testing.T) {
	s, err := store.Open(filepath.Join(t.TempDir(), "sqlite"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	entries := [][2]string{
		{string([]byte{0x01, 0x00}), "a"},
		{string([]byte{0x01, 0x05}), "b"},
		{string([]byte{0x01, 0x09}), "c"},
		{string([]byte{0x02, 0x00}), "outside"},
	}
	for _, e := range entries {
		if err := s.Set([]byte(e[0]), []byte(e[1])); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}

	got, err := s.ScanRange([]byte{0x01, 0x00}, []byte{0x01, 0x09}, 0)
	if err != nil {
		t.Fatalf("ScanRange: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("ScanRange returned %d entries, want 3: %+v", len(got), got)
	}
	wantOrder := []string{"a", "b", "c"}
	for i, kv := range got {
		if string(kv.Value) != wantOrder[i] {
			t.Fatalf("ScanRange[%d] = %q, want %q (must be ascending by key)", i, kv.Value, wantOrder[i])
		}
	}

	// A tighter range must exclude the endpoints outside it.
	got, err = s.ScanRange([]byte{0x01, 0x01}, []byte{0x01, 0x08}, 0)
	if err != nil {
		t.Fatalf("ScanRange (tight): %v", err)
	}
	if len(got) != 1 || string(got[0].Value) != "b" {
		t.Fatalf("ScanRange (tight) = %+v, want just {b}", got)
	}

	// limit caps the result count without affecting ordering.
	got, err = s.ScanRange([]byte{0x01, 0x00}, []byte{0x01, 0x09}, 2)
	if err != nil {
		t.Fatalf("ScanRange (limit=2): %v", err)
	}
	if len(got) != 2 || string(got[0].Value) != "a" || string(got[1].Value) != "b" {
		t.Fatalf("ScanRange (limit=2) = %+v, want {a, b}", got)
	}
}

func TestScanPrefix(t *testing.T) {
	s, err := store.Open(filepath.Join(t.TempDir(), "sqlite"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	entries := map[string]string{
		string([]byte{0x00, 0x01, 0x02, 'a'}): "1",
		string([]byte{0x00, 0x01, 0x02, 'b'}): "2",
		string([]byte{0x00, 0x01, 0x03, 'a'}): "3",
		string([]byte{0xFF}):                  "4",
	}
	for k, v := range entries {
		if err := s.Set([]byte(k), []byte(v)); err != nil {
			t.Fatalf("Set(%x): %v", k, err)
		}
	}

	prefix := []byte{0x00, 0x01, 0x02}
	got, err := s.ScanPrefix(prefix, 0)
	if err != nil {
		t.Fatalf("ScanPrefix(%x): %v", prefix, err)
	}
	if len(got) != 2 {
		t.Fatalf("ScanPrefix(%x) returned %d entries, want 2: %+v", prefix, len(got), got)
	}
	for _, kv := range got {
		if !bytes.HasPrefix(kv.Key, prefix) {
			t.Fatalf("ScanPrefix(%x) returned key %x not matching the prefix", prefix, kv.Key)
		}
	}

	// A prefix ending in 0xFF exercises prefixUpperBound's carry logic,
	// same boundary case TestCountPrefix already covers for CountPrefix.
	if err := s.Set([]byte{0x00, 0xFF, 'x'}, []byte("5")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	carryPrefix := []byte{0x00, 0xFF}
	got, err = s.ScanPrefix(carryPrefix, 0)
	if err != nil {
		t.Fatalf("ScanPrefix(%x): %v", carryPrefix, err)
	}
	if len(got) != 1 || string(got[0].Value) != "5" {
		t.Fatalf("ScanPrefix(%x) = %+v, want just {5} (must not also match the unrelated 0xFF top-level key)", carryPrefix, got)
	}
}

func TestReopenPersists(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "sqlite")

	s1, err := store.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := s1.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2, err := store.Open(dir)
	if err != nil {
		t.Fatalf("re-Open: %v", err)
	}
	defer s2.Close()
	v, err := s2.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get after reopen: %v", err)
	}
	if string(v) != "v" {
		t.Fatalf("Get after reopen: got %q, want %q", v, "v")
	}
}

func TestWithTxCommitsOnSuccess(t *testing.T) {
	s, err := store.Open(filepath.Join(t.TempDir(), "sqlite"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	if err := s.WithTx(func(tx *store.Tx) error {
		if err := tx.Set([]byte("a"), []byte("1")); err != nil {
			return err
		}
		return tx.Set([]byte("b"), []byte("2"))
	}); err != nil {
		t.Fatalf("WithTx: %v", err)
	}

	for k, want := range map[string]string{"a": "1", "b": "2"} {
		v, err := s.Get([]byte(k))
		if err != nil {
			t.Fatalf("Get(%q): %v", k, err)
		}
		if string(v) != want {
			t.Fatalf("Get(%q) = %q, want %q", k, v, want)
		}
	}
}

// TestWithTxRollsBackOnError is the regression test for the bug WithTx
// exists to close: pkg/kvfsm's OpTxn and cascade-delete each make more
// than one write per raft log entry, and before WithTx existed those
// writes autocommitted independently -- a failure partway through left a
// genuine partial-apply state on disk. This proves the fix: an error
// returned partway through fn rolls back every write fn already made in
// the same call, not just the one that failed.
func TestWithTxRollsBackOnError(t *testing.T) {
	s, err := store.Open(filepath.Join(t.TempDir(), "sqlite"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	// Pre-existing state that must survive the failed transaction
	// untouched.
	if err := s.Set([]byte("untouched"), []byte("orig")); err != nil {
		t.Fatalf("Set: %v", err)
	}

	wantErr := errors.New("deliberate failure partway through")
	err = s.WithTx(func(tx *store.Tx) error {
		if err := tx.Set([]byte("a"), []byte("1")); err != nil {
			return err
		}
		if err := tx.Delete([]byte("untouched")); err != nil {
			return err
		}
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("WithTx error = %v, want %v", err, wantErr)
	}

	if _, err := s.Get([]byte("a")); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("Get(a) after rollback: got %v, want ErrNotFound (the Set inside the failed tx must not have landed)", err)
	}
	v, err := s.Get([]byte("untouched"))
	if err != nil {
		t.Fatalf("Get(untouched) after rollback: %v", err)
	}
	if string(v) != "orig" {
		t.Fatalf("Get(untouched) after rollback: got %q, want %q (the Delete inside the failed tx must not have landed)", v, "orig")
	}
}

// TestWithTxReadsOwnWrites confirms Tx's Get/CountPrefix/ScanPrefix see
// writes made earlier in the same transaction, before it commits -- the
// property pkg/kvfsm's OpTxn precondition-then-write split and
// applyCascadeDelete's scan-then-delete-each loop both rely on implicitly.
func TestWithTxReadsOwnWrites(t *testing.T) {
	s, err := store.Open(filepath.Join(t.TempDir(), "sqlite"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	if err := s.WithTx(func(tx *store.Tx) error {
		if err := tx.Set([]byte("prefix:1"), []byte("v1")); err != nil {
			return err
		}
		if err := tx.Set([]byte("prefix:2"), []byte("v2")); err != nil {
			return err
		}
		count, err := tx.CountPrefix([]byte("prefix:"))
		if err != nil {
			return err
		}
		if count != 2 {
			t.Fatalf("CountPrefix inside tx = %d, want 2", count)
		}
		v, err := tx.Get([]byte("prefix:1"))
		if err != nil {
			return err
		}
		if string(v) != "v1" {
			t.Fatalf("Get inside tx = %q, want %q", v, "v1")
		}
		return nil
	}); err != nil {
		t.Fatalf("WithTx: %v", err)
	}
}
