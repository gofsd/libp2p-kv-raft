package luacmd_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/gofsd/libp2p-kv-raft/examples/luacmd"
	"github.com/gofsd/libp2p-kv-raft/pkg/logrecord"
)

const testAuthor = "12D3KooWTestAuthorPeerID"

func newTestCatalog() *luacmd.Catalog {
	return luacmd.NewCatalog(luacmd.Memory(), testAuthor)
}

func mustPut(t *testing.T, c *luacmd.Catalog, id, name, code string) luacmd.Script {
	t.Helper()
	stored, err := c.Put(context.Background(), luacmd.Script{ID: id, Name: name, Code: code})
	if err != nil {
		t.Fatalf("Put(%s): %v", id, err)
	}
	return stored
}

func TestPutThenGetReturnsWhatWasStored(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog()
	code := `kv.log("hello from outer begin")`

	stored := mustPut(t, c, "outer", "Outer", code)
	if stored.SHA256 != luacmd.Sum(code) {
		t.Errorf("Put returned hash %q, want %q", stored.SHA256, luacmd.Sum(code))
	}
	if stored.Author != testAuthor {
		t.Errorf("Put returned author %q, want %q", stored.Author, testAuthor)
	}
	if stored.Rev.IsZero() {
		t.Error("Put returned a zero revision time")
	}

	got, err := c.Get(ctx, "outer")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Code != code || got.Name != "Outer" || got.SHA256 != stored.SHA256 {
		t.Errorf("Get returned %+v, want the revision Put stored (%+v)", got, stored)
	}
}

// The hash Put returns is what goes into the catalog Command's spec, and
// the hash the runner recomputes is what it compares against -- so it has
// to be the hash of the bytes that actually came back out of the journal,
// not merely of what went in.
func TestStoredHashCoversWhatComesBackOut(t *testing.T) {
	c := newTestCatalog()
	code := "return {narrative = \"unicode: ✓ — and a \\\"quote\\\"\\nand a newline\"}"

	stored := mustPut(t, c, "hashy", "Hashy", code)
	got, err := c.Get(context.Background(), "hashy")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Code != code {
		t.Fatalf("round trip changed the source:\n stored %q\n got    %q", code, got.Code)
	}
	if luacmd.Sum(got.Code) != stored.SHA256 {
		t.Errorf("hash of the retrieved source (%s) differs from the hash Put reported (%s)", luacmd.Sum(got.Code), stored.SHA256)
	}
}

func TestGetIsNotFoundForAnIDNothingWasWrittenUnder(t *testing.T) {
	_, err := newTestCatalog().Get(context.Background(), "never-existed")
	if !errors.Is(err, luacmd.ErrNotFound) {
		t.Errorf("Get returned %v, want ErrNotFound", err)
	}
}

// The latest revision wins, and every earlier one stays readable. This is
// the whole reason scripts live in the journal rather than under a key.
func TestLatestRevisionWinsAndHistoryKeepsTheRest(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog()

	mustPut(t, c, "outer", "Outer", `return {narrative = "v1"}`)
	mustPut(t, c, "outer", "Outer", `return {narrative = "v2"}`)
	third := mustPut(t, c, "outer", "Outer renamed", `return {narrative = "v3"}`)

	got, err := c.Get(ctx, "outer")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !strings.Contains(got.Code, "v3") || got.Name != "Outer renamed" {
		t.Errorf("Get returned %+v, want the third revision", got)
	}
	if !got.Rev.Equal(third.Rev) {
		t.Errorf("Get returned revision %s, want %s", got.Rev, third.Rev)
	}

	history, err := c.History(ctx, "outer")
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	if len(history) != 3 {
		t.Fatalf("History returned %d revisions, want 3", len(history))
	}
	for i, want := range []string{"v1", "v2", "v3"} {
		if !strings.Contains(history[i].Code, want) {
			t.Errorf("History[%d] is %q, want the one containing %q -- revisions must come back in the order they were written", i, history[i].Code, want)
		}
	}
}

func TestHistoryIsEmptyRatherThanNotFoundForAnUnknownID(t *testing.T) {
	history, err := newTestCatalog().History(context.Background(), "never-existed")
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	if len(history) != 0 {
		t.Errorf("History returned %d revisions for an id nothing was written under", len(history))
	}
}

func TestDeleteHidesTheScriptButKeepsItsHistory(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog()
	mustPut(t, c, "doomed", "Doomed", `return {narrative = "still here"}`)

	if err := c.Delete(ctx, "doomed"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := c.Get(ctx, "doomed"); !errors.Is(err, luacmd.ErrNotFound) {
		t.Errorf("Get after Delete returned %v, want ErrNotFound", err)
	}

	history, err := c.History(ctx, "doomed")
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	if len(history) != 2 {
		t.Fatalf("History returned %d revisions after a delete, want 2 (the script and its tombstone)", len(history))
	}
	if !strings.Contains(history[0].Code, "still here") {
		t.Error("the deleted revision's source is no longer readable through History")
	}
	if !history[1].Deleted {
		t.Error("the last revision after a delete is not marked as a tombstone")
	}
	if history[1].Name != "Doomed" {
		t.Errorf("tombstone name is %q, want the name it carried in life so a history reads sensibly", history[1].Name)
	}
}

func TestDeleteIsNotFoundForAnAlreadyDeletedScript(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog()
	mustPut(t, c, "doomed", "Doomed", `return {}`)

	if err := c.Delete(ctx, "doomed"); err != nil {
		t.Fatalf("first Delete: %v", err)
	}
	if err := c.Delete(ctx, "doomed"); !errors.Is(err, luacmd.ErrNotFound) {
		t.Errorf("second Delete returned %v, want ErrNotFound", err)
	}
}

// An id is a name, not a grave -- a script deleted by mistake has to be
// restorable under the same id, because every catalog Command pointing at
// it names that id.
func TestPutResurrectsADeletedScript(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog()
	mustPut(t, c, "phoenix", "Phoenix", `return {narrative = "first life"}`)
	if err := c.Delete(ctx, "phoenix"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	mustPut(t, c, "phoenix", "Phoenix", `return {narrative = "second life"}`)
	got, err := c.Get(ctx, "phoenix")
	if err != nil {
		t.Fatalf("Get after resurrect: %v", err)
	}
	if !strings.Contains(got.Code, "second life") {
		t.Errorf("Get returned %q after a resurrecting Put", got.Code)
	}
}

func TestListReturnsTheLatestOfEachLiveScriptInIDOrder(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog()
	mustPut(t, c, "outer", "Outer", `return {narrative = "outer v1"}`)
	mustPut(t, c, "inner", "Inner", `return {narrative = "inner v1"}`)
	mustPut(t, c, "outer", "Outer", `return {narrative = "outer v2"}`)
	mustPut(t, c, "gone", "Gone", `return {}`)
	if err := c.Delete(ctx, "gone"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	scripts, err := c.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(scripts) != 2 {
		t.Fatalf("List returned %d scripts, want 2 (a deleted one must not appear)", len(scripts))
	}
	if scripts[0].ID != "inner" || scripts[1].ID != "outer" {
		t.Errorf("List returned ids %s, %s -- want them in ascending id order", scripts[0].ID, scripts[1].ID)
	}
	if !strings.Contains(scripts[1].Code, "outer v2") {
		t.Errorf("List returned %q for outer, want its latest revision", scripts[1].Code)
	}
}

// One script's revisions must not leak into another's, which for
// length-prefixed keys means checking the case that would break a naive
// prefix scan: an id that is a prefix of another id.
func TestOneScriptsRevisionsDoNotLeakIntoAnothers(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog()
	mustPut(t, c, "run", "Run", `return {narrative = "run"}`)
	mustPut(t, c, "runner", "Runner", `return {narrative = "runner"}`)
	mustPut(t, c, "runner", "Runner", `return {narrative = "runner again"}`)

	history, err := c.History(ctx, "run")
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	if len(history) != 1 {
		t.Fatalf("History(run) returned %d revisions, want 1 -- runner's revisions leaked in", len(history))
	}
	got, err := c.Get(ctx, "run")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !strings.Contains(got.Code, `"run"`) {
		t.Errorf("Get(run) returned %q", got.Code)
	}
}

func TestPutRejectsWhatCannotBeStored(t *testing.T) {
	cases := []struct {
		name   string
		script luacmd.Script
		want   string
	}{
		{"no id", luacmd.Script{Name: "N", Code: "return {}"}, "id must not be empty"},
		{"id too long", luacmd.Script{ID: strings.Repeat("x", 257), Name: "N", Code: "return {}"}, "id exceeds"},
		{"no name", luacmd.Script{ID: "i", Code: "return {}"}, "name must not be empty"},
		{"name too long", luacmd.Script{ID: "i", Name: strings.Repeat("x", 257), Code: "return {}"}, "name exceeds"},
		{"code too big", luacmd.Script{ID: "i", Name: "N", Code: strings.Repeat("-", 64<<10+1)}, "exceeds"},
		{"empty code", luacmd.Script{ID: "i", Name: "N", Code: "  "}, "empty"},
		{"does not compile", luacmd.Script{ID: "i", Name: "N", Code: "if then end"}, "compile script"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := newTestCatalog().Put(context.Background(), tc.script)
			if err == nil {
				t.Fatalf("Put accepted %s", tc.name)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("Put error is %q, want it to mention %q", err, tc.want)
			}
		})
	}
}

// A script that fails validation must not have written anything -- a
// half-stored revision would be visible to every reader forever.
func TestARejectedPutStoresNothing(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog()

	if _, err := c.Put(ctx, luacmd.Script{ID: "bad", Name: "Bad", Code: "if then end"}); err == nil {
		t.Fatal("Put accepted a script that does not compile")
	}
	history, err := c.History(ctx, "bad")
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	if len(history) != 0 {
		t.Errorf("a rejected Put left %d revisions behind", len(history))
	}
}

// Sum is pinned by a golden value because a catalog Command stores what it
// returns and the runner recompares it later, possibly after an upgrade:
// changing the algorithm would not fail here, it would make every already-
// registered Lua command refuse to run.
func TestSumIsStable(t *testing.T) {
	// Cross-checked against an independent implementation:
	//   printf '%s' 'kv.log("hello from outer begin")' | sha256sum
	const want = "3d357ba72980e65655b6f27518558dd82c8965e0742f41159eb20e7ee1cd9e9d"
	if got := luacmd.Sum(`kv.log("hello from outer begin")`); got != want {
		t.Errorf("Sum returned %s, want %s -- the hash a stored Command pins must not move", got, want)
	}
}

// A record whose stored hash disagrees with its stored source is refused
// rather than returned. This catches corruption, not tampering (see
// decodeScript's own comment) -- but a reader that silently served bytes
// under a hash that never covered them would make the runner's own check
// meaningless, since it verifies against what this returns.
func TestACorruptRevisionIsRefusedRatherThanReturned(t *testing.T) {
	ctx := context.Background()
	j := luacmd.Memory()
	c := luacmd.NewCatalog(j, testAuthor)

	rnd, err := logrecord.NewRand()
	if err != nil {
		t.Fatalf("NewRand: %v", err)
	}
	ts := time.Now()
	key, err := logrecord.BuildKey(luacmd.ScriptKind, "corrupt", ts, rnd)
	if err != nil {
		t.Fatalf("BuildKey: %v", err)
	}
	value, err := logrecord.Record{
		Kind:         luacmd.ScriptKind,
		UnitID:       "corrupt",
		Timestamp:    ts,
		AuthorPeerID: testAuthor,
		Fields:       map[string]string{"name": "Corrupt", "sha256": luacmd.Sum("what was registered")},
		Narrative:    "what is actually stored",
	}.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if err := j.Append(ctx, key, value); err != nil {
		t.Fatalf("Append: %v", err)
	}

	if _, err := c.Get(ctx, "corrupt"); err == nil {
		t.Fatal("Get returned a revision whose stored hash does not cover its stored source")
	} else if !strings.Contains(err.Error(), "corrupt") {
		t.Errorf("Get error is %q, want it to say the revision is corrupt", err)
	}
	if _, err := c.List(ctx); err == nil {
		t.Error("List returned a corrupt revision instead of reporting it")
	}
}
