package luacmd_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/gofsd/libp2p-kv-raft/examples/luacmd"
)

// A command's structured answer travels as JSON in one reserved field
// (luacmd.FieldResult), which examples/relations/journalcmd already writes
// and this package now reads. These cover both directions and the edges
// D11 promises, since "if we know the structure we can use it" means a
// script author is relying on exactly those.

// The headline case: a child answers with structure and the parent indexes
// it, rather than parsing a sentence.
func TestAChildsResultIsIndexableByTheParent(t *testing.T) {
	env := newFakeEnv()
	env.setLog("instance-1", luacmd.LogEntry{
		InstanceID: "instance-1",
		Fields: map[string]string{
			"status": "ok",
			// The shape journalcmd's OpForm answers with.
			"result": `{"op":"form","form":{"columns":[{"heading":"operator","kind":"term"},{"heading":"pieces","kind":"number"}]}}`,
		},
		Narrative: "form read",
	})

	result := mustRun(t, env, `
local id, res = kv.run("shift-log", {op = "form"}, 1)
local columns = res.result.form.columns
return {fields = {
  op = res.result.op,
  count = #columns,
  second = columns[2].heading,
}}
`)
	if result.Fields["op"] != "form" {
		t.Errorf("op = %q", result.Fields["op"])
	}
	if result.Fields["count"] != "2" {
		t.Errorf("column count = %q, want 2 -- the array did not arrive as a sequence", result.Fields["count"])
	}
	if result.Fields["second"] != "pieces" {
		t.Errorf("second column = %q, want pieces", result.Fields["second"])
	}
}

// A command that answers in prose must not break a script that hoped for
// structure -- the producer is somebody else's command.
func TestAResultThatIsNotJSONLeavesTheRecordsResultNil(t *testing.T) {
	env := newFakeEnv()
	env.setLog("instance-1", luacmd.LogEntry{
		InstanceID: "instance-1",
		Fields:     map[string]string{"status": "ok", "result": "written by hand, not JSON"},
		Narrative:  "done",
	})

	result := mustRun(t, env, `
local id, res = kv.run("legacy", {}, 1)
return {fields = {
  structured = tostring(res.result ~= nil),
  raw = res.fields.result,
}}
`)
	if result.Fields["structured"] != "false" {
		t.Errorf("res.result is set for a non-JSON result")
	}
	if result.Fields["raw"] != "written by hand, not JSON" {
		t.Errorf("the raw string is no longer reachable: %q", result.Fields["raw"])
	}
}

func TestAMissingResultIsSimplyNil(t *testing.T) {
	env := newFakeEnv()
	env.setLog("instance-1", luacmd.LogEntry{
		InstanceID: "instance-1",
		Fields:     map[string]string{"status": "ok"},
		Narrative:  "no structured answer here",
	})

	result := mustRun(t, env, `
local id, res = kv.run("plain", {}, 1)
if res.result == nil then return {fields = {saw = "nil"}} end
return {fields = {saw = "something"}}
`)
	if result.Fields["saw"] != "nil" {
		t.Errorf("saw = %q", result.Fields["saw"])
	}
}

// The other direction: a script returns structure of its own, which lands
// in the same field a Go handler would write.
func TestAScriptCanReturnStructure(t *testing.T) {
	result := mustRun(t, newFakeEnv(), `
return {
  result = {checked = 2, names = {"operator", "pieces"}, nested = {ok = true}},
  fields = {status = "ok"},
  narrative = "form read",
}
`)
	if result.Fields["status"] != "ok" || result.Narrative != "form read" {
		t.Errorf("the ordinary halves of the result were disturbed: %+v", result)
	}

	var decoded struct {
		Checked float64  `json:"checked"`
		Names   []string `json:"names"`
		Nested  struct {
			OK bool `json:"ok"`
		} `json:"nested"`
	}
	if err := json.Unmarshal([]byte(result.Fields[luacmd.FieldResult]), &decoded); err != nil {
		t.Fatalf("the result field is not JSON: %v (%q)", err, result.Fields[luacmd.FieldResult])
	}
	if decoded.Checked != 2 || len(decoded.Names) != 2 || decoded.Names[0] != "operator" || !decoded.Nested.OK {
		t.Errorf("decoded = %+v", decoded)
	}
}

func TestReturningOnlyAResultIsLegal(t *testing.T) {
	result := mustRun(t, newFakeEnv(), `return {result = {answer = 42}}`)
	if !strings.Contains(result.Fields[luacmd.FieldResult], `"answer":42`) {
		t.Errorf("result field = %q", result.Fields[luacmd.FieldResult])
	}
}

// Both spellings mean the same thing, so quietly dropping one would lose
// data the script believed it returned.
func TestReturningAResultTwiceIsRefused(t *testing.T) {
	_, err := run(t, newFakeEnv(), `return {result = {a = 1}, fields = {result = "also here"}}`)
	if err == nil {
		t.Fatal("a result given both ways was accepted")
	}
	if !strings.Contains(err.Error(), "use one") {
		t.Errorf("error = %q", err)
	}
}

// A round trip through both directions at once: the script reads a child's
// structure and returns structure derived from it.
func TestStructureSurvivesARoundTrip(t *testing.T) {
	env := newFakeEnv()
	env.setLog("instance-1", luacmd.LogEntry{
		InstanceID: "instance-1",
		Fields:     map[string]string{"status": "ok", "result": `{"items":[{"id":"a","n":1},{"id":"b","n":2}]}`},
	})

	result := mustRun(t, env, `
local id, res = kv.run("lister", {}, 1)
local total, ids = 0, {}
for _, item in ipairs(res.result.items) do
  total = total + item.n
  ids[#ids + 1] = item.id
end
return {result = {total = total, ids = ids}}
`)
	var decoded struct {
		Total float64  `json:"total"`
		IDs   []string `json:"ids"`
	}
	if err := json.Unmarshal([]byte(result.Fields[luacmd.FieldResult]), &decoded); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded.Total != 3 || len(decoded.IDs) != 2 || decoded.IDs[1] != "b" {
		t.Errorf("decoded = %+v", decoded)
	}
}

func TestJSONDecodeAndEncodeAreAvailableToAScript(t *testing.T) {
	// The escape hatch for a command that answers with JSON somewhere
	// other than the result field -- its narrative, say, which is what
	// every kvmobile binding effectively does.
	env := newFakeEnv()
	env.setLog("instance-1", luacmd.LogEntry{
		InstanceID: "instance-1",
		Fields:     map[string]string{"status": "ok"},
		Narrative:  `{"peer_id":"12D3KooWabc","groups":["relay","channel"]}`,
	})

	result := mustRun(t, env, `
local id, res = kv.run("some-binding", {}, 1)
local decoded = kv.json_decode(res.narrative)
local again = kv.json_encode({groups = decoded.groups})
local bad, err = kv.json_decode("not json at all")
return {fields = {
  peer = decoded.peer_id,
  first = decoded.groups[1],
  reencoded = again,
  bad = tostring(bad),
  err = err,
}}
`)
	if result.Fields["peer"] != "12D3KooWabc" || result.Fields["first"] != "relay" {
		t.Errorf("decode did not work: %v", result.Fields)
	}
	if result.Fields["reencoded"] != `{"groups":["relay","channel"]}` {
		t.Errorf("re-encoded = %q", result.Fields["reencoded"])
	}
	if result.Fields["bad"] != "nil" || result.Fields["err"] != "not JSON" {
		t.Errorf("a bad decode should answer nil plus a message, got bad=%q err=%q", result.Fields["bad"], result.Fields["err"])
	}
}

func TestJSONEncodeRaisesOnSomethingItCannotEncode(t *testing.T) {
	_, err := run(t, newFakeEnv(), `local t = {} t.self = t return {narrative = kv.json_encode(t)}`)
	if err == nil {
		t.Fatal("encoding a self-referential table was accepted")
	}
	if !strings.Contains(err.Error(), "refers to itself") {
		t.Errorf("error = %q", err)
	}
}

// The edges D11 writes down. A script author relying on the structure has
// to be able to rely on these too, so they are pinned rather than left to
// be discovered.
func TestTheMappingsEdgesAreWhatTheDocumentationSays(t *testing.T) {
	env := newFakeEnv()
	env.setLog("instance-1", luacmd.LogEntry{
		InstanceID: "instance-1",
		Fields: map[string]string{
			"status": "ok",
			"result": `{"missing":null,"big_id":"9007199254740993","n":1.5,"yes":true}`,
		},
	})

	result := mustRun(t, env, `
local id, res = kv.run("edges", {}, 1)
return {fields = {
  -- null arrives as nil, so the key is simply not there
  missing = tostring(res.result.missing == nil),
  -- an id past 2^53 has to travel as a string to survive
  big = res.result.big_id,
  fractional = tostring(res.result.n),
  boolean = tostring(res.result.yes),
  -- an empty table encodes as an object, never an array
  empty = kv.json_encode({}),
}}
`)
	if result.Fields["missing"] != "true" {
		t.Errorf("null did not become nil: %q", result.Fields["missing"])
	}
	if result.Fields["big"] != "9007199254740993" {
		t.Errorf("a big id sent as a string came back as %q", result.Fields["big"])
	}
	if result.Fields["fractional"] != "1.5" {
		t.Errorf("fractional = %q", result.Fields["fractional"])
	}
	if result.Fields["boolean"] != "true" {
		t.Errorf("boolean = %q", result.Fields["boolean"])
	}
	if result.Fields["empty"] != "{}" {
		t.Errorf("an empty table encoded as %q, want {} -- see fromLua's own doc comment", result.Fields["empty"])
	}
}

func TestAnOversizedResultSaysWhichPartIsTooBig(t *testing.T) {
	env := newFakeEnv()
	opts := testOptions(env)
	opts.MaxResultBytes = 200

	_, err := luacmd.Run(context.Background(), `
local big = {}
for i = 1, 200 do big[i] = "padding-value-" .. i end
return {result = big, narrative = "small"}
`, opts)
	if err == nil {
		t.Fatal("an oversized result was accepted")
	}
	if !strings.Contains(err.Error(), "of it the returned value") {
		t.Errorf("error %q does not point at the payload, which is the part a script can shrink", err)
	}
}
