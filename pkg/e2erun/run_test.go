package e2erun

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/gofsd/libp2p-kv-raft/pkg/e2edata"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// Most of this package is SSH/process orchestration that needs a live
// environment to exercise meaningfully (verified manually against a real
// spawned kvnode -- see this package's use from magefile.go's E2E
// namespace). These tests cover the pure logic pieces that don't.

func TestResolveBootstrapPlaceholder(t *testing.T) {
	if got := ResolveBootstrapPlaceholder(BootstrapToken, "/ip4/1.2.3.4/tcp/4101/p2p/12D3Koo..."); got != "/ip4/1.2.3.4/tcp/4101/p2p/12D3Koo..." {
		t.Fatalf("ResolveBootstrapPlaceholder(token) = %q, want the resolved multiaddr", got)
	}
	if got := ResolveBootstrapPlaceholder("some literal value", "/ip4/1.2.3.4/tcp/4101/p2p/12D3Koo..."); got != "some literal value" {
		t.Fatalf("ResolveBootstrapPlaceholder(non-token) = %q, want unchanged", got)
	}
}

func TestInterpretSendEventResult(t *testing.T) {
	okMsg, err := shmevent.NewGetPublicKey()
	if err != nil {
		t.Fatalf("NewGetPublicKey: %v", err)
	}
	if err := okMsg.GetPublicKey().SetPubKey([]byte("key-bytes")); err != nil {
		t.Fatalf("SetPubKey: %v", err)
	}
	okEv, err := e2edata.EventFromMsg(okMsg)
	if err != nil {
		t.Fatalf("EventFromMsg(ok): %v", err)
	}
	okJSON := mustJSON(t, okEv)
	status, errMsg := interpretSendEventResult(okJSON, "", nil)
	if status != e2edata.StatusPass || errMsg != "" {
		t.Fatalf("interpretSendEventResult(pass) = (%d, %q), want (StatusPass, \"\")", status, errMsg)
	}

	// kvctl-cli sendevent prints the response JSON to stdout *then* exits
	// 1 for an EventError response -- interpretSendEventResult must still
	// read that JSON, not just report the nonzero exit opaquely (a real
	// bug caught running this against a live deployed node).
	errRespMsg, err := shmevent.NewError("store: key not found")
	if err != nil {
		t.Fatalf("NewError: %v", err)
	}
	errEv, err := e2edata.EventFromMsg(errRespMsg)
	if err != nil {
		t.Fatalf("EventFromMsg(error): %v", err)
	}
	errJSON := mustJSON(t, errEv)
	status, errMsg = interpretSendEventResult(errJSON, "", errExitStatus1)
	if status != e2edata.StatusFail || errMsg != "store: key not found" {
		t.Fatalf("interpretSendEventResult(error, exit 1) = (%d, %q), want (StatusFail, \"store: key not found\")", status, errMsg)
	}

	status, errMsg = interpretSendEventResult("", "connection refused", errExitStatus1)
	if status != e2edata.StatusFail || errMsg == "" {
		t.Fatalf("interpretSendEventResult(empty stdout, real failure) = (%d, %q), want (StatusFail, non-empty)", status, errMsg)
	}

	status, errMsg = interpretSendEventResult("not json", "", nil)
	if status != e2edata.StatusFail || errMsg == "" {
		t.Fatalf("interpretSendEventResult(garbage) = (%d, %q), want (StatusFail, non-empty)", status, errMsg)
	}
}

var errExitStatus1 = errors.New("exit status 1")

func TestStatusName(t *testing.T) {
	cases := map[int]string{
		e2edata.StatusPass:    "PASS",
		e2edata.StatusSkipped: "SKIP",
		e2edata.StatusFail:    "FAIL",
		99:                    "FAIL",
	}
	for status, want := range cases {
		if got := statusName(status); got != want {
			t.Fatalf("statusName(%d) = %q, want %q", status, got, want)
		}
	}
}

func TestParseRowResults(t *testing.T) {
	rowIdxs := []int{10, 20, 30}

	out, err := parseRowResults([]byte(`[
		{"index":0,"pass":true},
		{"index":1,"pass":false,"error":"store: key not found"},
		{"index":2,"pass":true}
	]`), rowIdxs, "test driver")
	if err != nil {
		t.Fatalf("parseRowResults: %v", err)
	}
	if out[10].status != e2edata.StatusPass {
		t.Fatalf("row 10 (index 0) = %+v, want pass", out[10])
	}
	if out[20].status != e2edata.StatusFail || out[20].errMsg != "store: key not found" {
		t.Fatalf("row 20 (index 1) = %+v, want fail with the recorded error", out[20])
	}
	if out[30].status != e2edata.StatusPass {
		t.Fatalf("row 30 (index 2) = %+v, want pass", out[30])
	}
}

func TestParseRowResultsMissingRowFailsClosed(t *testing.T) {
	rowIdxs := []int{10, 20}

	// Only one of the two expected results came back -- e.g. the driver
	// process crashed partway through.
	out, err := parseRowResults([]byte(`[{"index":0,"pass":true}]`), rowIdxs, "test driver")
	if err != nil {
		t.Fatalf("parseRowResults: %v", err)
	}
	if out[10].status != e2edata.StatusPass {
		t.Fatalf("row 10 = %+v, want pass", out[10])
	}
	if out[20].status != e2edata.StatusFail {
		t.Fatalf("row 20 (never reported) = %+v, want fail-closed, not a silent pass", out[20])
	}
}

func TestParseRowResultsInvalidJSON(t *testing.T) {
	if _, err := parseRowResults([]byte("not json"), []int{1}, "test driver"); err == nil {
		t.Fatal("parseRowResults with invalid JSON: want error, got nil")
	}
}

func TestParseTypes(t *testing.T) {
	if got, err := ParseTypes(""); err != nil || got != AllTypes() {
		t.Fatalf("ParseTypes(\"\") = %+v, %v, want AllTypes(), nil", got, err)
	}
	got, err := ParseTypes("desktop,android")
	if err != nil {
		t.Fatalf("ParseTypes: %v", err)
	}
	want := Types{Desktop: true, Android: true}
	if got != want {
		t.Fatalf("ParseTypes(\"desktop,android\") = %+v, want %+v", got, want)
	}
	// Case/whitespace-insensitive.
	if got, err := ParseTypes(" WEB "); err != nil || got != (Types{Web: true}) {
		t.Fatalf("ParseTypes whitespace/case handling = %+v, %v", got, err)
	}
	if _, err := ParseTypes("not-a-real-type"); err == nil {
		t.Fatal("ParseTypes with an unknown entry: want error, got nil")
	}
}

// ONLY's target numbering (1=desktop, 2=android, 3=web) is a public
// interface -- a script or a human types these numbers -- so this pins the
// exact mapping, not just "some subset comes back selected".
func TestParseOnly(t *testing.T) {
	got, err := ParseOnly("1,2")
	if err != nil {
		t.Fatalf("ParseOnly(\"1,2\"): %v", err)
	}
	want := Types{Desktop: true, Android: true}
	if got != want {
		t.Fatalf("ParseOnly(\"1,2\") = %+v, want %+v (1=desktop, 2=android)", got, want)
	}

	for _, tc := range []struct {
		numbers string
		want    Types
	}{
		{"1", Types{Desktop: true}},
		{"2", Types{Android: true}},
		{"3", Types{Web: true}},
		{" 2 , 3 ", Types{Android: true, Web: true}},
	} {
		got, err := ParseOnly(tc.numbers)
		if err != nil {
			t.Fatalf("ParseOnly(%q): %v", tc.numbers, err)
		}
		if got != tc.want {
			t.Fatalf("ParseOnly(%q) = %+v, want %+v", tc.numbers, got, tc.want)
		}
	}

	// Unlike ParseTypes, an empty/whitespace value is an error, not "run
	// everything" -- see EnvOnly's own doc comment on why.
	for _, bad := range []string{"", "  ", "0", "4", "abc", "1,", "1,4"} {
		if _, err := ParseOnly(bad); err == nil {
			t.Fatalf("ParseOnly(%q): want error, got nil", bad)
		}
	}
}

// EXCLUDE=2,3 must run everything except android+web -- leaving only desktop.
func TestParseExclude(t *testing.T) {
	got, err := ParseExclude("2,3")
	if err != nil {
		t.Fatalf("ParseExclude(\"2,3\"): %v", err)
	}
	want := Types{Desktop: true}
	if got != want {
		t.Fatalf("ParseExclude(\"2,3\") = %+v, want %+v (everything except android+web)", got, want)
	}

	// Excluding every target leaves nothing selected -- a legal (if
	// useless) result, not an error; EnvExclude only rejects a value that
	// names *no* target at all.
	got, err = ParseExclude("1,2,3")
	if err != nil {
		t.Fatalf("ParseExclude(\"1,2,3\"): %v", err)
	}
	if got != (Types{}) {
		t.Fatalf("ParseExclude(\"1,2,3\") = %+v, want the zero Types (nothing selected)", got)
	}

	for _, bad := range []string{"", "  ", "0", "4", "abc"} {
		if _, err := ParseExclude(bad); err == nil {
			t.Fatalf("ParseExclude(%q): want error, got nil", bad)
		}
	}
}

func TestSelectedTypesConflicts(t *testing.T) {
	t.Setenv(EnvTypes, "")
	t.Setenv(EnvOnly, "")
	t.Setenv(EnvExclude, "")

	if got, err := SelectedTypes(); err != nil || got != AllTypes() {
		t.Fatalf("SelectedTypes with nothing set = %+v, %v, want AllTypes(), nil", got, err)
	}

	t.Setenv(EnvTypes, "desktop")
	if got, err := SelectedTypes(); err != nil || got != (Types{Desktop: true}) {
		t.Fatalf("SelectedTypes with only %s set = %+v, %v", EnvTypes, got, err)
	}
	t.Setenv(EnvTypes, "")

	t.Setenv(EnvOnly, "1,2")
	if got, err := SelectedTypes(); err != nil || got != (Types{Desktop: true, Android: true}) {
		t.Fatalf("SelectedTypes with only %s set = %+v, %v", EnvOnly, got, err)
	}
	t.Setenv(EnvOnly, "")

	t.Setenv(EnvExclude, "2,3")
	if got, err := SelectedTypes(); err != nil || got != (Types{Desktop: true}) {
		t.Fatalf("SelectedTypes with only %s set = %+v, %v", EnvExclude, got, err)
	}
	t.Setenv(EnvExclude, "")

	// Setting more than one at once must fail closed, not silently prefer
	// one -- see SelectedTypes' own doc comment.
	t.Setenv(EnvTypes, "desktop")
	t.Setenv(EnvOnly, "1")
	if _, err := SelectedTypes(); err == nil {
		t.Fatalf("SelectedTypes with both %s and %s set: want error, got nil", EnvTypes, EnvOnly)
	}
	t.Setenv(EnvTypes, "")
	t.Setenv(EnvOnly, "")

	t.Setenv(EnvOnly, "1")
	t.Setenv(EnvExclude, "2")
	if _, err := SelectedTypes(); err == nil {
		t.Fatalf("SelectedTypes with both %s and %s set: want error, got nil", EnvOnly, EnvExclude)
	}
	t.Setenv(EnvOnly, "")
	t.Setenv(EnvExclude, "")

	t.Setenv(EnvTypes, "desktop")
	t.Setenv(EnvOnly, "1")
	t.Setenv(EnvExclude, "2")
	if _, err := SelectedTypes(); err == nil {
		t.Fatal("SelectedTypes with all three set: want error, got nil")
	}
}

func mustJSON(t *testing.T, ev e2edata.Event) string {
	t.Helper()
	data, err := json.Marshal(ev)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return string(data)
}
