package luacmd_test

import (
	"strings"
	"testing"

	"github.com/gofsd/libp2p-kv-raft/examples/luacmd"
)

// TestCheckAcceptsAValidChunk covers the shape a real script has -- a
// couple of statements and a return -- rather than a bare expression,
// since a chunk is what Check actually compiles.
func TestCheckAcceptsAValidChunk(t *testing.T) {
	code := `
local who = kv.inputs.who or "nobody"
kv.log("hello from inner: " .. who)
return {fields = {status = "ok"}, narrative = "hello from inner: " .. who}
`
	if err := luacmd.Check(code); err != nil {
		t.Fatalf("Check rejected a valid chunk: %v", err)
	}
}

// TestCheckRejectsASyntaxError is the whole point of the function: the
// error has to name the problem well enough for whoever typed the script
// to fix it, so this asserts the line number survives rather than just
// that some error came back.
func TestCheckRejectsASyntaxError(t *testing.T) {
	err := luacmd.Check("local x = \nif then end\n")
	if err == nil {
		t.Fatal("Check accepted a chunk with a syntax error")
	}
	if !strings.Contains(err.Error(), "luacmd: compile script") {
		t.Errorf("error is not wrapped by this package: %v", err)
	}
	if !strings.Contains(err.Error(), "line") {
		t.Errorf("error does not locate the problem, so it cannot be acted on: %v", err)
	}
}

// TestCheckRejectsAnEmptyScript exists because an empty chunk is
// syntactically *valid* Lua -- it compiles fine and does nothing -- so
// without an explicit check, storing a script whose textarea was never
// filled in would succeed and then produce a command that silently
// returns nothing every time it runs.
func TestCheckRejectsAnEmptyScript(t *testing.T) {
	for _, code := range []string{"", "   ", "\n\t\n"} {
		if err := luacmd.Check(code); err == nil {
			t.Errorf("Check accepted empty script %q", code)
		}
	}
}

// TestCheckDoesNotRunTheScript guards the "without running any of it" half
// of Check's contract. A chunk whose top level would error immediately
// (and, here, could only do so by executing) must still compile clean --
// otherwise Check would be running arbitrary submitted code at the moment
// somebody merely saves it.
func TestCheckDoesNotRunTheScript(t *testing.T) {
	if err := luacmd.Check(`error("this must not be executed at compile time")`); err != nil {
		t.Fatalf("Check appears to execute the chunk: %v", err)
	}
}

// TestCheckCompilesInAClosedState pins the doc comment's claim that Check
// compiles in the same closed state the runner uses. A reference to a
// stdlib table that SkipOpenLibs never opened is a *runtime* lookup in
// Lua, not a compile-time one, so this must still compile -- proving the
// precheck stays a compile step and never becomes a "does this script only
// use allowed globals" check it cannot actually perform.
func TestCheckCompilesInAClosedState(t *testing.T) {
	if err := luacmd.Check(`local f = io.open("/etc/passwd") return f`); err != nil {
		t.Fatalf("Check rejected a chunk over an unopened library, which is a runtime concern: %v", err)
	}
}
