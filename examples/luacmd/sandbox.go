package luacmd

import (
	"context"

	lua "github.com/yuin/gopher-lua"
)

// The VM a script runs in, and the reasoning about what it can and cannot
// reach. A script here is not a plugin somebody vetted -- it arrives from
// whoever could write the journal and runs on a device holding a raft
// voter's identity -- so the interesting question is not "what should a
// script be able to do" but "what can it do that nobody intended".

// callStackSize and registrySize bound one run's Lua stacks. The call
// stack is the one that matters: unbounded recursion in a script would
// otherwise grow it until the process dies, and dying is a far worse
// answer than the script getting a "stack overflow" error it can be shown.
// Both are well above anything a command script legitimately needs and
// well below anything that threatens a phone.
const (
	callStackSize = 200
	registrySize  = 1024 * 20
)

// openLibs is every standard library a script gets. io, os, package,
// debug, coroutine and channel are never opened, so those names do not
// exist rather than existing and refusing.
//
// Not opening the package library is *not* what removes require, though --
// see removedGlobals. gopher-lua installs require and module from its base
// library (baselib.go), so a sandbox that skips the package library and
// stops there still hands a script both of them.
var openLibs = []struct {
	name string
	open lua.LGFunction
}{
	{lua.BaseLibName, lua.OpenBase},
	{lua.TabLibName, lua.OpenTable},
	{lua.StringLibName, lua.OpenString},
	{lua.MathLibName, lua.OpenMath},
}

// removedGlobals is what OpenBase installs that a sandbox must not keep.
//
// Two groups, for two different reasons:
//
//   - dofile, loadfile, require, module: read the filesystem of whichever
//     device is running the script. Nothing in this package's model says a
//     script may do that. require and module are the surprising two: they
//     look like they belong to the package library, which is never opened,
//     but gopher-lua installs them from its *base* library, so they are
//     present in an otherwise closed state until removed by name. A test
//     asserts each of these is nil, which is how that was found.
//
//   - load, loadstring, getfenv, setfenv, newproxy, _printregs: build or
//     re-point code and environments at runtime. A sandbox that can be
//     rewritten from inside is not one; these also defeat the "what a
//     script may call is decided before it starts" property everything
//     else here relies on.
//
// pcall and xpcall are deliberately *not* on this list, which is worth
// spelling out because the obvious reasoning says they should be: they
// catch Lua errors, gopher-lua signals a spent context by raising one, so
// a script that wrapped an endless loop in pcall would appear to be able
// to swallow its own deadline and run forever.
//
// It cannot, and the reason is in vm.go's mainLoopWithContext: when the
// context is done it raises *and returns out of the interpreter loop*. The
// loop stops; there is no next instruction to catch anything with, at any
// pcall depth. Checked rather than reasoned about -- four escape shapes
// (an unprotected outer loop, recursive re-entry, a fully pcall-wrapped
// nest, and repeat/until false) each stopped on a 200ms deadline instead
// of outliving it, and TestPcallCannotOutliveTheDeadline keeps one of them
// running against this sandbox so the property is not merely asserted
// here.
//
// print is likewise absent because it is replaced rather than removed --
// see installKV, which points it at kv.log so that a script author's
// reflex does the useful thing instead of writing to a daemon's stdout
// nobody is reading.
var removedGlobals = []string{
	"dofile", "loadfile", "require", "module",
	"load", "loadstring", "getfenv", "setfenv", "newproxy", "_printregs",
	"collectgarbage",
	"print",
}

// newState returns a Lua state with only openLibs opened, removedGlobals
// gone, and ctx installed so that the deadline (and a cancelled run) stops
// the script between instructions.
//
// Two limits this deliberately does not claim to enforce, both worth
// knowing before deciding what may be registered as a Lua command:
//
//   - Memory. gopher-lua has no allocation cap, so string.rep("x", 1e9)
//     is bounded by the device's RAM and nothing else. The deadline does
//     not help: allocating is fast.
//   - Instruction count. There is no public hook to count them, so
//     "how long may this run" is answered in wall-clock time only. That
//     is the more useful bound anyway for work that waits on other
//     commands, but it is not the same promise.
func newState(ctx context.Context) *lua.LState {
	state := lua.NewState(lua.Options{
		SkipOpenLibs:  true,
		CallStackSize: callStackSize,
		RegistrySize:  registrySize,
	})
	for _, lib := range openLibs {
		state.Push(state.NewFunction(lib.open))
		state.Push(lua.LString(lib.name))
		state.Call(1, 0)
	}
	for _, name := range removedGlobals {
		state.SetGlobal(name, lua.LNil)
	}
	state.SetContext(ctx)
	return state
}
