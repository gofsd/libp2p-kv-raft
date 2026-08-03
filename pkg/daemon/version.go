package daemon

import (
	"runtime"
	"runtime/debug"

	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// libp2pModulePath is the dependency currentBuildInfo looks up in this
// binary's own module graph (via runtime/debug.ReadBuildInfo) to report
// which go-libp2p release the running process was actually built against --
// the same question "is this deployed node using the latest version of this
// lib" comes down to for the transport this whole project is built on.
const libp2pModulePath = "github.com/libp2p/go-libp2p"

// currentBuildInfo answers shmevent.EventGetVersion: it reads this running
// process's own Go build stamping (populated automatically by the toolchain
// whenever the binary was built from within a git checkout, which every
// build path this project ships -- `go build`, mage's Build/BuildLinux/
// BuildWindows/BuildAndroid -- goes through) rather than anything baked in
// at compile time via -ldflags -X, so it needs no build-system changes to
// stay accurate and works identically for `go run`/`go test` too. Called
// fresh on every request (like currentBuildInfo's sibling advertisedAddrs):
// cheap -- debug.ReadBuildInfo just returns the process's own
// already-computed build metadata, no I/O -- and this way a value never
// goes stale relative to whatever process is actually answering.
func currentBuildInfo() shmevent.VersionInfo {
	info := shmevent.VersionInfo{GoVersion: runtime.Version()}
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return info
	}
	for _, s := range bi.Settings {
		switch s.Key {
		case "vcs.revision":
			info.Commit = s.Value
		case "vcs.time":
			info.BuildTime = s.Value
		case "vcs.modified":
			info.Dirty = s.Value == "true"
		}
	}
	for _, dep := range bi.Deps {
		if dep.Path == libp2pModulePath {
			info.Libp2pVersion = dep.Version
			break
		}
	}
	return info
}
