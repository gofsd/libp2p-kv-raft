//go:build mage

package main

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"github.com/magefile/mage/mg"

	"github.com/gofsd/libp2p-kv-raft/pkg/e2edata"
	"github.com/gofsd/libp2p-kv-raft/pkg/e2erun"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// E2E groups the end-to-end test/deploy pipeline behind `mage e2e:<method>`
// -- see pkg/e2edata for the single testdata file format (versions, node
// identities, test rows) and pkg/e2erun for what actually deploying and
// running a row means per platform.
//
// This replaced a stub `E2e()` target that ran `go test -tags=e2e ./...`
// against a build tag no file in this repo ever used -- i.e. it always
// silently did nothing. mg.Namespace also can't coexist with a same-named
// bare function target (mage's CLI target discovery would collide "e2e"
// the function against "e2e:" the namespace prefix), so replacing it was
// required, not optional, to add these targets at all.
type E2E mg.Namespace

func testdataPath() (string, error) {
	root, err := repoRoot()
	if err != nil {
		return "", err
	}
	return filepath.Join(root, e2edata.DefaultPath), nil
}

// NewVersion records a new e2e test version stamped with this repo's
// current semver (from git tags -- the same version `mage patch`/`minor`/
// `major` manage), shared across every platform's implementation since
// this is one monorepo with one release version, not per-platform version
// numbers (see e2edata.File.Versions' doc comment). Subsequent
// `e2e:addtest` calls target it instead of whatever version was current
// before.
//
// Usage: mage e2e:newversion
func (E2E) NewVersion() error {
	v, err := getCurrentVersion()
	if err != nil {
		return err
	}
	path, err := testdataPath()
	if err != nil {
		return err
	}
	f, err := e2edata.Load(path)
	if err != nil {
		return err
	}
	id := f.NewVersion(v.String())
	if err := f.Save(path); err != nil {
		return err
	}
	fmt.Printf("✅ version %d: %s\n", id, v.String())
	return nil
}

// AddNode generates a fresh deterministic Ed25519 identity for platform
// ("desktop", "android", "web", or "remote" -- the SSH-deployed bootstrap
// leader, though e2e:bootstrap provisions that one automatically if it
// doesn't exist yet, so this is rarely needed for "remote" directly) and
// records it, printing the node id later e2e:addtest calls reference.
//
// Usage: mage e2e:addnode <platform>
func (E2E) AddNode(platform string) error {
	path, err := testdataPath()
	if err != nil {
		return err
	}
	f, err := e2edata.Load(path)
	if err != nil {
		return err
	}
	id, node, err := f.AddNode(e2edata.Platform(platform))
	if err != nil {
		return err
	}
	if err := f.Save(path); err != nil {
		return err
	}
	fmt.Printf("✅ node %d (%s): %s\n", id, node.Platform, node.PeerID)
	return nil
}

// AddTest appends a test row against the current (not yet published)
// version -- creating version 1 automatically if none exists yet -- that
// sends one raw pkg/shmevent to nodeID: eventName is the event's name (see
// pkg/shmevent.EventName -- "set_key", "set_field", "get_key", "get_field",
// "get_public_key", "get_private_key", "add"), id is this message's own
// correlation id (pick a nonzero value and reuse it as a later row's
// sourceID/destID to link them -- e.g. a set_key row with id=100 followed
// by a set_field row with sourceID=100 -- since pkg/e2erun dispatches rows
// through kvctl-cli sendevent, which only randomizes an id left at its
// zero value, an explicit id here is preserved exactly through to the
// wire), sourceID/destID are the relational reference fields (0 for
// unused), and value is plain text (see pkg/e2edata.Event's doc comment on
// how binary values are represented). An "add" row's value may be the
// literal string "BOOTSTRAP" to mean "the live bootstrap leader's address,
// whatever it is at run time" (see pkg/e2erun.BootstrapToken) instead of a
// frozen address.
//
// Usage: mage e2e:addtest <nodeID> <eventName> <id> '<fieldsJSON>'
// fieldsJSON is a JSON object of the event's own named fields (see
// api/shmevent.capnp's per-variant field lists, or e2edata.Event's doc
// comment) -- e.g. `{"key":"hello","value":"world"}` for set, or `{}`/
// omitted for a no-field event like leave. "" is shorthand for `{}`.
func (E2E) AddTest(nodeID int, eventName string, id int, fieldsJSON string) error {
	if _, ok := shmevent.EventFromName(eventName); !ok {
		return fmt.Errorf("e2e:addtest: unknown event name %q", eventName)
	}
	var fields map[string]string
	if fieldsJSON != "" {
		if err := json.Unmarshal([]byte(fieldsJSON), &fields); err != nil {
			return fmt.Errorf("e2e:addtest: parse fields json: %w", err)
		}
	}
	path, err := testdataPath()
	if err != nil {
		return err
	}
	f, err := e2edata.Load(path)
	if err != nil {
		return err
	}
	ev := e2edata.Event{Op: eventName, ID: uint16(id), Fields: fields}
	if _, err := ev.ToMsg(); err != nil {
		return fmt.Errorf("e2e:addtest: %w", err)
	}
	row, err := f.AddTest(nodeID, ev)
	if err != nil {
		return err
	}
	if err := f.Save(path); err != nil {
		return err
	}
	fmt.Printf("✅ added row: version %d, node %d, event %s\n", row.Version, row.Node, eventName)
	return nil
}

// DeleteNode tears down whatever real process/data nodeID's platform has
// running -- a local kvnode process for a desktop node, or the SSH
// bootstrap daemon and its entire remote directory for the remote node
// (see pkg/e2erun.DeleteNode) -- then removes it from the testdata file.
// Nodes are never deleted automatically by e2e:current/e2e:all, precisely
// so a deployed node stays around for a human to poke at; this is the
// explicit, deliberate teardown command for when that's no longer wanted.
//
// Usage: mage e2e:deletenode <nodeID>
func (E2E) DeleteNode(nodeID int) error {
	path, err := testdataPath()
	if err != nil {
		return err
	}
	f, err := e2edata.Load(path)
	if err != nil {
		return err
	}
	if err := e2erun.DeleteNode(f, nodeID); err != nil {
		return err
	}
	if err := f.Save(path); err != nil {
		return err
	}
	fmt.Printf("✅ node %d deleted\n", nodeID)
	return nil
}

// DestroyAll tears down every node currently in the testdata file at once --
// the same real teardown DeleteNode does (kill the local desktop process,
// kill the SSH bootstrap daemon and wipe its whole remote directory), just
// for every node id instead of naming one. Android/web nodes have no
// persistent process this pipeline manages, so "destroying" them is just
// removing their testdata.json entry (see pkg/e2erun.DeleteNode's doc
// comment).
//
// Saves the file even if some node's teardown failed, so whichever nodes
// *did* get torn down aren't left looking like they're still around --
// e2erun.DeleteAllNodes's own doc comment covers why one failure doesn't
// stop the rest.
//
// Usage: mage e2e:destroyall
func (E2E) DestroyAll() error {
	path, err := testdataPath()
	if err != nil {
		return err
	}
	f, err := e2edata.Load(path)
	if err != nil {
		return err
	}
	destroyErr := e2erun.DeleteAllNodes(f)
	if err := f.Save(path); err != nil {
		return err
	}
	if destroyErr != nil {
		return destroyErr
	}
	fmt.Println("✅ all nodes destroyed")
	return nil
}

// GC prunes desktop/android/web e2e nodes that no row from the last
// keepVersions versions (default 3 if <=0) references -- the "which nodes
// are actually stale" judgment call DeleteNode/DestroyAll leave entirely to
// a human today, with no way to answer it short of reading every row by
// hand. Never touches the PlatformRemote node: it's the one shared
// long-lived bootstrap leader every other node joins (see BootstrapHost's
// doc comment), not a per-run artifact, so it's never a GC candidate
// regardless of which versions still reference it.
//
// Dry-run by default -- prints the candidate node ids and returns without
// deleting anything. Pass "yes" as the second argument to actually tear
// them down (the same real teardown DeleteNode performs), matching this
// pipeline's existing rule that node teardown is always explicit and
// human-invoked, never silently automatic.
//
// Usage: mage e2e:gc <keepVersions> ""     (dry run: lists what would be pruned)
// Usage: mage e2e:gc <keepVersions> yes    (actually deletes)
//
// mage requires every positional argument on the command line (no true
// optional trailing args -- confirmed against this repo's own
// e2e:channelfiletransfer, whose doc comment reads "[sizeBytes]" but still
// errors "not enough arguments" if the CLI call omits it entirely), so the
// dry-run call must pass "" explicitly rather than being invoked with just
// keepVersions.
func (E2E) GC(keepVersions int, confirm string) error {
	if keepVersions <= 0 {
		keepVersions = 3
	}
	path, err := testdataPath()
	if err != nil {
		return err
	}
	f, err := e2edata.Load(path)
	if err != nil {
		return err
	}

	cutoff := f.CurrentVersion() - keepVersions
	keep := make(map[int]bool)
	for _, r := range f.Rows {
		if r.Version > cutoff {
			keep[r.Node] = true
		}
	}

	var candidates []int
	for id, node := range f.Nodes {
		if node.Platform == e2edata.PlatformRemote || keep[id] {
			continue
		}
		candidates = append(candidates, id)
	}
	sort.Ints(candidates)

	if len(candidates) == 0 {
		fmt.Println("✅ no stale nodes (nothing outside the last", keepVersions, "version(s))")
		return nil
	}

	if confirm != "yes" {
		fmt.Printf("e2e:gc: %d node(s) referenced by no row newer than version %d:\n", len(candidates), cutoff)
		for _, id := range candidates {
			fmt.Printf("  node %d (%s): %s\n", id, f.Nodes[id].Platform, f.Nodes[id].PeerID)
		}
		fmt.Println("dry run -- re-run as `mage e2e:gc", keepVersions, "yes` to actually delete these")
		return nil
	}

	var errs []string
	for _, id := range candidates {
		if err := e2erun.DeleteNode(f, id); err != nil {
			errs = append(errs, fmt.Sprintf("node %d: %v", id, err))
			continue
		}
		fmt.Printf("✅ node %d pruned\n", id)
	}
	if err := f.Save(path); err != nil {
		return err
	}
	if len(errs) > 0 {
		return fmt.Errorf("e2e:gc: failed to prune %d node(s):\n%s", len(errs), strings.Join(errs, "\n"))
	}
	return nil
}

// Bootstrap deploys (or confirms already running, idempotently) the shared
// e2e bootstrap/leader node on the SSH server -- see
// pkg/e2erun.EnsureBootstrap for exactly what that involves and how it
// avoids disturbing any other node already running there.
//
// Usage: mage e2e:bootstrap
func (E2E) Bootstrap() error {
	root, err := repoRoot()
	if err != nil {
		return err
	}
	path, err := testdataPath()
	if err != nil {
		return err
	}
	f, err := e2edata.Load(path)
	if err != nil {
		return err
	}
	multiaddr, webTransportAddr, peerID, err := e2erun.EnsureBootstrap(root, path, f)
	if err != nil {
		return err
	}
	if err := f.Save(path); err != nil {
		return err
	}
	fmt.Printf("✅ bootstrap %s ready at %s (webtransport: %s)\n", peerID, multiaddr, webTransportAddr)
	return nil
}

// BootstrapAll ensures every node currently recorded in the testdata file
// has its real process up and running, without running any test rows: the
// SSH-deployed remote leader (same as e2e:bootstrap -- and, same as that
// command, this provisions a fresh remote identity first if the file has
// none yet), plus a real local kvnode process for every desktop node.
// Android/web nodes have no persistent process to pre-start -- see
// pkg/e2erun.EnsureAllDesktopNodes's doc comment -- e2e:current/e2e:all
// drive those fresh each time regardless.
//
// Usage: mage e2e:bootstrapall
func (E2E) BootstrapAll() error {
	root, err := repoRoot()
	if err != nil {
		return err
	}
	path, err := testdataPath()
	if err != nil {
		return err
	}
	f, err := e2edata.Load(path)
	if err != nil {
		return err
	}
	multiaddr, webTransportAddr, peerID, err := e2erun.EnsureBootstrap(root, path, f)
	if err != nil {
		return err
	}
	if err := f.Save(path); err != nil {
		return err
	}
	fmt.Printf("✅ bootstrap %s ready at %s (webtransport: %s)\n", peerID, multiaddr, webTransportAddr)

	return e2erun.EnsureAllDesktopNodes(root, f, multiaddr)
}

// Current runs only the rows recorded since the last published version --
// what should run before every routine push (see the pre-push hook in
// scripts/git-hooks/pre-push). On full success it advances
// PublishedVersion, so the next e2e:current only covers whatever's new
// again -- this is the "version increment based on new tests" behavior. In
// particular, running e2e:current a second time with nothing new since the
// last success is a true no-op (prints "no rows to run" and returns
// immediately). Android's real command-execution UI surface
// (RunCode/real-camera scan, see pkg/e2erun/android_optical.go) is never
// part of this automated pipeline at all -- it needs a real camera, so it's
// manual/hardware-gated (see TestManualOpticalScan's own doc comment), run
// directly via `go test`, not through any mage e2e target. Set E2E_TYPES
// (comma-separated: desktop, web, android) to run only some test types
// instead of everything -- see pkg/e2erun.ParseTypes's doc comment;
// unset/empty runs everything, same as before this existed. ONLY/EXCLUDE
// are numeric alternatives to E2E_TYPES -- 1=desktop, 2=android, 3=web (see
// pkg/e2erun.EnvOnly's doc comment) -- ONLY=1,2 runs exactly
// desktop+android, EXCLUDE=2,3 runs everything except android+web (so
// desktop only). Setting more than one of E2E_TYPES/ONLY/EXCLUDE at once is
// an error, not a silently-resolved precedence.
//
// Usage: mage e2e:current
// Usage: E2E_TYPES=web mage e2e:current
// Usage: ONLY=1,2 mage e2e:current
// Usage: EXCLUDE=2,3 mage e2e:current
func (E2E) Current() error {
	return runE2ERows(func(f *e2edata.File) []int { return f.PendingRows() }, e2eRunOptions{
		markPublishedOnSuccess: true,
	})
}

// All runs every recorded test row across every version, regardless of
// what's already published -- a full regression pass. Never advances
// PublishedVersion and never tears down existing nodes first (unlike
// Release below), so running it manually for a spot check neither changes
// what the next e2e:current considers "new" nor destroys whatever nodes are
// already sitting around from prior runs. Set E2E_TYPES (or numeric
// ONLY/EXCLUDE) the same way Current does to narrow which test types
// actually run.
//
// Usage: mage e2e:all
// Usage: E2E_TYPES=android mage e2e:all
// Usage: ONLY=1 mage e2e:all
// Usage: EXCLUDE=3 mage e2e:all
func (E2E) All() error {
	return runE2ERows(func(f *e2edata.File) []int { return f.AllRowIndices() }, e2eRunOptions{})
}

// Release runs the same full regression pass as All -- every recorded row
// -- but first destroys every existing e2e node (the same real teardown
// `mage e2e:destroyall` does), and advances PublishedVersion on success.
// This is the gate the pre-push hook (see scripts/git-hooks/pre-push) runs
// instead of e2e:current whenever the push includes a version tag (`mage
// patch`/`minor`/`major`/`alpha`/`beta`/`rc` followed by `git push
// --tags`): a version bump is exactly the point where the whole suite
// should be reconfirmed from a genuinely clean slate, not just replayed
// against whatever nodes happened to survive routine e2e:current runs
// during development. Every row's node is transparently reprovisioned
// under a fresh identity before it runs (see e2erun.Run's reprovision
// step, the same recovery path `mage e2e:deletenode`/`destroyall` already
// rely on) -- there is no separate `mage e2e:destroyall` step to run by
// hand first. Set E2E_TYPES (or numeric ONLY/EXCLUDE) the same way
// Current/All do -- e.g. `ONLY=1,2 mage patch && git push --tags` runs the
// version-tag release gate against just desktop+android, skipping web for
// that push. This is the one place a caller reaching for "skip some
// targets on this particular push" most likely lands, but the variables
// work identically across Current/All/Release.
//
// Usage: mage e2e:release
// Usage: E2E_TYPES=android mage e2e:release
// Usage: ONLY=1,2 mage e2e:release
// Usage: EXCLUDE=2,3 mage e2e:release
func (E2E) Release() error {
	return runE2ERows(func(f *e2edata.File) []int { return f.AllRowIndices() }, e2eRunOptions{
		markPublishedOnSuccess: true,
		destroyAllFirst:        true,
	})
}

// ChannelFileTransfer drives a real, large, bidirectional Raw Channel file
// transfer between a fresh local desktop node and a connected Android
// emulator/device (desktop -> android, then android -> desktop, each
// verified byte-for-byte via SHA-256) -- see
// pkg/e2erun.RunChannelFileTransferScenario's own doc comment for the full
// mechanism, and README's "Raw Channel"/"Data plane: pkg/chandata"
// sections for what it's actually proving. sizeBytes defaults to 1GiB
// (1073741824) if 0/omitted -- pass a smaller value for a quick sanity
// check before committing to the real thing. Requires gomobile + adb + a
// connected android device/emulator, the same prerequisites the android
// e2e commands above need. Deliberately never part of e2e:current/e2e:all:
// it has no recorded row/version of its own in testdata.json (this is a
// live capability check, not a replayable regression row), and a full
// 1GiB transfer is far too slow for a routine pre-push gate -- run it
// directly whenever this codebase's Channel data plane itself needs
// verifying against real hardware. Every file either side creates
// (desktop's generated source file, android's own generated source file
// and received copy) is deleted before this returns, pass or fail; the
// throwaway desktop node this spins up is also stopped and its data dir
// removed, unlike this project's other e2e nodes (see that function's own
// doc comment on why this one specifically is never left running).
//
// Usage: mage e2e:channelfiletransfer [sizeBytes]
func (E2E) ChannelFileTransfer(sizeBytes int) error {
	if sizeBytes <= 0 {
		sizeBytes = 1 << 30 // 1GiB
	}
	root, err := repoRoot()
	if err != nil {
		return err
	}
	return e2erun.RunChannelFileTransferScenario(root, int64(sizeBytes))
}

// e2eRunOptions is runE2ERows' behavior switch set -- see Current/All/
// Release's own doc comments for what each combination means in practice.
type e2eRunOptions struct {
	// markPublishedOnSuccess advances PublishedVersion to CurrentVersion
	// once every selected row passes.
	markPublishedOnSuccess bool
	// destroyAllFirst tears down every existing e2e node (the same real
	// teardown e2e:destroyall does) before running -- see Release's doc
	// comment on why a version-bump gate wants a genuinely clean slate
	// rather than reusing whatever nodes survived prior runs.
	destroyAllFirst bool
}

func runE2ERows(selectRows func(*e2edata.File) []int, opts e2eRunOptions) error {
	types, err := e2erun.SelectedTypes()
	if err != nil {
		return err
	}
	root, err := repoRoot()
	if err != nil {
		return err
	}
	path, err := testdataPath()
	if err != nil {
		return err
	}
	f, err := e2edata.Load(path)
	if err != nil {
		return err
	}

	if opts.destroyAllFirst && len(f.Nodes) > 0 {
		fmt.Println("e2e: destroying all existing nodes for a clean run...")
		destroyErr := e2erun.DeleteAllNodes(f)
		if err := f.Save(path); err != nil {
			return err
		}
		if destroyErr != nil {
			return destroyErr
		}
	}

	rows := selectRows(f)
	if len(rows) == 0 {
		fmt.Println("✅ no rows to run")
		return nil
	}
	runErr := e2erun.Run(root, path, f, rows, types)
	if runErr == nil && opts.markPublishedOnSuccess {
		f.MarkPublished()
		if err := f.Save(path); err != nil {
			return err
		}
	}
	return runErr
}
