// Package e2erun implements the deploy-and-run engine behind the mage
// e2e:* targets: it turns pkg/e2edata's recorded rows into real actions --
// a real SSH-deployed bootstrap/leader node, real locally-spawned desktop
// kvnode processes, a real Playwright-driven browser check for web rows --
// and writes each row's outcome back into the file.
package e2erun

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/e2edata"
	"github.com/gofsd/libp2p-kv-raft/pkg/registry"
)

// EnvTypes is the environment variable Run reads (via SelectedTypes) to
// filter which test types actually execute, e.g.
// `E2E_TYPES=web,androidui mage e2e:all`. Unset or empty runs everything --
// mage e2e:all/e2e:current keep their documented zero-argument, "run
// everything" signature (see magefile.go's own doc comment on why mage
// targets take no optional/variadic args); this is additive filtering
// layered on top via the environment, the same pattern EnvE2EHome already
// uses for local desktop node isolation.
//
// EnvOnly/EnvExclude below are numeric alternatives to this same
// selection, for a caller who'd rather type `ONLY=1,2` than
// `E2E_TYPES=desktop,android` -- e.g. scripting a push-tag release against
// a fixed target list without hand-typing names. All three ultimately
// produce the same Types value; SelectedTypes rejects setting more than
// one of them at once rather than picking a silent precedence order (see
// its own doc comment).
const EnvTypes = "E2E_TYPES"

// EnvOnly/EnvExclude are EnvTypes' numeric counterparts: 1-based target
// numbers instead of names, in a fixed order (1=desktop, 2=android,
// 3=web, 4=androidui -- see targetNumber) chosen to match how these are
// usually talked about (the three platforms, then the UI-walk variant of
// android), not Types' own field declaration order. `ONLY=1,2` runs
// exactly desktop+android and nothing else; `EXCLUDE=2,3` runs everything
// except android and web (desktop+androidui here, since EXCLUDE starts
// from AllTypes and subtracts). Neither accepts an empty value the way
// EnvTypes does ("run everything") -- an empty ONLY/EXCLUDE is ambiguous
// (did the caller mean "no targets" or "forgot to set it"?), so
// SelectedTypes only ever consults them when actually non-empty and
// ParseOnly/ParseExclude reject an empty/all-whitespace value outright.
const (
	EnvOnly    = "ONLY"
	EnvExclude = "EXCLUDE"
)

// Types selects which of this pipeline's test types a Run actually
// executes. Desktop covers both PlatformDesktop and PlatformRemote rows --
// there's no separate "remote"/"relay" toggle, since the SSH-deployed
// bootstrap/relay leader is required infrastructure every other type joins
// through (EnsureBootstrap always runs regardless of what's selected here),
// not an independently runnable test of its own. Android and AndroidUI are
// separate because they're genuinely separate test mechanisms sharing one
// android identity (see runAndroidRows's doc comment): Android is the raw
// E2ETest wire-protocol rows recorded in testdata.json's Rows, AndroidUI is
// the catalog-driven UiCommandE2ETest walk (testdata.json's UICases) --
// either can be selected without the other.
type Types struct {
	Desktop   bool
	Web       bool
	Android   bool
	AndroidUI bool
}

// AllTypes is every type enabled -- Run's behavior when EnvTypes is unset,
// preserving mage e2e:all/e2e:current's original "run everything" behavior
// exactly.
func AllTypes() Types { return Types{Desktop: true, Web: true, Android: true, AndroidUI: true} }

// ParseTypes parses a comma-separated EnvTypes value ("desktop,web",
// "androidui", "android,androidui", ...) into a Types selection --
// case-insensitive, whitespace around each entry ignored. An empty string
// means "run everything" (AllTypes()), so unsetting/clearing the
// environment variable is the same as never having filtered anything.
func ParseTypes(s string) (Types, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return AllTypes(), nil
	}
	var t Types
	for part := range strings.SplitSeq(s, ",") {
		switch strings.ToLower(strings.TrimSpace(part)) {
		case "desktop":
			t.Desktop = true
		case "web":
			t.Web = true
		case "android":
			t.Android = true
		case "androidui", "android-ui", "android_ui":
			t.AndroidUI = true
		case "remote", "relay":
			// Not an independent toggle -- see Types' doc comment. Accepted
			// here (rather than rejected) only so spelling it out
			// explicitly for clarity ("desktop,relay") doesn't error.
		default:
			return Types{}, fmt.Errorf("e2erun: unknown %s entry %q (want desktop, web, android, androidui, or remote)", EnvTypes, part)
		}
	}
	return t, nil
}

// targetNumber maps one of ONLY/EXCLUDE's 1-based target numbers to the
// same type name ParseTypes/EnvTypes already accepts -- see EnvOnly's own
// doc comment for the chosen ordering and why it differs from Types'
// field order.
func targetNumber(n int) (string, bool) {
	switch n {
	case 1:
		return "desktop", true
	case 2:
		return "android", true
	case 3:
		return "web", true
	case 4:
		return "androidui", true
	default:
		return "", false
	}
}

// parseNumericTargets parses a comma-separated ONLY/EXCLUDE value ("1,2",
// "3", ...) into the type names targetNumber maps each entry to --
// case/whitespace handling matches ParseTypes. varName is EnvOnly or
// EnvExclude, used only to make an error message name the actual
// offending variable.
func parseNumericTargets(varName, s string) ([]string, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, fmt.Errorf("e2erun: %s must name at least one target number (1=desktop, 2=android, 3=web, 4=androidui)", varName)
	}
	var names []string
	for part := range strings.SplitSeq(s, ",") {
		part = strings.TrimSpace(part)
		n, err := strconv.Atoi(part)
		if err != nil {
			return nil, fmt.Errorf("e2erun: %s: %q is not a target number (want 1-4: 1=desktop, 2=android, 3=web, 4=androidui)", varName, part)
		}
		name, ok := targetNumber(n)
		if !ok {
			return nil, fmt.Errorf("e2erun: %s: %d is not a valid target number (want 1-4: 1=desktop, 2=android, 3=web, 4=androidui)", varName, n)
		}
		names = append(names, name)
	}
	return names, nil
}

// ParseOnly parses EnvOnly's numeric value into a Types selection --
// exactly the named targets, nothing else. Unlike ParseTypes, an
// empty/all-whitespace value is rejected rather than treated as "run
// everything" -- see EnvOnly's own doc comment.
func ParseOnly(s string) (Types, error) {
	names, err := parseNumericTargets(EnvOnly, s)
	if err != nil {
		return Types{}, err
	}
	return ParseTypes(strings.Join(names, ","))
}

// ParseExclude parses EnvExclude's numeric value into a Types selection --
// AllTypes() with the named targets subtracted out.
func ParseExclude(s string) (Types, error) {
	names, err := parseNumericTargets(EnvExclude, s)
	if err != nil {
		return Types{}, err
	}
	excluded, err := ParseTypes(strings.Join(names, ","))
	if err != nil {
		return Types{}, err
	}
	all := AllTypes()
	return Types{
		Desktop:   all.Desktop && !excluded.Desktop,
		Web:       all.Web && !excluded.Web,
		Android:   all.Android && !excluded.Android,
		AndroidUI: all.AndroidUI && !excluded.AndroidUI,
	}, nil
}

// SelectedTypes reads and parses EnvTypes/EnvOnly/EnvExclude from the
// environment -- the entry point magefile.go's E2E targets use. Exactly
// one of the three may be set (non-empty) at a time; setting more than
// one is rejected outright, naming which variables conflict, rather than
// silently picking one to win -- which one "wins" wouldn't be obvious
// from either variable's own value, and a caller who set two by mistake
// (e.g. a leftover E2E_TYPES from a previous run alongside a new ONLY)
// should find out before a real e2e pass runs against the wrong set, not
// after.
func SelectedTypes() (Types, error) {
	typesVal := strings.TrimSpace(os.Getenv(EnvTypes))
	onlyVal := strings.TrimSpace(os.Getenv(EnvOnly))
	excludeVal := strings.TrimSpace(os.Getenv(EnvExclude))

	var conflicting []string
	if typesVal != "" {
		conflicting = append(conflicting, EnvTypes)
	}
	if onlyVal != "" {
		conflicting = append(conflicting, EnvOnly)
	}
	if excludeVal != "" {
		conflicting = append(conflicting, EnvExclude)
	}
	if len(conflicting) > 1 {
		return Types{}, fmt.Errorf("e2erun: %s are mutually exclusive -- set only one", strings.Join(conflicting, " and "))
	}

	switch {
	case typesVal != "":
		return ParseTypes(typesVal)
	case onlyVal != "":
		return ParseOnly(onlyVal)
	case excludeVal != "":
		return ParseExclude(excludeVal)
	default:
		return AllTypes(), nil
	}
}

// Run executes every row in rowIndices (indices into f.Rows -- see
// e2edata.File.PendingRows/AllRowIndices) whose node's platform is enabled
// in types, updating each such row's Status/Error in place and saving f to
// path after every single row, so a crash or Ctrl-C mid-run still leaves
// already-recorded outcomes on disk instead of losing the whole batch. A
// row whose type isn't selected is left completely untouched -- its
// previously recorded Status/Error stands, it's neither re-run nor
// overwritten with a synthetic Skipped result, since "don't run this type"
// is a deliberate choice, not the same thing as this row having just
// failed to run.
//
// It always provisions/confirms the SSH bootstrap leader first (see
// EnsureBootstrap) since EventAdd rows join against it via BootstrapToken,
// and every type's rows dial through it -- this happens regardless of
// types, since there's no way to run *any* row without it. It returns an
// error if any non-skipped selected row ends up failing, or if
// types.AndroidUI is set and UiCommandE2ETest itself failed (that check
// has no f.Rows entry of its own to record a per-row outcome into -- see
// runAndroidRows' doc comment); the caller should still treat f/path as
// having the real, saved results even when this returns an error.
func Run(repoRoot, path string, f *e2edata.File, rowIndices []int, types Types) error {
	bootstrapMultiaddr, bootstrapWebTransportAddr, bootstrapPeerID, err := EnsureBootstrap(repoRoot, path, f)
	if err != nil {
		return fmt.Errorf("e2erun: ensure bootstrap: %w", err)
	}
	fmt.Fprintf(os.Stderr, "e2erun: bootstrap %s ready at %s (webtransport: %s)\n", bootstrapPeerID, bootstrapMultiaddr, bootstrapWebTransportAddr)

	kvnodeBin, kvctlBin, err := buildNativeBinaries(repoRoot)
	if err != nil {
		return err
	}

	// Reprovision any row whose node was deleted (mage
	// e2e:deletenode/destroyall) out from under it while the row itself
	// remained -- must happen before the grouping loop below, which reads
	// f.Nodes[row.Node].Platform to decide android/web/other and would
	// otherwise misroute (or the later per-row loop would report "unknown
	// node id") every such row. A row recorded before Row.Platform existed
	// and whose node was already gone by the time that backfill ran (see
	// Load) has nothing to recover from and is left alone -- it still hits
	// the existing "unknown node id" handling futher down, same as before
	// this existed.
	reprovisioned := false
	for _, idx := range rowIndices {
		row := &f.Rows[idx]
		if _, ok := f.Nodes[row.Node]; ok || row.Platform == "" {
			continue
		}
		if _, err := f.EnsureNode(row.Node, row.Platform); err != nil {
			return fmt.Errorf("e2erun: reprovision node %d (%s): %w", row.Node, row.Platform, err)
		}
		fmt.Fprintf(os.Stderr, "e2erun: reprovisioned node %d (%s) -- its previous identity was deleted but rows still reference it\n", row.Node, row.Platform)
		reprovisioned = true
	}
	if reprovisioned {
		if err := f.Save(path); err != nil {
			return err
		}
	}

	// Every known Android node identity needs relay standing on the
	// bootstrap before any of its rows can ever reach it -- see
	// GrantRelayAccess's doc comment on why a browser's very first "add"
	// otherwise hangs out the full relay-reservation timeout against a
	// freshly (re)provisioned bootstrap; Android needs the identical grant
	// for the identical reason: buildAndroidAAR bakes this same live
	// bootstrapMultiaddr in as *both* leaderMultiaddr and relayMultiaddr
	// (see that function's own ldflags), and an emulator with no port
	// forwarding is exactly as unreachable-without-a-relay-reservation as a
	// browser tab -- caught by e2e:release's real destroyAllFirst wiping the
	// bootstrap's group memberships and every Android row then failing with
	// "context deadline exceeded" (the whole app stuck retrying its own
	// AutoRelay reservation, never even answering local IPC calls) even
	// though desktop/remote rows, which dial the bootstrap directly, kept
	// passing.
	//
	// Android stays on this admin-side grant rather than the self-service
	// request web (runWebNode's own synthetic row 0) and desktop (runRow's
	// PlatformDesktop branch) now make for themselves: both of those have
	// an explicit window to request standing *before* the row that needs
	// it dials anything, but Android's regular rows run through
	// Kvmobile.start()'s baked-in, fully automatic join -- there's no
	// point in that sequence to slot an explicit RequestPublicAccess call
	// in ahead of it without restructuring E2ETest.kt's setup itself (the
	// same restructuring the pair scenario's own explicit
	// StartSoloWithKeyAndPort/StartPendingWithKeyAndPort calls *do* have
	// room for, and does use self-service -- see
	// runAndroidPairScenarioOn's own RequestRelayAccess steps).
	//
	// Granted unconditionally here (not gated on "did the bootstrap just
	// get reprovisioned") since it's a cheap, idempotent no-op against a
	// bootstrap that already has this standing recorded.
	for _, node := range f.Nodes {
		if node.Platform != e2edata.PlatformAndroid {
			continue
		}
		if err := GrantRelayAccess(bootstrapPeerID, node.PeerID); err != nil {
			return err
		}
	}

	var androidRowIdxs, webRowIdxs, otherRowIdxs, skippedByType []int
	for _, idx := range rowIndices {
		node := f.Nodes[f.Rows[idx].Node]
		switch node.Platform {
		case e2edata.PlatformAndroid:
			if types.Android {
				androidRowIdxs = append(androidRowIdxs, idx)
			} else {
				skippedByType = append(skippedByType, idx)
			}
		case e2edata.PlatformWeb:
			if types.Web {
				webRowIdxs = append(webRowIdxs, idx)
			} else {
				skippedByType = append(skippedByType, idx)
			}
		default: // PlatformDesktop, PlatformRemote, or an unknown node id (still surfaced below)
			if types.Desktop {
				otherRowIdxs = append(otherRowIdxs, idx)
			} else {
				skippedByType = append(skippedByType, idx)
			}
		}
	}
	if len(skippedByType) > 0 {
		fmt.Fprintf(os.Stderr, "e2erun: %d row(s) excluded by %s filter (left as previously recorded, not re-run)\n", len(skippedByType), EnvTypes)
	}

	// Android and web rows each run as one batch per node -- a real
	// gomobile bind + gradle install + instrumented test run per android
	// node (see runAndroidRows's doc comment), and a real Playwright run
	// per web node with that node's own recorded identity baked in (see
	// runWebRows's doc comment) -- rather than per-row like desktop/remote,
	// since rebuilding/reinstalling/relaunching a browser per row would be
	// prohibitively slow. Both are resolved up front here instead of
	// through runRow's per-row dispatch.
	androidResults, androidUICases, androidUIErr := runAndroidRows(repoRoot, f, androidRowIdxs, bootstrapMultiaddr, types.AndroidUI, f.UICases)
	webResults := runWebRows(repoRoot, f, webRowIdxs, bootstrapWebTransportAddr)

	failures := 0
	runRows := append(append(append([]int{}, androidRowIdxs...), webRowIdxs...), otherRowIdxs...)
	for _, idx := range runRows {
		row := &f.Rows[idx]
		node, ok := f.Nodes[row.Node]
		switch {
		case !ok:
			row.Status = e2edata.StatusFail
			row.Error = fmt.Sprintf("unknown node id %d", row.Node)
			failures++
		case node.Platform == e2edata.PlatformAndroid:
			outcome := androidResults[idx]
			row.Status = outcome.status
			row.Error = outcome.errMsg
			if outcome.status == e2edata.StatusFail {
				failures++
			}
		case node.Platform == e2edata.PlatformWeb:
			outcome := webResults[idx]
			row.Status = outcome.status
			row.Error = outcome.errMsg
			if outcome.status == e2edata.StatusFail {
				failures++
			}
		default:
			status, errMsg := runRow(kvnodeBin, kvctlBin, row.Node, node, bootstrapMultiaddr, row.Event)
			row.Status = status
			row.Error = errMsg
			if status == e2edata.StatusFail {
				failures++
			}
		}
		fmt.Fprintf(os.Stderr, "e2erun: row %d (version %d, node %d, event %s): %s\n",
			idx, row.Version, row.Node, row.Event.Op, statusName(row.Status))
		if err := f.Save(path); err != nil {
			return err
		}
	}

	if types.AndroidUI {
		result := &e2edata.AndroidUIResult{RanAt: time.Now(), Cases: androidUICases}
		if androidUIErr != nil {
			fmt.Fprintf(os.Stderr, "e2erun: android UI command test: FAIL: %v\n", androidUIErr)
			result.Status = e2edata.StatusFail
			result.Error = androidUIErr.Error()
			failures++
		} else {
			fmt.Fprintln(os.Stderr, "e2erun: android UI command test: PASS")
			result.Status = e2edata.StatusPass
		}
		f.AndroidUIResult = result
		if err := f.Save(path); err != nil {
			return err
		}
	}

	if types.AndroidUI {
		if pairResult := runAndroidPairScenario(repoRoot, bootstrapMultiaddr); pairResult != nil {
			if pairResult.Status == e2edata.StatusFail {
				fmt.Fprintf(os.Stderr, "e2erun: android join/recruit pair scenario: FAIL: %s\n", pairResult.Error)
				failures++
			} else {
				fmt.Fprintln(os.Stderr, "e2erun: android join/recruit pair scenario: PASS")
			}
			f.AndroidUIPairResult = pairResult
			if err := f.Save(path); err != nil {
				return err
			}
		}
	}

	if failures > 0 {
		return fmt.Errorf("e2erun: %d failure(s) (rows + checks)", failures)
	}
	return nil
}

func statusName(status int) string {
	switch status {
	case e2edata.StatusPass:
		return "PASS"
	case e2edata.StatusSkipped:
		return "SKIP"
	default:
		return "FAIL"
	}
}

// runRow dispatches one row to the right execution path: a PlatformRemote
// node goes over ssh; PlatformDesktop identities get a real local kvnode
// process and a real sendevent call. PlatformAndroid/PlatformWeb rows never
// reach here -- Run resolves them as a batch via runAndroidRows/runWebRows
// before this per-row loop even starts (see those functions' doc comments
// for why).
func runRow(kvnodeBin, kvctlBin string, nodeID int, node e2edata.Node, bootstrapMultiaddr string, ev e2edata.Event) (status int, errMsg string) {
	if ev.Op == "bootstrap_or_join_cluster" {
		resolved := ResolveBootstrapPlaceholder(ev.Fields["leader_addr"], bootstrapMultiaddr)
		// " learner" for a desktop node, the same marker and the same
		// reason android.go's own join carries it: this shared bootstrap
		// leader is long-lived and never torn down, while an e2e desktop
		// node is a laptop process that restarts between runs and sits
		// behind NAT on a link that comes and goes. Joined as a *voter* it
		// made the cluster two-voter -- majority of two is two, so every
		// one of those restarts took quorum with it, and this project's own
		// voterCountWarning says exactly that about the size. Confirmed in
		// the wild rather than reasoned about: the shared cluster was found
		// with the desktop node as leader and the VPS as its only other
		// voter, and rows across every platform were failing with
		// "leadership lost while committing log"/"not leader and no leader
		// known" whenever that laptop blinked. Learner keeps the sole voter
		// (and so leadership) on the stable public host, which is where
		// CLAUDE.md's connectivity policy says it belongs.
		if node.Platform == e2edata.PlatformDesktop {
			resolved += " learner"
		}
		ev = withField(ev, "leader_addr", resolved)
	}
	ev = expandEventFields(ev, ExpandRowValue)

	switch node.Platform {
	case e2edata.PlatformRemote:
		return retryReadsIfNeeded(ev, func() (int, string) { return sendEventRemote(node.PeerID, ev) })
	case e2edata.PlatformDesktop:
		if err := EnsureLocalDesktopNode(kvnodeBin, nodeID, node, bootstrapMultiaddr); err != nil {
			return e2edata.StatusFail, err.Error()
		}
		// Self-service, not the admin-side grant GrantRelayAccess gives
		// web/Android: a desktop e2e node already runs with -relay-peer
		// set to the same bootstrap (see EnsureLocalDesktopNode's own doc
		// comment on why -- a desktop dialing in over a real WAN link
		// needs exactly the same relay reservation web/Android do, this
		// sandbox's own direct-reachable path just happens not to
		// exercise that need). Requesting standing itself needs none, so
		// this can run before the row's own "add" ever does -- unlike
		// web/Android's regular flow, where the equivalent request would
		// have to happen *before* Start()'s own automatic join, and
		// nothing in that flow offers a window to do so without
		// restructuring it (see GrantRelayAccess's own doc comment).
		// Idempotent (PutPeerGroup is a plain set-membership write), so
		// calling it again for every row of an already-granted identity
		// is harmless -- matches GrantRelayAccess's own "unconditional,
		// not gated" reasoning.
		if bootstrapMultiaddr != "" {
			publicAccessEv := e2edata.Event{Op: "public_access", Fields: map[string]string{"target_peer": bootstrapMultiaddr}}
			if status, errMsg := sendEventLocal(kvctlBin, node.PeerID, publicAccessEv); status != e2edata.StatusPass {
				return e2edata.StatusFail, fmt.Sprintf("request public access: %s", errMsg)
			}
		}
		return retryReadsIfNeeded(ev, func() (int, string) { return sendEventLocal(kvctlBin, node.PeerID, ev) })
	default:
		return e2edata.StatusFail, fmt.Sprintf("unknown platform %q", node.Platform)
	}
}

// rowOutcome is a row's (status, error) pair -- the same shape
// e2edata.Row.Status/Error records, factored out so the android/web batch
// runners can return a batch of them keyed by row index.
type rowOutcome struct {
	status int
	errMsg string
}

// platformRowResult is the shape both E2ETest.kt (Android) and
// tests/e2e.spec.js (web) write their per-row results file as.
type platformRowResult struct {
	Index int    `json:"index"`
	Pass  bool   `json:"pass"`
	Error string `json:"error"`
}

// parseRowResults maps a platform driver's results JSON (indices into the
// per-node event list the caller built) back to rowIdxs (indices into
// f.Rows), filling in a failure for any row the driver never reported --
// e.g. because a row after it crashed the whole process. driverName is
// folded into that fallback message only.
func parseRowResults(resultsJSON []byte, rowIdxs []int, driverName string) (map[int]rowOutcome, error) {
	var parsed []platformRowResult
	if err := json.Unmarshal(resultsJSON, &parsed); err != nil {
		return nil, fmt.Errorf("e2erun: parse %s results: %w (raw: %s)", driverName, err, resultsJSON)
	}

	out := make(map[int]rowOutcome, len(rowIdxs))
	seen := make(map[int]bool, len(rowIdxs))
	for _, r := range parsed {
		if r.Index < 0 || r.Index >= len(rowIdxs) {
			continue
		}
		idx := rowIdxs[r.Index]
		seen[idx] = true
		if r.Pass {
			out[idx] = rowOutcome{status: e2edata.StatusPass}
		} else {
			out[idx] = rowOutcome{status: e2edata.StatusFail, errMsg: r.Error}
		}
	}
	for _, idx := range rowIdxs {
		if !seen[idx] {
			out[idx] = rowOutcome{status: e2edata.StatusFail, errMsg: driverName + " reported no result for this row"}
		}
	}
	return out, nil
}

// readRetryAttempts/readRetryDelay bound retryReadsIfNeeded's total wait to
// a few seconds -- generous relative to hashicorp/raft's own commit/apply
// latency on a real WAN link, but still a hard bound so a genuinely broken
// read fails the row instead of hanging.
const (
	readRetryAttempts = 10
	readRetryDelay    = 300 * time.Millisecond

	// writeLeaderRetryAttempts (~60s at readRetryDelay's 300ms cadence,
	// matching E2ETest.kt's WRITE_LEADER_RETRY_BUDGET_MS and
	// mobile/kvmobile's own callTimeout/relay-reservation timeouts for the
	// same device/link combination) covers the specific window confirmed
	// directly across several real runs: it's always the *first* forwarded
	// write in a session that hits this, right after a fresh join, before
	// this identity's own local raft.Raft() has received its first leader
	// announcement -- a later write in the same session never does. 10-
	// and 25-attempt budgets both proved not quite long enough for that
	// window. Not a workaround for a leader that's down for good.
	writeLeaderRetryAttempts = 200
)

// transientLeaderErrors are hashicorp/raft error strings that mean a write
// briefly hit the leader mid-election -- a real but transient condition,
// distinct from an application-level rejection (a bad signature, a
// rejected join, a genuinely missing key) that should still fail on the
// first try. Caught directly: a real android forwarded set_field row
// failed with "not leader and no leader known" at the exact moment the
// shared e2e bootstrap leader's own daemon.log logged "leadership lost
// while committing log" entries, traced to system-wide memory pressure
// from an unrelated process sharing that host (not a bug in this project's
// raft/forwarding code) -- see pkg/daemon.go's handleAdd/handleSet, which
// return these exact strings, and mobile/kvmobile/kvmobile.go's own doc
// comment on raftHeartbeatTimeout/raftElectionTimeout, which already
// documents "not leader and no leader known" as something "observed
// directly" against a real leader. Retrying is safe: add/set_key/set_field
// are naturally idempotent from this test's own perspective.
var transientLeaderErrors = []string{
	"leadership lost while committing log",
	"not leader and no leader known",
}

func isTransientLeaderError(errMsg string) bool {
	for _, s := range transientLeaderErrors {
		if strings.Contains(errMsg, s) {
			return true
		}
	}
	return false
}

// retryReadsIfNeeded retries dispatch a few times, with a short delay
// between attempts: unconditionally for EventGetField/EventGetKey -- a
// raft follower's local read can briefly lag just behind a Set that only
// just committed on the leader (see e.g. web-app/README.md's "Running it"
// section, which documents this same caveat for the browser client's own
// Get), so a GetField row placed right after the SetField row that wrote
// what it reads needs a little slack, not a hard requirement that
// replication has already caught up in zero time -- and, for every other
// event type, only if the failure is a transientLeaderError (see that
// var's doc comment): a real failure there (a bad signature, a rejected
// join, a genuinely missing key) still fails on the first try, unmasked.
func retryReadsIfNeeded(ev e2edata.Event, dispatch func() (int, string)) (int, string) {
	isRead := ev.Op == "get_field_by_key" || ev.Op == "get_field_by_registry" || ev.Op == "get_key"

	status, errMsg := dispatch()
	if status == e2edata.StatusPass {
		return status, errMsg
	}
	if !isRead && !isTransientLeaderError(errMsg) {
		return status, errMsg
	}

	attempts := readRetryAttempts
	if !isRead {
		attempts = writeLeaderRetryAttempts
	}
	for range attempts {
		time.Sleep(readRetryDelay)
		status, errMsg = dispatch()
		if status == e2edata.StatusPass {
			return status, errMsg
		}
		if !isRead && !isTransientLeaderError(errMsg) {
			return status, errMsg
		}
	}
	return status, errMsg
}

func sendEventLocal(kvctlBin, peerID string, ev e2edata.Event) (int, string) {
	argJSON, err := json.Marshal(ev)
	if err != nil {
		return e2edata.StatusFail, err.Error()
	}
	cmd := exec.Command(kvctlBin, "sendevent", peerID, string(argJSON))
	// KVSTORE_HOME points kvctl-cli's own registry.Open() at the same
	// e2e-isolated registry EnsureLocalDesktopNode writes peerID's entry
	// into (localE2EHome, not this process's own KVSTORE_HOME/default) --
	// see EnsureLocalDesktopNode's doc comment on why sendevent needs a
	// registry entry to exist at all now.
	if e2eHome, err := localE2EHome(); err == nil {
		cmd.Env = append(os.Environ(), registry.EnvHome+"="+e2eHome)
	}
	var stdout, stderr strings.Builder
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	runErr := cmd.Run()
	// kvctl-cli sendevent always prints the response JSON to stdout before
	// exiting -- including the EventError case, where it exits 1 *after*
	// printing (see cmd/kvctl-cli's cmdSendEvent) -- so a nonzero exit
	// isn't itself a reason to discard stdout; only fall back to the raw
	// process error when there's nothing on stdout to parse. Caught by
	// this producing a useless "exit status 1: " error (stderr always
	// empty on that path) against a real deployed node before this fix.
	return interpretSendEventResult(stdout.String(), stderr.String(), runErr)
}

func sendEventRemote(peerID string, ev e2edata.Event) (int, string) {
	argJSON, err := json.Marshal(ev)
	if err != nil {
		return e2edata.StatusFail, err.Error()
	}
	// KVSTORE_HOME points kvctl-cli at BootstrapRemoteDir/registry.json --
	// deployRegistryEntry's own doc comment on why that file (not the ssh
	// user's shared default) is what this needs to resolve peerID's IPC
	// token through.
	remoteCmd := fmt.Sprintf("KVSTORE_HOME=%s %s/bin/kvctl-cli sendevent %s %s", BootstrapRemoteDir, BootstrapRemoteDir, peerID, shellQuote(string(argJSON)))
	stdout, stderr, runErr := sshOutputAnyExit(BootstrapHost, remoteCmd)
	return interpretSendEventResult(stdout, stderr, runErr)
}

func interpretSendEventResult(stdout, stderr string, runErr error) (int, string) {
	stdout = strings.TrimSpace(stdout)
	var resp e2edata.Event
	if err := json.Unmarshal([]byte(stdout), &resp); err != nil {
		if runErr != nil {
			return e2edata.StatusFail, fmt.Sprintf("%v: %s", runErr, stderr)
		}
		return e2edata.StatusFail, fmt.Sprintf("parse sendevent output %q: %v", stdout, err)
	}
	if resp.Op == "error" {
		return e2edata.StatusFail, resp.Fields["message"]
	}
	return e2edata.StatusPass, ""
}

// buildNativeBinaries compiles kvnode/kvctl-cli for the host's own
// OS/ARCH (unlike EnsureBootstrap's linux/amd64 cross-compile), used to
// drive local desktop test nodes and local sendevent calls.
func buildNativeBinaries(repoRoot string) (kvnodeBin, kvctlBin string, err error) {
	binDir := filepath.Join(repoRoot, ".e2e-build", "native")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		return "", "", err
	}
	for _, pkg := range []string{"./cmd/kvnode", "./cmd/kvctl-cli"} {
		name := filepath.Base(pkg)
		out := filepath.Join(binDir, name)
		cmd := exec.Command("go", "build", "-o", out, pkg)
		cmd.Dir = repoRoot
		cmd.Stdout = os.Stderr
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			return "", "", fmt.Errorf("e2erun: build %s: %w", pkg, err)
		}
	}
	return filepath.Join(binDir, "kvnode"), filepath.Join(binDir, "kvctl-cli"), nil
}
