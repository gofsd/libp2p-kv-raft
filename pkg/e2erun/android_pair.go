package e2erun

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	lp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"

	"github.com/gofsd/libp2p-kv-raft/pkg/e2edata"
)

// loopbackTCPAddrRe matches GetOwnAddr's own last-resort fallback shape,
// "/ip4/127.0.0.1/tcp/<port>/p2p/<peerid>" -- the only address either of
// this scenario's throwaway devices ever has, since neither is configured
// with any relay peers (see runAndroidPairScenarioOn's AAR-build comment)
// and an emulator has no real public address of its own either.
var loopbackTCPAddrRe = regexp.MustCompile(`^/ip4/127\.0\.0\.1/tcp/(\d+)/p2p/(.+)$`)

// forwardLoopbackAddr rewrites addr (serial's own GetOwnAddr result) into
// one a *different* emulator can actually dial. Two separate Android
// emulator instances are network-isolated from each other by default --
// serial's own 127.0.0.1 means nothing to any other emulator -- but every
// emulator can reach the host machine via the fixed alias 10.0.2.2, so
// `adb forward tcp:0 tcp:<devicePort>` (letting adb allocate a free host
// port) plus rewriting the address to 10.0.2.2:<that host port> bridges
// the two emulators' otherwise-separate networks through the host
// running this very process. Returns the rewritten address and a cleanup
// func that removes the forward once this scenario is done with it.
func forwardLoopbackAddr(serial, addr string) (string, func(), error) {
	m := loopbackTCPAddrRe.FindStringSubmatch(addr)
	if m == nil {
		return "", nil, fmt.Errorf("address %q is not a /ip4/127.0.0.1/tcp/<port>/p2p/<peerid> loopback address this scenario knows how to bridge between emulators", addr)
	}
	devicePort, peerID := m[1], m[2]
	out, err := exec.Command("adb", "-s", serial, "forward", "tcp:0", "tcp:"+devicePort).Output()
	if err != nil {
		return "", nil, fmt.Errorf("adb -s %s forward tcp:0 tcp:%s: %w", serial, devicePort, err)
	}
	hostPort := strings.TrimSpace(string(out))
	cleanup := func() {
		_ = exec.Command("adb", "-s", serial, "forward", "--remove", "tcp:"+hostPort).Run()
	}
	return fmt.Sprintf("/ip4/10.0.2.2/tcp/%s/p2p/%s", hostPort, peerID), cleanup, nil
}

// androidPairApp mirrors android.go's androidAppID constant -- kept as a
// separate name here only so this file reads self-contained about which
// app it's driving.
const androidPairApp = androidAppID

// pairKeyHex converts a freshly generated e2edata identity's raw ed25519
// private key into the hex-encoded, libp2p-marshaled format
// StartSoloWithKey/StartPendingWithKey's own keyHex parameter expects
// (mobile/kvmobile's importIdentity: hex.DecodeString then
// crypto.UnmarshalPrivateKey) -- the same conversion
// e2edata.WriteDesktopKeyFile applies before writing a key *file*, just
// returned as a string here instead, since these two identities are
// typed into a UI field, never written to a desktop-style key file.
func pairKeyHex(priv []byte) (string, error) {
	lp2pPriv, err := lp2pcrypto.UnmarshalEd25519PrivateKey(priv)
	if err != nil {
		return "", fmt.Errorf("unmarshal ed25519 private key: %w", err)
	}
	marshaled, err := lp2pcrypto.MarshalPrivateKey(lp2pPriv)
	if err != nil {
		return "", fmt.Errorf("marshal libp2p key: %w", err)
	}
	return hex.EncodeToString(marshaled), nil
}

// uiOp is one CommandCatalog.kt command to run within a single
// instrumentation invocation -- label must exactly match a real
// CommandSpec.label.
type uiOp struct {
	label  string
	inputs []string
}

// pairStep is one logical action within runAndroidPairScenario's fixed
// sequence, run against one device. Every `adb shell am instrument`
// invocation is a genuinely fresh process (confirmed live: kvmobile's
// package-level session state does NOT survive between them, unlike an
// ordinary long-running Android app process a user would keep in the
// foreground) -- so any step past a device's very first touch must
// re-issue that device's own "resume" call (StartSoloWithKey/
// StartPendingWithKey, both safe to call again against an already-
// provisioned dataDir -- see mobile/kvmobile.isAlreadyBootstrappedErr and
// StartPending's own doc comment on never re-sending EventAdd) as a
// prefix op in the *same* invocation, before the step's real action --
// resumeOp is nil only for each device's first step, where the action
// itself already is that device's initial StartSoloWithKey/
// StartPendingWithKey call.
type pairStep struct {
	reportAs string
	resumeOp *uiOp
	// action is built lazily -- called only once this step actually runs,
	// since several steps' own inputs (a ticket string) are only known
	// once an *earlier* step's validate callback has captured it. A
	// plain uiOp value here (evaluated once, at steps-slice-construction
	// time, before any step has actually run) was the exact bug an
	// earlier version of this file had: every ticket-carrying input came
	// through empty, captured before the value it needed existed yet.
	action   func() uiOp
	serial   string
	validate func(output string) error
	// dialHold, when set, marks this step as a cross-device dial: serial
	// is about to connect *out* to dialHold's own device, which needs to
	// still be up and listening at that exact moment -- see runDialStep's
	// doc comment for why a plain serial step (the treatment every other
	// entry in this slice gets) can never satisfy that for a dial
	// specifically.
	dialHold *pairHold
}

// pairHold names the device (and its resume op) a dial step's own action
// needs kept alive concurrently -- see runDialStep.
type pairHold struct {
	serial string
	resume uiOp
}

// runAndroidPairScenario drives a genuine two-device Join/RecruitPeer
// round trip -- see this package's own design notes (README.md's e2e
// section) for why this can't be expressed as ordinary android_ui_cases
// entries (ordering + cross-device data threading, not a flat
// independent-per-label sweep). Returns nil (log a message, don't fail
// the run) if fewer than three devices/emulators are currently connected
// -- this scenario is opt-in by device count, not part of every e2e run.
//
// Deliberately three, not two: runAndroidRows' own existing single-device
// flow (still unqualified-serial-free at every *other* call site, see
// that function's own comment) always claims connectedAndroidSerials()'s
// first entry for the long-lived shared android node -- the one whose
// real, valuable persisted raft membership must never be touched. This
// scenario's own pm clear step is destructive by design (see below), so
// it skips index 0 unconditionally and only ever uses two *other*
// devices, rather than risk wiping that shared node's data out from
// under the very flow that depends on it surviving across runs.
func runAndroidPairScenario(repoRoot string) *e2edata.AndroidPairResult {
	serials, err := connectedAndroidSerials()
	if err != nil {
		fmt.Fprintf(os.Stderr, "e2erun: android pair scenario: skipped: %v\n", err)
		return nil
	}
	if len(serials) < 3 {
		fmt.Fprintf(os.Stderr, "e2erun: android pair scenario: skipped: needs 3 connected devices/emulators (1 reserved for the shared android node + 2 for this scenario), found %d\n", len(serials))
		return nil
	}
	return runAndroidPairScenarioOn(repoRoot, serials[1], serials[2])
}

// runAndroidPairScenarioOn is runAndroidPairScenario's actual mechanism,
// against two explicitly named serials -- split out from the device-count/
// reservation policy above so it can also be driven directly (e.g. by a
// developer with only two spare devices/emulators available, accepting
// the responsibility of picking two that aren't the shared android node
// themselves) without needing a third device just to exercise the same
// step sequence.
func runAndroidPairScenarioOn(repoRoot string, serialA, serialB string) *e2edata.AndroidPairResult {
	result := &e2edata.AndroidPairResult{RanAt: time.Now()}
	fail := func(err error) *e2edata.AndroidPairResult {
		result.Status = e2edata.StatusFail
		result.Error = err.Error()
		return result
	}

	// Full uninstall, not just `pm clear`: these two devices are
	// throwaway and get a fresh install every run, so nothing here needs
	// to survive -- and unlike a data-only clear, uninstall also recovers
	// from a device that already has androidPairApp (or its
	// androidTest counterpart, installed as "<androidPairApp>.test")
	// installed under a *different* signing key than this run's build
	// will produce (e.g. a previous run's or another machine's debug
	// keystore) -- confirmed live: a leftover mismatched-signature
	// install makes gradleInstall's installDebug/installDebugAndroidTest
	// fail outright with INSTALL_FAILED_UPDATE_INCOMPATIBLE, which
	// `pm clear` alone can never recover from since it never removes the
	// existing APK/certificate.
	for _, serial := range []string{serialA, serialB} {
		for _, pkg := range []string{androidPairApp, androidPairApp + ".test"} {
			uninstall := exec.Command("adb", "uninstall", pkg)
			withSerial(uninstall, serial)
			_ = uninstall.Run() // best-effort: nothing to uninstall on a first-ever run
		}
	}

	// One build, installed onto both devices -- neither StartSoloWithKey
	// nor StartPendingWithKey reference build-time identity/leader at
	// all, so this AAR's own bootstrapMultiaddr/identity are irrelevant;
	// left empty deliberately (see UiCommandE2ETest.kt's preamble
	// comment on why an empty leaderMultiaddr, not a real one, is what
	// keeps this build from provisioning some *other* identity before
	// this scenario's own explicit-keyHex calls ever run).
	if err := buildAndroidAAR(repoRoot, e2edata.Node{PrivateKey: ""}, "", ""); err != nil {
		return fail(fmt.Errorf("build AAR: %w", err))
	}
	if err := gradleInstall(repoRoot, serialA); err != nil {
		return fail(fmt.Errorf("install on %s: %w", serialA, err))
	}
	if err := gradleInstall(repoRoot, serialB); err != nil {
		return fail(fmt.Errorf("install on %s: %w", serialB, err))
	}

	keyHexA, err := freshKeyHex()
	if err != nil {
		return fail(fmt.Errorf("generate identity A: %w", err))
	}
	keyHexB, err := freshKeyHex()
	if err != nil {
		return fail(fmt.Errorf("generate identity B: %w", err))
	}
	// Fixed, arbitrary ports (not 0/ephemeral): every `adb shell am
	// instrument` invocation is a genuinely fresh process (see this
	// scenario's own doc comment), so an ephemeral port would be
	// re-chosen at random on each resume, silently invalidating any
	// address captured in an earlier invocation before a later one (on
	// the *other* device) ever gets to dial it -- confirmed live, see
	// StartSoloWithKeyAndPort's doc comment. Distinct per device only so
	// two daemons never fight over the same port if ever run against a
	// single shared network namespace.
	const portA, portB = "47101", "47102"
	resumeA := &uiOp{label: "Cluster: StartSoloWithKeyAndPort", inputs: []string{keyHexA, portA}}
	resumeB := &uiOp{label: "Cluster: StartPendingWithKeyAndPort", inputs: []string{keyHexB, portB}}

	var (
		addrB, tokenB, ticketB  string
		inviteA, addrA, ticketA string
		peerIDB                 string
	)
	var cleanupForwards []func()
	defer func() {
		for _, cleanup := range cleanupForwards {
			cleanup()
		}
	}()

	stepsBeforeRecruit := []pairStep{
		{
			reportAs: "Cluster: StartSoloWithKey (A, prep)", serial: serialA,
			action: func() uiOp { return *resumeA },
		},
		{
			reportAs: "Cluster: StartPendingWithKey (B, prep)", serial: serialB,
			action:   func() uiOp { return *resumeB },
			validate: func(output string) error { peerIDB = output; return nil },
		},
		{
			reportAs: "Cluster: GetOwnAddr (B, prep)", serial: serialB, resumeOp: resumeB,
			action: func() uiOp { return uiOp{label: "Cluster: GetOwnAddr"} },
			validate: func(output string) error {
				bridged, cleanup, err := forwardLoopbackAddr(serialB, output)
				if err != nil {
					return err
				}
				cleanupForwards = append(cleanupForwards, cleanup)
				addrB = bridged
				return nil
			},
		},
	}

	stepsAfterRecruit := []pairStep{
		{
			reportAs: "Cluster: RecruitPeer (receiver)", serial: serialB, resumeOp: resumeB,
			action:   func() uiOp { return uiOp{label: "Cluster: ListClusterMembers"} },
			validate: func(output string) error { return validateClusterMembers(output, 2, "", "") },
		},
		{
			reportAs: "Cluster: CreateJoinInvite (A, prep)", serial: serialA, resumeOp: resumeA,
			action:   func() uiOp { return uiOp{label: "Cluster: CreateJoinInvite", inputs: []string{"voter"}} },
			validate: func(output string) error { inviteA = output; return nil },
		},
		{
			reportAs: "Cluster: GetOwnAddr (A, prep)", serial: serialA, resumeOp: resumeA,
			action: func() uiOp { return uiOp{label: "Cluster: GetOwnAddr"} },
			validate: func(output string) error {
				bridged, cleanup, err := forwardLoopbackAddr(serialA, output)
				if err != nil {
					return err
				}
				cleanupForwards = append(cleanupForwards, cleanup)
				addrA = bridged
				ticketA = addrA + "#" + inviteA
				return nil
			},
		},
		{
			reportAs: "Cluster: Join (sender)", serial: serialB, resumeOp: resumeB,
			action:   func() uiOp { return uiOp{label: "Cluster: Join", inputs: []string{ticketA}} },
			dialHold: &pairHold{serial: serialA, resume: *resumeA},
		},
		{
			reportAs: "Cluster: Join (receiver)", serial: serialA, resumeOp: resumeA,
			action:   func() uiOp { return uiOp{label: "Cluster: ListClusterMembers"} },
			validate: func(output string) error { return validateClusterMembers(output, 2, peerIDB, "voter") },
		},
	}

	var cases []e2edata.AndroidPairCaseResult
	overallErr := error(nil)
	runStep := func(step pairStep) {
		entry := e2edata.AndroidPairCaseResult{Command: step.reportAs}
		action := step.action()
		ops := []uiOp{action}
		if step.resumeOp != nil {
			ops = []uiOp{*step.resumeOp, action}
		}
		var output string
		var err error
		if step.dialHold != nil {
			output, err = runDialStep(step.dialHold.serial, step.dialHold.resume, step.serial, ops)
		} else {
			output, err = runUISteps(step.serial, ops)
		}
		if err == nil && step.validate != nil {
			err = step.validate(output)
		}
		if err != nil {
			entry.Pass = false
			entry.Error = err.Error()
			if overallErr == nil {
				overallErr = fmt.Errorf("%s: %w", step.reportAs, err)
			}
		} else {
			entry.Pass = true
		}
		cases = append(cases, entry)
	}

	// runRecruitDance drives CreateJoinRequest(B)+RecruitPeer(A) as its own
	// hand-rolled dance, not two ordinary runStep calls: unlike every other
	// step's own resumeOp, CreateJoinRequest's resulting token lives only
	// in the specific in-process daemon that minted it (see pkg/daemon's
	// joinRequestToken field's own doc comment -- confirmed directly: a
	// pending node has no raft instance yet, so there's nothing durable to
	// persist that token to), so the ordinary "kill this device's process,
	// resume it fresh in a new one for the next step" treatment every
	// other pairStep gets would lose it outright before RecruitPeer ever
	// got to redeem it -- runDialStep's own fresh-resume hold (below,
	// still used by the CreateJoinInvite-backed Join direction, where A's
	// invite genuinely is raft-persisted and so *does* survive a fresh
	// resume) can't be reused here. Instead, B's CreateJoinRequest call and
	// its own subsequent hold run as ONE continuous instrumentation
	// invocation/process (runUIStepsBackgroundPeek), with A's RecruitPeer
	// dial only starting once that same still-running process has
	// confirmed -- via its own incrementally-written results file, not by
	// waiting for the whole invocation to exit -- that it actually minted
	// a token.
	runRecruitDance := func() {
		tokenEntry := e2edata.AndroidPairCaseResult{Command: "Cluster: CreateJoinRequest (B, prep)"}
		holdOps := []uiOp{*resumeB, {label: "Cluster: CreateJoinRequest"}, {label: "Test: SleepMillis", inputs: []string{dialHoldMillis}}}
		token, wait, err := runUIStepsBackgroundPeek(serialB, holdOps, "Cluster: CreateJoinRequest", createJoinRequestPeekTimeout)
		if err != nil {
			tokenEntry.Pass = false
			tokenEntry.Error = err.Error()
			cases = append(cases, tokenEntry, e2edata.AndroidPairCaseResult{Command: "Cluster: RecruitPeer (sender)"})
			if overallErr == nil {
				overallErr = fmt.Errorf("%s: %w", tokenEntry.Command, err)
			}
			return
		}
		tokenEntry.Pass = true
		cases = append(cases, tokenEntry)
		tokenB = token
		ticketB = addrB + "#" + tokenB

		recruitEntry := e2edata.AndroidPairCaseResult{Command: "Cluster: RecruitPeer (sender)"}
		recruitOps := []uiOp{*resumeA, {label: "Cluster: RecruitPeer", inputs: []string{ticketB, "learner"}}}
		_, recruitErr := runUISteps(serialA, recruitOps)
		if waitErr := wait(); recruitErr == nil && waitErr != nil {
			recruitErr = fmt.Errorf("receiver hold on %s: %w", serialB, waitErr)
		}
		if recruitErr != nil {
			recruitEntry.Pass = false
			recruitEntry.Error = recruitErr.Error()
			if overallErr == nil {
				overallErr = fmt.Errorf("%s: %w", recruitEntry.Command, recruitErr)
			}
		} else {
			recruitEntry.Pass = true
		}
		cases = append(cases, recruitEntry)
	}

	for _, step := range stepsBeforeRecruit {
		runStep(step)
	}
	runRecruitDance()
	for _, step := range stepsAfterRecruit {
		runStep(step)
	}

	result.Cases = cases
	if overallErr != nil {
		result.Status = e2edata.StatusFail
		result.Error = overallErr.Error()
	} else {
		result.Status = e2edata.StatusPass
	}
	return result
}

// freshKeyHex generates a new throwaway ed25519 identity and returns its
// StartSoloWithKey/StartPendingWithKey-ready hex form (see pairKeyHex).
func freshKeyHex() (string, error) {
	_, priv, err := e2edata.GenerateIdentity()
	if err != nil {
		return "", fmt.Errorf("generate identity: %w", err)
	}
	return pairKeyHex(priv)
}

// dialHoldMillis is how long recvSerial's freshly (re-)resumed daemon
// stays up, via a synthetic "Test: SleepMillis" op appended after its own
// resume, while runDialStep's sender concurrently dials in from a
// different physical device -- generous enough to comfortably cover
// dialStartupStagger plus a real emulator's own dial + libp2p security
// handshake round trip.
const dialHoldMillis = "25000"

// dialStartupStagger is how long runDialStep waits after starting
// recvSerial's hold before starting senderSerial's own dial --
// recvResume's instrumentation invocation needs a moment to actually be
// up before the dial can succeed at all (process spin-up, gomobile
// daemon init, listen socket bind), and -- confirmed live, via the Join
// direction's own "leader rejected join: ERR: not leader" failure at a
// 5s stagger -- when recvSerial already has a *real*, persisted raft
// voter identity (e.g. runAndroidPairScenarioOn's device A, restarted
// here to redeem CreateJoinInvite's own invite), it isn't enough merely
// to be listening: a freshly restarted single-voter raft instance only
// re-establishes itself as leader after its own election timeout
// (mobile/kvmobile's raftElectionTimeout, 4s) elapses with no
// heartbeat -- so the stagger needs real headroom past that, not just
// past process startup, or a dial arriving before re-election completes
// gets legitimately rejected as premature, not a connectivity failure.
// Fixed, not polled: nothing in this scenario's UI-command plumbing
// surfaces "am I listening yet"/"am I leader yet" as a queryable signal.
const dialStartupStagger = 12 * time.Second

// runDialStep runs a genuine cross-device dial: recvSerial's resume op is
// kept alive concurrently (via a "Test: SleepMillis" hold, started first)
// while senderOps -- senderSerial's own resume-prefixed dial action --
// runs after a short startup stagger. Every step in this scenario only
// stays "up" for the duration of its own single `adb shell am instrument`
// invocation (see pairStep's doc comment on resumeOp) -- confirmed live
// (via a tcpdump capture on the host loopback interface: the
// adb-forwarded TCP handshake completed, but the far end sent a bare FIN
// within ~2.5ms, before any bytes were exchanged) that a plain serial
// sequence of single-device steps can never satisfy a dial -- by the time
// the sender's own step ran, the receiver's *prior* step's instrumentation
// process, and with it its daemon's listen socket, had already exited.
// Blocks until both the hold and the dial finish (not just the dial) so
// the *next* step's own instrumentation invocation against recvSerial
// never overlaps this one still winding down. Returns senderOps' own
// captured output (see runUISteps); a hold failure only surfaces if the
// dial itself otherwise succeeded, since the dial's own error is always
// the more direct signal when both fail together.
func runDialStep(recvSerial string, recvResume uiOp, senderSerial string, senderOps []uiOp) (string, error) {
	holdDone := make(chan error, 1)
	go func() {
		_, err := runUISteps(recvSerial, []uiOp{recvResume, {label: "Test: SleepMillis", inputs: []string{dialHoldMillis}}})
		holdDone <- err
	}()
	time.Sleep(dialStartupStagger)
	output, dialErr := runUISteps(senderSerial, senderOps)
	if holdErr := <-holdDone; dialErr == nil && holdErr != nil {
		dialErr = fmt.Errorf("receiver hold on %s: %w", recvSerial, holdErr)
	}
	return output, dialErr
}

// createJoinRequestPeekTimeout bounds how long runUIStepsBackgroundPeek
// waits, in runRecruitDance's own use of it, for B's CreateJoinRequest op
// to complete and appear in its own incrementally-written results file --
// generous enough to cover a real emulator's own process spin-up,
// StartPendingWithKeyAndPort resume, and the CreateJoinRequest round trip
// itself.
const createJoinRequestPeekTimeout = 20 * time.Second

// peekPollInterval is how often runUIStepsBackgroundPeek re-pulls serial's
// results file while waiting for peekLabel's own entry to appear.
const peekPollInterval = 300 * time.Millisecond

// runUIStepsBackgroundPeek starts ops (see runUISteps) on serial as a
// background instrumentation invocation and returns as soon as
// peekLabel's own result first appears in the device's own
// ui_e2e_results.json -- UiCommandE2ETest.kt writes that file after every
// entry, not just once at the very end, specifically so this can observe
// an early op's result while a *later* op in that same invocation (e.g. a
// trailing "Test: SleepMillis" hold) is deliberately still running,
// keeping serial's underlying process -- and so whatever daemon an
// earlier op in this same invocation resumed -- alive for a
// concurrently-dialing peer on a different device. runUISteps itself can
// never do this: it only ever returns once the *entire* invocation exits,
// which for a call including a trailing hold is by design too late.
//
// Returns peekLabel's captured output, and a wait func the caller must
// call (and block on) once done needing that liveness, before any
// subsequent step targets serial again -- awaiting the invocation's real
// exit and folding in every op's own pass/fail the same way runUISteps
// itself does.
func runUIStepsBackgroundPeek(serial string, ops []uiOp, peekLabel string, peekTimeout time.Duration) (output string, wait func() error, err error) {
	cases := make(map[string]e2edata.UICase, len(ops))
	for _, op := range ops {
		cases[op.label] = e2edata.UICase{Inputs: op.inputs, Execute: true, Expect: e2edata.UIExpectSucceeded}
	}
	casesJSON, err := json.Marshal(cases)
	if err != nil {
		return "", nil, fmt.Errorf("encode ops: %w", err)
	}

	type invocationResult struct {
		results []e2edata.UICaseResult
		err     error
	}
	done := make(chan invocationResult, 1)
	go func() {
		results, err := runUICommandTest(string(casesJSON), serial, true)
		done <- invocationResult{results, err}
	}()

	wait = func() error {
		res := <-done
		byLabel := make(map[string]e2edata.UICaseResult, len(res.results))
		for _, r := range res.results {
			byLabel[r.Command] = r
		}
		for _, op := range ops {
			r, ok := byLabel[op.label]
			if !ok {
				return fmt.Errorf("%s: no result returned", op.label)
			}
			if !r.Pass {
				return fmt.Errorf("%s: %s", op.label, r.Error)
			}
		}
		return res.err
	}

	deviceResultsPath := fmt.Sprintf("/sdcard/Android/data/%s/files/ui_e2e_results.json", androidAppID)
	localResultsPath := filepath.Join(os.TempDir(), fmt.Sprintf("kvraft-e2e-android-ui-peek-%s.json", strings.ReplaceAll(serial, ":", "_")))
	defer os.Remove(localResultsPath)
	deadline := time.Now().Add(peekTimeout)
	for time.Now().Before(deadline) {
		select {
		case res := <-done:
			// The whole invocation already finished -- e.g. the resume
			// itself failed -- before ever producing peekLabel. Replay res
			// back onto done (buffered, so this never blocks) so the
			// caller's own later wait() call still sees it, then report the
			// miss.
			done <- res
			return "", wait, fmt.Errorf("%s: invocation finished without ever producing this result", peekLabel)
		default:
		}
		pull := exec.Command("adb", "-s", serial, "pull", deviceResultsPath, localResultsPath)
		if pull.Run() == nil {
			if data, readErr := os.ReadFile(localResultsPath); readErr == nil {
				var results []e2edata.UICaseResult
				if json.Unmarshal(data, &results) == nil {
					for _, r := range results {
						if r.Command != peekLabel {
							continue
						}
						if !r.Pass {
							return "", wait, fmt.Errorf("%s: %s", peekLabel, r.Error)
						}
						return r.Output, wait, nil
					}
				}
			}
		}
		time.Sleep(peekPollInterval)
	}
	return "", wait, fmt.Errorf("%s: no result within %s", peekLabel, peekTimeout)
}

// runUISteps runs ops (each a real CommandCatalog.kt command, see uiOp) in
// order, within one instrumentation invocation on serial, via
// runUICommandTest's existing adb/base64/pull/parse plumbing --
// onlyListedCases set so this invocation only touches these commands.
// Every op must pass; returns the *last* op's captured Output (see
// UICaseResult's doc comment) -- ops before it are resume/prep calls
// whose own output is never needed, only that they succeeded.
func runUISteps(serial string, ops []uiOp) (output string, err error) {
	cases := make(map[string]e2edata.UICase, len(ops))
	for _, op := range ops {
		cases[op.label] = e2edata.UICase{Inputs: op.inputs, Execute: true, Expect: e2edata.UIExpectSucceeded}
	}
	casesJSON, err := json.Marshal(cases)
	if err != nil {
		return "", fmt.Errorf("encode ops: %w", err)
	}
	results, err := runUICommandTest(string(casesJSON), serial, true)
	byLabel := make(map[string]e2edata.UICaseResult, len(results))
	for _, r := range results {
		byLabel[r.Command] = r
	}
	for _, op := range ops {
		r, ok := byLabel[op.label]
		if !ok {
			return "", fmt.Errorf("%s: no result returned", op.label)
		}
		if !r.Pass {
			return "", fmt.Errorf("%s: %s", op.label, r.Error)
		}
	}
	if err != nil {
		// runUICommandTest's own error means *some* case in this
		// invocation failed -- but every op we actually asked for passed
		// per the per-label check above (results can never contain more
		// than what onlyListedCases asked for), so this is unreachable in
		// practice; kept only so a genuinely new failure mode here still
		// surfaces instead of being silently swallowed.
		return "", err
	}
	return byLabel[ops[len(ops)-1].label].Output, nil
}

// validateClusterMembers parses a ListClusterMembers JSON array (see
// mobile/kvmobile.ClusterMember) and checks it has exactly wantCount
// entries, and -- if wantPeerID is non-empty -- that one of them matches
// wantPeerID with role wantRole.
func validateClusterMembers(clusterMembersJSON string, wantCount int, wantPeerID, wantRole string) error {
	var members []struct {
		PeerID string `json:"peer_id"`
		Role   string `json:"role"`
	}
	if err := json.Unmarshal([]byte(clusterMembersJSON), &members); err != nil {
		return fmt.Errorf("parse ListClusterMembers output %q: %w", clusterMembersJSON, err)
	}
	if len(members) != wantCount {
		return fmt.Errorf("ListClusterMembers = %d member(s), want %d (%s)", len(members), wantCount, clusterMembersJSON)
	}
	if wantPeerID == "" {
		return nil
	}
	for _, m := range members {
		if m.PeerID == wantPeerID {
			if m.Role != wantRole {
				return fmt.Errorf("member %s has role %q, want %q", wantPeerID, m.Role, wantRole)
			}
			return nil
		}
	}
	return fmt.Errorf("ListClusterMembers %s does not contain expected peer %s", clusterMembersJSON, wantPeerID)
}
