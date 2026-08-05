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
// "/ip4/127.0.0.1/tcp/<port>/p2p/<peerid>" -- what either of this
// scenario's throwaway devices falls back to advertising if its relay
// reservation through the VPS (see the RequestRelayAccess steps ahead of
// every GetOwnAddr call) hasn't completed yet. Both GetOwnAddr steps'
// validate callbacks treat a match as a retriable failure, not something
// to work around: this project's own adb-forward-through-10.0.2.2 bridge
// (what an earlier version of this scenario used instead) fundamentally
// cannot work in this project's own e2e environment -- confirmed directly,
// 100% ping loss to the emulator's own QEMU gateway from inside it -- so
// silently accepting a loopback address here would only defer this exact
// failure to a much less legible spot later (Join's own dial).
var loopbackTCPAddrRe = regexp.MustCompile(`^/ip4/127\.0\.0\.1/tcp/(\d+)/p2p/(.+)$`)

// androidPairApp mirrors android.go's androidAppID constant -- kept as a
// separate name here only so this file reads self-contained about which
// app it's driving.
const androidPairApp = androidAppID

// buildAndroidPairAAR is buildAndroidAAR's pair-scenario counterpart:
// leaderMultiaddr stays empty (neither StartSoloWithKey nor
// StartPendingWithKey ever reference it -- see
// runAndroidPairScenarioOn's own build-site comment), but relayMultiaddr
// is set to relayMultiaddr so every kvmobile Start variant's shared
// daemon.Config{RelayPeers: relayPeers()} wiring picks it up regardless of
// which one actually runs -- what makes RequestRelayAccess (this
// scenario's own midOps ahead of every GetOwnAddr call) have a real target
// to ask standing from.
func buildAndroidPairAAR(repoRoot, relayMultiaddr, serial string) error {
	aarPath := filepath.Join(repoRoot, "android-app", "app", "libs", "kvmobile.aar")
	if err := os.MkdirAll(filepath.Dir(aarPath), 0o755); err != nil {
		return err
	}
	ldflags := fmt.Sprintf("-X %[1]s.relayMultiaddr=%[2]s", androidGoPackage, relayMultiaddr)
	cmd := exec.Command("gomobile", "bind", "-target=android", "-androidapi", "26",
		"-ldflags", ldflags, "-o", aarPath, "./mobile/kvmobile")
	cmd.Dir = repoRoot
	withSerial(cmd, serial)
	return runCaptured(cmd, "gomobile bind")
}

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
	// settleMillis, when non-zero, inserts a "Test: SleepMillis" op after
	// resumeOp/ensureRelay and before action, within the same invocation.
	// No step needs it any more -- the waits it used to approximate are
	// now waited on properly (see ensureRelay) -- but it stays as the one
	// way to express "this device needs a moment for something this
	// scenario cannot observe directly".
	settleMillis string
	// ensureRelay inserts a "Cluster: RequestRelayAccess" op right after
	// resumeOp and before action, in the same invocation. That call blocks
	// until this device actually holds a relay reservation (see
	// pkg/daemon's dialAndSubmitPublicAccess), so it is how a step says
	// "this device has to be genuinely reachable through the relay before
	// my action runs" -- for GetOwnAddr, whose whole output is that
	// address, and for any device about to be dialed. It replaces the
	// fixed settle-sleeps this scenario used to guess with: the grant is
	// idempotent and cheap when standing already exists, and unlike a
	// sleep it cannot be too short.
	ensureRelay bool
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
	// retries bounds how many additional times runStep re-runs this step,
	// fresh resume and all, after a failure. Kept small and deliberate:
	// the failures this scenario used to paper over with retries (a
	// device's explicit relay dial refused by libp2p's own dial backoff,
	// armed by AutoRelay moments earlier in the same process; a settle
	// that was too short for a reservation that had not landed yet) are
	// fixed at their source now -- see pkg/daemon's clearDialBackoff and
	// dialAndSubmitPublicAccess -- so a retry here is a hedge against a
	// genuinely flaky emulator, not the mechanism the scenario relies on.
	retries int
}

// pairHold names the device (and its resume op) a dial step's own action
// needs kept alive concurrently -- see runDialStep.
type pairHold struct {
	serial string
	resume uiOp
}

// runAndroidPairScenario drives a genuine two-device Join/RecruitPeer/Leave
// round trip -- see this package's own design notes (README.md's e2e
// section) for why this can't be expressed as ordinary android_ui_cases
// entries (ordering + cross-device data threading, not a flat
// independent-per-label sweep). Returns nil (log a message, don't fail
// the run) if fewer than two devices/emulators are currently connected --
// this scenario is opt-in by device count, not part of every e2e run.
//
// Prefers three devices over two: runAndroidRows' own existing
// single-device flow (still unqualified-serial-free at every *other* call
// site, see that function's own comment) always claims
// connectedAndroidSerials()'s first entry for the long-lived shared android
// node -- the one whose real, valuable persisted raft membership should
// never be touched if it can be helped. This scenario's own pm
// clear/uninstall steps are destructive by design (see runAndroidPairScenarioOn),
// so with three or more devices connected it skips index 0 unconditionally
// and only ever uses two *other* devices, avoiding any risk to that shared
// node's data. With exactly two devices connected -- this project's own e2e
// environment among them -- there is no spare third device to isolate this
// scenario onto, so it falls back to reusing index 0 anyway and accepts
// that risk rather than never running this scenario at all: whatever the
// shared android node had installed/persisted on that device is wiped by
// this scenario's own uninstall step, and the next ordinary
// e2e:current/e2e:all run reprovisions it from scratch there (the same
// self-healing reprovision path a deleted node identity already goes
// through, see run.go's own reprovision loop) rather than resuming prior
// state.
func runAndroidPairScenario(repoRoot, bootstrapMultiaddr string) *e2edata.AndroidPairResult {
	serials, err := connectedAndroidSerials()
	if err != nil {
		fmt.Fprintf(os.Stderr, "e2erun: android pair scenario: skipped: %v\n", err)
		return nil
	}
	switch {
	case len(serials) >= 3:
		return runAndroidPairScenarioOn(repoRoot, bootstrapMultiaddr, serials[1], serials[2])
	case len(serials) == 2:
		fmt.Fprintln(os.Stderr, "e2erun: android pair scenario: only 2 device(s)/emulator(s) connected -- reusing device 0 (normally reserved for the shared android node); its installed app and persisted data will be wiped by this scenario's own uninstall step")
		return runAndroidPairScenarioOn(repoRoot, bootstrapMultiaddr, serials[0], serials[1])
	default:
		fmt.Fprintf(os.Stderr, "e2erun: android pair scenario: skipped: needs at least 2 connected devices/emulators, found %d\n", len(serials))
		return nil
	}
}

// runAndroidPairScenarioOn is runAndroidPairScenario's actual mechanism,
// against two explicitly named serials -- split out from the device-count/
// reservation policy above so it can also be driven directly against
// whichever two serials the caller names, without needing a third device
// just to exercise the same step sequence. runAndroidPairScenario's own
// two-device fallback is exactly this: it accepts the responsibility of
// picking device 0 (the shared android node) itself when no spare third
// device exists, rather than never running this scenario at all -- a
// developer with only two spare devices/emulators of their own can drive
// this the same way, by naming them directly.
func runAndroidPairScenarioOn(repoRoot, bootstrapMultiaddr, serialA, serialB string) *e2edata.AndroidPairResult {
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
	// nor StartPendingWithKey reference build-time leaderMultiaddr at all,
	// so it stays empty deliberately (see UiCommandE2ETest.kt's preamble
	// comment on why that, not a real leader, is what keeps this build
	// from provisioning some *other* identity before this scenario's own
	// explicit-keyHex calls ever run) -- but relayMultiaddr is set to the
	// live bootstrap, since every kvmobile Start variant's shared
	// daemon.Config{RelayPeers: relayPeers()} wiring reads that same
	// build-time value regardless of which one actually runs, and this
	// scenario's own RequestRelayAccess midOps (ahead of every GetOwnAddr
	// call below) need a real relay target to ask standing from.
	if err := buildAndroidPairAAR(repoRoot, bootstrapMultiaddr, ""); err != nil {
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

	stepsBeforeRecruit := []pairStep{
		{
			reportAs: "Cluster: StartSoloWithKey (A, prep)", serial: serialA,
			action: func() uiOp { return *resumeA },
		},
		{
			// Granted before A touches the relay for anything else. Every
			// build bakes relayMultiaddr in (see buildAndroidPairAAR), so
			// A's daemon starts dialing the relay on its own the moment it
			// resumes -- and until standing exists that dial cannot
			// succeed. Doing this first means A holds a reservation for the
			// rest of the scenario, and (see pkg/daemon's clearDialBackoff)
			// its own later dials are no longer hostage to whatever
			// AutoRelay's background attempts did to libp2p's per-peer dial
			// backoff.
			//
			// retries: 1 because this specific step is where a WAN hiccup
			// actually costs something. Observed live: a bare "dial tcp4
			// ... i/o timeout" to the relay failed this step, and every
			// later step needing A to be reachable failed with it -- A
			// ended up paying for its first grant in the middle of the
			// recruit invocation instead, while the other device's hold
			// was already running down.
			reportAs: "Cluster: RequestRelayAccess (A, prep)", serial: serialA, resumeOp: resumeA, retries: 1,
			action: func() uiOp { return relayAccessOp },
		},
		{
			reportAs: "Cluster: StartPendingWithKey (B, prep)", serial: serialB,
			action:   func() uiOp { return *resumeB },
			validate: func(output string) error { peerIDB = output; return nil },
		},
		{
			// retries: 1 -- see the identical step for A above.
			reportAs: "Cluster: RequestRelayAccess (B, prep)", serial: serialB, resumeOp: resumeB, retries: 1,
			action: func() uiOp { return relayAccessOp },
		},
		{
			// ensureRelay, not a settle-sleep: RequestRelayAccess returns
			// only once this device actually holds a reservation, so the
			// address read straight after it is the real relayed one. A
			// loopback answer here is still treated as a hard failure --
			// two emulators have no way to reach each other directly (this
			// project's own environment: 100% packet loss to the emulator's
			// own QEMU gateway), so accepting one would only defer the same
			// failure to Join's dial, where it reads as a connectivity bug
			// instead of a missing reservation.
			reportAs: "Cluster: GetOwnAddr (B, prep)", serial: serialB, resumeOp: resumeB, ensureRelay: true,
			action: func() uiOp { return uiOp{label: "Cluster: GetOwnAddr"} },
			validate: func(output string) error {
				if loopbackTCPAddrRe.MatchString(output) {
					return fmt.Errorf("GetOwnAddr returned a bare loopback address (%q) even after RequestRelayAccess reported a reservation", output)
				}
				addrB = output
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
			// Same shape as B's own GetOwnAddr step above: this is a fresh
			// process, so it needs its own reservation before it can report
			// a relayed address, and RequestRelayAccess is what waits for
			// one.
			reportAs: "Cluster: GetOwnAddr (A, prep)", serial: serialA, resumeOp: resumeA, ensureRelay: true,
			action: func() uiOp { return uiOp{label: "Cluster: GetOwnAddr"} },
			validate: func(output string) error {
				if loopbackTCPAddrRe.MatchString(output) {
					return fmt.Errorf("GetOwnAddr returned a bare loopback address (%q) even after RequestRelayAccess reported a reservation", output)
				}
				addrA = output
				ticketA = addrA + "#" + inviteA
				return nil
			},
		},
		{
			// ensureRelay on the *sender* too, not just the receiver: a
			// join sends this device's own advertised address for the
			// leader to store in raft permanently (see pkg/daemon's join),
			// so B joining before its own reservation exists would enrol a
			// loopback address nobody can ever reach. join() would
			// otherwise discover that itself and stall up to 45s inside
			// awaitRelayAddr, which is time A's hold would have to cover.
			reportAs: "Cluster: Join (sender)", serial: serialB, resumeOp: resumeB, ensureRelay: true,
			action:   func() uiOp { return uiOp{label: "Cluster: Join", inputs: []string{ticketA}} },
			dialHold: &pairHold{serial: serialA, resume: *resumeA},
		},
		{
			reportAs: "Cluster: Join (receiver)", serial: serialA, resumeOp: resumeA,
			action:   func() uiOp { return uiOp{label: "Cluster: ListClusterMembers"} },
			validate: func(output string) error { return validateClusterMembers(output, 2, peerIDB, "voter") },
		},
	}

	// Leave: B (the joiner) asks to be removed from A's cluster --
	// kvmobile.Leave's own doc comment confirms this blocks on the
	// forwarded raft.RemoveServer completing before returning, then stops
	// B's daemon, so A's subsequent ListClusterMembers needs no retry/poll
	// to see the shrink -- the same "synchronous, no extra wait needed"
	// assumption stepsAfterRecruit's own post-Join check above already
	// relies on. B has nothing further to do afterward (its daemon is
	// already stopped), so unlike every other step here, this one needs no
	// trailing resume for B.
	stepsAfterLeave := []pairStep{
		{
			// A dial step, like Join before it: leaving is a forwarded
			// raft.RemoveServer to whoever leads the cluster (see
			// pkg/daemon's ForwardLeaveProtocolID), so the *other* voter
			// has to be up and reachable at that exact moment. Every other
			// step here only needs its own device. Caught live: with A's
			// process not running, B -- one voter of two, freshly resumed
			// and with nobody to hear a heartbeat from -- failed with
			// "not leader and no leader known", which reads like a
			// membership bug and is really nobody being home.
			//
			// ensureRelay because A's address in raft's configuration is a
			// /p2p-circuit one, so B needs its own reservation to dial it.
			reportAs: "Cluster: Leave (B)", serial: serialB, resumeOp: resumeB, ensureRelay: true,
			action:   func() uiOp { return uiOp{label: "Cluster: Leave"} },
			dialHold: &pairHold{serial: serialA, resume: *resumeA},
		},
		{
			reportAs: "Cluster: Leave (verify via A)", serial: serialA, resumeOp: resumeA,
			action:   func() uiOp { return uiOp{label: "Cluster: ListClusterMembers"} },
			validate: func(output string) error { return validateClusterMembers(output, 1, "", "") },
		},
	}

	var cases []e2edata.AndroidPairCaseResult
	overallErr := error(nil)
	// attemptStep runs step exactly once: build+run its ops, then validate.
	// Factored out of runStep so retries (see pairStep.retries) can call it
	// repeatedly, each time from a fresh resume -- a fresh Swarm, with none
	// of a prior attempt's own libp2p dial-backoff state.
	attemptStep := func(step pairStep) (output string, err error) {
		action := step.action()
		var ops []uiOp
		if step.resumeOp != nil {
			ops = append(ops, *step.resumeOp)
		}
		if step.ensureRelay {
			ops = append(ops, relayAccessOp)
		}
		if step.settleMillis != "" {
			ops = append(ops, uiOp{label: "Test: SleepMillis", inputs: []string{step.settleMillis}})
		}
		ops = append(ops, action)
		if step.dialHold != nil {
			output, err = runDialStep(step.dialHold.serial, step.dialHold.resume, step.serial, ops)
		} else {
			output, err = runUISteps(step.serial, ops)
		}
		if err == nil && step.validate != nil {
			err = step.validate(output)
		}
		return output, err
	}

	runStep := func(step pairStep) {
		entry := e2edata.AndroidPairCaseResult{Command: step.reportAs}
		var err error
		start := time.Now()
		for attempt := 0; attempt <= step.retries; attempt++ {
			_, err = attemptStep(step)
			if err == nil {
				break
			}
		}
		// Elapsed time per step, on stderr: this scenario's failures are
		// overwhelmingly about *when* things happened relative to each
		// other (a hold that ended before the other device dialed, a
		// reservation that had not landed yet), and a bare pass/fail list
		// cannot show that.
		logStep(step.reportAs, start, err)
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
		danceStart := time.Now()
		tokenEntry := e2edata.AndroidPairCaseResult{Command: "Cluster: CreateJoinRequest (B, prep)"}
		// relayAccessOp first: A is about to dial B through the relay, so B
		// has to hold a reservation before that dial -- and since the peek
		// below waits for CreateJoinRequest, which the device-side runner
		// now genuinely runs *after* it (see e2edata.UICase.Order), a token
		// in hand also means B is reachable.
		holdOps := []uiOp{*resumeB, relayAccessOp, {label: "Cluster: CreateJoinRequest"}, {label: "Test: SleepMillis", inputs: []string{dialHoldMillis}}}
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
		logStep(tokenEntry.Command, danceStart, nil)
		cases = append(cases, tokenEntry)
		tokenB = token
		ticketB = addrB + "#" + tokenB

		recruitStart := time.Now()
		recruitEntry := e2edata.AndroidPairCaseResult{Command: "Cluster: RecruitPeer (sender)"}
		// relayAccessOp before the recruit for the same reason the Join
		// step needs it: RecruitProtocolID hands the recruited device this
		// device's own advertised address to join back to, so A must hold
		// its reservation before it recruits, or B is handed a loopback
		// address to join.
		recruitOps := []uiOp{*resumeA, relayAccessOp, {label: "Cluster: RecruitPeer", inputs: []string{ticketB, "learner"}}}
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
		logStep(recruitEntry.Command, recruitStart, recruitErr)
		cases = append(cases, recruitEntry)
	}

	for _, step := range stepsBeforeRecruit {
		runStep(step)
	}
	runRecruitDance()
	for _, step := range stepsAfterRecruit {
		runStep(step)
	}
	for _, step := range stepsAfterLeave {
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

// logStep prints one step's outcome and how long it took, to stderr --
// see runStep's own call site for why elapsed time specifically is what
// this scenario needs reported.
func logStep(name string, start time.Time, err error) {
	status := "ok"
	if err != nil {
		status = "FAIL"
	}
	fmt.Fprintf(os.Stderr, "e2erun: android pair: %-42s %-4s %s\n", name, status, time.Since(start).Round(time.Second))
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

// dialHoldMillis is how long recvSerial's daemon stays up, via a synthetic
// "Test: SleepMillis" op at the end of its hold, while runDialStep's
// sender dials in from a different physical device.
//
// It has to cover the sender's *entire* invocation, not just its dial:
// the sender is a fresh process too, so before it can dial it has to
// start up, resume its own daemon and obtain its own relay reservation
// (tens of seconds -- see relayAccessOp). If the hold ends first, the
// receiver's process exits, the relay drops its reservation on that
// disconnect, and the sender's dial fails with NO_RESERVATION -- a
// failure that reads exactly like a relay problem and is really a
// scheduling one. Sized just under UiCommandE2ETest's own per-op ceiling
// (RUN_TIMEOUT_MS), the most a single sleep op can ask for.
const dialHoldMillis = "170000"

// relayAccessOp asks this device's own daemon for relay standing and --
// since pkg/daemon's dialAndSubmitPublicAccess waits for the reservation
// it enables -- returns only once this device is genuinely reachable
// through the relay. Cheap to repeat: the grant itself is idempotent, and
// on a device that already holds a reservation the wait returns
// immediately.
//
// Every step that needs this device to be dialable, or to be able to
// report its own real address, runs this first. It replaced a pair of
// fixed settle-sleeps (20s after a resume, hoping AutoRelay had finished)
// that were both unreliable and, as it turned out, not even running when
// they were supposed to: the device-side runner walked ops in catalog
// order until e2edata.UICase.Order existed, so a "Test" category sleep
// always ran *after* the "Cluster" command it was meant to precede.
var relayAccessOp = uiOp{label: "Cluster: RequestRelayAccess", inputs: []string{"e2e pair scenario"}}

// relayReadyPeekTimeout bounds how long a cross-device step waits for the
// device being dialed to report that it holds a relay reservation (its own
// "Cluster: RequestRelayAccess" result). Generous because that call
// deliberately blocks on the reservation itself (see pkg/daemon's
// dialAndSubmitPublicAccess), and a real emulator through a real VPS relay
// was measured taking ~30-40s from grant to reservation -- several times
// what the same sequence costs on a desktop.
const relayReadyPeekTimeout = 120 * time.Second

// runDialStep runs a genuine cross-device dial: recvSerial's daemon is
// resumed and kept alive concurrently (via a trailing "Test: SleepMillis"
// hold) while senderOps -- senderSerial's own resume-prefixed dial action
// -- runs against it. Every step in this scenario only stays "up" for the
// duration of its own single `adb shell am instrument` invocation (see
// pairStep's doc comment on resumeOp) -- confirmed live (via a tcpdump
// capture on the host loopback interface: the TCP handshake completed, but
// the far end sent a bare FIN within ~2.5ms, before any bytes were
// exchanged) that a plain serial sequence of single-device steps can never
// satisfy a dial: by the time the sender's own step ran, the receiver's
// prior step's process, and with it its daemon's listener, had already
// exited.
//
// The receiver's hold begins with relayAccessOp, and the sender's dial
// does not start until that op has actually completed -- observed through
// the receiver's own incrementally-written results file, not guessed at.
// That is the entire startup-coordination mechanism now: it replaced a
// fixed 12s stagger that was really standing in for three different waits
// at once (process spin-up, a relay reservation, and a restarted
// single-voter raft instance re-electing itself), any of which could be
// slower than the guess. Waiting on the reservation subsumes all three:
// it takes tens of seconds and cannot complete before the daemon is up,
// which is far past raft's own 4s election timeout (mobile/kvmobile's
// raftElectionTimeout).
//
// Blocks until both the hold and the dial finish (not just the dial) so
// the next step's own invocation against recvSerial never overlaps this
// one still winding down. Returns senderOps' own captured output (see
// runUISteps); a hold failure only surfaces if the dial itself otherwise
// succeeded, since the dial's own error is always the more direct signal
// when both fail together.
func runDialStep(recvSerial string, recvResume uiOp, senderSerial string, senderOps []uiOp) (string, error) {
	recvOps := []uiOp{recvResume, relayAccessOp, {label: "Test: SleepMillis", inputs: []string{dialHoldMillis}}}
	_, wait, err := runUIStepsBackgroundPeek(recvSerial, recvOps, relayAccessOp.label, relayReadyPeekTimeout)
	if err != nil {
		return "", fmt.Errorf("receiver %s never became reachable through the relay: %w", recvSerial, err)
	}

	output, dialErr := runUISteps(senderSerial, senderOps)
	if holdErr := wait(); dialErr == nil && holdErr != nil {
		dialErr = fmt.Errorf("receiver hold on %s: %w", recvSerial, holdErr)
	}
	return output, dialErr
}

// createJoinRequestPeekTimeout bounds how long runUIStepsBackgroundPeek
// waits, in runRecruitDance's own use of it, for B's CreateJoinRequest op
// to complete and appear in its own incrementally-written results file.
// Sized for what actually precedes that op in the same invocation: a real
// emulator's process spin-up, a StartPendingWithKeyAndPort resume, and --
// the long pole -- relayAccessOp waiting on a relay reservation (see
// relayReadyPeekTimeout).
const createJoinRequestPeekTimeout = 150 * time.Second

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
	cases := orderedCases(ops)
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
// orderedCases turns ops into the device-side runner's case map, stamping
// each with its own 1-based position -- see e2edata.UICase.Order for why a
// map alone cannot express "run these in this order", and what that cost
// this scenario before that field existed.
func orderedCases(ops []uiOp) map[string]e2edata.UICase {
	cases := make(map[string]e2edata.UICase, len(ops))
	for i, op := range ops {
		cases[op.label] = e2edata.UICase{
			Inputs:  op.inputs,
			Execute: true,
			Expect:  e2edata.UIExpectSucceeded,
			Order:   i + 1,
		}
	}
	return cases
}

func runUISteps(serial string, ops []uiOp) (output string, err error) {
	cases := orderedCases(ops)
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
