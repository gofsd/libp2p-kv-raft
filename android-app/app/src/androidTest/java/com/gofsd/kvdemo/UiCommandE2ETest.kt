package com.gofsd.kvdemo

import android.util.Base64
import android.util.Log
import androidx.compose.runtime.snapshots.Snapshot
import androidx.compose.ui.semantics.SemanticsProperties
import androidx.compose.ui.semantics.getOrNull
import androidx.compose.ui.test.ComposeTimeoutException
import androidx.compose.ui.test.junit4.createAndroidComposeRule
import androidx.compose.ui.test.onAllNodesWithTag
import androidx.compose.ui.test.onNodeWithTag
import androidx.compose.ui.test.onRoot
import androidx.compose.ui.test.performClick
import androidx.compose.ui.test.printToLog
import androidx.compose.ui.test.performScrollTo
import androidx.compose.ui.test.performScrollToIndex
import androidx.compose.ui.test.performTextInput
import androidx.compose.ui.test.performTouchInput
import androidx.compose.ui.test.swipeLeft
import androidx.compose.ui.test.swipeRight
import androidx.test.platform.app.InstrumentationRegistry
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kvmobile.ChannelCallback
import kvmobile.Kvmobile
import org.json.JSONArray
import org.json.JSONObject
import org.junit.Assert
import org.junit.Rule
import org.junit.Test
import java.io.File
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

/**
 * Real-UI-driven instrumented test for android-app's real-camera two-device optical scan harness
 * (pkg/e2erun/android_optical.go, driven by test/e2e/testdata.json's android_optical_cases) --
 * the only way any android command executes now (see CommandCatalog.kt/CommandDetailScreen's own
 * doc comments on the RunCode rewrite: every command runs by generating a DataMatrix on one
 * device, a real camera scanning it on another, and a confirm tap executing it there). Unlike
 * E2ETest (a separate class that calls Kvmobile.sendEvent directly, never touching a single
 * screen -- still the cross-platform wire-protocol row-replay check, unaffected by any of this),
 * this drives the actual app: the swipeable pager's Default group ("Commands"/"Groups" pseudo-
 * items, see GroupPageScreen/PagerScreen), CommandDetailScreen's dynamically-rendered param fields
 * and "Generate DataMatrix" button, the persistent camera scanner, and RunConfirmDialog/
 * RecruitConfirmDialog's confirm taps.
 *
 * Drives the app via Jetpack Compose's test APIs (onNodeWithTag/performClick/performTextInput)
 * against the [Modifier.testTag]s each screen sets, not Espresso View matchers or Activity-class
 * identity -- this app is one Activity + a NavHost of Composable routes (see AppRoot.kt), which
 * eliminated any distinct Activity subclass to check "am I on the detail screen" against, and any
 * `R.id.*`-addressable View to findViewById. Back navigation is driven directly through the
 * current Activity's own `OnBackPressedDispatcher` (see this class's own [pressBack]), not
 * `Espresso.pressBack()` -- confirmed live against a real emulator that the two are *not*
 * interchangeable here: `Espresso.pressBack()` reliably finished the Activity outright ("Pressed
 * back and killed the app") on the very first pop, even though the identical back press worked
 * correctly when sent manually (`adb shell input keyevent 4`) and Compose Navigation's own back
 * handling is otherwise unremarkable -- Espresso's pressBack implementation predates
 * `OnBackPressedDispatcher`/predictive back and does not reliably route through the callback chain
 * NavHost registers with it, at least at the AndroidX versions this project currently pins.
 * Driving the dispatcher directly sidesteps that gap entirely.
 *
 * Unlike an earlier version of this class, there is no whole-catalog "cases" sweep anymore: a
 * command only executes by being scanned, and a scan is inherently a two-device, real-hardware
 * event, so there is nothing left for a single-device instrumentation invocation to sweep through
 * on its own. pkg/e2erun/android_optical.go's runOpticalMethod instead invokes exactly one of
 * [generateAndHold]/[awaitAndVerifyScan]/[verifyLogContains] at a time (via `-e class
 * com.gofsd.kvdemo.UiCommandE2ETest#<method>`), orchestrating device A (generates, holds) and
 * device B (scans, confirms, executes) around one test/e2e/testdata.json android_optical_cases
 * entry -- see that file's own doc comment for the full two-device design, and
 * TestManualOpticalScan for how to run it against a real rig.
 */
private const val TAG = "KVDemo"

class UiCommandE2ETest {

    @get:Rule
    val composeTestRule = createAndroidComposeRule<MainActivity>()

    private companion object {
        // "FAILED" only ever appears in CommandExecutor's own catch-branch formatting -- never in
        // a successful result string in practice, so it's a reliable discriminator without
        // needing to parse the result's own JSON.
        fun assertSucceeded(line: String) =
            Assert.assertFalse("expected success, got: $line", line.contains("FAILED"))

        fun assertRejected(line: String) =
            Assert.assertTrue("expected a rejection (this device is a learner, not a voter), got: $line", line.contains("FAILED"))

        // Accepts either outcome -- for commands whose success depends on shared-leader
        // configuration this test doesn't control, where the only thing actually worth proving is
        // that Execute produces *a* clean, well-formed result either way, never a crash.
        fun assertNoCrash(@Suppress("UNUSED_PARAMETER") line: String) = Unit

        // Backs OpticalExpectSpec.Result's "contains:<substring>" convention (see
        // resultExpectation) -- unlike assertSucceeded/assertRejected/assertNoCrash, which only
        // ever look at whether the result line contains the literal word "FAILED", this actually
        // checks the command's own reported effect against an expected value.
        fun assertContains(line: String, want: String) =
            Assert.assertTrue("expected result to contain \"$want\", got: $line", line.contains(want))

        // Bounds how long awaitAndVerifyScan waits for CommandExecutor's background coroutine to
        // post a result after Execute is tapped -- generous enough to cover a real forwarded-write
        // round trip to the shared remote leader (and OpenChannel/RedeemExecInvite's own up-to-60s
        // internal timeouts, see kvmobile's callTimeout) without hanging the whole suite forever if
        // something is genuinely stuck. Also covers RequestRelayAccess's own real relay
        // reservation (measured ~30-40s on an emulator through the deployed VPS relay when
        // granting fresh) and the hold sleep that keeps a device's daemon alive for the whole of
        // another device's own start-up-and-reserve before it can dial in.
        const val RUN_TIMEOUT_MS = 180_000L

        // Prefix every generateAndHold/awaitAndVerifyScan/verifyLogContains log line with this so
        // `adb logcat -s KVDemo:* | grep OPTICAL` isolates the optical-scan harness's own
        // diagnostic trail from the rest of this app's KVDemo-tagged logging (AppRoot's own
        // "AUTO: scan received"/"ACTION_REQUIRED: ..." lines included) -- added after a run whose
        // only failure evidence was a bare timeout/stack trace left real questions ("did a scan
        // even happen? what did it decode? how far did my own code get before the crash?")
        // unanswerable without this.
        const val OPTICAL_LOG_TAG = "OPTICAL:"

        // Bounds waitForScreen's poll for a navigation target's testTag to actually appear in the
        // composition. 10s is generous for what's normally a same-process transition with no
        // network involved -- this exists for the rare case it isn't instant, not to paper over a
        // real hang.
        const val NAV_TIMEOUT_MS = 10_000L

        // generateAndHoldAll/awaitAndVerifyScanAll's own per-case pacing -- unlike the single-case
        // methods' much larger HoldMillis/TimeoutMs (tuned for a relaunch-per-case run, generous
        // enough to cover a from-scratch relay reservation on top of the scan itself), both
        // devices already hold live relay standing throughout an "All" batch (established once at
        // the top, not re-earned per case), so a real camera scan alone -- confirmed live to
        // typically decode within ~1s once both devices are already warmed up -- is the only
        // per-case latency left to budget for.
        //
        // BATCH_HOLD_MILLIS must stay >= BATCH_TIMEOUT_MS -- confirmed live the hard way, running
        // the full 90-case catalog, that this is not just a tuning nicety but a correctness
        // invariant: generateAndHoldAll advances to the next case on a fixed clock with no
        // feedback from B at all, so if a single case ever costs B close to its own full
        // BATCH_TIMEOUT_MS (one slow camera focus, one deeper category's extra navigation), A can
        // already be showing a *later* case's code by the time B starts waiting for this one. Once
        // that happens B is permanently behind -- every subsequent wait times out (A always
        // finishes moving on before B's slower, timeout-bound loop catches up), which is exactly
        // what a live run showed: clean case-by-case progress through ~12 cases, then an
        // unbroken chain of "'runConfirmExecute' not shown after 15000ms" for every case after.
        // Keeping the hold at least as long as the timeout restores the invariant that matters:
        // A never moves past a code before B's own worst-case wait for it has had a chance to
        // land, so a single slow case costs at most one case's worth of margin, not the rest of
        // the run.
        //
        // 15s/20s turned out to still be too tight once the signal-based advance (see
        // awaitCaseSignalOrCeiling) made these ceilings genuine fallbacks rather than the normal
        // pace-setter: confirmed live across repeated full 90-case runs that ordinary,
        // non-broken camera decode latency occasionally exceeds 15s even well after warmup and
        // with a clean line of sight (no fixed case index is consistently the slow one -- it
        // moves around run to run), so a 15s per-case ceiling still occasionally aborted an
        // otherwise-healthy run. Raised with real margin over the observed range (near-instant to
        // ~8s in healthy runs).
        //
        // 25s/30s then hit the same wall again on a real rig, one notch further out: a run that
        // had already scanned 45 consecutive cases cleanly died on case 46
        // ("channel_close_write", whose code is *sparser* than most -- a single short param -- so
        // this was decode latency, not a denser-code effect). Because a missed signal is fatal to
        // the whole batch by design, one transient slow decode throws away every case after it:
        // at roughly one miss per ~45 cases, a clean 90-case run is close to a coin flip, and the
        // rerun costs ~10 minutes of real hardware time. Raising the ceiling is close to free by
        // contrast -- the signal-based advance (awaitCaseSignalOrCeiling) means a healthy case
        // still advances the instant B signals, so these numbers are only ever paid on a case
        // that is genuinely struggling, and only for the one case rather than the run.
        const val BATCH_HOLD_MILLIS = 60_000L
        const val BATCH_TIMEOUT_MS = 50_000L

        /** How many times [selectGroupUntilRegistered] re-taps a group option before giving up. */
        const val GROUP_SELECT_ATTEMPTS = 4

        // How many of the case's own timeout budgets [awaitScannedTag] is allowed to spend on a
        // scan before giving up, spent in [SCAN_LOOK_MS] slices with a re-arm between them, each
        // re-arm asking device A for a matching extension of its own hold -- so
        // BATCH_HOLD_MILLIS >= BATCH_TIMEOUT_MS still holds for every look, see that constant's own
        // doc comment for why that invariant is not negotiable.
        //
        // This was 2, on the reasoning that a case which hasn't produced its tag after a second
        // full look at a code device A is *still* holding isn't going to produce one on a third.
        // Raised to 3 after a live run showed the flaw in that argument: an attempt can be spent
        // without ever being a real look. Case 5 ("dial_submit_command_ping") lost attempt 1
        // outright to the `Can't create handler inside thread ... that has not called
        // Looper.prepare()` framework flake this class documents on [awaitScannedTag], leaving
        // exactly one clean look before the case -- and with it the remaining 85 cases -- was
        // declared failed. Three attempts means a case still gets two genuine looks after losing
        // one to that flake, and costs nothing on a healthy case, which returns on the first.
        //
        // Raised again to 5 once every look started ending in device A re-showing the code (see
        // [awaitCaseSignalOrCeiling]) rather than just holding it: each look is now a genuinely
        // different stimulus, not the same one repeated, so more of them is worth something. A run
        // died with a case that had not decoded across 10 looks of an unchanging screen.
        //
        // Each attempt now gets the case's *whole* timeout_ms rather than an equal share of it,
        // because A no longer moves on underneath a case still being retried: B tells it (see
        // [signalCaseRetry]/[MAX_HOLD_EXTENSIONS]) and A extends its hold. Splitting the budget was
        // what the hold >= timeout invariant forced back when A's ceiling was fixed -- three looks
        // of a third the length each, so a decode that genuinely needed 40s could never get it. A
        // healthy case still returns on the first attempt in seconds and pays nothing for this.
        const val SCAN_ATTEMPTS = 5

        // How long one look waits before [awaitScannedTag] stops waiting and re-arms, *within* an
        // attempt's own budget. The whole budget used to be spent as a single look, which made
        // every stall cost its full 50s before anything was done about it.
        //
        // Worth doing because of what the stalls turned out to be. Classified across a 90/90 run,
        // all five were the same mode: not one frame decoded for the entire look, while the camera
        // stayed demonstrably healthy (fresh frames, sharpness 0.019 against a best of 0.023, the
        // app's scan collector still subscribed) and the code sat legibly in frame the whole time
        // -- a screenshot of the scanning device's own preview, cropped and run through the same
        // ZXing pipeline offline, decodes it. What ends that state is the camera rebind [forceRescan]
        // asks for, which is the one thing that resets this camera's 3A. So the rebind should not
        // wait out a budget sized for a slow decode; a decode that is going to happen at all happens
        // in 1.5-3s here, so a look with nothing after 10s is already anomalous.
        const val SCAN_LOOK_MS = 10_000L

        // How many times device A will extend a case's own hold ceiling on B's retry signal before
        // giving up on it anyway -- a backstop against a B that somehow signals retries forever,
        // not a budget anyone should expect to reach. Comfortably above the number of looks
        // [SCAN_ATTEMPTS] x timeout_ms can be divided into at [SCAN_LOOK_MS] apiece (B signals once
        // per re-arm, so this has to outlast the looks rather than the attempts), and each extension
        // only ever means "keep showing the same code", which costs A nothing.
        const val MAX_HOLD_EXTENSIONS = 20

        // Prefix distinguishing B's "still working on this one, keep it on screen" keepalive from
        // its ordinary "this case passed, move on" signal on the same channel (see
        // [signalCaseDone]/[signalCaseRetry]). A case id can never collide with it -- they come
        // from test/e2e/testdata.json's own android_optical_cases and are plain lowercase
        // identifiers -- so one channel carries both without any framing beyond this.
        const val RETRY_SIGNAL_PREFIX = "retry:"

        /**
         * Marker text for "this app process can no longer receive scans at all" -- see
         * [awaitScannedTag]. Matched verbatim by pkg/e2erun/android_optical.go, which re-runs the
         * whole batch in fresh processes when it sees it, so the two strings have to stay in step.
         */
        const val SCAN_COLLECTOR_GONE = "AppRoot's scan collector is gone"

        /** How long [forceRescan] gives MainScannerWidget to notice its rebind request and get the camera streaming again -- its watchdog polls on REFOCUS_INTERVAL_MS (3s), plus a moment for the bind itself. */
        const val REBIND_SETTLE_MS = 5_000L


        /** The one substitution token an optical case's own Result may reference -- see [resultExpectation]. */
        const val SELF_PEER_ID_TOKEN = "{{selfPeerID}}"

        /**
         * The log book device A owns for the optical rig's Journal cases, and the Command device B
         * submits against -- see [ensureJournalBook]. These names are the contract between that
         * setup and testdata.json's own android_optical_cases entries, which name the same command
         * id in their params: a case saying "optical-journal" and a device serving "optical-shift-log"
         * would leave every Journal case timing out with nothing to say why, so they live here as
         * constants rather than being typed twice.
         *
         * The columns are the same shift log examples/relations' own tests and README use, minus
         * the ones a submitter never fills in: what a device writes is a form, and the form comes
         * from these declarations.
         */
        const val OPTICAL_JOURNAL_LOG = "1"
        const val OPTICAL_JOURNAL_COMMAND_ID = "optical-journal"
        const val OPTICAL_JOURNAL_GROUP_ID = "optical-journal-writers"
        const val OPTICAL_JOURNAL_COLUMNS =
            """{"operator":"term","machine":"term","result":"term","pieces":"number","remarks":"text"}"""
        const val OPTICAL_JOURNAL_OPERATORS = """["Ivanova","Petrov"]"""

        /** How long [awaitOwnCircuitAddr] waits for this device to publish a relay circuit address, and how often it re-checks. */
        const val CIRCUIT_ADDR_TIMEOUT_MS = 90_000L
        const val CIRCUIT_ADDR_POLL_MS = 2_000L

        /**
         * How many times [awaitSession] re-attempts a join before starting the batch anyway, and
         * how long it waits between them. Sized against what it is retrying: a relay that refused
         * the circuit on resource limits, which frees up on the order of minutes, not seconds --
         * four attempts 20s apart covers a full minute of that without meaningfully delaying a
         * device whose session (the normal case) is already up on the first check.
         */
        const val SESSION_ATTEMPTS = 4
        const val SESSION_RETRY_DELAY_MS = 20_000L

        // Do NOT reach for MainScannerManager.setZoom here to make a code bigger in frame, however
        // reasonable it looks: on this rig's scanning device it silently ends decoding for the
        // rest of the process. Measured across five two-device runs, every successful decode fell
        // *before* a setZoomRatio call and not one fell after -- the camera stays bound, the
        // preview keeps updating and looks correctly magnified, the analyzer keeps being handed
        // frames, and ZXing simply never finds a code in any of them again. The pixels-per-module
        // problem zoom appears to solve is real, but the lever that actually works is
        // MainScannerWidget's own ImageAnalysis resolution (see its analysisSelector, raised to
        // 1920x1440 for exactly this) -- with that in place the densest code this app generates
        // decodes in ~1.4s at this rig's fixed geometry, no zoom involved.

        // generateAndHoldAll holds its *first* case for this much longer than BATCH_HOLD_MILLIS --
        // device B's own join (to device A, which generateAndHoldAll's own preamble only just
        // brought up) needs a real relay reservation of its own, re-established from scratch every
        // process launch, confirmed live to take up to ~90s on a real device. Every later case
        // shares that same already-established standing (kvmobile.go's own SELFHEAL persists the
        // grant server-side, and libp2p's reservation-once-open keeps working across the rest of
        // the run), so only the first case needs this much room -- confirmed live: without it, A
        // raced through and finished generating an entire short batch before B had even reached
        // screen_main, let alone started watching.
        const val FIRST_CASE_HOLD_MILLIS = 100_000L

        // awaitAndVerifyScanAll's own counterpart to FIRST_CASE_HOLD_MILLIS: device A holds case
        // index 0 on screen for up to FIRST_CASE_HOLD_MILLIS regardless of how quickly B actually
        // caught it (a fixed sleep, not signalled early on B's success), so case index 0's *own*
        // code can still be on screen for that entire span, and case index 1's code cannot appear
        // until that span elapses -- confirmed live: with BATCH_TIMEOUT_MS's ordinary 15s budget
        // on both, a fast join (B catching case 0 in seconds) made case 1's wait time out at 15s
        // while A was still ~85s away from ever showing it, cascading into every case after it
        // timing out too. Applied to indices 0 and 1 only -- case 0 needs it for the same
        // worst-case-join reason FIRST_CASE_HOLD_MILLIS exists at all (up to ~90s, observed live),
        // and case 1 needs it purely to outlast A's fixed hold on case 0; index 2 onward is back on
        // BATCH_HOLD_MILLIS/BATCH_TIMEOUT_MS's already-synchronized fast cadence.
        const val FIRST_CASE_AWAIT_TIMEOUT_MS = FIRST_CASE_HOLD_MILLIS + 20_000L

        // Both devices are already part of the same live raft cluster for the whole batch (A the
        // solo leader, B a joined learner), but the raft-replicated KV store is the wrong transport
        // for this: confirmed live even A's own *local* Submit (no forwarding, no network hop at
        // all) routinely took several real seconds -- a raft commit, unsurprisingly, since that's
        // exactly what Submit is for. Kvmobile's Channel API (OpenChannel/ListenChannel/
        // SendChannelData -- see mobile/kvmobile/channel.go's own doc comment: "a persistent,
        // unreplicated, bidirectional, multipurpose stream to another peer") is the actual
        // purpose-built low-latency path this project already has for exactly this kind of
        // one-off, no-consensus-needed notification, so this mechanism uses that instead: A calls
        // ListenChannel once (see [startCaseSignalListener]) and records whatever case_id arrives
        // in [lastSignaledCaseID]; B calls OpenChannel to A's own peer id once (see
        // [openCaseSignalChannel]) and SendChannelData's the current case_id every time it finishes
        // one (see [signalCaseDone]).
        //
        // B only ever signals a case *done* once it has actually PASSED -- a failed case (wrong/
        // missing scan, rejected result that didn't match what was expected) has nothing useful for
        // A to advance into, so it never claims one. What B does send while a case is still in
        // doubt is a retry keepalive (see [signalCaseRetry]), one per re-arm, which asks A to keep
        // the *same* code on screen for another ceiling's worth of time instead of moving on.
        //
        // That distinction is what makes a transient miss survivable. Without it, one code that
        // happened not to decode inside a single ceiling ended the entire run: measured live, a
        // 90-case run died on case 1 of 90 for exactly this reason, having decoded nothing at all
        // in 137 seconds, while the identical case decoded in 1.5s minutes later and a repeat run
        // went 90/90 with three cases needing a re-arm along the way. A missing signal *and* no
        // keepalive within the ceiling still means one of two things, both still fatal to the rest
        // of the run -- B has given up on this case, or the signal channel itself is broken --
        // so both sides still abort the whole batch there (see generateAndHoldAll/
        // awaitAndVerifyScanAll's own loops) rather than cascading through every remaining case as
        // a wall of timeouts, which is what an earlier, always-signal-regardless-of-outcome version
        // of this mechanism did in practice.
        const val SIGNAL_POLL_INTERVAL_MS = 200L
        // Must be one of shmevent.ChannelPurposeName's own fixed strings ("data"/"control"/
        // "video") or a plain decimal byte -- confirmed live an arbitrary label like
        // "optical_case_done" is rejected outright ("unknown purpose"), not just logged oddly.
        // "data" is the correct, semantically-plain choice for this arbitrary small payload.
        const val SIGNAL_CHANNEL_PURPOSE = "data"
    }

    /** Set by [startCaseSignalListener]'s ChannelCallback (on the channel's own pump thread, per its own doc comment -- hence AtomicReference, not a plain var) to whatever case_id B most recently signalled as done. */
    private val lastSignaledCaseID = AtomicReference<String>()

    /**
     * Device A's own view of B's retry keepalives (see [RETRY_SIGNAL_PREFIX]): the case id most
     * recently asked about, and a counter of how many keepalives have arrived in total. The counter
     * is what [awaitCaseSignalOrCeiling] actually watches -- the case id alone can't distinguish a
     * second keepalive for the same case from the first one it already extended on, and every
     * keepalive is meant to buy one extension.
     */
    private val lastRetryCaseID = AtomicReference<String>()
    private val retrySignalCount = AtomicLong(0)

    /**
     * Device-A setup, called once before the case loop: opens a listener for device B's own
     * [signalCaseDone] channel and records every case_id it delivers into [lastSignaledCaseID].
     * `Kvmobile.listenChannel` blocks internally until a peer actually connects (see its own doc
     * comment), so this runs on a background coroutine rather than the caller's -- calling it
     * inline would block [generateAndHoldAll] from ever generating case 0's own code while waiting
     * for a connection B won't even attempt until well into its own setup. Dispatched via
     * `Dispatchers.IO` specifically, the same mechanism every other background Kvmobile call in
     * this app already uses (see CommandExecutor.execute/AppRoot's own Kvmobile.start()) -- an
     * earlier version of this spawned a raw `java.lang.Thread` instead, confirmed live that crashes
     * the whole process with a Go runtime fatal error ("bulkBarrierPreWrite: unaligned arguments")
     * the moment a long-lived blocking gomobile call runs on a thread the Go runtime never
     * otherwise sees; Dispatchers.IO's own managed thread pool doesn't have that problem.
     */
    private fun startCaseSignalListener(opticalTag: String) {
        CoroutineScope(Dispatchers.IO).launch {
            val result = runCatching {
                Kvmobile.listenChannel(object : ChannelCallback {
                    override fun onData(purpose: String, chunk: ByteArray) {
                        val payload = String(chunk, StandardCharsets.UTF_8)
                        if (payload.startsWith(RETRY_SIGNAL_PREFIX)) {
                            lastRetryCaseID.set(payload.removePrefix(RETRY_SIGNAL_PREFIX))
                            retrySignalCount.incrementAndGet()
                        } else {
                            lastSignaledCaseID.set(payload)
                        }
                    }
                    override fun onClosed(reason: String) {
                        Log.i(TAG, "$opticalTag signal channel closed: $reason")
                    }
                })
            }
            Log.i(TAG, "$opticalTag signal channel listener -> ${result.getOrNull()} (error=${result.exceptionOrNull()?.message})")
        }
    }

    /**
     * Polls [lastSignaledCaseID] for up to [maxMillis], returning true as soon as it reads back
     * [caseID] rather than always waiting the full duration, or false once [maxMillis] elapses with
     * no matching signal -- the caller ([generateAndHoldAll]) treats false as fatal to the whole
     * batch, not just this case (see the class's own doc comment on [SIGNAL_POLL_INTERVAL_MS] for
     * why).
     *
     * A retry keepalive for this same case (see [signalCaseRetry]) buys another full [maxMillis]
     * instead, up to [MAX_HOLD_EXTENSIONS] times, and runs [onRetry] -- which re-shows the same
     * code rather than merely continuing to hold it. A keepalive naming some *other* case is
     * ignored: it can only be a straggler from a case A has already advanced past, and honouring it
     * would extend the wrong code's hold.
     *
     * Re-showing matters because of what B is actually stuck on when it asks. Measured across
     * several 90-case runs, a stuck case is not a slow decode: the scanning device's camera stays
     * healthy by every measure it has (fresh frames, unchanged sharpness, its own analyzer running)
     * and simply never finds a code in a scene that has not changed in over two minutes, while the
     * very same frame, screenshotted off that device's preview, decodes offline instantly. B's own
     * camera rebind does not break the spell. What has never once failed to is the scene changing
     * -- which is what a human does without thinking, taking the code away and bringing it back.
     * So that is what A does here.
     */
    private fun awaitCaseSignalOrCeiling(caseID: String, maxMillis: Long, onRetry: () -> Unit = {}): Boolean {
        var deadline = System.currentTimeMillis() + maxMillis
        var seenRetries = retrySignalCount.get()
        var extensions = 0
        while (System.currentTimeMillis() < deadline) {
            if (lastSignaledCaseID.get() == caseID) return true
            val retries = retrySignalCount.get()
            if (retries != seenRetries) {
                seenRetries = retries
                if (lastRetryCaseID.get() == caseID && extensions < MAX_HOLD_EXTENSIONS) {
                    extensions++
                    Log.i(TAG, "$OPTICAL_LOG_TAG case=$caseID device B asked for another look -- re-showing this code and holding it a further ${maxMillis}ms (extension $extensions/$MAX_HOLD_EXTENSIONS)")
                    runCatching { onRetry() }
                        .onFailure { Log.w(TAG, "$OPTICAL_LOG_TAG case=$caseID could not re-show this code, holding the existing one instead: $it") }
                    deadline = System.currentTimeMillis() + maxMillis
                }
            }
            Thread.sleep(SIGNAL_POLL_INTERVAL_MS)
        }
        return false
    }

    /**
     * Device-B setup, called once before the case loop: discovers device A's own peer id (the
     * "leader" entry in this device's own live cluster membership -- it already joined A as a
     * learner well before this point) and opens a signal channel to it. Returns null (logged, not
     * thrown) on any failure -- [signalCaseDone] already treats a missing channel the same as a
     * failed send, which correctly starves A into aborting the batch either way.
     */
    private fun openCaseSignalChannel(opticalTag: String): String? {
        val leaderPeerID = runCatching {
            val members = JSONArray(Kvmobile.listClusterMembers())
            (0 until members.length())
                .map { members.getJSONObject(it) }
                .first { it.getString("role") == "leader" }
                .getString("peer_id")
        }.getOrNull()
        if (leaderPeerID == null) {
            Log.w(TAG, "$opticalTag openCaseSignalChannel: could not discover leader peer id from listClusterMembers")
            return null
        }
        val channelID = runCatching {
            Kvmobile.openChannel(
                leaderPeerID,
                object : ChannelCallback {
                    override fun onData(purpose: String, chunk: ByteArray) = Unit
                    override fun onClosed(reason: String) {
                        Log.i(TAG, "$opticalTag signal channel to $leaderPeerID closed: $reason")
                    }
                },
            )
        }.getOrNull()
        Log.i(TAG, "$opticalTag signal channel to leader($leaderPeerID) -> $channelID")
        return channelID
    }

    /** Device B's own signal that it has finished (and passed) [caseID] -- see the class's own doc comment on [SIGNAL_POLL_INTERVAL_MS]. Best-effort: a failure here is indistinguishable to A from B never having passed the case at all, which is the correct fallback -- A aborts the batch either way. */
    private fun signalCaseDone(caseID: String, signalChannelID: String?, opticalTag: String) {
        if (signalChannelID == null) {
            Log.w(TAG, "$opticalTag signalCaseDone($caseID): no signal channel open, skipping")
            return
        }
        val result = runCatching { Kvmobile.sendChannelData(signalChannelID, SIGNAL_CHANNEL_PURPOSE, caseID.toByteArray(StandardCharsets.UTF_8)) }
        if (result.isFailure) {
            Log.w(TAG, "$opticalTag signalCaseDone($caseID) failed: ${result.exceptionOrNull()?.message}")
        }
    }

    /**
     * Device B's own "this case isn't done, don't take the code away yet" keepalive, sent once per
     * re-arm from [awaitScannedTag] -- see [RETRY_SIGNAL_PREFIX] and [awaitCaseSignalOrCeiling].
     * Same best-effort treatment as [signalCaseDone]: if it doesn't land, A simply falls back to
     * the pre-keepalive behaviour of timing the case out at its own ceiling.
     *
     * Silently does nothing when there's no channel and no case in flight, which is the single-case
     * [awaitAndVerifyScan] path -- nothing on the other side is holding a ceiling there for this to
     * extend (that method's device-A counterpart is a plain fixed sleep).
     */
    private fun signalCaseRetry(opticalTag: String) {
        val channelID = caseSignalChannelID ?: return
        val caseID = currentCaseID ?: return
        val payload = RETRY_SIGNAL_PREFIX + caseID
        val result = runCatching { Kvmobile.sendChannelData(channelID, SIGNAL_CHANNEL_PURPOSE, payload.toByteArray(StandardCharsets.UTF_8)) }
        if (result.isFailure) {
            Log.w(TAG, "$opticalTag signalCaseRetry($caseID) failed: ${result.exceptionOrNull()?.message}")
        } else {
            Log.i(TAG, "$opticalTag asked device A to keep holding this code for another look")
        }
    }

    /**
     * Device B's own signal channel to A, and the case it is currently working on -- both set by
     * [awaitAndVerifyScanAll] so [signalCaseRetry] can reach A from deep inside a case's own scan
     * wait without every intermediate helper having to thread them through. Null on the single-case
     * [awaitAndVerifyScan] path, which has no batch to keep in step.
     */
    private var caseSignalChannelID: String? = null
    private var currentCaseID: String? = null

    /**
     * Device-A half of the real-camera optical-scan e2e harness (pkg/e2erun/android_optical.go,
     * driven by test/e2e/testdata.json's android_optical_cases): decodes a single
     * [e2edata.OpticalGenerateSpec]-shaped JSON object (plus its own "case_id"/"hold_millis")
     * from the "opticalSpec" instrumentation arg, generates it via [generateOneCase], and holds
     * the result on screen for a real camera on a second device to read. A thin single-case
     * wrapper kept for standalone spot-checking (e.g. `-e class ...#generateAndHold` against one
     * case by hand) -- [generateAndHoldAll] is what pkg/e2erun/android_optical.go actually drives
     * now, looping every case in one still-alive session instead of relaunching per case. Silently
     * returns if the arg is absent, same "this test invoked some other way" tolerance
     * [instrumentationArgJson] gives any missing/unparsable arg.
     */
    @Test
    fun generateAndHold() {
        val context = InstrumentationRegistry.getInstrumentation().targetContext
        val spec = instrumentationArgJson("opticalSpec") ?: return
        val caseID = spec.getString("case_id")
        val holdMillis = spec.optLong("hold_millis", 30_000L)
        val opticalTag = "$OPTICAL_LOG_TAG generateAndHold case=$caseID"
        Log.i(TAG, "$opticalTag starting")

        startSoloAndRequestRelay(context, opticalTag)
        waitForScreen("screen_main")
        val entry = generateOneCase(context, spec, opticalTag)
        writeResults(context, JSONArray().put(entry))
        Log.i(TAG, "$opticalTag holding for ${holdMillis}ms")
        Thread.sleep(holdMillis)
        Log.i(TAG, "$opticalTag hold complete, returning")
    }

    /**
     * Batched device-A half of the optical harness: decodes {"specs": [...]} (an array of
     * [e2edata.OpticalGenerateSpec]-shaped objects, each with its own "case_id") from the
     * "opticalSpecs" instrumentation arg, and loops [generateOneCase] over every one of them in a
     * single still-alive session -- the StartSolo/RequestRelayAccess preamble runs exactly once
     * for the whole batch, not once per case, which is the entire point: an earlier per-case
     * design relaunched a fresh instrumentation process (so a fresh Android app process, a fresh
     * Kvmobile session bootstrap) for every single case, and that relaunch overhead, confirmed
     * live, dwarfed the actual navigation/scan work when multiplied by a 90-case run. Between
     * cases, dismisses the generated code and navigates back to screen_main (see
     * [resetToMainAfterGenerate]) rather than letting the Activity tear down, so the next case
     * starts from the same clean state generateAndHold's own single-case version gets for free
     * from a fresh process. Holds each case until device B signals it's done over a direct signal
     * channel (see [awaitCaseSignalOrCeiling]/[startCaseSignalListener]), not for a fixed sleep --
     * [BATCH_HOLD_MILLIS]/[FIRST_CASE_HOLD_MILLIS] remain the fallback ceiling if that signal never
     * arrives, not the normal-case wait (confirmed live this distinction matters: a fixed hold
     * shorter than B's own worst-case per-case time is a correctness bug, not just lost speed --
     * see those constants' own doc comments). Writes the accumulated results array after every
     * case, not just at the end, so the Go harness could observe partial progress if it ever needed
     * to (it doesn't today -- it only reads the file once the whole invocation has finished).
     */
    @Test
    fun generateAndHoldAll() {
        val context = InstrumentationRegistry.getInstrumentation().targetContext
        val arg = instrumentationArgJson("opticalSpecs") ?: return
        val specs = arg.getJSONArray("specs")
        val opticalTag = "$OPTICAL_LOG_TAG generateAndHoldAll"
        Log.i(TAG, "$opticalTag starting, ${specs.length()} case(s)")

        startSoloAndRequestRelay(context, opticalTag)
        waitForScreen("screen_main")
        startCaseSignalListener(opticalTag)

        val results = JSONArray()
        for (i in 0 until specs.length()) {
            val spec = specs.getJSONObject(i)
            val caseID = spec.optString("case_id", "?")
            val caseTag = "$opticalTag case=$caseID (${i + 1}/${specs.length()})"
            val entry = try {
                generateOneCase(context, spec, caseTag)
            } catch (e: Throwable) {
                Log.w(TAG, "$caseTag FAIL error=${e.message}")
                JSONObject().put("command", "OpticalGenerate: $caseID").put("pass", false).put("error", e.message ?: e.toString())
            }
            results.put(entry)
            writeResults(context, results)
            // Matches awaitAndVerifyScanAll's own `i <= 1` extension (see
            // FIRST_CASE_AWAIT_TIMEOUT_MS's doc comment) -- confirmed live this asymmetry is a
            // real bug, not just an unnecessary ceiling gap: B's own budget for case index 1 is
            // already this generous, so a merely-slow-not-broken decode there (observed live: a
            // camera refocus mid-decode pushed a single scan past 20s even after the rig was
            // already warmed up) made A give up and abort the whole batch before B's own,
            // correctly-sized wait ever had a chance to succeed.
            //
            // A case may also carry its own hold_millis (see OpticalScanCase's own doc comment
            // and pkg/e2erun/android_optical.go, which validates hold >= timeout before either
            // device ever sees it) -- taken as a floor, never a cap, so a per-case budget can
            // only ever widen the two indices above, not undercut them.
            val holdCeilingMillis = maxOf(
                spec.optLong("hold_millis", BATCH_HOLD_MILLIS),
                if (i <= 1) FIRST_CASE_AWAIT_TIMEOUT_MS else 0L,
            )
            Log.i(TAG, "$caseTag awaiting B's completion signal, ceiling ${holdCeilingMillis}ms")
            val reshow = {
                // The same two calls the loop itself uses to move between cases, just aimed back at
                // the case already in flight: dismiss what's on screen, return to screen_main, and
                // generate this very spec again. Generating a RunCode or a NavCode is pure
                // rendering (see RunCode.encode -- no session, no daemon call), so for 86 of the 90
                // cases this is free; the few ticket cases mint a fresh ticket, which is exactly as
                // valid to scan as the one it replaces.
                resetToMainAfterGenerate(spec.optString("target", ""))
                generateOneCase(context, spec, "$caseTag re-show")
                Unit
            }
            if (!awaitCaseSignalOrCeiling(caseID, holdCeilingMillis, reshow)) {
                Log.w(TAG, "$caseTag no completion signal from B within ${holdCeilingMillis}ms -- aborting the rest of the batch (every remaining case would just be generated for nobody to catch)")
                break
            }
            resetToMainAfterGenerate(spec.optString("target", ""))
        }
        Log.i(TAG, "$opticalTag all ${specs.length()} case(s) generated, returning")
    }

    /**
     * Device A's own StartSolo/RequestRelayAccess preamble, shared by [generateAndHold] and
     * [generateAndHoldAll] -- device A's own build deliberately bakes no leaderMultiaddr (see
     * TestManualOpticalScan's own doc comment on why: a leader baked to device A's *own* address,
     * correct for device B's build, would make device A's automatic Kvmobile.start() try to dial
     * itself), so AppRoot's own auto-Start always fails harmlessly here, and this device needs its
     * own explicit, idempotent bootstrap before anything that needs a signing key or a real
     * dialable address can work. Both calls are safe to repeat against an already-provisioned/
     * already-granted state (see StartSolo's and RequestRelayAccess's own doc comments).
     *
     * Then waits for this device to actually become *reachable*, not merely granted. Device B's
     * whole run depends on dialing this device through the relay (its build bakes this device's
     * `/p2p-circuit/` address as leaderMultiaddr), and RequestRelayAccess returning a grant id is
     * not the same event as AutoRelay having landed a circuit reservation off the back of it --
     * the grant is what *permits* the reservation, which is established afterwards, and until it
     * lands this device advertises no circuit address for anyone to dial. Confirmed live, twice:
     * B failed with "join cluster: ... failed to dial <A>: all dials failed" while A's own log
     * showed a perfectly successful `requestRelayAccess -> <grant id>` a second earlier, and a
     * later GetOwnAddr on A (once it had been up a while) returned the expected
     * `/ip4/.../p2p-circuit/p2p/<A>` address all along.
     *
     * Polling GetOwnAddr for that address is a direct check of the thing B actually needs, and it
     * belongs here rather than as a longer fixed sleep on the Go side (pkg/e2erun's head start),
     * which can only ever guess at it from outside. Non-fatal on timeout: a device that never
     * reserves still has to report its real failures per-case rather than dying in the preamble.
     */
    private fun startSoloAndRequestRelay(context: android.content.Context, opticalTag: String) {
        val startSoloResult = runCatching { Kvmobile.startSolo(context.filesDir.absolutePath) }
        Log.i(TAG, "$opticalTag startSolo -> ${startSoloResult.getOrNull()} (error=${startSoloResult.exceptionOrNull()?.message})")
        val relayResult = runCatching { Kvmobile.requestRelayAccess("optical e2e generateAndHold") }
        Log.i(TAG, "$opticalTag requestRelayAccess -> ${relayResult.getOrNull()} (error=${relayResult.exceptionOrNull()?.message})")
        awaitOwnCircuitAddr(opticalTag)
    }

    /**
     * Polls `Kvmobile.getOwnAddr()` until it reports a `/p2p-circuit/` address -- i.e. until this
     * device is genuinely dialable by the other one. See [startSoloAndRequestRelay] for why a
     * granted relay permit alone isn't enough.
     */
    private fun awaitOwnCircuitAddr(opticalTag: String) {
        val deadline = System.currentTimeMillis() + CIRCUIT_ADDR_TIMEOUT_MS
        while (System.currentTimeMillis() < deadline) {
            val addr = runCatching { Kvmobile.getOwnAddr() }.getOrDefault("")
            if (addr.contains("/p2p-circuit/")) {
                Log.i(TAG, "$opticalTag reachable via relay: $addr")
                return
            }
            Thread.sleep(CIRCUIT_ADDR_POLL_MS)
        }
        Log.w(
            TAG,
            "$opticalTag still has no /p2p-circuit/ address after ${CIRCUIT_ADDR_TIMEOUT_MS}ms -- " +
                "device B will probably fail to dial this device; continuing anyway so each case reports its own real result",
        )
    }

    /**
     * Generates one [spec]-described case -- shared by [generateAndHold] (one case, fresh
     * process) and [generateAndHoldAll] (many cases, one session) -- and returns its
     * "OpticalGenerate: <case_id>" result entry. Assumes the caller is already on the pager's
     * Default group (both callers arrange this: a fresh process starts there; a later batch
     * iteration is put back there by [resetToMainAfterGenerate]) and has already done the
     * StartSolo/RequestRelayAccess preamble ([startSoloAndRequestRelay]). `target == "run"` opens
     * the Default group's "Commands" picker, selects `name`, fills in `params` on the resulting
     * CommandDetailScreen, and taps Generate; `target == "nav_group"` has no bare-QR generation
     * path left at all (see that branch's own inline comment) so it seeds a log entry and drives
     * that entry's own Group QR button instead. Either way leaves the result on screen
     * (generatedDataMatrixImage) for the caller to hold/dismiss, since how long to hold and what
     * happens next differs between the two callers.
     */
    private fun generateOneCase(context: android.content.Context, spec: JSONObject, opticalTag: String): JSONObject {
        val caseID = spec.getString("case_id")
        val target = spec.getString("target")
        val category = spec.optString("category", "")
        fun field(i: Int) = spec.getJSONArray("params").getString(i)
        val fieldCount = spec.optJSONArray("params")?.length() ?: 0

        when (target) {
            "run" -> {
                val name = spec.getString("name")
                val allCommandsForLookup = buildCommands(context.filesDir.absolutePath, OutputLog::append)
                val commandSpec = allCommandsForLookup.first { it.category == category && it.name == name }

                // Dispatch: DialSubmitCommand is the one case that needs a RunCommandDispatcher
                // handler registered on *this* device before generating -- its callback is an
                // in-process subscription with no persistence across separate instrumentation
                // invocations (a fresh `am instrument` launch starts a fresh process/daemon
                // session), so nothing would ever observe device B's dial-in dispatch during the
                // hold below without doing this here, in the very process that then stays alive
                // holding the code on screen. Called directly through CommandExecutor, the same
                // primitive RunConfirmDialog's own Execute button calls -- this is test-harness
                // setup, not a production UI path, so it doesn't need a scan of its own any more
                // than the StartSolo/RequestRelayAccess preamble does. CommandCatalog.kt's own
                // param order for DialSubmitCommand is [targetAddr, commandID, inputsJSON, note],
                // so field(1) is the commandID to register a handler for.
                if (category == "Dispatch" && name == "DialSubmitCommand" && fieldCount > 1) {
                    val dispatcherCommandID = field(1)
                    Log.i(TAG, "$opticalTag registering RunCommandDispatcher for commandID=$dispatcherCommandID")
                    val dispatcherSpec = allCommandsForLookup.first { it.category == "Dispatch" && it.name == "RunCommandDispatcher" }
                    val dispatcherResult = runBlocking { CommandExecutor.execute(dispatcherSpec, listOf(dispatcherCommandID)) }
                    Log.i(TAG, "$opticalTag RunCommandDispatcher -> $dispatcherResult")
                }

                // Journal cases need this device to actually own a log book
                // and be answering for it before device B scans anything --
                // the same "harness setup, not a production UI path"
                // reasoning as the dispatcher pre-registration above, and
                // for the same underlying reason: a RunCommandDispatcher
                // subscription lives in this process and would not survive
                // being set up anywhere else.
                if (category == "Journal") {
                    ensureJournalBook(allCommandsForLookup, opticalTag)
                }

                // "{{selfAddr}}" is the one substitution token an optical case's own params can
                // reference (e.g. a Dispatch: DialSubmitCommand case naming its own generating
                // device as the dial target) -- only known live, resolved here rather than by the
                // Go harness since "self" naturally means "this device, i.e. whichever one is
                // running generateAndHold(All)," not something worth threading a value back out to
                // Go and into a second invocation's args for.
                fun resolveField(i: Int): String {
                    val raw = field(i)
                    if (raw != "{{selfAddr}}") return raw
                    return runCatching { Kvmobile.getOwnAddr() }.getOrDefault(raw)
                }

                Log.i(TAG, "$opticalTag navigating to command_detail for ${commandSpec.label}, params=${(0 until fieldCount).map { resolveField(it) }}")
                navigateToCommandDetailViaPicker(commandSpec.category, commandSpec.name)
                waitForScreen("screen_command_detail")
                for (i in 0 until fieldCount) {
                    composeTestRule.onNodeWithTag("param_$i").performTextInput(resolveField(i))
                }
                composeTestRule.onNodeWithTag("generateDataMatrixButton").performClick()
                Log.i(TAG, "$opticalTag tapped generateDataMatrixButton")
            }
            "nav_group" -> {
                // GroupPickerScreen's own Submit now *enters* a group (a plain state change,
                // see PagerScreen.kt's currentGroup) instead of generating a bare NavCode.Group
                // QR the way it used to -- the only place left to mint one is a log row's own
                // Group QR button (see LogScreen.kt), which needs a log entry in that category to
                // hang the button off of. Runs the category's own cheapest command (0-param if
                // one exists, else just the first) purely to seed that entry -- its outcome
                // (pass/fail) is irrelevant, CommandExecutor.execute records either way, exactly
                // like the DialSubmitCommand dispatcher pre-registration above this uses the same
                // "call CommandExecutor directly, this is harness setup not a production UI path"
                // reasoning for.
                val allCommandsForLookup = buildCommands(context.filesDir.absolutePath, OutputLog::append)
                val inCategory = allCommandsForLookup.filter { it.category == category }
                val probeSpec = inCategory.firstOrNull { it.params.isEmpty() } ?: inCategory.first()
                Log.i(TAG, "$opticalTag seeding a log entry in group=\"$category\" via probe ${probeSpec.label}")
                runBlocking { CommandExecutor.execute(probeSpec, probeSpec.params.map { "" }) }
                val entryID = OutputLog.snapshot().last().id

                waitForScreen("screen_main")
                composeTestRule.onNodeWithTag("screen_main").performTouchInput { swipeLeft() }
                waitForScreen("screen_log")
                composeTestRule.onNodeWithTag("logList").performScrollToIndex(OutputLog.snapshot().size - 1)
                composeTestRule.onNodeWithTag("log_groupqr_$entryID").performClick()
                Log.i(TAG, "$opticalTag tapped log_groupqr_$entryID")
            }
            else -> throw IllegalArgumentException("unknown optical generate target: $target")
        }

        waitForScreen("generatedDataMatrixImage")
        Log.i(TAG, "$opticalTag generatedDataMatrixImage rendered, writing result")
        return JSONObject().put("command", "OpticalGenerate: $caseID").put("pass", true)
    }

    /**
     * Makes this device the owner of the optical rig's log book, once per
     * instrumentation session: declares the book's columns, closes one
     * vocabulary so a case can prove a closed set is actually enforced,
     * publishes the Command device B submits against, and starts serving
     * it.
     *
     * The group is created **public**, which is the one thing here that
     * would be wrong outside a test rig: a public group admits any peer
     * with no membership record at all (see pkg/kvctl's
     * isPermittedForCommand). That is deliberate -- this device has no way
     * to learn device B's peer id before B has scanned anything, and the
     * property these cases exist to check is the journal round trip, not
     * the catalog's own membership check, which pkg/kvctl and kvmobile
     * both test directly. A real deployment names the peers it admits.
     *
     * Idempotent across cases and across runs, with one expected
     * exception: every step is find-or-create (a column already declared
     * as the same type, a vocabulary value already interned, a command
     * already published), and the whole thing is skipped after the first
     * Journal case in a session -- but closing an already-closed
     * vocabulary is deliberately an error in the journal itself, so a
     * second run logs `vocabulary of <field> is already closed` here.
     * That line is expected and means the book is in the state these
     * cases need, which is why every step is logged rather than thrown:
     * a re-run against a device whose book already exists must not abort
     * the batch.
     *
     * Verified live on the two-device rig (2026-08-19): all seven Journal
     * cases pass, including the device-signed countersignature. One
     * scheduling caveat worth knowing before running them as a filtered
     * mini-batch: device B checks its *own* replica for the command's
     * group standing, and a freshly joined B can be a heartbeat or two
     * behind (kvmobile widens raft timeouts to 4s), so a Journal case run
     * first fails with "is not permitted to submit command". In the full
     * suite they run late and B is long warm; in a mini-batch, put a
     * cheap case such as get_own_addr in front of them.
     */
    /** Whether [ensureJournalBook] has already run in this instrumentation session. */
    private var journalBookReady = false

    private fun ensureJournalBook(allCommands: List<CommandSpec>, opticalTag: String) {
        if (journalBookReady) return
        journalBookReady = true

        fun step(category: String, name: String, params: List<String>) {
            val spec = allCommands.first { it.category == category && it.name == name }
            val outcome = runCatching { runBlocking { CommandExecutor.execute(spec, params) } }
            Log.i(TAG, "$opticalTag journal setup ${spec.label} -> ${outcome.getOrNull() ?: outcome.exceptionOrNull()?.message}")
        }

        step("Journal", "Define", listOf(OPTICAL_JOURNAL_LOG, OPTICAL_JOURNAL_COLUMNS))
        step("Journal", "Vocabulary", listOf(OPTICAL_JOURNAL_LOG, "operator", OPTICAL_JOURNAL_OPERATORS, "true"))

        val selfPeerID = runCatching { Kvmobile.peerID() }.getOrDefault("")
        step("Command", "CreateCommand", listOf(OPTICAL_JOURNAL_COMMAND_ID, "Optical shift log", selfPeerID))
        step("Group", "CreateGroup", listOf(OPTICAL_JOURNAL_GROUP_ID, "Optical shift log writers", "true"))
        step("Links", "AddCommandToGroup", listOf(OPTICAL_JOURNAL_COMMAND_ID, OPTICAL_JOURNAL_GROUP_ID))
        step("Journal", "Serve", listOf(OPTICAL_JOURNAL_COMMAND_ID, OPTICAL_JOURNAL_LOG))
    }

    /**
     * Returns [generateAndHoldAll] to the pager's Default group after one case's generated code
     * has had its hold window: dismisses the GeneratedDataMatrixDialog, then undoes whatever
     * [generateOneCase] did to get there. "run" pushed two real routes (commandPicker,
     * commandDetail -- both real `NavController` destinations), so `pressBack()` twice pops back
     * to the still-alive pager route underneath. "nav_group" pushed no route at all -- it only
     * swiped the pager from its list page to its log page (see that branch's own comment) -- so
     * undoing it is a swipe back, not a pop. Either way waits for screen_default_group
     * specifically, not just screen_main: that tag now sits on the pager route's own outer
     * wrapper regardless of which of its two pages is showing, so it alone can't distinguish "back
     * on the Default group's list, ready for mainListItem_commands/groups" from "still on the log
     * page" the way it could before this app had more than one screen per route.
     */
    private fun resetToMainAfterGenerate(target: String) {
        composeTestRule.onNodeWithTag("generatedDataMatrixClose").performClick()
        when (target) {
            "run" -> {
                // Pop back to the pager route itself -- bounded, not a fixed count, since the two
                // navigateToCommandDetailViaPicker/ViaGroup paths (see their own doc comments)
                // leave different stack depths: Commands picker pushes commandPicker+commandDetail
                // (2 deep), Groups picker pushes commandDetail alone (1 deep -- GroupPickerScreen's
                // own Submit already pops itself before commandDetail is pushed). Popping
                // adaptively handles both without threading which path this case took all the way
                // through to here.
                var pops = 0
                while (pops < 3 && !onDefaultGroupOrLeaveChip()) {
                    pressBack()
                    pops++
                }
                // The Groups-picker path leaves currentGroup pointed at this case's own category,
                // not Default -- leave it the same way a human would, via the chip, so every case
                // starts the next one from the identical Default-group state regardless of which
                // path it took to get here.
                if (composeTestRule.onAllNodesWithTag("groupContextLeave").fetchSemanticsNodes(atLeastOneRootRequired = false).isNotEmpty()) {
                    composeTestRule.onNodeWithTag("groupContextLeave").performClick()
                }
            }
            "nav_group" -> {
                composeTestRule.onNodeWithTag("screen_main").performTouchInput { swipeRight() }
                // See settleAfterNavigation's own doc comment (2) -- confirmed live this swipe's
                // own fling/settle animation isn't always done by the time performTouchInput
                // returns, and the very next case's first tap can land while it's still settling.
                settleAfterNavigation()
            }
        }
        waitForScreen("screen_default_group")
    }

    /** True once back on the pager's own route -- either Default's list directly, or a non-Default group's list (identifiable by its leave chip) -- used by [resetToMainAfterGenerate]'s adaptive pop loop. */
    private fun onDefaultGroupOrLeaveChip(): Boolean =
        nodeExists("screen_default_group") || nodeExists("groupContextLeave")

    /**
     * Device-B half of the real-camera optical-scan e2e harness -- see [generateAndHold]'s doc
     * comment for the overall design. Decodes a single [e2edata.OpticalExpectSpec]-shaped JSON
     * object (plus its own "case_id"/"timeout_ms") from the "opticalExpect" instrumentation arg,
     * waits for it via [awaitOneCase], and throws (failing this JUnit test outright) if it
     * didn't pass. A thin single-case wrapper kept for standalone spot-checking --
     * [awaitAndVerifyScanAll] is what pkg/e2erun/android_optical.go actually drives now.
     */
    @Test
    fun awaitAndVerifyScan() {
        val context = InstrumentationRegistry.getInstrumentation().targetContext
        val spec = instrumentationArgJson("opticalExpect") ?: return
        val caseID = spec.getString("case_id")
        val opticalTag = "$OPTICAL_LOG_TAG awaitAndVerifyScan case=$caseID"
        Log.i(TAG, "$opticalTag starting")

        requestRelay(opticalTag)
        waitForScreen("screen_main")
        val entry = awaitOneCase(spec, opticalTag)
        writeResults(context, JSONArray().put(entry))
        if (!entry.getBoolean("pass")) {
            throw AssertionError(entry.optString("error"))
        }
    }

    /**
     * Batched device-B half of the optical harness: decodes {"specs": [...]} (an array of
     * [e2edata.OpticalExpectSpec]-shaped objects, each with its own "case_id") from the
     * "opticalExpects" instrumentation arg, and loops [awaitOneCase] over every one of them in a
     * single still-alive session -- see [generateAndHoldAll]'s own doc comment for why (the
     * identical per-case relaunch cost applies symmetrically to this side). A failed case here is
     * still recorded (so its own error is visible), but the loop stops right there rather than
     * attempting the rest -- see the class's own doc comment on [SIGNAL_POLL_INTERVAL_MS] for why:
     * this side's own [signalCaseDone] only ever fires on a pass, so a failed case here also means
     * A's matching [generateAndHoldAll] is about to abort its own batch the moment this case's hold
     * ceiling elapses, and there is nothing left worth waiting for. Falls back to
     * [BATCH_TIMEOUT_MS] for a case that carries no timeout_ms of its own (most of them -- see that
     * constant's own doc comment); a case that does carry one gets it, which is what lets an
     * optically harder code (a dense signed ticket, say) buy time without slowing the rest down.
     * Writes an "OpticalReady" entry as soon as this device's own RequestRelayAccess completes and
     * the camera is live, before the case loop starts -- purely diagnostic (how long this device's
     * own one-time setup actually took, on a run where the first case still failed to show up in
     * time), not something pkg/e2erun/android_optical.go gates on: device A's own
     * generateAndHoldAll starts well before this call is even invoked (see that function's own doc
     * comment for why the ordering runs the other way -- B's join target has to already be up), so
     * there is no "wait for B" moment left on the Go side to signal into.
     */
    @Test
    fun awaitAndVerifyScanAll() {
        val context = InstrumentationRegistry.getInstrumentation().targetContext
        val arg = instrumentationArgJson("opticalExpects") ?: return
        val specs = arg.getJSONArray("specs")
        val opticalTag = "$OPTICAL_LOG_TAG awaitAndVerifyScanAll"
        Log.i(TAG, "$opticalTag starting, ${specs.length()} case(s)")

        requestRelay(opticalTag)
        waitForScreen("screen_main")
        awaitSession(context, opticalTag)
        val signalChannelID = openCaseSignalChannel(opticalTag)
        caseSignalChannelID = signalChannelID

        val results = JSONArray()
        results.put(JSONObject().put("command", "OpticalReady").put("pass", true))
        writeResults(context, results)
        Log.i(TAG, "$opticalTag ready, camera scanner is live")

        for (i in 0 until specs.length()) {
            val spec = specs.getJSONObject(i)
            val caseID = spec.getString("case_id")
            // A floor, not an override: a case carrying its own (larger) timeout_ms keeps it, the
            // same way generateAndHoldAll treats hold_millis on A's side -- the two must stay in
            // step or the batch desynchronizes.
            if (i <= 1) spec.put("timeout_ms", maxOf(spec.optLong("timeout_ms", 0L), FIRST_CASE_AWAIT_TIMEOUT_MS))
            val caseTag = "$opticalTag case=$caseID (${i + 1}/${specs.length()})"
            currentCaseID = caseID
            val entry = awaitOneCase(spec, caseTag)
            results.put(entry)
            writeResults(context, results)
            if (!entry.getBoolean("pass")) {
                Log.w(TAG, "$caseTag failed -- not signalling, and stopping here rather than attempting cases A has no reason to still be generating")
                break
            }
            signalCaseDone(caseID, signalChannelID, caseTag)
        }
        Log.i(TAG, "$opticalTag all ${specs.length()} case(s) awaited, returning")
    }

    /**
     * Device B's own RequestRelayAccess call, shared by [awaitAndVerifyScan] and
     * [awaitAndVerifyScanAll] -- this device's own build bakes a real leaderMultiaddr (device A's
     * own address) that AppRoot's automatic Kvmobile.start() already joins on launch, never
     * StartSolo here (would bootstrap this device its own separate cluster instead of resuming
     * its role as A's learner). A relay reservation is not persisted across process restarts,
     * though, and a "run"/"ticket" outcome's own outbound dial needs this device to hold its own
     * standing, not just the target -- see CLAUDE.md's "both ends need their own relay standing"
     * note. Best-effort and safe to repeat (idempotent once already granted); a "nav_group"
     * outcome needs no daemon call at all, so this genuinely cannot regress that one.
     *
     * Also force-expands the scanner ([ScannerCoordinator.expanded]) for the rest of this
     * session: nothing in this harness ever taps the minimized bubble's own expand button, so
     * without this every automated run scans from that small ~80dp preview the whole time --
     * doesn't change the underlying ImageAnalysis resolution/capture (a separate CameraX use
     * case, unaffected by how large Preview is drawn on screen), but does make it possible for
     * whoever physically aims this device to actually judge framing/focus by eye instead of a
     * bubble too small to usefully inspect.
     */
    /**
     * Makes sure this device actually has a live session before the batch starts, retrying
     * `Kvmobile.start` if AppRoot's own automatic one did not get there.
     *
     * Without this a whole run can be lost to a single transient join failure. Measured live: the
     * relay refused the circuit to device A with `RESOURCE_LIMIT_EXCEEDED` after several runs in
     * quick succession, so this device came up with no session at all, and case 1 -- whose
     * expectation is `contains:{{selfPeerID}}` -- failed on a peer id that could not be resolved,
     * taking the other 89 cases with it. The relay's own limits free up on their own within
     * minutes, which is exactly the kind of failure a retry is for.
     *
     * `Kvmobile.start` is the right call to repeat: it returns the existing peer id immediately if
     * a session is already up (see startOnce's `started` short-circuit), and its own SELFHEAL path
     * re-requests relay standing on a failed join, so a retry here is both harmless and more than
     * a plain re-attempt. Non-fatal on exhaustion -- the cases themselves report what a missing
     * session breaks, and a "nav_group" case needs no session at all.
     */
    private fun awaitSession(context: android.content.Context, opticalTag: String) {
        repeat(SESSION_ATTEMPTS) { attempt ->
            val peerID = runCatching { Kvmobile.peerID() }.getOrDefault("")
            if (peerID.isNotEmpty()) {
                if (attempt > 0) Log.i(TAG, "$opticalTag session up after ${attempt + 1} attempt(s): $peerID")
                return
            }
            Log.w(TAG, "$opticalTag no session yet on attempt ${attempt + 1}/$SESSION_ATTEMPTS -- retrying Kvmobile.start")
            val started = runCatching { Kvmobile.start(context.filesDir.absolutePath) }
            Log.i(TAG, "$opticalTag Kvmobile.start -> ${started.getOrNull()} (error=${started.exceptionOrNull()?.message})")
            if (started.getOrNull()?.isNotEmpty() == true) return
            Thread.sleep(SESSION_RETRY_DELAY_MS)
        }
        Log.w(TAG, "$opticalTag still has no session after $SESSION_ATTEMPTS attempt(s) -- continuing anyway, the cases will report what that breaks")
    }

    private fun requestRelay(opticalTag: String) {
        val relayResult = runCatching { Kvmobile.requestRelayAccess("optical e2e awaitAndVerifyScan") }
        Log.i(TAG, "$opticalTag requestRelayAccess -> ${relayResult.getOrNull()} (error=${relayResult.exceptionOrNull()?.message})")
        composeTestRule.runOnUiThread { ScannerCoordinator.expanded = true }
    }


    /**
     * Waits for one [spec]-described expected outcome and confirms/executes it -- shared by
     * [awaitAndVerifyScan] (one case, fresh process) and [awaitAndVerifyScanAll] (many cases, one
     * session) -- returning its "OpticalScan: <case_id>" result entry. Never throws: a timeout or
     * assertion failure is caught and recorded as a failing entry instead, since
     * [awaitAndVerifyScanAll]'s own loop needs to move on to the next case regardless. Assumes
     * the caller is already on screen_main and has already called [requestRelay] -- true for a
     * fresh process, and true between batch iterations since every kind below returns here on its
     * own (an overlay dialog dismissing, or an explicit [pressBack] after a real navigation) by
     * the time this returns.
     */
    private fun awaitOneCase(spec: JSONObject, opticalTag: String): JSONObject {
        val caseID = spec.getString("case_id")
        val kind = spec.getString("kind")
        val timeoutMs = spec.optLong("timeout_ms", BATCH_TIMEOUT_MS)
        val resultLabel = "OpticalScan: $caseID"
        Log.i(TAG, "$opticalTag kind=$kind starting, timeoutMs=$timeoutMs")
        logCompositionHealth("the start of $caseID")

        // AppRoot's own scan dispatch collapses the scanner back to its minimized bubble after
        // every decode (success or not) -- see its own doc comment. requestRelay's initial expand
        // only covers the very first case in a batch, so re-assert it before every case here too.
        composeTestRule.runOnUiThread { ScannerCoordinator.expanded = true }

        fun pass(output: String = ""): JSONObject {
            Log.i(TAG, "$opticalTag PASS output=$output")
            return JSONObject().put("command", resultLabel).put("pass", true).put("output", output)
        }
        fun fail(error: String): JSONObject {
            Log.w(TAG, "$opticalTag FAIL error=$error")
            return JSONObject().put("command", resultLabel).put("pass", false).put("error", error)
        }

        return try {
            when (kind) {
                "run" -> {
                    awaitScannedTag("runConfirmExecute", timeoutMs, opticalTag)
                    val params = readTagText("runConfirmParams")
                    Log.i(TAG, "$opticalTag runConfirmExecute appeared, params=$params")
                    val priorLogSize = OutputLog.snapshot().size
                    composeTestRule.onNodeWithTag("runConfirmExecute").performClick()
                    Log.i(TAG, "$opticalTag tapped Execute, waiting for a new OutputLog entry (prior size=$priorLogSize)")
                    try {
                        composeTestRule.waitUntil(RUN_TIMEOUT_MS) { OutputLog.snapshot().size > priorLogSize }
                    } catch (e: ComposeTimeoutException) {
                        throw AssertionError("no OutputLog entry appeared after tapping Execute", e)
                    }
                    val resultBody = OutputLog.snapshot().last().body
                    Log.i(TAG, "$opticalTag OutputLog result=$resultBody")
                    settleAfterDialog("runConfirmExecute")
                    val selfPeerID = runCatching { Kvmobile.peerID() }.getOrDefault("")
                    resultExpectation(spec.optString("result", "succeeded"), selfPeerID)(resultBody)
                    // CommandExecutor's own success format is "(args) ->\n$result" -- strip that
                    // fixed prefix to recover $result alone (e.g. DialSubmitCommand's instance
                    // id), needed by pkg/e2erun/android_optical.go's own VerifyOnDeviceA
                    // cross-device check; a failure line has no "->\n" separator at all, so
                    // substringAfter's own default (the untouched string) is exactly right there
                    // too.
                    pass(resultBody.substringAfter("->\n", resultBody))
                }
                "nav_group" -> {
                    // Wrapped in try/finally, not just a trailing cleanup after recordPass: a
                    // NavCode.Group scan enters a real group (see PagerScreen.kt's currentGroup),
                    // so a title mismatch below has to leave it before this returns too, or the
                    // *next* case in an awaitAndVerifyScanAll batch would start from the wrong
                    // group. Entering a group pushes no NavController route at all now (unlike the
                    // pre-pager-rewrite app, where this scan navigated to a distinct
                    // "commands/{category}" route) -- so leaving one is the groupContextLeave chip
                    // (see PagerScreen.kt's GroupContextBar), not a pressBack().
                    try {
                        awaitScannedTag("screen_commands", timeoutMs, opticalTag)
                        Log.i(TAG, "$opticalTag screen_commands appeared at ${System.currentTimeMillis()}")
                        settleAfterNavigation()
                        Log.i(TAG, "$opticalTag settle complete at ${System.currentTimeMillis()}")
                        val title = readTagText("categoryTitle")
                        val want = spec.getString("category_title")
                        Log.i(TAG, "$opticalTag categoryTitle=\"$title\" want=\"$want\"")
                        if (title != want) throw AssertionError("categoryTitle=\"$title\", want \"$want\"")
                        pass()
                    } finally {
                        // Only meaningful once the scan actually entered a group (a timeout above
                        // means it never left the Default group, so this is a harmless no-op then).
                        if (composeTestRule.onAllNodesWithTag("groupContextLeave").fetchSemanticsNodes(atLeastOneRootRequired = false).isNotEmpty()) {
                            composeTestRule.onNodeWithTag("groupContextLeave").performClick()
                            waitForScreen("screen_default_group")
                            Log.i(TAG, "$opticalTag left group, back to Default, returning")
                        }
                    }
                }
                "ticket" -> {
                    awaitScannedTag("recruitConfirmApprove", timeoutMs, opticalTag)
                    Log.i(TAG, "$opticalTag recruitConfirmApprove appeared")
                    composeTestRule.onNodeWithTag("recruitConfirmApprove").performClick()
                    Log.i(TAG, "$opticalTag tapped Approve")
                    settleAfterDialog("recruitConfirmApprove")
                    pass()
                }
                else -> throw IllegalArgumentException("unknown optical expect kind: $kind")
            }
        } catch (e: Throwable) {
            // Log the throwable itself, not just its message: a failure here aborts the whole
            // batch (see awaitAndVerifyScanAll's own break), so the ~10 minutes of real
            // two-device hardware time it costs to reproduce is worth far more than the few
            // logcat lines a stack trace adds. Learned the hard way -- a live run died on
            // "Can't create handler inside thread ... that has not called Looper.prepare()",
            // which names no class of ours at all (nothing in this app or harness constructs a
            // Handler/Looper), so the bare message was not enough to tell which library call on
            // which thread actually threw it.
            Log.w(TAG, "$opticalTag FAIL (stack trace follows)", e)
            fail(e.message ?: e.toString())
        }
    }

    /**
     * Waits for whatever UI a scanned code is supposed to produce -- [tag] being a confirmation
     * dialog's confirm button ("run"/"ticket") or a navigation target's screen ("nav_group") --
     * re-arming the scanner between attempts rather than losing the whole batch to one missed
     * decode.
     *
     * Applies to *every* kind, not just the confirm-dialog ones. This started out as
     * `awaitConfirmButton`, used only by "run"/"ticket", while "nav_group" called
     * [waitForTagWithTimeout] directly -- one look, no re-arm. Confirmed live that the asymmetry
     * is what it looks like: a run whose case 1 decoded in 200ms then sat through the whole of
     * case 2 ("nav_group_kv") without a single decode, device B's watchdog reporting a healthy
     * camera (sharpness 0.0131 against a best of 0.0153, fresh frames throughout) and device A
     * holding the code on screen for the full 120s. A "run" case in that state gets a rebind and
     * a second look and usually recovers; "nav_group" had no such recovery, so the same transient
     * miss that costs a "run" case a few seconds killed the remaining 88 cases outright.
     *
     * The case this exists for, confirmed live on a real two-device run: case 13 of 90
     * (cluster_kick) decoded perfectly -- device B's own app logged `run-code scan decoded:
     * RunCode(category=Cluster, name=Kick, ...)` and then `RunConfirmDialog shown` 1.7s into the
     * case -- yet [waitForTagWithTimeout] never once found `runConfirmExecute` across the following
     * 50s, and since a failed case stops the batch by design (see [awaitAndVerifyScanAll]), the
     * remaining 77 cases died with it. The one clue was this class's own predicate-error
     * diagnostic: the semantics lookup threw exactly once during that window, with
     * `RuntimeException: Can't create handler inside thread Thread[pool-7-thread-1,5,main] that has
     * not called Looper.prepare()` -- a framework thread, no code of ours anywhere on the stack --
     * after which every later poll ran cleanly and simply never saw a dialog.
     *
     * The recovery is to look again -- in [SCAN_LOOK_MS] slices, for up to [SCAN_ATTEMPTS] times the
     * case's own timeout budget, each re-arm buying a matching extension of device A's own hold (see
     * [signalCaseRetry]), so the two devices stay in step however long the case ends up taking --
     * with [forceRescan] in between: device A holds one single code on screen for the whole case, so
     * without re-arming the dedup a second wait would watch a scanner that has already decided it
     * has nothing new to report. Confirmed live to work exactly as intended on the very next full
     * run: case 64 of 90 (links_remove_command_from_group) hit the same silent no-dialog wait,
     * re-armed, decoded 5s later, passed, and the run finished 90/90 instead of stopping at 64.
     *
     * An accessibility-tree fallback was tried here first and removed: `UiAutomation`'s
     * `rootInActiveWindow` returns null on this rig even with `FLAG_RETRIEVE_INTERACTIVE_WINDOWS`
     * set on its service info, so it could see neither the dialog nor anything else, on two
     * separate runs. Worth knowing before reaching for it again -- reading the real window
     * hierarchy is not available to this harness as things stand.
     */
    private fun awaitScannedTag(tag: String, timeoutMs: Long, opticalTag: String) {
        val budgetMs = timeoutMs.coerceAtLeast(1L) * SCAN_ATTEMPTS
        val deadline = System.currentTimeMillis() + budgetMs
        var lastError: Throwable? = null
        var look = 0
        while (System.currentTimeMillis() < deadline) {
            val slice = minOf(SCAN_LOOK_MS, deadline - System.currentTimeMillis()).coerceAtLeast(1L)
            try {
                waitForTagWithTimeout(tag, slice)
                return
            } catch (e: IllegalStateException) {
                lastError = e
                look++
                Log.w(TAG, "$opticalTag '$tag' not shown on look $look after ${slice}ms (${(deadline - System.currentTimeMillis()) / 1000}s of budget left): ${e.message}")
                // The one state where looking again is provably pointless, so say so immediately
                // rather than spending the rest of the budget on it. AppRoot dispatches every scan
                // from a single collect; with no collector, a decoded frame reaches nothing, and
                // the camera bind -- an effect in the same composition -- is gone with it, so
                // nothing decodes either. Confirmed live twice: a run spent seventeen looks in this
                // state, and another thirteen, without one scan being dispatched, while device A
                // dutifully re-showed the code each time. Recreating the Activity from here does
                // not bring it back (tried: the composition never re-ran, no camera re-bind, the
                // count stayed 0), and nothing else in this process can rebuild a composition. Only
                // a fresh process can, which is the Go orchestrator's job -- so fail fast with a
                // message it recognises (see android_optical.go's opticalCollectorGoneMarker) and
                // let it re-run the batch from scratch.
                if (scanCollectorCount() == 0) {
                    throw IllegalStateException("$SCAN_COLLECTOR_GONE -- AppRoot dispatches every scan from one collector and it is no longer subscribed, so no scan can reach the app and no further case in this process can pass")
                }
                // Ask A to keep this same code up *before* re-arming, not after: the extension only
                // helps if it reaches A while A is still holding, and the re-arm below deliberately
                // spends REBIND_SETTLE_MS doing nothing.
                signalCaseRetry(opticalTag)
                // Only the first re-arm of a case dumps the semantics tree. It answers "was the
                // dialog there and the lookup blind, or was it genuinely never composed?", which is
                // a property of the stall, not of each individual retry -- and at one dump per look
                // a stubborn case would bury its own log in repeats of the same answer.
                forceRescan(opticalTag, verbose = look == 1)
            }
        }
        throw lastError ?: IllegalStateException("'$tag' not shown after ${budgetMs}ms across $look look(s)")
    }

    /**
     * Re-arms the scanner so the code device A is *still* holding on screen decodes again: clears
     * [ScannerCoordinator]'s own last-payload dedup (without which an unchanged code in front of
     * the camera never re-emits, by design -- see its own doc comment) and re-expands the scanner,
     * which AppRoot collapses after every decode. Deliberately does *not* press Back to clear a
     * possibly-showing dialog first: on the pager's own start destination a stray back press
     * finishes the Activity outright, which would turn a recoverable missed dialog into a dead
     * instrumentation process, and a fresh scan replaces any pending dialog's state anyway.
     */
    private fun forceRescan(opticalTag: String, verbose: Boolean = true) {
        Log.i(TAG, "$opticalTag re-arming the scanner for another look at the same code")
        // Reflection rather than a plain `ScannerCoordinator.resetDedup()`: the field is private
        // because nothing in the app has any business clearing it (a human rescanning naturally
        // looks away and back, which changes the decoded bytes on its own), and adding a test-only
        // hook to the app would mean rebuilding and reinstalling *both* devices' app APKs -- which
        // are not the same build (device B's bakes device A's leaderMultiaddr, device A's bakes
        // none, see mobile/kvmobile), so a shared rebuild would break whichever device got the
        // wrong flavor. The androidTest APK, by contrast, is identical on both and reinstallable on
        // its own. Failure here is logged, not thrown: the re-arm is a bonus attempt at recovery,
        // and the caller's remaining wait is still worth making without it.
        runCatching {
            val field = ScannerCoordinator::class.java.getDeclaredField("lastScanned")
            field.isAccessible = true
            field.set(ScannerCoordinator, null)
        }.onFailure { Log.w(TAG, "$opticalTag could not clear the scanner's dedup state: $it") }
        composeTestRule.runOnUiThread { ScannerCoordinator.expanded = true }

        // A missed scan is not always a missed *decode*. Measured live on a 90-case run that died
        // on case 16: the app logged `DataMatrix decoded from camera frame (49 bytes)`, `scan
        // received`, `run-code scan decoded: RunCode(category=Cluster, name=PeerID)` and
        // `RunConfirmDialog shown` -- everything short of the dialog's own composition, whose log
        // line never appeared -- and from that moment on the app received no further scans at all,
        // while device A held the same code up for another 150 seconds and the offline decoder
        // read that very frame out of a screenshot of B's own preview without difficulty. So the
        // camera was fine and the state was set; what stopped was Compose acting on it.
        //
        // AppRoot sets `pendingRun` from a coroutine on the composition's own dispatcher, and a
        // state write only invalidates a composition once its snapshot's apply notifications have
        // been sent. Asking for that explicitly costs nothing when nothing is pending (the common
        // case, since every healthy case here recomposes on its own) and is the one lever that
        // turns a write nobody observed into a dialog. Followed by an idle-sync so this returns
        // only once any recomposition it just unblocked has actually run.
        composeTestRule.runOnUiThread { Snapshot.sendApplyNotifications() }
        composeTestRule.waitForIdle()

        // Diagnostics for the same failure: a live collector count says whether AppRoot's own scan
        // collector is still subscribed at all (it was not receiving anything on that run, which a
        // count of 0 would explain outright), and the semantics dump says what the harness's own
        // lookup was really looking at when it "ran cleanly and simply never found" the dialog.
        // Reflection again, for the same reason the dedup clear above uses it: only the private
        // MutableSharedFlow carries subscriptionCount, and the public SharedFlow this object
        // exposes deliberately doesn't.
        Log.i(TAG, "$opticalTag scan flow has ${scanCollectorCount() ?: "?"} collector(s) after re-arm")
        if (verbose) {
            runCatching { composeTestRule.onRoot().printToLog(TAG) }
                .onFailure { Log.w(TAG, "$opticalTag could not dump the semantics tree: $it") }
        }

        // Clearing the dedup only helps if the frames themselves are usable. By the time this runs,
        // device A has been holding one code on screen for a full attempt's worth of seconds and
        // nothing decoded -- the case where, measured live, the camera's own 3A state had gone bad
        // (frame sharpness collapsing ~6x mid-run and staying there, with the preview beside it
        // still looking fine). A rebind is the one thing that resets it; see
        // MainScannerManager.requestRebind. Costs a second or so of dropped frames, out of an
        // attempt budget of tens of seconds.
        MainScannerManager.requestRebind()
        Thread.sleep(REBIND_SETTLE_MS)
    }

    /**
     * How many collectors [ScannerCoordinator]'s scan flow currently has -- 1 in a healthy app, and
     * the single most decisive thing this harness can ask when a case stops decoding.
     *
     * Reflection because only the private MutableSharedFlow carries `subscriptionCount`; the public
     * SharedFlow the object exposes deliberately doesn't. Null if the field can't be read at all,
     * which is never treated as a failure -- it just means this check has nothing to say.
     */
    /**
     * Asks Compose whether its composition is still healthy, and says so in the log.
     *
     * Compose's test environment does not report a composition that has failed until something
     * interacts with it: it holds the exception and rethrows it at the next `waitForIdle` or
     * semantics fetch, once. So a probe *is* the diagnosis -- calling waitForIdle here either
     * returns quietly or hands over the exact throwable that killed the composition, which is the
     * one thing the "collector is gone" symptom never revealed on its own.
     *
     * Paired with [scanCollectorCount] at the start of every case so a death can be pinned to the
     * single case that caused it rather than to the later case that first noticed. Both are cheap;
     * the collector read is a field access and an idle composition makes waitForIdle a no-op.
     */
    private fun logCompositionHealth(where: String) {
        val collectors = scanCollectorCount()
        val idle = runCatching { composeTestRule.waitForIdle() }
        val failure = idle.exceptionOrNull()
        if (failure != null) {
            Log.w(TAG, "AUTO: composition is not healthy at $where (scan collectors=$collectors) -- waitForIdle threw", failure)
        } else if (collectors != 1) {
            Log.w(TAG, "AUTO: composition idled cleanly at $where but the scan flow has $collectors collector(s), not 1")
        }
    }

    private fun scanCollectorCount(): Int? = runCatching {
        val field = ScannerCoordinator::class.java.getDeclaredField("_scans")
        field.isAccessible = true
        (field.get(ScannerCoordinator) as MutableSharedFlow<*>).subscriptionCount.value
    }.getOrNull()

    /**
     * Device-A-side verification for optical-scan cases whose outcome is only observable in
     * *this* device's own Activity Log, not device B's -- currently just "Dispatch:
     * DialSubmitCommand" cases, where B executing the scanned RunCode makes B dial back and
     * submit against A's own RunCommandDispatcher, so the actual dispatch is only ever recorded
     * in A's own [OutputLog], never B's. Decodes {"contains": "...", "timeoutMs": N} from the
     * "opticalVerify" instrumentation arg and waits for any [OutputLog] entry (title or body)
     * containing that substring -- the "Dispatching <commandID>/<instance_id> from <peerID>" line
     * RunCommandDispatcher's own `appendLog` call produces (see CommandCatalog.kt), with the Go
     * harness having already substituted the real instance_id it read back from
     * [awaitAndVerifyScan]'s own result. Polls [OutputLog] directly rather than navigating to
     * LogScreen -- same object either way, no UI round trip needed.
     */
    @Test
    fun verifyLogContains() {
        val context = InstrumentationRegistry.getInstrumentation().targetContext
        val spec = instrumentationArgJson("opticalVerify") ?: return
        val want = spec.getString("contains")
        val timeoutMs = spec.optLong("timeoutMs", 30_000L)
        val opticalTag = "$OPTICAL_LOG_TAG verifyLogContains"
        Log.i(TAG, "$opticalTag starting, want=\"$want\" timeoutMs=$timeoutMs, current OutputLog=${OutputLog.snapshot().map { it.title }}")

        waitForScreen("screen_main")
        try {
            composeTestRule.waitUntil(timeoutMs) {
                OutputLog.snapshot().any { it.title.contains(want) || it.body.contains(want) }
            }
            Log.i(TAG, "$opticalTag PASS, matched entry present in ${OutputLog.snapshot().map { it.title }}")
            writeResults(context, JSONArray().put(JSONObject().put("command", "OpticalVerify").put("pass", true)))
        } catch (e: ComposeTimeoutException) {
            Log.w(TAG, "$opticalTag FAIL, no match after ${timeoutMs}ms -- full OutputLog=${OutputLog.snapshot()}")
            writeResults(
                context,
                JSONArray().put(
                    JSONObject().put("command", "OpticalVerify").put("pass", false)
                        .put("error", "no matching log entry within ${timeoutMs}ms containing \"$want\""),
                ),
            )
            throw AssertionError("no matching log entry containing \"$want\"", e)
        }
    }

    /**
     * Rig-setup utility, not part of the generate/scan round trip: decodes
     * {"category":"...","name":"...","params":[...]} from the "opticalDirect" instrumentation arg
     * and runs it straight through [CommandExecutor.execute] -- the exact primitive
     * [generateAndHold]'s own RunCommandDispatcher pre-registration already calls directly, just
     * exposed as its own single-purpose entry point here. Exists because bringing up a real
     * two-device optical rig needs to read back a genuine on-device value (most commonly device
     * A's own GetOwnAddr, to bake as device B's build-time leaderMultiaddr) before either
     * `generateAndHold`/`awaitAndVerifyScan` ever run -- and there is deliberately no other way to
     * do that now that CommandDetailScreen's Run button is gone and ConnectDeviceScreen's own
     * GetOwnAddr auto-fill went with it. Writes a single ui_e2e_results.json entry the same
     * `am instrument -e class ...#executeDirect -e opticalDirect <base64>` caller can pull back
     * with the same plumbing runOpticalMethod already has.
     */
    @Test
    fun executeDirect() {
        val context = InstrumentationRegistry.getInstrumentation().targetContext
        val spec = instrumentationArgJson("opticalDirect") ?: return
        val category = spec.getString("category")
        val name = spec.getString("name")
        val params = spec.optJSONArray("params")?.let { arr -> (0 until arr.length()).map { arr.getString(it) } } ?: emptyList()
        val holdMillis = spec.optLong("hold_millis", 0L)
        Log.i(TAG, "$OPTICAL_LOG_TAG executeDirect ${category}: $name(${params.joinToString(", ")})")

        runCatching { Kvmobile.startSolo(context.filesDir.absolutePath) }
        runCatching { Kvmobile.requestRelayAccess("optical e2e executeDirect") }

        val commandSpec = buildCommands(context.filesDir.absolutePath, OutputLog::append)
            .firstOrNull { it.category == category && it.name == name }
        if (commandSpec == null) {
            Log.w(TAG, "$OPTICAL_LOG_TAG executeDirect: no such command $category: $name")
            writeResults(context, JSONArray().put(JSONObject().put("command", "OpticalDirect").put("pass", false).put("error", "no such command $category: $name")))
            return
        }
        val result = runBlocking { CommandExecutor.execute(commandSpec, params) }
        Log.i(TAG, "$OPTICAL_LOG_TAG executeDirect result=$result")
        writeResults(
            context,
            JSONArray().put(JSONObject().put("command", "OpticalDirect").put("pass", true).put("output", result.substringAfter("->\n", result))),
        )
        if (holdMillis > 0) {
            Log.i(TAG, "$OPTICAL_LOG_TAG executeDirect holding for ${holdMillis}ms")
            Thread.sleep(holdMillis)
            Log.i(TAG, "$OPTICAL_LOG_TAG executeDirect hold complete, returning")
        }
    }

    /**
     * Resolves an OpticalExpectSpec.Result string ("succeeded"/"rejected"/"no_crash"/
     * "contains:<substring>", the same convention e2edata.ExpectSucceeded/Rejected/NoCrash
     * describe) into an assertion against the post-Execute OutputLog body -- [awaitAndVerifyScan]'s
     * "run" kind only. "{{selfPeerID}}" is the one substitution token an optical case's own Result
     * can reference (e.g. a GetOwnAddr case's "contains:{{selfPeerID}}") -- only known live, the
     * same reason the old catalog sweep's token substitution existed at all.
     *
     * An *unresolved* token is a hard failure rather than a silent substitution. The caller reads
     * it as `runCatching { Kvmobile.peerID() }.getOrDefault("")`, so a device whose session never
     * came up yields "" -- and substituting that turns `contains:{{selfPeerID}}` into
     * `contains:""`, which every possible result line satisfies. Confirmed live, and precisely
     * the wrong outcome: a run in which device B's `Kvmobile.start()` had failed outright still
     * recorded its first case as PASS (with the body literally reading "FAILED: kvmobile: Start
     * has not completed successfully yet"), hiding the one failure that most needed reporting
     * behind a green case. A token that cannot be resolved means the assertion cannot be
     * evaluated at all, which is a failure, not a pass.
     */
    private fun resultExpectation(name: String, selfPeerID: String): (String) -> Unit = when {
        name == "rejected" -> ::assertRejected
        name == "no_crash" -> ::assertNoCrash
        name.startsWith("contains:") -> { line: String ->
            val want = name.removePrefix("contains:")
            if (want.contains(SELF_PEER_ID_TOKEN) && selfPeerID.isEmpty()) {
                throw AssertionError(
                    "expectation \"$name\" references $SELF_PEER_ID_TOKEN but this device has no peer id " +
                        "(Kvmobile.peerID() failed -- its session almost certainly never started), so the " +
                        "check cannot be evaluated. Result line was: $line",
                )
            }
            assertContains(line, want.replace(SELF_PEER_ID_TOKEN, selfPeerID))
        }
        else -> ::assertSucceeded
    }

    /**
     * Writes the results file after every entry, not just once at the very end: lets
     * pkg/e2erun/android_optical.go's peekOpticalResult pull and observe [generateAndHold]'s own
     * result (e.g. that the code actually rendered) via `adb pull` while this SAME instrumentation
     * invocation is still holding the screen open for a concurrently-dialing peer on a different
     * device.
     */
    private fun writeResults(context: android.content.Context, results: JSONArray) {
        File(context.getExternalFilesDir(null), "ui_e2e_results.json").writeText(results.toString())
    }

    /**
     * Pops one entry off the current back stack via the Activity's own
     * `OnBackPressedDispatcher` -- see class doc comment for why this, not `Espresso.pressBack()`,
     * is what actually works here. Runs on the main thread (the dispatcher isn't thread-safe to
     * call from a test's own background thread) and waits for the resulting recomposition to
     * settle before returning, the same guarantee `performClick`/`performTextInput` already give
     * every other navigation action in this file.
     */
    private fun pressBack() {
        composeTestRule.activity.onBackPressedDispatcher.let { dispatcher ->
            composeTestRule.runOnUiThread { dispatcher.onBackPressed() }
        }
        composeTestRule.waitForIdle()
    }

    /**
     * Polls for [tag] to appear anywhere in the current composition, or throws once
     * NAV_TIMEOUT_MS elapses -- this test's Compose-native replacement for the old
     * Activity-class-identity check, since a single-Activity Compose app has no distinct Activity
     * subclass per screen to check against. Each screen's root Composable carries a stable
     * "screen_*" testTag exactly for this purpose.
     */
    private fun waitForScreen(tag: String) {
        try {
            composeTestRule.waitUntil(NAV_TIMEOUT_MS) { nodeExistsQuietly(tag) }
        } catch (e: ComposeTimeoutException) {
            throw IllegalStateException("screen '$tag' not shown after ${NAV_TIMEOUT_MS}ms", e)
        }
    }

    /**
     * Same as [waitForScreen] but with a caller-supplied budget instead of the fixed
     * NAV_TIMEOUT_MS -- used by [awaitAndVerifyScan], whose wait is for a real camera to actually
     * read a real screen, not an in-process same-activity navigation, and so needs a much more
     * generous, tunable timeout than ordinary Compose recomposition ever does.
     */
    private fun waitForTagWithTimeout(tag: String, timeoutMs: Long) {
        // Records whatever the predicate last choked on, so a timeout can say *why* it never saw
        // the tag. Without this the swallow in nodeExistsQuietly is a diagnostic dead end: a
        // wedged Compose idle-sync and a genuinely absent node produce the identical bare
        // "'X' not shown after Nms", which cost a full two-device run to tell apart once already.
        var lastPredicateError: Throwable? = null
        var predicateErrors = 0
        try {
            composeTestRule.waitUntil(timeoutMs) {
                try {
                    nodeExists(tag)
                } catch (e: Throwable) {
                    lastPredicateError = e
                    predicateErrors++
                    // Logged as it happens, not only summarised in a timeout message that may never
                    // be reached: this is where a failed composition's stored exception surfaces
                    // (see nodeExistsQuietly), and the summary keeps only the last one.
                    Log.w(TAG, "AUTO: semantics lookup for '$tag' threw on poll $predicateErrors", e)
                    false
                }
            }
        } catch (e: ComposeTimeoutException) {
            val why = lastPredicateError?.let {
                " -- the semantics lookup itself failed $predicateErrors time(s), last: ${it::class.java.name}: ${it.message}"
            } ?: " -- the semantics lookup ran cleanly and simply never found it"
            throw IllegalStateException("'$tag' not shown after ${timeoutMs}ms$why", e)
        }
    }

    /**
     * [nodeExists] with any *transient* framework exception swallowed into "not there yet", for
     * use inside a `waitUntil` predicate. Only the polling predicates use this -- a real timeout
     * still surfaces as a real failure, and a genuinely wedged UI just times out the way it
     * always did.
     *
     * Fetching semantics nodes internally drives Compose's own idle-sync (waitForIdle ->
     * Espresso.onIdle), which races this app's continuously-recomposing live camera preview:
     * confirmed live, with a stack trace, when a 90-case batch died on case 9 with
     * "java.util.concurrent.ExecutionException: java.lang.IllegalArgumentException:
     * performMeasureAndLayout called during measure layout" thrown straight out of
     * [waitForTagWithTimeout]'s predicate. Nothing about that means the tag will never appear --
     * it means this one poll iteration happened to land mid-layout. Letting it propagate aborts
     * the entire run (a failed case stops the whole batch by design), which is a wildly
     * disproportionate outcome for "ask again in a few milliseconds", and costs ~10 minutes of
     * real two-device hardware time to retry from scratch.
     */
    private fun nodeExistsQuietly(tag: String): Boolean =
        try {
            nodeExists(tag)
        } catch (e: Throwable) {
            // Swallowed for control flow, but never silently. Compose's test environment holds a
            // failed composition's exception and rethrows it on the next interaction -- a semantics
            // fetch is an interaction -- so the one report of what killed a composition can arrive
            // here, at a poll whose only job was to ask whether a node exists yet. Losing it costs
            // a whole run to reproduce, so it goes to the log with its stack before being turned
            // into "not there yet".
            Log.w(TAG, "AUTO: semantics lookup for '$tag' threw (treated as not-yet-present)", e)
            false
        }

    /**
     * A settle needed after two distinct kinds of transition `waitForIdle()` alone doesn't fully
     * cover, confirmed live for both:
     *
     * (1) A scan-triggered navigation (see AppRoot.kt's `ScannerCoordinator.scans.collect { ... }`
     * listener's NavCode.Group branch) calls `navController.navigate(...)` from a coroutine
     * reacting to the camera scanner, not from a `performClick()` this class's own
     * `waitUntil`/`waitForIdle` reliably synchronize with -- the target screen's own testTag
     * appears in the semantics tree (satisfying [waitForTagWithTimeout]) while its
     * NavBackStackEntry's underlying Android-framework Lifecycle (a separate async dispatch loop
     * from Compose's own recomposition scheduling, which `waitForIdle` alone does not wait on) is
     * still mid-transition, not yet RESUMED -- tearing the Activity down at exactly that moment
     * crashed the whole instrumentation process with "State must be at least CREATED to move to
     * DESTROYED, but was INITIALIZED".
     *
     * (2) [resetToMainAfterGenerate]'s `performTouchInput { swipeRight() }` (undoing a "nav_group"
     * case's own swipe to the log page) -- confirmed live via a real two-device optical run: the
     * very next case's `mainListItem_commands` tap, fired immediately after `waitForScreen
     * ("screen_default_group")` succeeded, silently failed to register (no exception, no
     * `USER_TAP` log line, `screen_command_detail` simply never appeared) often enough to break a
     * real batch run -- `HorizontalPager`'s own fling/settle animation from the swipe apparently
     * doesn't always finish idling by the time `performTouchInput` returns control, and a tap that
     * lands while the pager is still mid-settle can get swallowed by its own gesture-priority
     * handling instead of reaching the underlying list item.
     *
     * A fixed settle, not a poll, in both cases: there is no test-visible signal for "this
     * NavBackStackEntry reached RESUMED" or "this pager's fling animation has fully settled" to
     * poll for instead.
     */
    private fun settleAfterNavigation() {
        composeTestRule.waitForIdle()
        Thread.sleep(500)
    }

    /**
     * Waits for the confirmation dialog [tag] belongs to to actually be gone, and for the
     * composition to go idle, before the caller declares a case finished.
     *
     * The case this exists for killed a run at case 30 of 90: 150ms after the previous case tapped
     * Execute and read its result back, the *app* died on the main thread with
     * `IllegalStateException: LayoutNode should be attached to an owner`, thrown from a Box
     * placement inside a layout pass -- a dialog window being torn down while a measure/layout pass
     * was still placing its nodes. Nothing on the harness side can catch that: it is an uncaught
     * exception in the app process, so the process dies and every remaining case with it.
     *
     * What the harness *can* do is stop racing the teardown. Reading an OutputLog entry (which is
     * ordinary Kotlin state, not Compose state) says the command finished, not that the dialog it
     * was tapped in has finished going away -- and the very next thing each case does is poll
     * semantics again, which forces exactly the measure/layout sync that crashed. Waiting for the
     * dialog to leave the tree first costs a few hundred milliseconds per case and closes that
     * window.
     */
    private fun settleAfterDialog(tag: String) {
        runCatching {
            composeTestRule.waitUntil(NAV_TIMEOUT_MS) { !nodeExistsQuietly(tag) }
        }.onFailure { Log.w(TAG, "AUTO: '$tag' still present after ${NAV_TIMEOUT_MS}ms of settling: $it") }
        composeTestRule.waitForIdle()
    }

    /**
     * Decodes a base64-encoded JSON object instrumentation arg named [argName] -- raw JSON's
     * quotes/braces don't survive `adb shell am instrument`'s own argument parsing. Returns null
     * (never throws) for a missing arg, invalid base64, or unparsable JSON -- callers
     * ([generateAndHold]/[awaitAndVerifyScan]/[verifyLogContains]) treat that as "this test was
     * invoked some other way," not a failure.
     */
    private fun instrumentationArgJson(argName: String): JSONObject? {
        val raw = InstrumentationRegistry.getArguments().getString(argName) ?: return null
        val decoded = runCatching { String(Base64.decode(raw, Base64.DEFAULT)) }.getOrNull() ?: return null
        return runCatching { JSONObject(decoded) }.getOrNull()
    }

    /**
     * Guards [navigateToCommandDetailViaPicker]'s Commands-picker branch to at most once per
     * instrumentation process -- see that function's own doc comment for why.
     */
    private var commandsPickerUsedOnce = false

    /**
     * From the pager's Default group (see GroupPageScreen), reaches [category]/[name]'s own
     * CommandDetailScreen. The first call in a given instrumentation process goes through the
     * "Commands" pseudo-item's search-filterable select (taps the option matching the exact
     * `"$category: $name"` label directly -- Material3's `DropdownMenu` is a plain scrollable
     * `Column`, not a `LazyColumn`, so every option already exists in the semantics tree
     * regardless of filter/scroll position) then Submit -- two real routes deep from the pager
     * (commandPicker, then commandDetail).
     *
     * Every call after the first goes through [navigateToCommandDetailViaGroup] instead --
     * confirmed live, the hard way, running a real two-device batch: `SearchableSelectDropdown`'s
     * `DropdownMenuItem` (used only by the Commands picker, given the flat ~104-entry catalog
     * needs it) silently fails to invoke its own `onClick` on the *second* time its `Popup` is
     * opened in the same process -- `performClick()`/`performTouchInput { click() }` both report
     * success (the semantics node is found, the synthetic event dispatches with no exception),
     * yet neither `onSelect` nor the Submit button's own `onClick` ever fires, confirmed via a
     * temporary diagnostic `Log.i` inside both. **Not a real app bug** -- a real `adb shell input
     * tap` at the same coordinates on the same second use works correctly every time, confirmed
     * live by hand; this is specific to how `AndroidComposeUiTestEnvironment` synthesizes touch
     * input against a `Popup`-hosted `DropdownMenuItem` recreated a second time. The plain
     * (non-searchable) `SelectDropdown` GroupPickerScreen uses does **not** have this limitation
     * -- confirmed live surviving two full uses in the same process back to back -- which is
     * exactly why [navigateToCommandDetailViaGroup] routes through it instead of retrying the
     * Commands picker.
     */
    private fun navigateToCommandDetailViaPicker(category: String, name: String) {
        if (!commandsPickerUsedOnce) {
            commandsPickerUsedOnce = true
            val label = "$category: $name"
            composeTestRule.onNodeWithTag("mainListItem_commands").performClick()
            waitForScreen("screen_command_picker")
            composeTestRule.onNodeWithTag("commandPickerSelect").performClick()
            // performScrollTo for the same reason selectGroupUntilRegistered needs it (see its
            // own doc comment): every option is in the semantics tree regardless of scroll
            // position, so a label far enough down this ~104-entry list is found but sits outside
            // the popup's visible window, and the synthesized click silently lands on nothing.
            // Only case 0 ever takes this branch, and the seed catalog's case 0 happens to be
            // near the top ("Cluster: GetOwnAddr") -- confirmed live that a case whose label
            // sorts last ("Test: SleepMillis") hangs here instead, never reaching Generate.
            composeTestRule.onNodeWithTag("commandPickerSelect_option_$label")
                .performScrollTo()
                .performClick()
            composeTestRule.onNodeWithTag("commandPickerOpen").performClick()
        } else {
            navigateToCommandDetailViaGroup(category, name)
        }
    }

    /**
     * The Commands-picker alternative [navigateToCommandDetailViaPicker] falls back to after its
     * first use -- see that function's own doc comment for why. Enters [category] via the
     * Groups picker's plain `SelectDropdown` (safe to reuse, unlike `SearchableSelectDropdown`),
     * then taps [name] directly from that group's own real `LazyColumn` command list -- no popup
     * involved at all for this second step, so it can't hit the same class of issue either.
     * `performScrollToIndex` first since a category's command list can run past the first
     * screenful (mirrors the pre-Phase-3 harness's own `clickListItem` helper, removed when the
     * "run" target stopped needing it for its *first* navigation -- reinstated here, inline,
     * for this fallback path only, now that it's needed again).
     */
    private fun navigateToCommandDetailViaGroup(category: String, name: String) {
        composeTestRule.onNodeWithTag("mainListItem_groups").performClick()
        waitForScreen("screen_group_picker")
        selectGroupUntilRegistered(category)
        composeTestRule.onNodeWithTag("groupPickerSubmit").performClick()
        waitForScreen("screen_commands")
        val namesInCategory = buildCommands(
            InstrumentationRegistry.getInstrumentation().targetContext.filesDir.absolutePath,
            OutputLog::append,
        ).filter { it.category == category }.map { it.name }
        composeTestRule.onNodeWithTag("itemList").performScrollToIndex(namesInCategory.indexOf(name))
        composeTestRule.onNodeWithTag("listItem_$name").performClick()
    }

    /**
     * Opens `groupPickerSelect` and taps [category]'s own option, retrying the whole popup
     * interaction until the selection has demonstrably registered.
     *
     * GroupPickerScreen's Submit is `enabled = selectedCategory != null`, so an option tap that
     * fails to fire its `onSelect` leaves Submit *disabled* -- and `performClick()` on a disabled
     * Compose button neither throws nor does anything, so the harness sails on and then waits out
     * the full NAV_TIMEOUT_MS for a `screen_commands` that can never arrive. Confirmed live
     * exactly that way: a 90-case run reached case 90 of 90 and died on "device A: screen
     * 'screen_commands' not shown after 10000ms", with no other symptom.
     *
     * The `performScrollTo()` is the actual fix, and it is not defensive padding: `DropdownMenu`
     * puts every option in the semantics tree regardless of scroll position, so a category far
     * enough down the list is *found* but sits outside the popup's visible window, and the
     * synthesized click lands on nothing while reporting success. That is deterministic, not
     * flaky -- confirmed live by this function's own diagnostic, which failed all 4 attempts on
     * the same category ("Test", the last one in the catalog) while all ~88 earlier navigations
     * to nearer-the-top categories passed. It also explains, retroactively, an earlier run that
     * died on case 90 of 90 with a bare "screen_commands not shown after 10000ms".
     *
     * The retry loop is kept on top of that for the genuinely intermittent popup-tap flakiness
     * [navigateToCommandDetailViaPicker] documents for `SearchableSelectDropdown` -- rarer on
     * this plain `SelectDropdown`, but "survives two uses back to back" (what was confirmed when
     * that fallback was written) is a much weaker guarantee than the ~88 uses a full catalog run
     * puts it through. Retrying rather than asserting is right for that part, because it's an
     * artifact of how the test framework synthesizes touch input against a recreated `Popup`, not
     * an app defect a human hits. Reopens the popup only when it isn't already showing, since a
     * failed option tap can leave it open and tapping the select field again would just close it.
     */
    private fun selectGroupUntilRegistered(category: String) {
        repeat(GROUP_SELECT_ATTEMPTS) { attempt ->
            runCatching {
                if (!nodeExists("groupPickerSelect_option_$category")) {
                    composeTestRule.onNodeWithTag("groupPickerSelect").performClick()
                }
                composeTestRule.onNodeWithTag("groupPickerSelect_option_$category")
                    .performScrollTo()
                    .performClick()
                composeTestRule.waitForIdle()
            }
            if (nodeEnabled("groupPickerSubmit")) return
            Log.w(TAG, "$OPTICAL_LOG_TAG groupPickerSelect did not register \"$category\" on attempt ${attempt + 1} -- retrying")
        }
        throw IllegalStateException("groupPickerSelect never registered a selection for \"$category\" after $GROUP_SELECT_ATTEMPTS attempts")
    }

    /** True if any node carrying [tag] currently exists in the semantics tree. */
    private fun nodeExists(tag: String): Boolean =
        composeTestRule.onAllNodesWithTag(tag).fetchSemanticsNodes(atLeastOneRootRequired = false).isNotEmpty()

    /** True if [tag]'s node exists and is not marked disabled -- see [selectGroupUntilRegistered]. */
    private fun nodeEnabled(tag: String): Boolean {
        val node = composeTestRule.onAllNodesWithTag(tag)
            .fetchSemanticsNodes(atLeastOneRootRequired = false)
            .firstOrNull() ?: return false
        return !node.config.contains(SemanticsProperties.Disabled)
    }

    /** Reads a plain Text composable's current content by [tag] (e.g. categoryTitle/runConfirmParams). */
    private fun readTagText(tag: String): String {
        val node = composeTestRule.onNodeWithTag(tag).fetchSemanticsNode()
        return node.config.getOrNull(SemanticsProperties.Text)?.joinToString("") { it.text } ?: ""
    }
}
