package com.gofsd.kvdemo

import android.util.Base64
import android.util.Log
import androidx.compose.ui.semantics.SemanticsProperties
import androidx.compose.ui.semantics.getOrNull
import androidx.compose.ui.test.ComposeTimeoutException
import androidx.compose.ui.test.junit4.createAndroidComposeRule
import androidx.compose.ui.test.onAllNodesWithTag
import androidx.compose.ui.test.onNodeWithTag
import androidx.compose.ui.test.performClick
import androidx.compose.ui.test.performScrollToIndex
import androidx.compose.ui.test.performTextInput
import androidx.test.platform.app.InstrumentationRegistry
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
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
import java.util.concurrent.atomic.AtomicReference

/**
 * Real-UI-driven instrumented test for android-app's real-camera two-device optical scan harness
 * (pkg/e2erun/android_optical.go, driven by test/e2e/testdata.json's android_optical_cases) --
 * the only way any android command executes now (see CommandCatalog.kt/CommandDetailScreen's own
 * doc comments on the RunCode rewrite: every command runs by generating a DataMatrix on one
 * device, a real camera scanning it on another, and a confirm tap executing it there). Unlike
 * E2ETest (a separate class that calls Kvmobile.sendEvent directly, never touching a single
 * screen -- still the cross-platform wire-protocol row-replay check, unaffected by any of this),
 * this drives the actual app: MainListScreen's Commands/Groups shortcuts, CommandDetailScreen's
 * dynamically-rendered param fields and "Generate DataMatrix" button, the persistent camera
 * scanner, and RunConfirmDialog/RecruitConfirmDialog's confirm taps.
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
        const val BATCH_HOLD_MILLIS = 30_000L
        const val BATCH_TIMEOUT_MS = 25_000L

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
        // B only ever signals a case it actually PASSED -- a failed case (wrong/missing scan,
        // rejected result that didn't match what was expected) has nothing useful for A to advance
        // into, so it stays silent instead. That makes a missing signal within the ceiling mean one
        // of exactly two things, both fatal to the rest of the run: B failed this case, or the
        // signal channel itself is broken -- either way there is no sound reason to keep generating
        // 90 codes nobody will ever correctly consume, so both sides abort the whole batch the first
        // time this happens (see generateAndHoldAll/awaitAndVerifyScanAll's own loops) rather than
        // cascading through every remaining case as a wall of timeouts, which is what an earlier,
        // always-signal-regardless-of-outcome version of this mechanism did in practice.
        const val SIGNAL_POLL_INTERVAL_MS = 200L
        // Must be one of shmevent.ChannelPurposeName's own fixed strings ("data"/"control"/
        // "video") or a plain decimal byte -- confirmed live an arbitrary label like
        // "optical_case_done" is rejected outright ("unknown purpose"), not just logged oddly.
        // "data" is the correct, semantically-plain choice for this arbitrary small payload.
        const val SIGNAL_CHANNEL_PURPOSE = "data"
    }

    /** Set by [startCaseSignalListener]'s ChannelCallback (on the channel's own pump thread, per its own doc comment -- hence AtomicReference, not a plain var) to whatever case_id B most recently signalled. */
    private val lastSignaledCaseID = AtomicReference<String>()

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
                        lastSignaledCaseID.set(String(chunk, StandardCharsets.UTF_8))
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
     */
    private fun awaitCaseSignalOrCeiling(caseID: String, maxMillis: Long): Boolean {
        val deadline = System.currentTimeMillis() + maxMillis
        while (System.currentTimeMillis() < deadline) {
            if (lastSignaledCaseID.get() == caseID) return true
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
            val holdCeilingMillis = if (i <= 1) FIRST_CASE_AWAIT_TIMEOUT_MS else BATCH_HOLD_MILLIS
            Log.i(TAG, "$caseTag awaiting B's completion signal, ceiling ${holdCeilingMillis}ms")
            if (!awaitCaseSignalOrCeiling(caseID, holdCeilingMillis)) {
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
     */
    private fun startSoloAndRequestRelay(context: android.content.Context, opticalTag: String) {
        val startSoloResult = runCatching { Kvmobile.startSolo(context.filesDir.absolutePath) }
        Log.i(TAG, "$opticalTag startSolo -> ${startSoloResult.getOrNull()} (error=${startSoloResult.exceptionOrNull()?.message})")
        val relayResult = runCatching { Kvmobile.requestRelayAccess("optical e2e generateAndHold") }
        Log.i(TAG, "$opticalTag requestRelayAccess -> ${relayResult.getOrNull()} (error=${relayResult.exceptionOrNull()?.message})")
    }

    /**
     * Generates one [spec]-described case -- shared by [generateAndHold] (one case, fresh
     * process) and [generateAndHoldAll] (many cases, one session) -- and returns its
     * "OpticalGenerate: <case_id>" result entry. Assumes the caller is already on screen_main
     * (both callers arrange this: a fresh process starts there; a later batch iteration is put
     * back there by [resetToMainAfterGenerate]) and has already done the StartSolo/
     * RequestRelayAccess preamble ([startSoloAndRequestRelay]). Navigates to whichever of the
     * app's two DataMatrix-generating screens `target` names, fills in `params`, taps Generate,
     * and confirms the code actually rendered -- leaves it on screen (generatedDataMatrixImage)
     * for the caller to hold/dismiss, since how long to hold and what happens next differs
     * between the two callers.
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
                navigateToCategories()
                val allCategories = allCommandsForLookup.map { it.category }.distinct()
                clickListItem(commandSpec.category, allCategories.indexOf(commandSpec.category))
                val namesInCategory = allCommandsForLookup.filter { it.category == commandSpec.category }.map { it.name }
                clickListItem(commandSpec.name, namesInCategory.indexOf(commandSpec.name))
                waitForScreen("screen_command_detail")
                for (i in 0 until fieldCount) {
                    composeTestRule.onNodeWithTag("param_$i").performTextInput(resolveField(i))
                }
                composeTestRule.onNodeWithTag("generateDataMatrixButton").performClick()
                Log.i(TAG, "$opticalTag tapped generateDataMatrixButton")
            }
            "nav_group" -> {
                composeTestRule.onNodeWithTag("mainListItem_groups").performClick()
                waitForScreen("screen_group_picker")
                Log.i(TAG, "$opticalTag on screen_group_picker, selecting category=\"$category\"")
                composeTestRule.onNodeWithTag("groupPickerSelect").performClick()
                composeTestRule.onNodeWithTag("groupPickerSelect_option_$category").performClick()
                composeTestRule.onNodeWithTag("groupPickerGenerate").performClick()
                Log.i(TAG, "$opticalTag tapped groupPickerGenerate")
            }
            else -> throw IllegalArgumentException("unknown optical generate target: $target")
        }

        waitForScreen("generatedDataMatrixImage")
        Log.i(TAG, "$opticalTag generatedDataMatrixImage rendered, writing result")
        return JSONObject().put("command", "OpticalGenerate: $caseID").put("pass", true)
    }

    /**
     * Returns [generateAndHoldAll] to screen_main after one case's generated code has had its
     * hold window: dismisses the GeneratedDataMatrixDialog, then pops however many nav levels
     * [generateOneCase] pushed for that case's own `target` (3 for "run" --
     * categories/commands/{category}/commandDetail; 1 for "nav_group" -- groupPicker), so the
     * next iteration starts from the identical screen_main state a fresh process would have
     * given it.
     */
    private fun resetToMainAfterGenerate(target: String) {
        composeTestRule.onNodeWithTag("generatedDataMatrixClose").performClick()
        val popCount = if (target == "run") 3 else 1
        repeat(popCount) { pressBack() }
        waitForScreen("screen_main")
    }

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
     * ceiling elapses, and there is nothing left worth waiting for. Uses [BATCH_TIMEOUT_MS]
     * uniformly (see that constant's own doc comment) rather than each case's own timeout_ms.
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
        val signalChannelID = openCaseSignalChannel(opticalTag)

        val results = JSONArray()
        results.put(JSONObject().put("command", "OpticalReady").put("pass", true))
        writeResults(context, results)
        Log.i(TAG, "$opticalTag ready, camera scanner is live")

        for (i in 0 until specs.length()) {
            val spec = specs.getJSONObject(i)
            val caseID = spec.getString("case_id")
            if (i <= 1) spec.put("timeout_ms", FIRST_CASE_AWAIT_TIMEOUT_MS)
            val caseTag = "$opticalTag case=$caseID (${i + 1}/${specs.length()})"
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
                    waitForTagWithTimeout("runConfirmExecute", timeoutMs)
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
                    // Wrapped in try/finally, not just a trailing pressBack after recordPass:
                    // scanning genuinely navigated here (unlike "run"/"ticket", which never leave
                    // screen_main), so a title mismatch below has to pop back to screen_main
                    // before this returns too, or the *next* case in an awaitAndVerifyScanAll
                    // batch would start from the wrong screen. An earlier version only popped
                    // back on the success path.
                    try {
                        waitForTagWithTimeout("screen_commands", timeoutMs)
                        Log.i(TAG, "$opticalTag screen_commands appeared at ${System.currentTimeMillis()}")
                        settleAfterScanTriggeredNavigation()
                        Log.i(TAG, "$opticalTag settle complete at ${System.currentTimeMillis()}")
                        val title = readTagText("categoryTitle")
                        val want = spec.getString("category_title")
                        Log.i(TAG, "$opticalTag categoryTitle=\"$title\" want=\"$want\"")
                        if (title != want) throw AssertionError("categoryTitle=\"$title\", want \"$want\"")
                        pass()
                    } finally {
                        // Only meaningful once the screen actually navigated (a timeout above
                        // means it never left screen_main, so this is a harmless no-op then) --
                        // see class doc comment for why Espresso.pressBack() isn't used here.
                        if (composeTestRule.onAllNodesWithTag("screen_commands").fetchSemanticsNodes(atLeastOneRootRequired = false).isNotEmpty()) {
                            pressBack()
                            Log.i(TAG, "$opticalTag pressBack complete, returning")
                        }
                    }
                }
                "ticket" -> {
                    waitForTagWithTimeout("recruitConfirmApprove", timeoutMs)
                    Log.i(TAG, "$opticalTag recruitConfirmApprove appeared")
                    composeTestRule.onNodeWithTag("recruitConfirmApprove").performClick()
                    Log.i(TAG, "$opticalTag tapped Approve")
                    pass()
                }
                else -> throw IllegalArgumentException("unknown optical expect kind: $kind")
            }
        } catch (e: Throwable) {
            fail(e.message ?: e.toString())
        }
    }

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
     */
    private fun resultExpectation(name: String, selfPeerID: String): (String) -> Unit = when {
        name == "rejected" -> ::assertRejected
        name == "no_crash" -> ::assertNoCrash
        name.startsWith("contains:") -> { line: String -> assertContains(line, name.removePrefix("contains:").replace("{{selfPeerID}}", selfPeerID)) }
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
     * Category/command lists are LazyColumns -- only the currently visible (plus a small prefetch
     * buffer) rows actually exist in the semantics tree at any moment, so a target further down
     * (e.g. "Cluster"'s ~30 commands, most below the fold on a phone-sized viewport) isn't
     * clickable, or even findable, until scrolled into view. [index] is that row's position in
     * the *real* on-screen list (CategoriesScreen's `categories`/CommandListScreen's `names`,
     * exactly as rendered), passed to `performScrollToIndex` on the enclosing LazyColumn (tagged
     * "itemList") to jump straight to it via `LazyListState`, the only approach confirmed live to
     * reliably reach far-down rows: `performScrollTo()` alone (tried first) only walks forward
     * from whatever's already composed and silently stops working once a target is far enough
     * past the initial viewport that it was never composed at all -- it threw "could not find any
     * node" for the 19th "Cluster" command ("CreateJoinInviteTicket") on a run where the first 18
     * had scrolled/clicked fine.
     */
    private fun clickListItem(text: String, index: Int) {
        composeTestRule.onNodeWithTag("itemList").performScrollToIndex(index)
        composeTestRule.onNodeWithTag("listItem_$text").performClick()
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
            composeTestRule.waitUntil(NAV_TIMEOUT_MS) {
                composeTestRule.onAllNodesWithTag(tag).fetchSemanticsNodes(atLeastOneRootRequired = false).isNotEmpty()
            }
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
        try {
            composeTestRule.waitUntil(timeoutMs) {
                composeTestRule.onAllNodesWithTag(tag).fetchSemanticsNodes(atLeastOneRootRequired = false).isNotEmpty()
            }
        } catch (e: ComposeTimeoutException) {
            throw IllegalStateException("'$tag' not shown after ${timeoutMs}ms", e)
        }
    }

    /**
     * A scan-triggered navigation (see AppRoot.kt's `ScannerCoordinator.scans.collect { ... }`
     * listener's NavCode.Group branch) calls `navController.navigate(...)` from a coroutine
     * reacting to the camera scanner, not from a `performClick()` this class's own
     * `waitUntil`/`waitForIdle` reliably synchronize with -- confirmed live: the target screen's
     * own testTag appears in the semantics tree (satisfying [waitForTagWithTimeout]) while its
     * NavBackStackEntry's underlying Android-framework Lifecycle (a separate async dispatch loop
     * from Compose's own recomposition scheduling, which `waitForIdle` alone does not wait on) is
     * still mid-transition, not yet RESUMED -- tearing the Activity down at exactly that moment
     * (recording a result and returning immediately, the normal end of this method) crashed the
     * whole instrumentation process with "State must be at least CREATED to move to DESTROYED,
     * but was INITIALIZED". A fixed settle, not a poll: there is no test-visible signal for "this
     * NavBackStackEntry reached RESUMED" to poll for instead.
     */
    private fun settleAfterScanTriggeredNavigation() {
        composeTestRule.waitForIdle()
        Thread.sleep(500)
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
     * Gets from a fresh launch (MainListScreen, "screen_main" -- see AppRoot's NavHost) to the
     * full category browser [generateAndHold]'s "run" target navigates through: taps the "Browse
     * all categories" link and waits for CategoriesScreen to appear.
     */
    private fun navigateToCategories() {
        waitForScreen("screen_main")
        composeTestRule.onNodeWithTag("browseCategories").performClick()
        waitForScreen("screen_categories")
    }

    /** Reads a plain Text composable's current content by [tag] (e.g. categoryTitle/runConfirmParams). */
    private fun readTagText(tag: String): String {
        val node = composeTestRule.onNodeWithTag(tag).fetchSemanticsNode()
        return node.config.getOrNull(SemanticsProperties.Text)?.joinToString("") { it.text } ?: ""
    }
}
