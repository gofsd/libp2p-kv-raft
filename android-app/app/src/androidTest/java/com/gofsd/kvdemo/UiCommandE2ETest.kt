package com.gofsd.kvdemo

import android.util.Base64
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
import kvmobile.Kvmobile
import org.json.JSONArray
import org.json.JSONObject
import org.junit.Assert
import org.junit.Rule
import org.junit.Test
import java.io.File

/**
 * Real-UI-driven instrumented test: unlike E2ETest (which calls
 * Kvmobile.sendEvent directly, never touching a single screen), this
 * clicks through the actual app -- CategoriesScreen's category list,
 * CommandListScreen's command list, CommandDetailScreen's dynamically-
 * rendered param fields and Run button -- for literally every CommandSpec
 * buildCommands() produces, so the catalog can never silently drift out
 * of coverage (a command added to CommandCatalog.kt with no entry in
 * [cases] below still gets full navigation coverage via [defaultCase],
 * just not a tailored execution).
 *
 * Drives the app via Jetpack Compose's test APIs (onNodeWithTag/
 * performClick/performTextInput) against the [Modifier.testTag]s each
 * screen sets, not Espresso View matchers or Activity-class identity --
 * this app moved from four Activities to one Activity + a NavHost of
 * Composable routes (see AppRoot.kt), which eliminated any distinct
 * Activity subclass to check "am I on the detail screen" against, and any
 * `R.id.*`-addressable View to findViewById. Back navigation is driven
 * directly through the current Activity's own `OnBackPressedDispatcher`
 * (see this class's own [pressBack]), not `Espresso.pressBack()` --
 * confirmed live against a real emulator that the two are *not*
 * interchangeable here: `Espresso.pressBack()` reliably finished the
 * Activity outright ("Pressed back and killed the app") on the very
 * first pop, even though the identical back press worked correctly when
 * sent manually (`adb shell input keyevent 4`) and Compose Navigation's
 * own back handling is otherwise unremarkable -- Espresso's pressBack
 * implementation predates `OnBackPressedDispatcher`/predictive back and
 * does not reliably route through the callback chain NavHost registers
 * with it, at least at the AndroidX versions this project currently
 * pins. Driving the dispatcher directly sidesteps that gap entirely.
 *
 * Called via `adb shell am instrument -e class
 * com.gofsd.kvdemo.UiCommandE2ETest -e cases <base64>` by
 * pkg/e2erun/android.go, same as E2ETest -- see that file's own doc
 * comment. Two different concerns, two different test classes: E2ETest
 * proves the raw shmevent wire protocol works from a mobile client; this
 * proves every command is actually reachable and operable through the
 * screens a real user taps. pkg/e2erun/android.go itself, and the
 * `e2edata.UICase`/`ui_e2e_results.json` contract below, are unaffected
 * by the Compose rewrite -- android.go only cares about this class's
 * name, the "cases"/"onlyListedCases" instrumentation arg names, and the
 * results file's JSON schema, never about Views/Activities.
 *
 * The "cases" instrumentation argument (see buildCasesFromArg) is
 * base64-encoded JSON -- pkg/e2erun/android.go's runUICommandTest doc
 * comment has the full story, but in short: raw JSON's quotes/braces
 * reliably get mangled somewhere between `adb shell` and `am`'s own
 * argument parser, so it's base64 here specifically to avoid that, not a
 * style preference. Decoded, it's the JSON form of pkg/e2edata.File.UICases,
 * keyed by CommandSpec.label -- this file no longer hardcodes per-command
 * test plans itself, so test/e2e/testdata.json is the single source of
 * truth for both the cross-platform wire-protocol rows and this Android UI
 * command walk. A label with no entry in the parsed map (or no "cases"
 * argument at all, e.g. this test invoked directly via `./gradlew
 * connectedAndroidTest` rather than through pkg/e2erun) still gets full
 * navigation-only coverage via [defaultCase] -- see
 * [runAllCommandsThroughUi]'s loop -- just not a tailored execution.
 *
 * This device's build-time identity joins the shared, long-lived e2e
 * leader as a raft *learner*, not a voter, on its very first join (see
 * buildAndroidAAR's joinSuffrage=learner ldflag) -- deliberately, so
 * voter-gated writes this test executes are safe to actually invoke for
 * real without mutating the shared cluster's membership. That first-join
 * suffrage is NOT a standing guarantee, though: Kvmobile.start's own join
 * is a no-op for an identity that's already a cluster member (see that
 * function's doc comment), so a test identity that was ever admitted as a
 * voter by an *earlier* run (before this suffrage-pinning existed, or via
 * any other path) stays a voter indefinitely -- confirmed directly against
 * this exact shared cluster (and, worse, confirmed to have been able to
 * un-confirm itself: an earlier version of the Kick case below targeted
 * this device's own peer id, which actually executed a real RemoveServer
 * against it once it turned out to be a voter -- see that case's own
 * comment for why it never does that again).
 *
 * This device's *own* listClusterMembers() view of its current role
 * cannot be trusted to answer "am I a voter right now" either: it's this
 * device's locally-replicated snapshot, and confirmed directly to go
 * permanently stale the moment this identity stops actively
 * voting/replicating (a demotion left it reporting itself "voter" long
 * after an independent, direct query against the real leader showed it
 * had actually become "learner," with no further update ever arriving).
 * So voter-gated cases below (CreateJoinInvite/RevokeJoinInvite/Kick,
 * Execute's leader-address resolution) can't key their expected outcome
 * off this device's own reported state the way an earlier version of this
 * file tried to -- they use assertNoCrash instead, the same tolerant
 * "either outcome is fine, just don't crash" treatment Channel: OpenChannel
 * already needed for an unrelated reason (network reachability, not state
 * staleness). Several other commands are genuinely destructive to this
 * device's own
 * membership in that shared cluster if actually invoked (Join/JoinWithKey
 * switch clusters outright; StartPending(WithKey) resets to no-cluster;
 * Stop/Leave/Rm tear the daemon or membership down entirely; RecruitPeer
 * and ListenChannel need a second real device/peer actively cooperating,
 * the latter blocking indefinitely with nobody to unblock it) -- those
 * are marked `execute = false` below: real navigation to their own detail
 * screen (title, param field count) is still verified, but Run is never
 * tapped, exactly the same "browsable but not safely invokable in this
 * shared, single-device topology" reasoning `mage e2e:all`'s own
 * documented one-time-row caveats already apply elsewhere in this
 * project's e2e design.
 *
 * Join/RecruitPeer's "needs a second real device" gap is closed
 * separately, by pkg/e2erun/android_pair.go -- a step-by-step (not
 * single-shot) orchestration across two throwaway, never-shared devices,
 * reusing this same class one small case at a time via the
 * `onlyListedCases` instrumentation arg (see below) rather than a whole
 * new test class. This class's own flat, single-device `cases` sweep
 * still deliberately excludes them, for the reasons above.
 */
class UiCommandE2ETest {

    @get:Rule
    val composeTestRule = createAndroidComposeRule<MainActivity>()

    /**
     * One CommandSpec's real-UI test plan. [inputs] are typed into its
     * param fields in order (must match spec.params.size when [execute]
     * is true -- checked below, a mismatch is a bug in this file, not a
     * real per-command failure). [execute] gates whether Run is actually
     * tapped; false means navigation-only (see class doc comment for
     * which commands and why). [expect] runs against the exact line
     * CommandDetailScreen's Run handler appended to its output log --
     * `"$label(args) ->\n$result"` on success or `"$label(args) FAILED:
     * $msg"` on a thrown exception -- called only when [execute] is true.
     */
    private class Case(
        val inputs: List<String> = emptyList(),
        val execute: Boolean = false,
        val expect: (String) -> Unit = { assertSucceeded(it) },
        // Re-taps Run (see runCommandWithRetry) for up to this long if the
        // first attempt's output line contains "FAILED", before giving up
        // and letting that failure reach [expect] -- for cases whose
        // success depends on this follower's own local raft apply catching
        // up with a write this same run just committed on the leader
        // moments earlier (KV: Get, below), the same follower-replication-
        // lag window E2ETest.kt's own sendWithRetry already retries around
        // for get_field/get_key. Zero (the default) means no retry.
        val retryBudgetMs: Long = 0,
        // 1-based position in a caller-defined sequence -- see
        // e2edata.UICase.Order and [runOrderedWalk]. Zero (the default)
        // means this case belongs to an unordered sweep, which is what
        // every caller but pkg/e2erun/android_pair.go runs.
        val order: Int = 0,
    )

    private companion object {
        // "FAILED" only ever appears in CommandDetailScreen's Run
        // handler's own catch-branch formatting -- never in a successful
        // result string in practice, so it's a reliable discriminator
        // without needing to parse the result's own JSON.
        fun assertSucceeded(line: String) =
            Assert.assertFalse("expected success, got: $line", line.contains("FAILED"))

        fun assertRejected(line: String) =
            Assert.assertTrue("expected a rejection (this device is a learner, not a voter), got: $line", line.contains("FAILED"))

        // Accepts either outcome -- for commands whose success depends on
        // shared-leader configuration this test doesn't control (e.g.
        // RequirePermitForLog), where the only thing actually worth
        // proving is that the UI surfaces *a* clean, well-formed result
        // either way, never a crash.
        fun assertNoCrash(@Suppress("UNUSED_PARAMETER") line: String) = Unit

        // Bounds how long runCommand waits for CommandDetailScreen's
        // background coroutine to post a result -- generous enough to
        // cover a real forwarded-write round trip to the shared remote
        // leader (and OpenChannel/RedeemExecInvite's own up-to-60s
        // internal timeouts, see kvmobile's callTimeout) without hanging
        // the whole suite forever if something is genuinely stuck.
        //
        // Raised from 65s once two commands legitimately outgrew it:
        // RequestRelayAccess, which waits on a real relay reservation
        // (measured ~30-40s on an emulator through the deployed VPS
        // relay), and pkg/e2erun/android_pair.go's hold sleep, which has
        // to keep this device's daemon alive for the whole of another
        // device's own start-up-and-reserve before it can dial in.
        const val RUN_TIMEOUT_MS = 180_000L
        const val POLL_INTERVAL_MS = 250L

        // Bounds waitForScreen's poll for a navigation target's testTag to
        // actually appear in the composition. 10s is generous for what's
        // normally a same-process transition with no network involved --
        // this exists for the rare case it isn't instant, not to paper
        // over a real hang.
        const val NAV_TIMEOUT_MS = 10_000L
    }

    @Test
    fun runAllCommandsThroughUi() {
        val context = InstrumentationRegistry.getInstrumentation().targetContext
        val dataDir = context.filesDir.absolutePath

        // Kvmobile.start is idempotent (safe to call again once already
        // running, see its own doc comment) -- calling it directly here
        // just to learn this device's own peer id and the shared
        // cluster's current leader without needing a UI round trip for
        // fixture data no real user would ever type in by hand. AppRoot's
        // own LaunchedEffect(Unit) already calls Start too, as part of
        // composeTestRule launching MainActivity for this test -- the two
        // calls racing/overlapping is the same idempotent-concurrent-safe
        // pattern this test relied on before the Compose rewrite (the old
        // MainActivity.onCreate's own background Thread called Start the
        // same way, independent of this fixture call). Best-effort:
        // pkg/e2erun/android_pair.go's AAR build deliberately bakes in no
        // leaderMultiaddr at all (its two devices only ever use
        // StartSoloWithKey/StartPendingWithKey, never this build-time-
        // leader Start), so Start throws "no leader multiaddr baked in at
        // build time" there on every invocation -- that's expected and
        // must not fail the whole test, since selfPeerID/leaderPeerID are
        // unused by that orchestration's own literal, pre-computed case
        // inputs anyway.
        val selfPeerID = runCatching { Kvmobile.start(dataDir) }.getOrDefault("")
        // Best-effort only: this device's own listClusterMembers() is its
        // locally-replicated snapshot, confirmed directly to go
        // permanently stale once this identity stops actively voting/
        // replicating (no further update ever arrives after that point,
        // not even a delayed one) -- so leaderPeerID below is a plausible
        // dial target, not a guaranteed-correct one, and no case may
        // assume this device's own reported role/suffrage reflects its
        // real current standing (see class doc comment).
        val members = runCatching { JSONArray(Kvmobile.listClusterMembers()) }.getOrNull()
        val leaderPeerID = (0 until (members?.length() ?: 0))
            .map { members!!.getJSONObject(it) }
            .firstOrNull { it.optString("role") == "leader" }
            ?.optString("peer_id") ?: selfPeerID

        val casesArg = InstrumentationRegistry.getArguments().getString("cases")
        val cases = buildCasesFromArg(casesArg, selfPeerID, leaderPeerID)
        // onlyListedCases: pkg/e2erun/android_pair.go's step-by-step
        // orchestration passes this so each single-case instrumentation
        // call only touches the one command it cares about, instead of
        // walking (and Run-tapping-or-not) the whole ~60-command catalog
        // every time -- absent (every other call site), behavior is
        // unchanged from before this flag existed.
        val onlyListedCases = (InstrumentationRegistry.getArguments().getString("onlyListedCases") ?: "false").toBoolean()

        val allCommands = buildCommands(dataDir) { }
        Assert.assertTrue("catalog is unexpectedly empty", allCommands.isNotEmpty())
        val commandsToWalk = if (onlyListedCases) allCommands.filter { cases.containsKey(it.label) } else allCommands

        // A caller that stamped every one of its cases with an `order`
        // (pkg/e2erun/android_pair.go, via e2edata.UICase.Order) is asking
        // for a *sequence*, not a sweep: run them in exactly that order,
        // re-entering the right category for each one, instead of the
        // catalog's own category-by-category walk. Without this the walk
        // order silently won, and a step's own settle-sleep -- being in the
        // "Test" category, walked after "Cluster" -- ran after the command
        // it was supposed to precede, so a scenario built on "resume, wait,
        // then act" never actually waited. The sweep path (no order stamped,
        // every other caller) is untouched.
        val orderedWalk = commandsToWalk
            .filter { (cases[it.label]?.order ?: 0) > 0 }
            .sortedBy { cases[it.label]!!.order }
        if (orderedWalk.size == commandsToWalk.size && orderedWalk.isNotEmpty()) {
            runOrderedWalk(orderedWalk, cases, context)
            return
        }

        val failures = mutableListOf<String>()
        val results = JSONArray()

        waitForScreen("screen_categories")
        // Real on-screen order/content, from the full unfiltered catalog
        // -- not commandsToWalk, which onlyListedCases may have filtered
        // down to a subset that no longer matches actual row positions.
        // See clickListItem's doc comment for why the index must come
        // from here.
        val allCategories = allCommands.map { it.category }.distinct()
        val categories = commandsToWalk.map { it.category }.distinct()
        for (category in categories) {
            clickListItem(category, allCategories.indexOf(category))
            val allNamesInCategory = allCommands.filter { it.category == category }.map { it.name }
            val commandsInCategory = commandsToWalk.filter { it.category == category }
            for (spec in commandsInCategory) {
                val entry = runOneCase(spec, cases[spec.label] ?: Case(), allNamesInCategory.indexOf(spec.name), failures)
                results.put(entry)
                writeResults(context, results)
            }
            pressBack()
        }

        if (failures.isNotEmpty()) {
            Assert.fail("${failures.size} of ${results.length()} command(s) failed:\n" + failures.joinToString("\n"))
        }
    }

    /**
     * Runs [ordered] one at a time, in exactly the order the caller asked
     * for (see [Case.order]), re-entering each command's own category for
     * every step rather than walking a category's commands together. That
     * costs one extra back-navigation per step and is the whole point: a
     * sequence like "resume this daemon, wait 20s, then read its address"
     * spans two categories, and grouping by category reorders it into
     * nonsense.
     */
    private fun runOrderedWalk(ordered: List<CommandSpec>, cases: Map<String, Case>, context: android.content.Context) {
        val failures = mutableListOf<String>()
        val results = JSONArray()

        // Real on-screen order/content -- see clickListItem's doc
        // comment for why this must come from the full, unfiltered
        // catalog rather than `ordered` (which is itself already a
        // caller-selected subset in this walk).
        val allCommands = buildCommands(InstrumentationRegistry.getInstrumentation().targetContext.filesDir.absolutePath) { }
        val allCategories = allCommands.map { it.category }.distinct()

        waitForScreen("screen_categories")
        for (spec in ordered) {
            clickListItem(spec.category, allCategories.indexOf(spec.category))
            val allNamesInCategory = allCommands.filter { it.category == spec.category }.map { it.name }
            val entry = runOneCase(spec, cases[spec.label] ?: Case(), allNamesInCategory.indexOf(spec.name), failures)
            results.put(entry)
            writeResults(context, results)
            // Back out of the category list too, so the next step starts
            // from the same categories screen this one did.
            pressBack()
        }

        if (failures.isNotEmpty()) {
            Assert.fail("${failures.size} of ${results.length()} command(s) failed:\n" + failures.joinToString("\n"))
        }
    }

    /**
     * Navigates into [spec] from its category list, optionally runs it, and
     * returns the result entry -- shared by the ordered walk and the flat
     * per-category sweep so both report identically. Leaves the UI back on
     * the category list (the `finally` press-back), whichever way it went.
     * [nameIndex] is spec.name's position in its category's real on-screen
     * command list -- see clickListItem's doc comment.
     */
    private fun runOneCase(spec: CommandSpec, case: Case, nameIndex: Int, failures: MutableList<String>): JSONObject {
        val label = spec.label
        val entry = JSONObject().put("command", label)
        try {
            clickListItem(spec.name, nameIndex)
            verifyDetailScreen(spec)
            if (case.execute) {
                Assert.assertEquals(
                    "$label: Case.inputs size must match spec.params size",
                    spec.params.size,
                    case.inputs.size,
                )
                val output = runCommandWithRetry(case.inputs, case.retryBudgetMs)
                case.expect(output)
                // Surface the command's real return value back to the Go
                // harness -- CommandDetailScreen's Run handler's success
                // format is "$label(args) ->\n$result", so strip that
                // fixed prefix to recover $result alone (e.g. GetOwnAddr's
                // address, CreateJoinRequest's token, ListClusterMembers'
                // JSON) -- needed by pkg/e2erun/android_pair.go's
                // cross-device orchestration, unused by the flat
                // per-label sweep.
                val prefix = "$label(${case.inputs.joinToString(", ")}) ->\n"
                if (output.startsWith(prefix)) {
                    entry.put("output", output.removePrefix(prefix))
                }
            }
            entry.put("pass", true)
        } catch (e: Throwable) {
            entry.put("pass", false)
            entry.put("error", e.message ?: e.toString())
            failures += "$label: ${e.message}"
        } finally {
            pressBack()
        }
        return entry
    }

    /**
     * Writes the results file after every entry, not just once at the very
     * end (an earlier version of this file did that): lets
     * pkg/e2erun/android_pair.go's runUIStepsBackgroundPeek pull and observe
     * an early op's own result (e.g. CreateJoinRequest's token) via `adb
     * pull` while this SAME instrumentation invocation is still running a
     * later op (e.g. a trailing "Test: SleepMillis" hold keeping this
     * process -- and so its daemon's listener -- alive for a
     * concurrently-dialing peer on a different device). The last entry's own
     * write already covers the final steady-state content, so there is no
     * separate write after the loop.
     */
    private fun writeResults(context: android.content.Context, results: JSONArray) {
        File(context.getExternalFilesDir(null), "ui_e2e_results.json").writeText(results.toString())
    }

    /**
     * Category/command lists are LazyColumns -- only the currently
     * visible (plus a small prefetch buffer) rows actually exist in the
     * semantics tree at any moment, so a target further down (e.g.
     * "Cluster"'s ~30 commands, most below the fold on a phone-sized
     * viewport) isn't clickable, or even findable, until scrolled into
     * view. [index] is that row's position in the *real* on-screen list
     * (CategoriesScreen's `categories`/CommandListScreen's `names`,
     * exactly as rendered -- see call sites for why this must come from
     * the full, unfiltered catalog, not `commandsToWalk`), passed to
     * `performScrollToIndex` on the enclosing LazyColumn (tagged
     * "itemList") to jump straight to it via `LazyListState`, the only
     * approach confirmed live to reliably reach far-down rows:
     * `performScrollTo()` alone (tried first) only walks forward from
     * whatever's already composed and silently stops working once a
     * target is far enough past the initial viewport that it was never
     * composed at all -- it threw "could not find any node" for the
     * 19th "Cluster" command ("CreateJoinInviteTicket") on a run where
     * the first 18 had scrolled/clicked fine.
     */
    private fun clickListItem(text: String, index: Int) {
        composeTestRule.onNodeWithTag("itemList").performScrollToIndex(index)
        composeTestRule.onNodeWithTag("listItem_$text").performClick()
    }

    /**
     * Pops one entry off the current back stack via the Activity's own
     * `OnBackPressedDispatcher` -- see class doc comment for why this,
     * not `Espresso.pressBack()`, is what actually works here. Runs on
     * the main thread (the dispatcher isn't thread-safe to call from a
     * test's own background thread) and waits for the resulting
     * recomposition to settle before returning, the same guarantee
     * `performClick`/`performTextInput` already give every other
     * navigation action in this file.
     */
    private fun pressBack() {
        composeTestRule.activity.onBackPressedDispatcher.let { dispatcher ->
            composeTestRule.runOnUiThread { dispatcher.onBackPressed() }
        }
        composeTestRule.waitForIdle()
    }

    /**
     * Polls for [tag] to appear anywhere in the current composition, or
     * throws once NAV_TIMEOUT_MS elapses -- this test's Compose-native
     * replacement for the old Activity-class-identity check
     * (waitForActivity/ActivityLifecycleMonitorRegistry), since a
     * single-Activity Compose app has no distinct Activity subclass per
     * screen to check against. Each screen's root Composable
     * (CategoriesScreen/CommandListScreen/CommandDetailScreen/LogScreen)
     * carries a stable "screen_*" testTag exactly for this purpose.
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

    private fun verifyDetailScreen(spec: CommandSpec) {
        waitForScreen("screen_command_detail")
        for (i in spec.params.indices) {
            composeTestRule.onNodeWithTag("param_$i").assertExists()
        }
        composeTestRule.onNodeWithTag("param_${spec.params.size}").assertDoesNotExist()
    }

    /** Reads outputText's current full text content from the semantics tree. */
    private fun readOutputText(): String {
        val node = composeTestRule.onNodeWithTag("outputText").fetchSemanticsNode()
        return node.config.getOrNull(SemanticsProperties.Text)?.joinToString("") { it.text } ?: ""
    }

    /**
     * Fills each param field in order, taps Run, and waits (bounded) for a
     * *new* result to appear -- comparing against outputText's length from
     * just before this tap, not just non-emptiness, so a second Run on the
     * same already-visited detail screen (see runCommandWithRetry) doesn't
     * just re-read a stale result left over from an earlier tap
     * (CommandDetailScreen's output state only ever appends, never
     * clears, same as the old CommandDetailActivity.appendOutput did).
     */
    private fun runCommand(inputs: List<String>): String {
        val priorText = readOutputText()

        for (i in inputs.indices) {
            composeTestRule.onNodeWithTag("param_$i").performTextInput(inputs[i])
        }
        composeTestRule.onNodeWithTag("runButton").performClick()

        try {
            composeTestRule.waitUntil(RUN_TIMEOUT_MS) { readOutputText().length > priorText.length }
        } catch (e: ComposeTimeoutException) {
            throw AssertionError("no output after ${RUN_TIMEOUT_MS}ms", e)
        }
        return readOutputText().substring(priorText.length).removePrefix("\n\n")
    }

    /** Retries [runCommand] (a fresh Run tap each time) while its output line reports "FAILED", up to retryBudgetMs total. */
    private fun runCommandWithRetry(inputs: List<String>, retryBudgetMs: Long): String {
        val deadline = System.currentTimeMillis() + retryBudgetMs
        while (true) {
            val output = runCommand(inputs)
            if (retryBudgetMs <= 0 || !output.contains("FAILED") || System.currentTimeMillis() >= deadline) return output
            Thread.sleep(POLL_INTERVAL_MS)
        }
    }

    /**
     * Parses the "cases" instrumentation argument -- base64-encoded JSON
     * object form of pkg/e2edata.File.UICases, keyed by CommandSpec.label
     * (see class doc comment for why base64: raw JSON here reliably breaks
     * `am instrument`'s own argument parsing) -- into this test's
     * per-command plans, substituting the runtime-only tokens
     * "{{selfPeerID}}"/"{{leaderPeerID}}"/"{{testKey}}"/"{{testValue}}" in
     * each input string -- those four values can't be frozen into a
     * committed file the way every other field can (this device's own
     * peer id, the cluster's currently-observed leader, and a
     * randomized-per-run KV test key/value that must stay unique across
     * runs against this long-lived shared cluster). casesArgBase64 missing,
     * not valid base64, or decoding to unparsable JSON is treated the same
     * as `"{}"` -- every command then falls back to [defaultCase]
     * (navigation-only), never a crash.
     *
     * Anything in the live catalog with no entry in the parsed map still
     * gets [defaultCase] (navigation-only) -- see class doc comment for
     * the reasoning behind each execute=true/false choice recorded in
     * test/e2e/testdata.json's "android_ui_cases" section.
     */
    private fun buildCasesFromArg(casesArgBase64: String?, selfPeerID: String, leaderPeerID: String): Map<String, Case> {
        val testKey = "e2e-ui-test-key"
        val testValue = "e2e-ui-test-value-${System.currentTimeMillis()}"
        val tokens = mapOf(
            "{{selfPeerID}}" to selfPeerID,
            "{{leaderPeerID}}" to leaderPeerID,
            "{{testKey}}" to testKey,
            "{{testValue}}" to testValue,
        )
        fun substitute(s: String): String {
            var result = s
            for ((token, value) in tokens) result = result.replace(token, value)
            return result
        }
        fun expectFor(name: String): (String) -> Unit = when (name) {
            "rejected" -> ::assertRejected
            "no_crash" -> ::assertNoCrash
            else -> ::assertSucceeded
        }

        val casesJson = casesArgBase64?.let {
            runCatching { String(Base64.decode(it, Base64.DEFAULT)) }.getOrNull()
        }
        val root = runCatching { JSONObject(casesJson ?: "{}") }.getOrDefault(JSONObject())
        val cases = mutableMapOf<String, Case>()
        for (label in root.keys()) {
            val obj = root.getJSONObject(label)
            val inputsArr = obj.optJSONArray("inputs")
            val inputs = (0 until (inputsArr?.length() ?: 0)).map { substitute(inputsArr!!.getString(it)) }
            cases[label] = Case(
                inputs = inputs,
                execute = obj.optBoolean("execute", false),
                expect = expectFor(obj.optString("expect", "succeeded")),
                retryBudgetMs = obj.optLong("retry_budget_ms", 0L),
                order = obj.optInt("order", 0),
            )
        }
        return cases
    }
}
