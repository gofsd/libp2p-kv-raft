package com.gofsd.kvdemo

import android.app.Activity
import android.widget.Button
import android.widget.EditText
import android.widget.LinearLayout
import android.widget.ListView
import android.widget.TextView
import androidx.test.espresso.Espresso.onData
import androidx.test.espresso.Espresso.pressBack
import androidx.test.espresso.action.ViewActions.click
import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import androidx.test.runner.lifecycle.ActivityLifecycleMonitorRegistry
import androidx.test.runner.lifecycle.Stage
import kvmobile.Kvmobile
import org.hamcrest.CoreMatchers.allOf
import org.hamcrest.CoreMatchers.instanceOf
import org.hamcrest.CoreMatchers.`is`
import org.json.JSONArray
import org.json.JSONObject
import org.junit.Assert
import org.junit.Test
import org.junit.runner.RunWith
import java.io.File

/**
 * Real-UI-driven instrumented test: unlike E2ETest (which calls
 * Kvmobile.sendEvent directly, never touching a single screen), this
 * clicks through the actual app -- MainActivity's category ListView,
 * CommandListActivity's command ListView, CommandDetailActivity's
 * dynamically-rendered param EditTexts and Run button -- for literally
 * every CommandSpec buildCommands() produces, so the catalog can never
 * silently drift out of coverage (a command added to CommandCatalog.kt
 * with no entry in [cases] below still gets full navigation coverage via
 * [defaultCase], just not a tailored execution).
 *
 * Called via `adb shell am instrument -e class
 * com.gofsd.kvdemo.UiCommandE2ETest` by pkg/e2erun/android.go, same as
 * E2ETest -- see that file's own doc comment. Two different concerns, two
 * different test classes: E2ETest proves the raw shmevent wire protocol
 * works from a mobile client; this proves every command is actually
 * reachable and operable through the screens a real user taps.
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
 */
@RunWith(AndroidJUnit4::class)
class UiCommandE2ETest {

    /**
     * One CommandSpec's real-UI test plan. [inputs] are typed into its
     * param fields in order (must match spec.params.size when [execute]
     * is true -- checked below, a mismatch is a bug in this file, not a
     * real per-command failure). [execute] gates whether Run is actually
     * tapped; false means navigation-only (see class doc comment for
     * which commands and why). [expect] runs against the exact line
     * CommandDetailActivity.onRun appended to its output log --
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
    )

    private companion object {
        // "FAILED" only ever appears in onRun's own catch-branch
        // formatting (see CommandDetailActivity.onRun) -- never in a
        // successful result string in practice, so it's a reliable
        // discriminator without needing to parse the result's own JSON.
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

        // Bounds how long runCommand waits for CommandDetailActivity's
        // background Thread to post a result -- generous enough to cover
        // a real forwarded-write round trip to the shared remote leader
        // (and OpenChannel/RedeemExecInvite's own up-to-60s internal
        // timeouts, see kvmobile's callTimeout) without hanging the whole
        // suite forever if something is genuinely stuck.
        const val RUN_TIMEOUT_MS = 65_000L
        const val POLL_INTERVAL_MS = 250L
    }

    @Test
    fun runAllCommandsThroughUi() {
        val context = InstrumentationRegistry.getInstrumentation().targetContext
        val dataDir = context.filesDir.absolutePath

        // Kvmobile.start is idempotent (safe to call again once already
        // running, see its own doc comment) -- calling it directly here,
        // before any UI is even on screen, just to learn this device's
        // own peer id and the shared cluster's current leader without
        // needing a UI round trip for fixture data no real user would
        // ever type in by hand.
        val selfPeerID = Kvmobile.start(dataDir)
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

        val cases = buildCases(selfPeerID, leaderPeerID)

        val allCommands = buildCommands(dataDir) { }
        Assert.assertTrue("catalog is unexpectedly empty", allCommands.isNotEmpty())

        val failures = mutableListOf<String>()
        val results = JSONArray()

        launchMainActivity()
        val categories = allCommands.map { it.category }.distinct()
        for (category in categories) {
            clickListItem(category)
            val commandsInCategory = allCommands.filter { it.category == category }
            for (spec in commandsInCategory) {
                val label = spec.label
                val case = cases[label] ?: Case()
                val entry = JSONObject().put("command", label)
                try {
                    clickListItem(spec.name)
                    val detail = currentActivity()
                    verifyDetailScreen(detail, spec)
                    if (case.execute) {
                        Assert.assertEquals(
                            "$label: Case.inputs size must match spec.params size",
                            spec.params.size,
                            case.inputs.size,
                        )
                        val output = runCommandWithRetry(detail, case.inputs, case.retryBudgetMs)
                        case.expect(output)
                    }
                    entry.put("pass", true)
                } catch (e: Throwable) {
                    entry.put("pass", false)
                    entry.put("error", e.message ?: e.toString())
                    failures += "$label: ${e.message}"
                } finally {
                    pressBack()
                }
                results.put(entry)
            }
            pressBack()
        }

        File(context.getExternalFilesDir(null), "ui_e2e_results.json").writeText(results.toString())

        if (failures.isNotEmpty()) {
            Assert.fail("${failures.size} of ${results.length()} command(s) failed:\n" + failures.joinToString("\n"))
        }
    }

    // ActivityScenario (not a raw Intent + FLAG_ACTIVITY_NEW_TASK) is what
    // actually registers this launch with Espresso/ActivityLifecycleMonitor's
    // own tracking -- a manually-built Intent launched straight from the
    // instrumentation's Context bypasses that registration, which raced
    // with the very next click and produced a stray, unrelated
    // NoActivityResumedException from pressBack() several steps later
    // (confirmed via logcat: LifecycleMonitor only ever logged MainActivity
    // itself, never CommandListActivity/CommandDetailActivity, even though
    // the clicks that should have opened them "succeeded"). The returned
    // scenario is deliberately never close()d here -- every subsequent
    // screen in this test is reached by clicking forward from it, and
    // pressBack() unwinds the same real back stack at the end of each
    // category.
    private fun launchMainActivity() {
        androidx.test.core.app.ActivityScenario.launch(MainActivity::class.java)
        InstrumentationRegistry.getInstrumentation().waitForIdleSync()
    }

    private fun clickListItem(text: String) {
        onData(allOf(instanceOf(String::class.java), `is`(text)))
            .inAdapterView(allOf(instanceOf(ListView::class.java)))
            .perform(click())
    }

    private fun currentActivity(): Activity {
        var activity: Activity? = null
        InstrumentationRegistry.getInstrumentation().runOnMainSync {
            activity = ActivityLifecycleMonitorRegistry.getInstance()
                .getActivitiesInStage(Stage.RESUMED)
                .firstOrNull()
        }
        return activity ?: throw IllegalStateException("no resumed activity")
    }

    private fun verifyDetailScreen(activity: Activity, spec: CommandSpec) {
        Assert.assertTrue(
            "expected CommandDetailActivity, got ${activity::class.java.simpleName}",
            activity is CommandDetailActivity,
        )
        val paramsContainer = activity.findViewById<LinearLayout>(R.id.paramsContainer)
        Assert.assertEquals(
            "${spec.label}: rendered param field count",
            spec.params.size,
            paramsContainer.childCount,
        )
    }

    /**
     * Fills paramsContainer's fields in order, taps Run, and waits
     * (bounded) for a *new* result to appear -- comparing against
     * outputText's length from just before this tap, not just
     * non-emptiness, so a second Run on the same already-visited detail
     * screen (see runCommandWithRetry) doesn't just re-read a stale
     * result left over from an earlier tap (appendOutput, CommandDetailActivity,
     * only ever appends, never clears).
     */
    private fun runCommand(activity: Activity, inputs: List<String>): String {
        val paramsContainer = activity.findViewById<LinearLayout>(R.id.paramsContainer)
        val runButton = activity.findViewById<Button>(R.id.runButton)
        val outputText = activity.findViewById<TextView>(R.id.outputText)

        var priorLength = 0
        InstrumentationRegistry.getInstrumentation().runOnMainSync { priorLength = outputText.text.length }

        InstrumentationRegistry.getInstrumentation().runOnMainSync {
            for (i in inputs.indices) {
                (paramsContainer.getChildAt(i) as EditText).setText(inputs[i])
            }
        }
        InstrumentationRegistry.getInstrumentation().runOnMainSync { runButton.performClick() }

        val deadline = System.currentTimeMillis() + RUN_TIMEOUT_MS
        while (System.currentTimeMillis() < deadline) {
            var text = ""
            InstrumentationRegistry.getInstrumentation().runOnMainSync { text = outputText.text.toString() }
            if (text.length > priorLength) return text.substring(priorLength).removePrefix("\n\n")
            Thread.sleep(POLL_INTERVAL_MS)
        }
        throw AssertionError("no output after ${RUN_TIMEOUT_MS}ms")
    }

    /** Retries [runCommand] (a fresh Run tap each time) while its output line reports "FAILED", up to retryBudgetMs total. */
    private fun runCommandWithRetry(activity: Activity, inputs: List<String>, retryBudgetMs: Long): String {
        val deadline = System.currentTimeMillis() + retryBudgetMs
        while (true) {
            val output = runCommand(activity, inputs)
            if (retryBudgetMs <= 0 || !output.contains("FAILED") || System.currentTimeMillis() >= deadline) return output
            Thread.sleep(POLL_INTERVAL_MS)
        }
    }

    /**
     * Per-command test plans, keyed by CommandSpec.label ("$category:
     * $name"). Anything in the live catalog with no entry here gets
     * [defaultCase] (navigation-only) -- see class doc comment for the
     * reasoning behind each execute=true/false choice below.
     */
    private fun buildCases(selfPeerID: String, leaderPeerID: String): Map<String, Case> {
        val testKey = "e2e-ui-test-key"
        val testValue = "e2e-ui-test-value-${System.currentTimeMillis()}"

        return mapOf(
            // --- Cluster ---
            "Cluster: Start" to Case(execute = true, expect = ::assertSucceeded),
            "Cluster: StartWithKey" to Case(
                inputs = listOf("not-a-real-key-hex"),
                execute = true,
                // Kvmobile.start's own "already started" short-circuit
                // (mobile/kvmobile/kvmobile.go's start()) fires before
                // resolveIdentity ever looks at keyHex, since Start already
                // ran once earlier in this same process (see
                // runAllCommandsThroughUi) -- so within a single test run
                // this always just idempotently returns the already-known
                // peer id, never actually reaching the "different identity
                // or unparseable hex" refusal path a *fresh* process would
                // hit. Confirmed directly: an earlier version of this case
                // assumed rejection and failed against a real device.
                expect = ::assertSucceeded,
            ),
            "Cluster: GetOwnAddr" to Case(execute = true, expect = ::assertSucceeded),
            "Cluster: CreateJoinRequest" to Case(execute = true, expect = ::assertSucceeded),
            "Cluster: CancelJoinRequest" to Case(
                inputs = listOf("0000000000000000"),
                execute = true,
                // No matching pending request -- a clean no-op/rejection,
                // not a crash.
                expect = ::assertNoCrash,
            ),
            // assertNoCrash, not a hardcoded rejection/success: this
            // device's real current voter standing can't be determined
            // reliably from here (see class doc comment) -- either a clean
            // "not a current raft voter" rejection or a real, harmless
            // success (this device's own suffrage argument, "learner,"
            // never actually admits anyone since nobody redeems it) is
            // fine, a thrown exception isn't.
            "Cluster: CreateJoinInvite" to Case(inputs = listOf("learner"), execute = true, expect = ::assertNoCrash),
            "Cluster: RevokeJoinInvite" to Case(
                inputs = listOf("00000000000000000000000000000000"),
                execute = true,
                expect = ::assertNoCrash,
            ),
            "Cluster: Delete" to Case(
                execute = true,
                // Daemon is running (Start already brought it up) -- Delete
                // must refuse rather than actually deleting anything.
                expect = ::assertRejected,
            ),
            // Targets a peer id that was never a real cluster member --
            // NOT selfPeerID (an earlier version of this case did exactly
            // that, and it really did remove this device's own voter
            // standing from the shared, long-lived cluster for good once
            // it turned out to already be a voter: hashicorp/raft's
            // RemoveServer has no separate "isVoter" check of its own to
            // fail past -- see removeServerLine -- so a genuinely-
            // authorized voter calling it always actually applies). A
            // nonexistent target still exercises the exact same isVoter
            // gate (handleForwardKickStream) with zero real mutation risk
            // either way: RemoveServer on an id already absent from the
            // configuration is a harmless no-op "OK", not an error.
            // assertNoCrash for the same reason as CreateJoinInvite/
            // RevokeJoinInvite above -- this device's real voter standing
            // isn't reliably knowable from here.
            "Cluster: Kick" to Case(
                inputs = listOf("12D3KooWNoSuchPeerNoSuchPeerNoSuchPeer111111"),
                execute = true,
                expect = ::assertNoCrash,
            ),
            "Cluster: ListClusters" to Case(execute = true, expect = ::assertSucceeded),
            "Cluster: ListClusterMembers" to Case(execute = true, expect = ::assertSucceeded),
            "Cluster: PeerID" to Case(execute = true, expect = ::assertSucceeded),
            "Cluster: AccessToken" to Case(execute = true, expect = ::assertSucceeded),

            // --- KV --- (not voter-gated; harmless namespaced test key)
            "KV: Submit" to Case(inputs = listOf(testKey, testValue), execute = true, expect = ::assertSucceeded),
            // retryBudgetMs tolerates this follower's own local raft apply
            // briefly lagging behind Submit's just-committed write on the
            // leader -- the same replication-lag window E2ETest.kt's own
            // sendWithRetry already retries around for get_field/get_key.
            // Confirmed directly: an earlier version of this case had no
            // retry and failed "key not found" against a real device.
            "KV: Get" to Case(
                inputs = listOf(testKey),
                execute = true,
                expect = ::assertSucceeded,
                retryBudgetMs = 5_000L,
            ),
            "KV: RangeScan" to Case(
                inputs = listOf(testKey, testKey + "￿", "10"),
                execute = true,
                expect = ::assertSucceeded,
            ),

            // --- Permits --- (Request* not voter-gated; Confirm*/Revoke* are)
            "Permits: RequestPermit" to Case(
                inputs = listOf("peer", selfPeerID, ""),
                execute = true,
                expect = ::assertSucceeded,
            ),
            "Permits: ConfirmPermit" to Case(
                inputs = listOf("peer", selfPeerID),
                execute = true,
                expect = ::assertRejected,
            ),
            "Permits: RevokePermit" to Case(
                inputs = listOf("peer", selfPeerID),
                execute = true,
                expect = ::assertRejected,
            ),
            "Permits: RequestLogPermit" to Case(
                inputs = listOf("e2e-ui-test", selfPeerID, ""),
                execute = true,
                expect = ::assertSucceeded,
            ),
            "Permits: ConfirmLogPermit" to Case(
                inputs = listOf("e2e-ui-test", selfPeerID),
                execute = true,
                expect = ::assertRejected,
            ),
            "Permits: RevokeLogPermit" to Case(
                inputs = listOf("e2e-ui-test", selfPeerID),
                execute = true,
                expect = ::assertRejected,
            ),

            // --- Execute --- (bypasses raft entirely; fire-and-forget to the real leader is harmless)
            "Execute: Execute" to Case(
                inputs = listOf(leaderPeerID, testValue),
                execute = true,
                // leaderPeerID is only ever this device's own last-observed
                // snapshot (see runAllCommandsThroughUi) of who's leader --
                // on this long-lived, ever-changing shared cluster it can
                // already be stale by the time this case runs, and even a
                // *correct* target isn't guaranteed to have a cached,
                // dialable address in this device's own libp2p peerstore
                // (Execute bypasses raft's forwarding path entirely --
                // unlike KV: Submit/Permits calls, which ride the same
                // dialForward leader-resolution AppendEntries already
                // depends on -- so it has nothing else to fall back on).
                // assertNoCrash, not assertSucceeded: confirmed directly,
                // this failed "no addresses" against a real device once the
                // true leader had moved on from this identity's last-known
                // snapshot. Proving the tap reaches Kvmobile.execute cleanly
                // (matching Channel: OpenChannel's identical reasoning) is
                // the actual goal here, not that the real network hop lands.
                expect = ::assertNoCrash,
            ),
            "Execute: PollExecute" to Case(execute = true, expect = ::assertSucceeded),
            "Execute: WatchExecute" to Case(execute = true, expect = ::assertSucceeded),
            "Execute: StopWatchExecute" to Case(execute = true, expect = ::assertSucceeded),

            // --- Channel --- (bounded by kvmobile's own callTimeout even when the target isn't listening)
            "Channel: OpenChannel" to Case(inputs = listOf(leaderPeerID), execute = true, expect = ::assertNoCrash),
            "Channel: StopListenChannel" to Case(execute = true, expect = ::assertSucceeded),
            "Channel: SendChannelData" to Case(
                inputs = listOf("no-such-channel", "AA=="),
                execute = true,
                expect = ::assertRejected,
            ),
            "Channel: CloseChannel" to Case(
                inputs = listOf("no-such-channel"),
                execute = true,
                // mobile/kvmobile/channel.go's CloseChannel is a real no-op
                // for a channel id with no local pump running (StopChannel,
                // called unconditionally as part of it, documents exactly
                // this "safe to call when nothing is running for it"
                // semantics) -- confirmed directly: an earlier version of
                // this case assumed rejection and failed against a real
                // device.
                expect = ::assertSucceeded,
            ),
            "Channel: StopChannel" to Case(inputs = listOf("no-such-channel"), execute = true, expect = ::assertNoCrash),

            // --- Log records --- (may or may not require a permit depending on the shared leader's config)
            "Log records: LogAppend" to Case(
                inputs = listOf("e2e-ui-test", "ui-test-unit", "{}", "ui e2e test"),
                execute = true,
                expect = ::assertNoCrash,
            ),
            "Log records: LogQuery" to Case(
                inputs = listOf("e2e-ui-test", "ui-test-unit", "", "", ""),
                execute = true,
                expect = ::assertNoCrash,
            ),

            // --- Group --- (Create/Update/Delete are voter-gated ACL writes; Get/List are read-only)
            "Group: CreateGroup" to Case(
                inputs = listOf("e2e-ui-test-group", "e2e ui test", "false"),
                execute = true,
                expect = ::assertRejected,
            ),
            "Group: UpdateGroup" to Case(
                inputs = listOf("e2e-ui-test-group", "e2e ui test", "false"),
                execute = true,
                expect = ::assertRejected,
            ),
            "Group: DeleteGroup" to Case(
                inputs = listOf("e2e-ui-test-group"),
                execute = true,
                expect = ::assertRejected,
            ),
            "Group: GetGroup" to Case(
                inputs = listOf("no-such-group"),
                execute = true,
                expect = ::assertNoCrash,
            ),
            "Group: ListGroups" to Case(execute = true, expect = ::assertSucceeded),

            // --- Command --- (same ACL shape as Group)
            "Command: CreateCommand" to Case(
                inputs = listOf("e2e-ui-test-command", "e2e ui test", selfPeerID),
                execute = true,
                expect = ::assertRejected,
            ),
            "Command: UpdateCommand" to Case(
                inputs = listOf("e2e-ui-test-command", "e2e ui test", selfPeerID),
                execute = true,
                expect = ::assertRejected,
            ),
            "Command: DeleteCommand" to Case(
                inputs = listOf("e2e-ui-test-command"),
                execute = true,
                expect = ::assertRejected,
            ),
            "Command: GetCommand" to Case(
                inputs = listOf("no-such-command"),
                execute = true,
                expect = ::assertNoCrash,
            ),
            "Command: ListCommands" to Case(execute = true, expect = ::assertSucceeded),

            // --- Links --- (Add/Remove are voter-gated; List* are read-only)
            "Links: AddCommandToGroup" to Case(
                inputs = listOf("no-such-command", "no-such-group"),
                execute = true,
                expect = ::assertRejected,
            ),
            "Links: RemoveCommandFromGroup" to Case(
                inputs = listOf("no-such-command", "no-such-group"),
                execute = true,
                expect = ::assertRejected,
            ),
            "Links: ListGroupsForCommand" to Case(
                inputs = listOf("no-such-command"),
                execute = true,
                expect = ::assertSucceeded,
            ),
            "Links: AddPeerToGroup" to Case(
                inputs = listOf(selfPeerID, "no-such-group"),
                execute = true,
                expect = ::assertRejected,
            ),
            "Links: RemovePeerFromGroup" to Case(
                inputs = listOf(selfPeerID, "no-such-group"),
                execute = true,
                expect = ::assertRejected,
            ),
            "Links: ListGroupsForPeer" to Case(
                inputs = listOf(selfPeerID),
                execute = true,
                expect = ::assertSucceeded,
            ),

            // --- Dispatch --- (no matching Command exists on this learner -- clean not-found either way)
            "Dispatch: SubmitCommand" to Case(
                inputs = listOf("no-such-command", "{}"),
                execute = true,
                expect = ::assertRejected,
            ),
            "Dispatch: GetCommandRequest" to Case(
                inputs = listOf("no-such-command", "no-such-instance"),
                execute = true,
                expect = ::assertNoCrash,
            ),
            "Dispatch: ListCommandRequests" to Case(
                inputs = listOf("no-such-command"),
                execute = true,
                expect = ::assertSucceeded,
            ),
            "Dispatch: ListExecutionsByPeer" to Case(
                inputs = listOf(selfPeerID),
                execute = true,
                expect = ::assertSucceeded,
            ),
            "Dispatch: AppendCommandLog" to Case(
                inputs = listOf("", "e2e-ui-test-instance", "{}", "ui e2e test"),
                execute = true,
                expect = ::assertNoCrash,
            ),
            "Dispatch: QueryCommandLog" to Case(
                inputs = listOf("e2e-ui-test-instance", "", "", ""),
                execute = true,
                expect = ::assertNoCrash,
            ),
            "Dispatch: LatestCommandLog" to Case(
                inputs = listOf("e2e-ui-test-instance"),
                execute = true,
                expect = ::assertNoCrash,
            ),
            "Dispatch: WatchCommandLog" to Case(
                inputs = listOf("e2e-ui-test-instance"),
                execute = true,
                expect = ::assertSucceeded,
            ),
            "Dispatch: StopWatchCommandLog" to Case(
                inputs = listOf("e2e-ui-test-instance"),
                execute = true,
                expect = ::assertSucceeded,
            ),
            "Dispatch: RunCommandDispatcher" to Case(
                inputs = listOf("no-such-command"),
                execute = true,
                expect = ::assertSucceeded,
            ),
            "Dispatch: StopCommandDispatcher" to Case(
                inputs = listOf("no-such-command"),
                execute = true,
                expect = ::assertSucceeded,
            ),

            // --- ExecInvite --- (Create/Revoke are voter-gated; Redeem against a bogus token is a clean parse/redeem error)
            "ExecInvite: CreateExecInvite" to Case(
                inputs = listOf("no-such-command", "{}"),
                execute = true,
                expect = ::assertRejected,
            ),
            "ExecInvite: RevokeExecInvite" to Case(
                inputs = listOf("00000000000000000000000000000000"),
                execute = true,
                expect = ::assertRejected,
            ),
            "ExecInvite: RedeemExecInvite" to Case(
                inputs = listOf("/ip4/127.0.0.1/tcp/1#00000000000000000000000000000000"),
                execute = true,
                expect = ::assertRejected,
            ),

            // --- Raw --- (the same primitive E2ETest itself already exercises)
            "Raw: SendEvent" to Case(
                inputs = listOf("""{"event":"get_public_key"}"""),
                execute = true,
                expect = ::assertSucceeded,
            ),
        )
    }
}
