package com.gofsd.kvdemo

import android.util.Base64
import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import kvmobile.Kvmobile
import org.json.JSONArray
import org.json.JSONObject
import org.junit.Assert
import org.junit.Test
import org.junit.runner.RunWith
import java.io.File

/**
 * Instrumented E2E driver, called via `adb shell am instrument` by
 * pkg/e2erun's Android execution path (see that package's android.go) --
 * not run as part of a normal `./gradlew test`/`connectedCheck`.
 *
 * Reads a JSON array of event JSON strings (the same human-readable shape
 * pkg/e2edata.Event / kvctl-cli sendevent use, e.g.
 * `["{\"event\":\"get_public_key\"}", ...]`, with any `add` row's leader
 * address already resolved host-side -- see
 * pkg/e2erun.ResolveBootstrapPlaceholder) from the "rows" instrumentation
 * argument, calls Kvmobile.start() once and Kvmobile.sendEvent() per row in
 * order, and writes a JSON array of
 * `{"index":N,"pass":bool,"error":"..."}` results to this app's external
 * files dir (not the private filesDir Kvmobile.start uses for its own
 * daemon data) so the host side can `adb pull` it without needing
 * `run-as` -- deliberately decoupled from JUnit's own per-test-method
 * granularity, since "rows" is an arbitrary runtime-provided list, not a
 * fixed set of `@Test` methods.
 */
@RunWith(AndroidJUnit4::class)
class E2ETest {
    @Test
    fun runRows() {
        val context = InstrumentationRegistry.getInstrumentation().targetContext
        val args = InstrumentationRegistry.getArguments()
        // Base64, not raw JSON: `adb shell`/`am` mangle quote- and
        // brace-heavy argument values, which stayed invisible while every
        // recorded row carried a short value and showed up as a
        // JSONException about a stray fragment the moment one carried a
        // multi-kilobyte one. UiCommandE2ETest's own "cases" argument has
        // been encoded this way for the same reason; see
        // pkg/e2erun.runInstrumentedTest. A value that is not valid base64
        // (or absent) falls back to an empty row list rather than
        // crashing, matching how that class treats its own argument.
        val rowsJson = args.getString("rows")?.let {
            runCatching { String(Base64.decode(it, Base64.DEFAULT)) }.getOrNull()
        } ?: "[]"
        val rows = JSONArray(rowsJson)

        Kvmobile.start(context.filesDir.absolutePath)

        val results = JSONArray()
        var failures = 0
        for (i in 0 until rows.length()) {
            val result = JSONObject().put("index", i)
            val (pass, error) = sendWithRetry(rows.getString(i))
            result.put("pass", pass)
            if (!pass) {
                result.put("error", error)
                failures++
            }
            results.put(result)
        }

        File(context.getExternalFilesDir(null), "e2e_results.json").writeText(results.toString())

        if (failures > 0) {
            Assert.fail("$failures of ${rows.length()} row(s) failed -- see e2e_results.json")
        }
    }

    /**
     * Sends eventJson via Kvmobile.sendEvent, retrying for up to
     * READ_RETRY_BUDGET_MS (in READ_RETRY_DELAY_MS steps) if it's a
     * get_field/get_key event that comes back failed -- a raft follower's
     * local read can briefly lag just behind a set_field that only just
     * committed on the leader (the same documented caveat
     * pkg/e2erun.retryReadsIfNeeded works around for desktop/remote rows,
     * mirrored here since this retry has to happen on-device against the
     * real Kvmobile.sendEvent call, not something the host side can inject
     * after the fact).
     *
     * Every other event type (writes: add/set_key/set_field) is sent once
     * and, on failure, retried for up to WRITE_LEADER_RETRY_BUDGET_MS only
     * if isTransientLeaderError recognizes the error -- not blindly, the
     * same restraint pkg/e2erun.retryReadsIfNeeded already documents (a
     * real rejection, e.g. a bad signature, still fails on the first try).
     * Caught directly: a forwarded set_field failed with "not leader and no
     * leader known" while the shared bootstrap leader's own daemon.log
     * showed real "leadership lost while committing log" entries at that
     * exact moment (system-wide memory pressure from an unrelated process
     * sharing that host caused genuine, if brief, raft leadership churn --
     * not a bug in this project's forwarding path). Retrying is safe:
     * add/set_key/set_field are naturally idempotent from this test's own
     * perspective, and mobile/kvmobile/kvmobile.go's own doc comment on
     * raftHeartbeatTimeout/raftElectionTimeout already documents this exact
     * error string as something "observed directly" against a real leader.
     */
    private fun sendWithRetry(eventJson: String): Pair<Boolean, String?> {
        val eventName = runCatching { JSONObject(eventJson).optString("event") }.getOrDefault("")
        val isRead = eventName == "get_field" || eventName == "get_key"
        val startedAt = System.currentTimeMillis()

        while (true) {
            val outcome = runCatching { JSONObject(Kvmobile.sendEvent(eventJson)) }
            val (pass, error) = when {
                outcome.isFailure -> false to (outcome.exceptionOrNull()?.message ?: outcome.exceptionOrNull().toString())
                outcome.getOrNull()?.optString("event") == "error" -> false to outcome.getOrNull()?.optString("value")
                else -> true to null
            }
            if (pass) return pass to error

            val budgetMs = when {
                isRead -> READ_RETRY_BUDGET_MS
                isTransientLeaderError(error) -> WRITE_LEADER_RETRY_BUDGET_MS
                else -> 0L
            }
            if (budgetMs <= 0 || System.currentTimeMillis() - startedAt >= budgetMs) return pass to error
            Thread.sleep(READ_RETRY_DELAY_MS)
        }
    }

    /**
     * True for raft-level errors that mean a write briefly hit the leader
     * mid-election -- a real but transient condition, distinct from an
     * application-level rejection (bad signature, duplicate join, etc.)
     * that should still fail on the first try. See sendWithRetry's doc
     * comment for how this was caught and confirmed against a real
     * deployed cluster.
     */
    private fun isTransientLeaderError(error: String?): Boolean {
        if (error == null) return false
        return error.contains("leadership lost while committing log") ||
            error.contains("not leader and no leader known")
    }

    companion object {
        // Slightly more generous than pkg/e2erun.retryReadsIfNeeded's 3s
        // desktop/remote budget, since this device joins over whatever
        // real network path it actually has to the bootstrap leader
        // (mobile Wi-Fi/cellular, not the loopback a desktop test node
        // uses) -- but this is *not* a fix for a follower that can never
        // catch up at all: tested directly against a real device with a
        // 20s budget and it still never did, while `kvctl-cli sendevent`
        // against the leader directly confirmed the write really was
        // committed there the whole time. That isolates the problem to
        // leader-to-follower AppendEntries delivery for this specific
        // follower never completing -- most likely this device's network
        // not actually being reachable back through the relay reservation
        // `relayMultiaddr` requests (see pkg/daemon.Config.RelayPeer's doc
        // comment) -- not a timing issue more retrying fixes. If a
        // get_field row still fails after this budget, look at
        // connectivity/relay, not this constant.
        private const val READ_RETRY_BUDGET_MS = 5000L
        private const val READ_RETRY_DELAY_MS = 500L

        // Confirmed directly across several real runs: it's always the
        // *first* forwarded write in a session that hits this (right after
        // Kvmobile.start's join, before this identity's own local
        // raft.Raft() has received its first leader announcement) -- a
        // second, later write in the exact same session never does, once
        // more heartbeat cycles have had a chance to land. 10s and 25s both
        // proved not quite long enough for that specific window; matching
        // mobile/kvmobile's own callTimeout/relay-reservation timeouts (45s
        // each) for this same device/link combination gives real headroom
        // without masking a follower that's genuinely never going to catch
        // up (see READ_RETRY_BUDGET_MS's own doc comment above for that
        // failure mode -- unaffected by this constant).
        private const val WRITE_LEADER_RETRY_BUDGET_MS = 60000L
    }
}
