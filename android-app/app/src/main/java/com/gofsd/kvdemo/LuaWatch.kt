package com.gofsd.kvdemo

import android.util.Log
import kvmobile.Kvmobile
import kvmobile.LogCallback
import org.json.JSONArray
import org.json.JSONObject

private const val TAG = "KVDemo"

/**
 * Turns one Lua run into live rows in the [OutputLog], by watching that instance's command log
 * from the moment it is submitted until it records a terminal entry.
 *
 * # Why this exists at all
 *
 * "Lua: Run" hands back an instance id and nothing else -- the run itself happens on whichever
 * device the command targets, and its lines are written to the replicated command log as it
 * works. Without something watching, a person who ran a script sees one row saying it was
 * submitted and then silence until they go looking. With it, a Lua command reads in the log list
 * exactly like every other command does: lines arriving as they happen.
 *
 * It is the same [kvmobile.Kvmobile.watchCommandLog] any app would use for any command -- there
 * is nothing Lua-specific about the mechanism. What is Lua-specific is only that a script writes
 * several lines rather than one, and that some of those lines name a *child* dispatch, which
 * [rowFor] surfaces in the row's own title so the chain is readable without expanding anything.
 *
 * # Stopping
 *
 * A watch is a standing poll loop on the device, so leaving one running for a finished run costs
 * something forever. This stops itself the moment a terminal entry arrives -- anything whose
 * status is not "running", which is the same rule every dispatcher in this repo uses to decide
 * whether a request still needs handling.
 *
 * Records already written before the watch starts arrive in the first callback, so a run that
 * finished quickly (or that was already running when this was called) still shows its whole log
 * rather than nothing.
 */
object LuaWatch {
    /** Instance ids already reported, so a record cannot become a second row. Defensive rather
     *  than load-bearing: kvmobile's watch tracks its own high-water mark and delivers only what
     *  is new after the first callback (which does carry everything already written). What this
     *  guards is a watch *restarted* for an instance -- the e2e rig running the same case twice,
     *  say -- which starts again from the beginning of that instance's log. */
    private val seen = mutableMapOf<String, MutableSet<String>>()

    /**
     * Starts watching [instanceID], attributing rows to [commandID]. Safe to call for an
     * instance already being watched: kvmobile replaces the existing watch rather than stacking
     * a second one.
     */
    @Synchronized
    fun start(commandID: String, instanceID: String) {
        if (instanceID.isBlank()) return
        seen[instanceID] = mutableSetOf()
        Log.i(TAG, "AUTO: watching Lua run $commandID/$instanceID for live log lines")
        runCatching {
            Kvmobile.watchCommandLog(instanceID, object : LogCallback {
                override fun onRecords(recordsJSON: String) {
                    onRecords(commandID, instanceID, recordsJSON)
                }
            })
        }.onFailure {
            Log.w(TAG, "RESULT: watching $instanceID failed: ${it.message}")
            OutputLog.append("Lua[$commandID] could not watch $instanceID: ${it.message}")
        }
    }

    /** Records one callback's worth of entries, and stops the watch once the run is over. */
    @Synchronized
    private fun onRecords(commandID: String, instanceID: String, recordsJSON: String) {
        val already = seen.getOrPut(instanceID) { mutableSetOf() }
        var finished = false

        runCatching {
            val arr = JSONArray(recordsJSON)
            for (i in 0 until arr.length()) {
                val record = arr.getJSONObject(i)
                // A record has no id of its own, so its timestamp plus its text is what
                // distinguishes it -- enough, since a run writes its lines one at a time.
                val key = record.optString("timestamp") + "|" + record.optString("narrative")
                if (!already.add(key)) continue

                val (title, status) = rowFor(commandID, record)
                OutputLog.record(title = title, body = record.toString(), status = status)
                if (status != LogStatus.INFO) finished = true
            }
        }.onFailure {
            Log.w(TAG, "RESULT: could not read records for $instanceID: ${it.message}")
        }

        if (finished) {
            Log.i(TAG, "AUTO: Lua run $commandID/$instanceID finished -- stopping its watch")
            runCatching { Kvmobile.stopWatchCommandLog(instanceID) }
            seen.remove(instanceID)
        }
    }

    /** [rowFrom]'s JSON adapter -- pulls the four things a row is built from out of one record. */
    fun rowFor(commandID: String, record: JSONObject): Pair<String, LogStatus> {
        val fields = record.optJSONObject("fields") ?: JSONObject()
        return rowFrom(
            commandID = commandID,
            status = fields.optString("status"),
            narrative = record.optString("narrative"),
            childCommand = fields.optString("child_command"),
            childInstance = fields.optString("child_instance"),
        )
    }

    /**
     * The row one record becomes: what a person reads in the log list without expanding it.
     *
     * A line that dispatched (or reported on) a child command names that child inline, because
     * "this run caused that run" is the thing a Lua chain is hard to follow without -- the
     * child's execution id is what you would otherwise have to open the row and read JSON to
     * find.
     *
     * A missing status means terminal, not unknown: that is the same rule every dispatcher in
     * this repo uses (anything but "running" is a final result), and treating it as still-running
     * here would leave a watch polling forever for a run that had already finished.
     *
     * Plain strings rather than the record itself so this is testable off-device -- Android's
     * org.json is a stub in JVM unit tests.
     */
    fun rowFrom(
        commandID: String,
        status: String,
        narrative: String,
        childCommand: String,
        childInstance: String,
    ): Pair<String, LogStatus> {
        val logStatus = when (status) {
            "running" -> LogStatus.INFO
            "error" -> LogStatus.FAILED
            else -> LogStatus.SUCCESS
        }
        val suffix = when {
            childInstance.isBlank() -> ""
            childCommand.isBlank() -> "  [child $childInstance]"
            else -> "  [child $childCommand/$childInstance]"
        }
        return "Lua[$commandID] $narrative$suffix" to logStatus
    }
}
