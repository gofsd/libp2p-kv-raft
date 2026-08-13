package com.gofsd.kvdemo

import android.util.Log
import java.io.File
import org.json.JSONArray
import org.json.JSONObject

/** Whether a log row reports a command that failed, one that succeeded, or a bare notification
 *  with no pass/fail of its own (a WatchExecute/WatchCommandLog/RunCommandDispatcher callback). */
enum class LogStatus { INFO, SUCCESS, FAILED }

/**
 * One row in [OutputLog] -- [title] is always shown, [body] only once the row is expanded.
 * [category]/[name]/[args] identify the [CommandSpec] this entry came from (blank/empty for
 * watch-callback notifications and other non-command entries, e.g. "Recruited") -- LogScreen's
 * row action buttons (Repeat, Group QR, Group) only render once these are populated.
 */
data class LogEntry(
    val id: Long,
    val title: String,
    val body: String,
    val status: LogStatus,
    val timestamp: Long,
    val category: String = "",
    val name: String = "",
    val args: List<String> = emptyList(),
)

/**
 * Process-wide log of every notification a standing watch delivers --
 * WatchExecute/WatchCommandLog/RunCommandDispatcher's callbacks (see
 * CommandCatalog.kt) -- plus, since every CommandDetailScreen Run now also
 * records here (see that screen's own doc comment), every command actually
 * executed anywhere in the app. Independent of whichever screen, if any,
 * happens to be visible when an entry is recorded: those daemon-side
 * subscriptions keep running regardless of what the UI is currently
 * showing, so a notification has nowhere per-screen it inherently belongs
 * to. [LogScreen] is this log's only reader, showing the full history at
 * any time plus live updates while it's the foregrounded screen (see
 * [setListener]) -- nothing recorded while you were elsewhere is ever lost,
 * just not shown until you open it.
 *
 * Persisted to `<dataDir>/output_log.json` (see [init]), not just held in
 * memory: a plain in-memory list used to make that "nothing... is ever
 * lost" doc comment above false the moment the process that recorded an
 * entry exited -- confirmed live running this project's own real-camera
 * optical-scan e2e harness (pkg/e2erun/android_optical.go), where device
 * A's RunCommandDispatcher registration and the resulting "Dispatching
 * ..." entry both live in generateAndHold's own instrumentation process,
 * which has already exited by the time a separate verifyLogContains
 * invocation checks for it -- that process's own [snapshot] was always
 * empty regardless of whether the dispatch genuinely happened. A real
 * human relaunching the app between "something dispatched in the
 * background" and "I opened Activity Log to check" hits the identical gap.
 *
 * Not a general-purpose event bus: only ever written to by [buildCommands]'s
 * watch-callback closures and CommandDetailScreen's Run button, only ever
 * read by LogScreen. `@Synchronized` because kvmobile's callbacks and every
 * screen's own lifecycle calls can both land on arbitrary threads.
 */
object OutputLog {
    private val entries = mutableListOf<LogEntry>()
    private var nextId = 0L
    private var listener: ((LogEntry) -> Unit)? = null
    private var logFile: File? = null
    private var initialized = false

    /**
     * Points this log at `<dataDir>/output_log.json` and loads whatever entries an earlier
     * process already left there -- AppRoot's own root LaunchedEffect(Unit) calls this once per
     * process, right alongside Kvmobile.start(context.filesDir.absolutePath), before anything
     * could possibly call [record]. Idempotent past the first call (repeat calls, even with a
     * different dataDir, are silent no-ops) so nothing needs to guard against calling it twice.
     */
    @Synchronized
    fun init(dataDir: String) {
        if (initialized) return
        initialized = true
        val file = File(dataDir, "output_log.json")
        logFile = file
        if (!file.exists()) return
        runCatching {
            val arr = JSONArray(file.readText())
            for (i in 0 until arr.length()) {
                val o = arr.getJSONObject(i)
                entries += LogEntry(
                    id = o.getLong("id"),
                    title = o.getString("title"),
                    body = o.optString("body"),
                    status = LogStatus.valueOf(o.getString("status")),
                    timestamp = o.getLong("timestamp"),
                    category = o.optString("category"),
                    name = o.optString("name"),
                    args = o.optJSONArray("args")?.let { a -> (0 until a.length()).map { a.getString(it) } } ?: emptyList(),
                )
            }
            nextId = (entries.maxOfOrNull { it.id } ?: -1L) + 1
        }.onFailure { Log.w("KVDemo", "OutputLog: failed to load $file: ${it.message}") }
    }

    /** Back-compat for the many existing plain-string call sites (watch callbacks, scan-triggered
     *  results) -- recorded as a single-line INFO entry with no separately expandable body. */
    @Synchronized
    fun append(line: String) {
        record(title = line, body = "", status = LogStatus.INFO)
    }

    @Synchronized
    fun record(
        title: String,
        body: String,
        status: LogStatus,
        category: String = "",
        name: String = "",
        args: List<String> = emptyList(),
    ): LogEntry {
        val entry = LogEntry(
            id = nextId++,
            title = title,
            body = body,
            status = status,
            timestamp = System.currentTimeMillis(),
            category = category,
            name = name,
            args = args,
        )
        entries += entry
        persist()
        listener?.invoke(entry)
        return entry
    }

    @Synchronized
    fun snapshot(): List<LogEntry> = entries.toList()

    /** At most one listener at a time -- the currently foregrounded screen that cares, if any. */
    @Synchronized
    fun setListener(l: ((LogEntry) -> Unit)?) {
        listener = l
    }

    private fun persist() {
        val file = logFile ?: return
        runCatching {
            val arr = JSONArray()
            for (e in entries) {
                val argsArr = JSONArray()
                for (a in e.args) argsArr.put(a)
                arr.put(
                    JSONObject()
                        .put("id", e.id)
                        .put("title", e.title)
                        .put("body", e.body)
                        .put("status", e.status.name)
                        .put("timestamp", e.timestamp)
                        .put("category", e.category)
                        .put("name", e.name)
                        .put("args", argsArr),
                )
            }
            file.writeText(arr.toString())
        }.onFailure { Log.w("KVDemo", "OutputLog: failed to persist to $file: ${it.message}") }
    }
}
