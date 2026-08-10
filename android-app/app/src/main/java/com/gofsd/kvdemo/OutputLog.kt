package com.gofsd.kvdemo

/** Whether a log row reports a command that failed, one that succeeded, or a bare notification
 *  with no pass/fail of its own (a WatchExecute/WatchCommandLog/RunCommandDispatcher callback). */
enum class LogStatus { INFO, SUCCESS, FAILED }

/** One row in [OutputLog] -- [title] is always shown, [body] only once the row is expanded. */
data class LogEntry(
    val id: Long,
    val title: String,
    val body: String,
    val status: LogStatus,
    val timestamp: Long,
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
 * Not a general-purpose event bus: only ever written to by [buildCommands]'s
 * watch-callback closures and CommandDetailScreen's Run button, only ever
 * read by LogScreen. `@Synchronized` because kvmobile's callbacks and every
 * screen's own lifecycle calls can both land on arbitrary threads.
 */
object OutputLog {
    private val entries = mutableListOf<LogEntry>()
    private var nextId = 0L
    private var listener: ((LogEntry) -> Unit)? = null

    /** Back-compat for the many existing plain-string call sites (watch callbacks, scan-triggered
     *  results) -- recorded as a single-line INFO entry with no separately expandable body. */
    @Synchronized
    fun append(line: String) {
        record(title = line, body = "", status = LogStatus.INFO)
    }

    @Synchronized
    fun record(title: String, body: String, status: LogStatus): LogEntry {
        val entry = LogEntry(id = nextId++, title = title, body = body, status = status, timestamp = System.currentTimeMillis())
        entries += entry
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
}
