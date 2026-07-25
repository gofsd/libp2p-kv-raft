package com.gofsd.kvdemo

/**
 * Process-wide log of every notification a standing watch delivers --
 * WatchExecute/WatchCommandLog/RunCommandDispatcher's callbacks (see
 * CommandCatalog.kt) -- independent of whichever CommandDetailActivity
 * screen, if any, happens to be visible when one arrives: those daemon-
 * side subscriptions keep running regardless of what the UI is currently
 * showing, so a notification has nowhere per-screen it inherently belongs
 * to. [ActivityLogActivity] is this log's only reader, showing the full
 * history at any time plus live updates while it's the foregrounded
 * screen (see [setListener]) -- nothing delivered while you were
 * elsewhere is ever lost, just not shown until you open it.
 *
 * Not a general-purpose event bus: only ever written to by
 * [buildCommands]'s watch-callback closures, only ever read by
 * ActivityLogActivity. `@Synchronized` because kvmobile's callbacks and
 * every Activity's own lifecycle calls can both land on arbitrary
 * threads.
 */
object OutputLog {
    private val lines = mutableListOf<String>()
    private var listener: ((String) -> Unit)? = null

    @Synchronized
    fun append(line: String) {
        lines += line
        listener?.invoke(line)
    }

    @Synchronized
    fun snapshot(): List<String> = lines.toList()

    /** At most one listener at a time -- the currently foregrounded screen that cares, if any. */
    @Synchronized
    fun setListener(l: ((String) -> Unit)?) {
        listener = l
    }
}
