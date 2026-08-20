package com.gofsd.kvdemo

import org.junit.Assert.assertEquals
import org.junit.Test

/**
 * [LuaWatch.rowFrom] decides what a person sees in the log list for one line of a Lua run, and
 * whether the run is over -- the second of which also decides when the watch stops polling. Both
 * are worth pinning: a wrong status either leaves a finished run watched forever or stops a
 * running one from reporting the rest of its lines.
 */
class LuaWatchTest {
    @Test
    fun `a running line is neutral and stops nothing`() {
        val (title, status) = LuaWatch.rowFrom("outer", "running", "hello from outer begin", "", "")
        assertEquals("Lua[outer] hello from outer begin", title)
        assertEquals(LogStatus.INFO, status)
    }

    @Test
    fun `a line naming a child shows the child inline`() {
        val (title, status) = LuaWatch.rowFrom(
            commandID = "outer",
            status = "running",
            narrative = "submitted inner as abc123",
            childCommand = "inner",
            childInstance = "abc123",
        )
        assertEquals("Lua[outer] submitted inner as abc123  [child inner/abc123]", title)
        assertEquals(LogStatus.INFO, status)
    }

    @Test
    fun `a child instance with no command name still shows`() {
        val (title, _) = LuaWatch.rowFrom("outer", "running", "inner finished", "", "abc123")
        assertEquals("Lua[outer] inner finished  [child abc123]", title)
    }

    @Test
    fun `a successful result reads as success`() {
        val (title, status) = LuaWatch.rowFrom("outer", "ok", "hello from outer end", "", "")
        assertEquals("Lua[outer] hello from outer end", title)
        assertEquals(LogStatus.SUCCESS, status)
    }

    @Test
    fun `a failed result reads as failure`() {
        val (_, status) = LuaWatch.rowFrom("outer", "error", "hello from outer failed: nope", "", "")
        assertEquals(LogStatus.FAILED, status)
    }

    // A handler written before progress reporting existed records no status at all, and every
    // dispatcher in this repo treats that as terminal. Reading it as still-running here would
    // leave the watch polling for a run that already finished.
    @Test
    fun `no status at all counts as finished`() {
        val (_, status) = LuaWatch.rowFrom("outer", "", "done, somehow", "", "")
        assertEquals(LogStatus.SUCCESS, status)
    }
}
