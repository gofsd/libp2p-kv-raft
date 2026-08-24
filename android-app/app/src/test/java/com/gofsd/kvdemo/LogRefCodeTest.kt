package com.gofsd.kvdemo

import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertSame
import org.junit.Test

/**
 * Covers the decision a scanned log reference turns on -- "does this belong to the form already
 * open?" -- and the state holder behind it. Runs as a plain JVM test (`./gradlew test`), which is
 * the whole reason [logRefTarget] is a pure function over two values rather than logic inlined in
 * AppRoot's scan collector: the behaviour this feature exists for is a *sequence* of scans, and
 * proving that a second label re-fills a form instead of restarting it should not require a camera,
 * a cluster and two phones.
 *
 * Encoding/decoding the code itself is pkg/logref's (and its Go tests'); resolving an id to a group
 * is kvmobile's. Neither is reachable from here -- both live behind the gomobile binding -- and
 * neither needs to be: what is left on this side is exactly this decision.
 */
class LogRefCodeTest {
    @Test
    fun scanWithNoFormOpenEntersTheGroup() {
        val target = logRefTarget(LogRefCode(4417, "Log records"), openFormCategory = null)
        assertEquals(LogRefTarget.EnterGroup("Log records"), target)
    }

    @Test
    fun scanWithSameGroupFormOpenRefillsItInPlace() {
        val target = logRefTarget(LogRefCode(4418, "Log records"), openFormCategory = "Log records")
        assertSame(LogRefTarget.RefillOpenForm, target)
    }

    /**
     * The person has turned to a different kind of thing, so the commands that apply to it are
     * what they need -- leaving the old form standing with a new id in it would be the one
     * genuinely wrong outcome here, since that form's command does not apply to this object.
     */
    @Test
    fun scanWithDifferentGroupFormOpenLeavesThatForm() {
        val target = logRefTarget(LogRefCode(9002, "KV"), openFormCategory = "Log records")
        assertEquals(LogRefTarget.EnterGroup("KV"), target)
    }

    /** The whole run, in the order a person actually performs it. */
    @Test
    fun aRunOfLabelsEntersOnceAndRefillsAfterwards() {
        var openForm: String? = null
        val targets = listOf(4417L, 4418L, 4419L).map { id ->
            val target = logRefTarget(LogRefCode(id, "Log records"), openForm)
            // Entering a group is followed by the person tapping a command, which is what puts a
            // form on screen for the scans that follow.
            if (target is LogRefTarget.EnterGroup) openForm = target.group
            target
        }
        assertEquals(LogRefTarget.EnterGroup("Log records"), targets[0])
        assertSame(LogRefTarget.RefillOpenForm, targets[1])
        assertSame(LogRefTarget.RefillOpenForm, targets[2])
    }

    @Test
    fun clearUnlessDropsAReferenceFromAnotherGroup() {
        ScannedLogRef.current = LogRefCode(4417, "Log records")
        ScannedLogRef.clearUnless("Log records")
        assertEquals(LogRefCode(4417, "Log records"), ScannedLogRef.current)

        ScannedLogRef.clearUnless("KV")
        assertNull(ScannedLogRef.current)
    }

    /** Leaving a group entirely (the context bar's X, which passes DEFAULT_GROUP) drops it too. */
    @Test
    fun clearUnlessDropsAReferenceOnLeavingTheGroup() {
        ScannedLogRef.current = LogRefCode(4417, "Log records")
        ScannedLogRef.clearUnless(DEFAULT_GROUP)
        assertNull(ScannedLogRef.current)
    }
}
