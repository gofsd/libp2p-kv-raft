package com.gofsd.kvdemo

import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.setValue

/**
 * A scanned *log reference* -- api/logref.capnp's Data Matrix code, decoded by
 * [kvmobile.Kvmobile.decodeLogRef] and resolved to a group by [kvmobile.Kvmobile.logRefGroup].
 *
 * The other three codes this app scans all tell the device to *do* something: a [RunCode] runs a
 * command, a [NavCode] opens a screen, a `join_request_ticket` admits a device. A log reference
 * says what the person is *looking at* -- a crate, an order, a line in a book -- and leaves the
 * doing to them.
 *
 * That difference is what the whole flow around it is shaped by. Scanning one enters the group its
 * record declares, so the commands that apply to that kind of thing are what the pager shows; the
 * person picks one, and [logId] is already in the form (see [CommandSpec.logIdParam]). Scanning
 * the *next* label with that form still open re-fills it in place rather than starting over --
 * that is [LogRefTarget.RefillOpenForm], and it is the reason this feature exists: a run of twenty
 * objects should cost twenty scans and one tap, not twenty scans and sixty taps.
 */
data class LogRefCode(val logId: Long, val group: String)

/**
 * What scanning a [LogRefCode] should do, given whichever command form is open at the time.
 *
 * Kept as a pure function ([logRefTarget]) over two plain values rather than folded into AppRoot's
 * scan collector, because the interesting half of this feature is a decision -- "same group or
 * not" -- and a decision worth a unit test should not need a camera, a cluster and two devices to
 * exercise (see LogRefCodeTest). AppRoot only carries it out.
 */
sealed class LogRefTarget {
    /**
     * The open form already belongs to the scanned id's group: leave the screen exactly where it
     * is and let the form re-fill its own log-id field from [ScannedLogRef]. No navigation, no
     * dialog, nothing for the person to confirm -- they are holding a phone over the next label,
     * and a prompt in that moment is the thing this feature removes.
     */
    object RefillOpenForm : LogRefTarget()

    /**
     * Enter [group]'s command list. Covers both "no form is open" (the ordinary first scan of a
     * run) and "a form is open but for a different group" -- the second one deliberately leaves
     * that form: the person is now looking at a different kind of thing, so the commands that
     * apply to it are what they need, not the ones that applied to the last one.
     */
    data class EnterGroup(val group: String) : LogRefTarget()
}

/**
 * [openFormCategory] is the CommandCatalog.kt category of the [CommandDetailScreen] currently on
 * top of the back stack, or null when the pager (or anything else) is showing.
 */
fun logRefTarget(code: LogRefCode, openFormCategory: String?): LogRefTarget =
    if (openFormCategory != null && openFormCategory == code.group) {
        LogRefTarget.RefillOpenForm
    } else {
        LogRefTarget.EnterGroup(code.group)
    }

/**
 * The log reference in force -- the object the device currently considers itself to be looking at,
 * set by AppRoot's scan dispatch and read by [CommandDetailScreen] to fill its log-id field.
 *
 * App-scoped state rather than a nav-route argument, for the same reason [ScannerCoordinator] is:
 * a scan can land while any screen is showing, and the screen that has to react to it is often one
 * that is *already composed*. Re-navigating to CommandDetailScreen with a new argument would
 * recreate it and throw away every other field the person had already typed, which for a form
 * whose whole point is that only one field changes per scan is exactly the wrong behaviour.
 * Compose state makes the re-fill an ordinary recomposition of the open screen instead.
 */
object ScannedLogRef {
    var current by mutableStateOf<LogRefCode?>(null)

    /**
     * Clears the reference when the device stops looking at that kind of thing -- called when the
     * group changes to anything but the reference's own (leaving a group via the context bar's X,
     * or entering an unrelated one). Without it a stale id would silently seed a form belonging to
     * a group the person navigated to by hand, minutes later, which reads as the app inventing an
     * input.
     */
    fun clearUnless(group: String) {
        if (current?.group != group) current = null
    }
}
