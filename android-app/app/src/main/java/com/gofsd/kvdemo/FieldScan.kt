package com.gofsd.kvdemo

import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.setValue

/**
 * The one form field, if any, currently waiting to be filled by the next scan -- object-history-app's
 * `CommandsViewModel.awaitFieldScan`/`scannedFieldUpdate` pair, ported onto this app's
 * [CommandDetailScreen].
 *
 * App-scoped state rather than a nav argument or screen-local state, for exactly the reason
 * [ScannedLogRef] is (see its doc comment): the scan arrives at AppRoot's collector, and the screen
 * that has to react to it is *already composed* with a form the person has half-filled. Re-navigating
 * with a new argument would throw all of that away.
 *
 * Two fields, not one, and the split matters:
 *
 *  - [armedIndex] is a *mode*. While it is non-null, AppRoot's scan collector diverts the very next
 *    scan into this field instead of dispatching it as a [RunCode]/[NavCode]/log reference/ticket
 *    (see AppRoot's first `onEach` branch). It also tints the armed field's own scan button, which is
 *    the only thing telling the person their next scan will not run anything.
 *  - [scanned] is a consumable *event*. It is keyed on the pair itself so that scanning two different
 *    codes into the same field in a row -- correcting a mis-scan, which is the common case -- applies
 *    both, rather than the second being deduped away as "no state change".
 *
 * The diversion is one scan long: the collector clears [armedIndex] as it consumes it, so a person
 * who arms a field and then walks away cannot leave the app in a state where ordinary scan-to-run has
 * silently stopped working. [disarm] covers the other exit -- leaving the form entirely.
 */
object PendingFieldScan {
    /** Which of the open form's params awaits the next scan, or null when none does. */
    var armedIndex by mutableStateOf<Int?>(null)

    /** A one-shot (param index, scanned text) for the open form to apply, then clear. */
    var scanned by mutableStateOf<Pair<Int, String>?>(null)

    /**
     * Forgets both -- called from [CommandDetailScreen]'s `onDispose`. Whatever field a form armed
     * is meaningless once that form is gone, and without this the *next* scan on whatever screen
     * happens to be showing would be swallowed into a param nobody can see.
     */
    fun disarm() {
        armedIndex = null
        scanned = null
    }
}
