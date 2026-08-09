package com.gofsd.kvdemo

import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.setValue
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.SharedFlow

/**
 * App-scoped home for the one persistent scanner's state, so it survives
 * navigating between screens instead of being recreated per-screen (see
 * [ScannerHost], mounted exactly once above the NavHost in AppRoot --
 * ported from object-history-app's identically-named/-shaped coordinator,
 * adapted to carry decoded [ByteArray] payloads instead of plain text,
 * since a scanned code here is capnp-encoded binary, not a human-typed
 * string -- see [DataMatrixCodec] for the ISO-8859-1 round trip that makes
 * that possible through ZXing's String-only reader/writer API).
 * [expanded] is plain Compose state -- readable/settable both by the
 * widget itself (tap to toggle) and by outside code.
 */
object ScannerCoordinator {
    var expanded by mutableStateOf(false)

    private val _scans = MutableSharedFlow<ByteArray>(replay = 0, extraBufferCapacity = 1)
    val scans: SharedFlow<ByteArray> = _scans

    // The camera analyzer decodes every frame independently, so the same
    // DataMatrix held in front of the camera for even a fraction of a
    // second yields several identical decodes in a row -- without this,
    // each one would re-trigger its own confirmation dialog. Only a
    // *change* in decoded bytes should re-emit.
    private var lastScanned: ByteArray? = null

    fun onScanned(bytes: ByteArray) {
        val last = lastScanned
        if (last != null && bytes.contentEquals(last)) return
        lastScanned = bytes
        _scans.tryEmit(bytes)
    }
}
