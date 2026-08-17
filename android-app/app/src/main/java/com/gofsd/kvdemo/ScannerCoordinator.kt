package com.gofsd.kvdemo

import android.os.Handler
import android.os.Looper
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

    private val mainHandler = Handler(Looper.getMainLooper())

    /**
     * Publishes a decoded payload to [scans] -- always from the main thread, never from the
     * camera's own decode thread that produced it.
     *
     * The caller is `ImageAnalysis`'s analyzer, which CameraX runs on a background executor, and
     * what collects [scans] is AppRoot: it shows dialogs and drives navigation, both of which are
     * main-thread-only. Emitting from the decode thread leaves the thread the collector resumes on
     * up to the collector's own dispatcher -- which is the main thread in the running app, and is
     * *not* under Compose's instrumentation test environment, where a continuation can be resumed
     * inline on whichever thread triggered it.
     *
     * That difference was not academic. Measured on the two-device optical rig: the collector body
     * ran on the camera's decode thread, `android.app.Dialog.<init>` threw "Can't create handler
     * inside thread Thread[pool-7-thread-1,5,main] that has not called Looper.prepare()", the
     * exception escaped the `collect` and killed the coroutine collecting it -- and with the only
     * collector gone, every later scan in that process reached nothing at all. Runs died at case 15
     * and case 19 of 90 this way, looking exactly like a camera that had stopped decoding.
     *
     * Posting here makes the "scans arrive on the main thread" contract the collector already
     * depends on true by construction, instead of true by coincidence of dispatchers. It also puts
     * the [lastScanned] dedup on one thread: it used to be read and written from the decode thread
     * with no synchronization at all.
     */
    fun onScanned(bytes: ByteArray) {
        if (Looper.myLooper() == Looper.getMainLooper()) {
            emitScan(bytes)
        } else {
            mainHandler.post { emitScan(bytes) }
        }
    }

    private fun emitScan(bytes: ByteArray) {
        val last = lastScanned
        if (last != null && bytes.contentEquals(last)) return
        lastScanned = bytes
        _scans.tryEmit(bytes)
    }
}
