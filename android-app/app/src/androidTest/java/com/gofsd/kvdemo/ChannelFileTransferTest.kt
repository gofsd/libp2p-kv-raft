package com.gofsd.kvdemo

import android.content.Context
import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import kvmobile.ChannelCallback
import kvmobile.Kvmobile
import org.json.JSONObject
import org.junit.Assert
import org.junit.Test
import org.junit.runner.RunWith
import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.security.MessageDigest
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

/**
 * Instrumented driver for pkg/e2erun's desktop<->android Channel bulk-
 * transfer scenario -- unlike E2ETest/UiCommandE2ETest (raw event replay
 * and generic catalog-UI walk respectively), this calls
 * Kvmobile.openChannel/sendChannelData/closeChannelWrite/closeChannel
 * directly in a tight loop, since streaming a multi-hundred-megabyte file
 * through CommandDetailActivity's one-EditText-per-tap UI (what
 * UiCommandE2ETest's generic walk uses) is neither practical nor what a
 * real sender does -- this is Kvmobile's actual app-facing API, the same
 * one CommandCatalog.kt's Channel commands wrap for a human tapping
 * through the UI, just driven from code instead of taps, exactly the same
 * relationship E2ETest itself has to sendEvent.
 *
 * This device is always the one that dials out (Kvmobile.openChannel(peerID,
 * ...), never ListenChannel), and it does so exactly once, for *both*
 * directions of the transfer -- not because android -> desktop couldn't
 * open its own separate channel in principle, but because desktop has no
 * way to dial *this* device back: pkg/daemon.dispatchChannelOpen resolves a
 * bare peer id through this node's own libp2p peerstore, which -- confirmed
 * live, driving an earlier two-invocation version of this same scenario --
 * has no address for a peer that only ever *dialed in* (the join this
 * device's own Kvmobile.start performs to reach desktop in the first place
 * populates *this* device's peerstore with desktop's address, not the
 * other way around; there is also no "connect to this explicit address"
 * primitive in pkg/kvctl/mobile/kvmobile independent of a raft join/
 * Channel dial to inject one). So instead: this device opens one channel to
 * desktop, desktop sends its file over it first (this device's own
 * onData/onClosed below receives it), then -- once desktop's own
 * CloseChannelWrite half-close is observed (onClosed fires with no reason,
 * the exact same "peer finished their own direction, this direction stays
 * open" signal TestChannelCloseWriteLeavesOtherDirectionOpen and
 * pkg/kvctl.pumpChannel already rely on) -- this device streams its own
 * generated payload back over that *same* channel and calls
 * CloseChannelWrite itself, then CloseChannel once both directions are
 * done. See pkg/e2erun/android_channel_transfer.go's own doc comment for
 * the host (desktop) side of this same sequence.
 *
 * Called via `adb shell am instrument -e class
 * com.gofsd.kvdemo.ChannelFileTransferTest ...` by that Go file, never as
 * part of a normal `./gradlew test`/`connectedCheck` run.
 *
 * Instrumentation args:
 *   peerID        desktop's peer id to dial (required)
 *   sizeBytes     exact payload size, decimal, same for both directions
 *                 (required)
 *   expectedHash  lowercase hex SHA-256 both directions' payload must
 *                 match -- safe to share between directions since the
 *                 payload is a pure function of size (see
 *                 writeDeterministicFile) (required)
 *   timeoutSeconds  how long to wait for desktop's own direction to finish
 *                    before giving up (optional, default 600)
 *
 * The payload itself is never pushed/pulled over adb: this device derives
 * its own outgoing copy from the same deterministic, position-only formula
 * the host used to compute expectedHash in the first place (byte(i % 256),
 * see fillPatternBuffer).
 *
 * Writes a JSON result object ({"sizeBytes","pass",["error"],
 * "receivedHash","sourceHash"} -- see transfer's own body) to this app's
 * external files dir as "channel_transfer_result.json" (same "external, not
 * private, so the host can adb pull without run-as" reasoning E2ETest.kt's
 * own doc comment gives).
 *
 * Every file this test itself creates (the generated outgoing payload, the
 * received copy) is deleted in a `finally` block before returning, pass or
 * fail.
 */
@RunWith(AndroidJUnit4::class)
class ChannelFileTransferTest {

    @Test
    fun transfer() {
        val context = InstrumentationRegistry.getInstrumentation().targetContext
        val args = InstrumentationRegistry.getArguments()
        val peerID = args.getString("peerID")
            ?: throw IllegalArgumentException("peerID instrumentation arg required")
        val sizeBytes = (args.getString("sizeBytes")
            ?: throw IllegalArgumentException("sizeBytes instrumentation arg required")).toLong()
        val expectedHash = args.getString("expectedHash")
            ?: throw IllegalArgumentException("expectedHash instrumentation arg required")
        val timeoutSeconds = (args.getString("timeoutSeconds") ?: "600").toLong()

        val resultFile = File(context.getExternalFilesDir(null), "channel_transfer_result.json")
        resultFile.delete()

        val result = JSONObject().put("sizeBytes", sizeBytes)
        try {
            Kvmobile.start(context.filesDir.absolutePath)
            runDuplexTransfer(context, peerID, sizeBytes, expectedHash, timeoutSeconds, result)
            result.put("pass", true)
        } catch (t: Throwable) {
            result.put("pass", false).put("error", t.message ?: t.toString())
        } finally {
            resultFile.writeText(result.toString())
        }

        if (!result.optBoolean("pass")) {
            Assert.fail(result.optString("error"))
        }
    }

    /**
     * Opens one channel to peerID and drives both directions over it in
     * sequence -- see this class's own doc comment for why one channel,
     * dialed only ever by this device, covers both.
     */
    private fun runDuplexTransfer(
        context: Context,
        peerID: String,
        sizeBytes: Long,
        expectedHash: String,
        timeoutSeconds: Long,
        result: JSONObject,
    ) {
        val recvFile = File(context.filesDir, "channel_transfer_recv.bin")
        recvFile.delete()
        val digest = MessageDigest.getInstance("SHA-256")
        val out = FileOutputStream(recvFile)
        val received = AtomicLong(0)
        val closedReason = AtomicReference<String>(null)
        val onDataFailure = AtomicReference<Throwable>(null)
        // Desktop's own CloseChannelWrite (its direction done) makes this
        // device's onClosed fire with an empty reason -- that is the
        // signal desktop's own SendChannel/receive direction has finished,
        // not that the whole channel ended (see
        // TestChannelCloseWriteLeavesOtherDirectionOpen's identical
        // desktop-side property, which this mirrors from the mobile side).
        val desktopDone = CountDownLatch(1)

        val cb = object : ChannelCallback {
            override fun onData(purpose: String, chunk: ByteArray) {
                try {
                    synchronized(digest) {
                        digest.update(chunk)
                        out.write(chunk)
                    }
                    received.addAndGet(chunk.size.toLong())
                } catch (t: Throwable) {
                    onDataFailure.compareAndSet(null, t)
                }
            }

            override fun onClosed(reason: String) {
                closedReason.set(reason)
                desktopDone.countDown()
            }
        }

        val channelID = Kvmobile.openChannel(peerID, cb)
        try {
            if (!desktopDone.await(timeoutSeconds, TimeUnit.SECONDS)) {
                throw IllegalStateException("timed out after ${timeoutSeconds}s waiting for desktop's own direction to finish (received ${received.get()}/$sizeBytes bytes so far)")
            }
            out.flush()
            out.close()
            onDataFailure.get()?.let { throw it }

            val reason = closedReason.get()
            if (!reason.isNullOrEmpty()) {
                throw IllegalStateException("channel closed with a non-empty reason before this device's own direction ever ran: $reason")
            }
            if (received.get() != sizeBytes) {
                throw IllegalStateException("received ${received.get()} bytes from desktop, want $sizeBytes")
            }
            val gotHash = digest.digest().joinToString("") { "%02x".format(it) }
            if (!gotHash.equals(expectedHash, ignoreCase = true)) {
                throw IllegalStateException("received payload hash $gotHash, want $expectedHash")
            }
            result.put("receivedHash", gotHash)

            // Deleted here, *before* sendPayload generates its own
            // sizeBytes file, not in this function's own `finally` below --
            // a real device has nowhere near enough free storage to hold
            // two ~1GB files at once, and there is no reason to keep this
            // one around once its hash is already verified.
            recvFile.delete()

            sendPayload(context, channelID, sizeBytes, result)
        } finally {
            recvFile.delete()
            runCatching { Kvmobile.closeChannel(channelID) }
        }
    }

    /**
     * Generates a deterministic sizeBytes payload under context.filesDir,
     * reads it back and streams it through SendChannelData in
     * pkg/chandata.MaxChunkSize-sized raw chunks (no base64 -- gobind
     * binds Go []byte to Kotlin ByteArray directly across the JNI
     * boundary; see ChannelCallback.OnData's own doc comment) on the
     * already-open channelID, then CloseChannelWrite (blocks until every
     * chunk actually reached the wire -- see that method's own doc
     * comment). The generated file is always deleted (`finally`) before
     * returning.
     */
    private fun sendPayload(context: Context, channelID: String, sizeBytes: Long, result: JSONObject) {
        val sendFile = File(context.filesDir, "channel_transfer_send.bin")
        try {
            val sourceDigest = writeDeterministicFile(sendFile, sizeBytes)
            result.put("sourceHash", sourceDigest)

            val digest = MessageDigest.getInstance("SHA-256")
            val buf = ByteArray(CHUNK_SIZE)
            var sent = 0L
            FileInputStream(sendFile).use { input ->
                while (true) {
                    val n = input.read(buf)
                    if (n <= 0) break
                    digest.update(buf, 0, n)
                    val chunk = if (n == buf.size) buf else buf.copyOf(n)
                    Kvmobile.sendChannelData(channelID, "data", chunk)
                    sent += n
                }
            }
            if (sent != sizeBytes) {
                throw IllegalStateException("read $sent bytes from generated file, want $sizeBytes")
            }
            Kvmobile.closeChannelWrite(channelID)
        } finally {
            sendFile.delete()
        }
    }

    /**
     * Writes exactly sizeBytes of the deterministic byte(i % 256) pattern
     * (the same formula pkg/daemon/channel_dataplane_test.go and
     * pkg/e2erun's own host-side generator use) to file, in CHUNK_SIZE
     * bursts, and returns its SHA-256 as lowercase hex. Because 256 evenly
     * divides CHUNK_SIZE, one CHUNK_SIZE-sized buffer filled once up front
     * (fillPatternBuffer) is byte-for-byte identical for every aligned
     * write -- no per-byte modulo work needed on the hot path.
     */
    private fun writeDeterministicFile(file: File, sizeBytes: Long): String {
        val digest = MessageDigest.getInstance("SHA-256")
        val buf = ByteArray(CHUNK_SIZE)
        fillPatternBuffer(buf)
        FileOutputStream(file).use { out ->
            var written = 0L
            while (written < sizeBytes) {
                val n = minOf(CHUNK_SIZE.toLong(), sizeBytes - written).toInt()
                out.write(buf, 0, n)
                digest.update(buf, 0, n)
                written += n
            }
        }
        return digest.digest().joinToString("") { "%02x".format(it) }
    }

    private fun fillPatternBuffer(buf: ByteArray) {
        for (i in buf.indices) buf[i] = (i % 256).toByte()
    }

    companion object {
        // Matches pkg/chandata.MaxChunkSize -- the most a single
        // SendChannelData/one signed wire frame may carry (see that
        // constant's own doc comment); using anything smaller here would
        // just mean more, smaller calls for no benefit, and anything
        // larger would be silently capped (and rejected in the ring path)
        // by the daemon on the other end.
        private const val CHUNK_SIZE = 256 * 1024
    }
}
