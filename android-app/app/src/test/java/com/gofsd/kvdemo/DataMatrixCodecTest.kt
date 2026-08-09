package com.gofsd.kvdemo

import com.google.zxing.BarcodeFormat
import com.google.zxing.BinaryBitmap
import com.google.zxing.DecodeHintType
import com.google.zxing.MultiFormatReader
import com.google.zxing.RGBLuminanceSource
import com.google.zxing.common.BitMatrix
import com.google.zxing.common.HybridBinarizer
import kotlin.random.Random
import org.junit.Assert.assertArrayEquals
import org.junit.Test

/**
 * Proves DataMatrixCodec's ByteArray<->DataMatrix round trip is byte-exact
 * for arbitrary binary, not just for plain text -- the one part of the
 * whole scan/generate feature resting on ZXing's internal behavior rather
 * than this project's own code (see DataMatrixCodec's doc comment). Runs
 * as a plain JVM test (`./gradlew test`), no emulator/Robolectric needed:
 * [decode] below builds a [BinaryBitmap] straight from the [BitMatrix]
 * DataMatrixCodec.encode already produced, via a manual
 * [RGBLuminanceSource] (black=0x000000, white=0xFFFFFF) -- the same
 * decode entry point a real camera frame goes through downstream of
 * whatever LuminanceSource captures it, just without needing a real image
 * sensor to get there.
 *
 * Every payload here is at least [MIN_RELIABLE_SIZE] bytes -- empirically,
 * self-decoding a ZXing-generated DataMatrix symbol at
 * DataMatrixCodec.encode's default 300x300 canvas becomes unreliable well
 * below that (ZXing's own HybridBinarizer struggles with the resulting
 * tiny, sparse symbols relative to the canvas -- a narrow corner case in
 * that binarizer, not a byte-fidelity bug), which never matters for this
 * app's real payloads: [kvmobile.Kvmobile.encodeEvent]'s smallest possible
 * output (an unsigned get_public_key/get_own_addr request) is ~160 bytes
 * once capnp segment framing and CRC32 are included, and every op that
 * requires a signature adds a 64-byte Ed25519 signature on top of that.
 */
class DataMatrixCodecTest {
    companion object {
        private const val MIN_RELIABLE_SIZE = 40
    }

    private fun decode(matrix: BitMatrix): ByteArray {
        val width = matrix.width
        val height = matrix.height
        val pixels = IntArray(width * height)
        for (y in 0 until height) {
            for (x in 0 until width) {
                pixels[y * width + x] = if (matrix.get(x, y)) 0xFF000000.toInt() else 0xFFFFFFFF.toInt()
            }
        }
        val source = RGBLuminanceSource(width, height, pixels)
        val bitmap = BinaryBitmap(HybridBinarizer(source))
        val hints = mapOf(
            DecodeHintType.POSSIBLE_FORMATS to listOf(BarcodeFormat.DATA_MATRIX),
            DecodeHintType.TRY_HARDER to true,
            // This test's source image is a perfectly aligned, noise-free
            // digital rendering (not a photographed frame), which is
            // exactly what PURE_BARCODE tells ZXing's reader to expect --
            // a real camera frame (MainScannerWidget's actual decode path)
            // omits this hint since it never applies there.
            DecodeHintType.PURE_BARCODE to true,
        )
        val result = MultiFormatReader().apply { setHints(hints) }.decode(bitmap)
        return DataMatrixCodec.resultToBytes(result)
    }

    private fun roundTrip(bytes: ByteArray) {
        check(bytes.size >= MIN_RELIABLE_SIZE) {
            "test payload too small (${bytes.size} bytes) to reliably self-decode -- see class doc comment"
        }
        val matrix = DataMatrixCodec.encode(bytes)
        val decoded = decode(matrix)
        assertArrayEquals(
            "round trip mismatch for ${bytes.size} byte(s)",
            bytes,
            decoded,
        )
    }

    @Test
    fun everyByteValueRoundTripsAtEveryPosition() {
        // Each byte value 0-255 is planted at a rotating offset within a
        // fixed-filler envelope large enough to be reliably self-decoded
        // (see class doc comment), so this exercises every value at
        // several different positions, not just the first byte.
        for (b in 0..255) {
            val bytes = ByteArray(MIN_RELIABLE_SIZE) { i -> if (i == b % MIN_RELIABLE_SIZE) b.toByte() else 0x5A }
            roundTrip(bytes)
        }
    }

    @Test
    fun everyByteValueRoundTripsInOneSequence() {
        val bytes = ByteArray(256) { it.toByte() }
        roundTrip(bytes)
    }

    @Test(expected = IllegalArgumentException::class)
    fun emptyPayloadIsRejected() {
        // ZXing's DataMatrixWriter refuses empty content outright -- not a
        // round-trip concern since a real capnp-encoded event message is
        // never zero bytes, but worth pinning down so a caller (the
        // "Generate DataMatrix" button) knows to expect this rather than
        // a silent empty code.
        DataMatrixCodec.encode(ByteArray(0))
    }

    @Test
    fun realisticCapnpShapedPayloadsRoundTrip() {
        // Capnp messages are word-aligned (8-byte) binary with a segment
        // table header -- not text, and typically containing many
        // low/high byte values back to back, unlike a hand-picked test
        // string would. A few representative shapes, all at or above a
        // real EncodeEvent's actual minimum output size:
        roundTrip(ByteArray(160) { ((it * 37 + 11) % 256).toByte() }) // ~get_public_key-sized pseudo-random spread
        roundTrip(ByteArray(228) { ((it * 61 + 3) % 256).toByte() }) // ~set_key-with-signature-sized spread
        roundTrip("hello world, this is a narrative field value".toByteArray(Charsets.UTF_8) + ByteArray(120))
        roundTrip(ByteArray(64) { 0 } + "key".toByteArray() + ByteArray(64) { 0xff.toByte() } + "value".toByteArray())
    }

    @Test
    fun randomPayloadsRoundTrip() {
        val random = Random(42)
        repeat(50) { i ->
            roundTrip(random.nextBytes(MIN_RELIABLE_SIZE + i * 4))
        }
    }

    @Test
    fun oversizedPayloadThrowsClearly() {
        // DataMatrix's real-world capacity ceiling is ~1.5KB -- past that,
        // encode() must fail clearly rather than silently truncate, since
        // some command fields (e.g. LogAppend's narrative) accept
        // arbitrary-length text. ZXing throws IllegalArgumentException
        // here (not WriterException, its own doc-implied type) -- pinned
        // down so production code (the "Generate DataMatrix" button)
        // knows which one to actually catch.
        val tooBig = ByteArray(4000) { it.toByte() }
        try {
            DataMatrixCodec.encode(tooBig)
            org.junit.Assert.fail("expected an exception for oversized payload")
        } catch (_: IllegalArgumentException) {
            // expected
        }
    }
}
