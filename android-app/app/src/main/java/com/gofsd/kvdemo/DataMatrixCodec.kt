package com.gofsd.kvdemo

import com.google.zxing.BarcodeFormat
import com.google.zxing.EncodeHintType
import com.google.zxing.MultiFormatWriter
import com.google.zxing.Result
import com.google.zxing.WriterException
import com.google.zxing.common.BitMatrix
import com.google.zxing.datamatrix.encoder.SymbolShapeHint

/**
 * Ports capnp-encoded event bytes (from [kvmobile.Kvmobile.encodeEvent], or
 * decoded back via [kvmobile.Kvmobile.decodeEvent]) through ZXing's
 * DataMatrix reader/writer, which are String-in/String-out rather than
 * byte-in/byte-out. ISO-8859-1 (Latin-1) is a lossless 1:1 byte<->char
 * mapping for every byte value 0-255, and neither ZXing's HighLevelEncoder
 * (write side) nor its DecodedBitStreamParser (read side) reinterprets
 * chars/bytes through any other charset as long as no ECI/CHARACTER_SET
 * hint is set -- this file deliberately sets none -- so round-tripping
 * arbitrary binary through [bytesToText]/[textToBytes] on both sides is
 * exact, not just "usually fine for text". See DataMatrixCodecTest for the
 * proof (every byte value 0x00-0xFF, plus realistic capnp-shaped payloads).
 *
 * This object only knows about ZXing's format-agnostic String/BitMatrix
 * types, not Android's Bitmap or CameraX's ImageProxy -- keeping it usable
 * from a plain JVM unit test with no emulator/Robolectric. The actual
 * camera capture (ScannerHost/MainScannerWidget) and Bitmap rendering
 * (the "Generate DataMatrix" button's popup) build on top of this.
 */
object DataMatrixCodec {
    fun bytesToText(bytes: ByteArray): String = String(bytes, Charsets.ISO_8859_1)

    fun textToBytes(text: String): ByteArray = text.toByteArray(Charsets.ISO_8859_1)

    /** [Result.getText] from a successful DataMatrix scan, converted back to the original bytes. */
    fun resultToBytes(result: Result): ByteArray = textToBytes(result.text)

    /**
     * Encodes bytes as a DataMatrix symbol. Throws when bytes exceeds the
     * symbol's capacity (DataMatrix tops out around ~1.5KB) -- a real
     * possibility given some command fields (e.g. LogAppend's narrative)
     * accept arbitrary-length text, so callers must handle this rather
     * than assume it always succeeds. ZXing itself is inconsistent about
     * which exception type this is: an empty payload or a too-small
     * requested canvas throws [IllegalArgumentException], while some
     * other encode failures throw [WriterException] -- callers should
     * catch both (see DataMatrixCodecTest for confirmation of exactly
     * which one an oversized payload actually throws in this ZXing
     * version).
     *
     * At the default 300x300 [size], self-decoding is unreliable for very
     * small inputs (empirically, under ~40 bytes -- the symbol becomes too
     * small relative to the canvas for ZXing's own HybridBinarizer to
     * binarize reliably, an artifact of that binarizer's fixed 8px block
     * size, not of anything this project controls). This is never a
     * concern for real [kvmobile.Kvmobile.encodeEvent] output: every
     * capnp-encoded event is at least ~160 bytes once its segment
     * framing, CRC32, and (for signed ops) 64-byte Ed25519 signature are
     * included -- see DataMatrixCodecTest for the measured safe range.
     */
    @Throws(WriterException::class)
    fun encode(bytes: ByteArray, size: Int = 300): BitMatrix {
        val hints = mapOf(EncodeHintType.DATA_MATRIX_SHAPE to SymbolShapeHint.FORCE_SQUARE)
        return MultiFormatWriter().encode(bytesToText(bytes), BarcodeFormat.DATA_MATRIX, size, size, hints)
    }
}
