package com.gofsd.kvdemo

import android.graphics.Bitmap
import android.graphics.Color
import androidx.compose.foundation.Image
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.size
import androidx.compose.material3.AlertDialog
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.remember
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.asImageBitmap
import androidx.compose.ui.platform.testTag
import androidx.compose.ui.unit.dp
import com.google.zxing.common.BitMatrix

/** Renders a ZXing [BitMatrix] (from [DataMatrixCodec.encode]) as a Bitmap for display. */
private fun BitMatrix.toBitmap(): Bitmap {
    val bmp = Bitmap.createBitmap(width, height, Bitmap.Config.ARGB_8888)
    for (x in 0 until width) {
        for (y in 0 until height) {
            bmp.setPixel(x, y, if (get(x, y)) Color.BLACK else Color.WHITE)
        }
    }
    return bmp
}

/**
 * Shown by CommandDetailScreen's "Generate DataMatrix" button: the
 * capnp-encoded bytes [kvmobile.Kvmobile.encodeEvent] returned for the
 * form's current inputs, rendered as a scannable code -- another device
 * running this app can scan it and get [ScanConfirmationDialog]'s
 * trigger-or-cancel prompt for the exact same event.
 */
@Composable
fun GeneratedDataMatrixDialog(matrix: BitMatrix, onDismiss: () -> Unit) {
    val bitmap = remember(matrix) { matrix.toBitmap() }
    AlertDialog(
        onDismissRequest = onDismiss,
        title = { Text("Scan this to trigger the event") },
        text = {
            Column {
                Image(
                    bitmap = bitmap.asImageBitmap(),
                    contentDescription = "Generated DataMatrix code",
                    modifier = Modifier.size(280.dp).testTag("generatedDataMatrixImage"),
                )
            }
        },
        confirmButton = {
            TextButton(onClick = onDismiss, modifier = Modifier.testTag("generatedDataMatrixClose")) { Text("Close") }
        },
    )
}
