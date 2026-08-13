package com.gofsd.kvdemo

import android.util.Log
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.AlertDialog
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.testTag
import androidx.compose.ui.text.font.FontFamily

/**
 * Shown when a scan decodes as neither a [RunCode], a [NavCode.Group], nor a
 * `join_request_ticket` event (see AppRoot's scan dispatch) -- every command in the app now
 * executes only via a scanned [RunCode], so there is nothing left to offer to "trigger" here, only
 * a way to tell the human this code wasn't recognized rather than silently doing nothing. Shows
 * the raw scanned text as a debugging aid (e.g. confirming a stray/foreign barcode was scanned by
 * mistake) and dismisses -- never executes anything.
 */
@Composable
fun UnrecognizedScanDialog(rawText: String, onDismiss: () -> Unit) {
    LaunchedEffect(rawText) {
        Log.w("KVDemo", "ACTION_REQUIRED: unrecognized scan, waiting for human to dismiss UnrecognizedScanDialog")
    }

    AlertDialog(
        onDismissRequest = onDismiss,
        title = { Text("Unrecognized code") },
        text = {
            Column(Modifier.verticalScroll(rememberScrollState())) {
                Text("Not a recognized command/nav code. Raw scanned content:", modifier = Modifier.testTag("unrecognizedScanBody"))
                Text(rawText, fontFamily = FontFamily.Monospace, modifier = Modifier.testTag("unrecognizedScanRawText"))
            }
        },
        confirmButton = {
            TextButton(onClick = onDismiss, modifier = Modifier.testTag("unrecognizedScanClose")) { Text("Close") }
        },
    )
}
