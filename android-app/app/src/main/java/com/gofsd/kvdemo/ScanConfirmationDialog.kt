package com.gofsd.kvdemo

import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.AlertDialog
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.testTag
import androidx.compose.ui.text.font.FontFamily
import org.json.JSONObject

/**
 * Shown on every successful scan (see AppRoot's ScannerCoordinator.scans
 * collector): the scanner is active on every screen at all times, and
 * this is the "permanent scan events" confirmation the app surfaces
 * before ever actually submitting anything against the current cluster --
 * scanning alone never triggers a write, only a subsequent tap on
 * "Trigger" does.
 *
 * [decodedJson] is [kvmobile.Kvmobile.decodeEvent]'s own pkg/e2edata.Event
 * JSON shape (`{"event":"...","fields":{...}}`) when the scanned bytes
 * were a valid capnp event; when decoding fails (e.g. the code is one of
 * this project's existing join/exec-invite ticket strings, not a raw
 * event), [decodedJson] is null and [rawText] is shown instead so the
 * scan is never silently dropped -- deep-linking into those existing
 * ticket-redemption flows is out of scope here, this is a fallback
 * display only.
 */
@Composable
fun ScanConfirmationDialog(
    decodedJson: String?,
    rawText: String,
    onConfirm: () -> Unit,
    onDismiss: () -> Unit,
) {
    if (decodedJson != null) {
        val pretty = runCatching { JSONObject(decodedJson).toString(2) }.getOrDefault(decodedJson)
        AlertDialog(
            onDismissRequest = onDismiss,
            title = { Text("Trigger scanned event?") },
            text = {
                Column(Modifier.verticalScroll(rememberScrollState())) {
                    Text(pretty, fontFamily = FontFamily.Monospace, modifier = Modifier.testTag("scanDialogBody"))
                }
            },
            confirmButton = {
                TextButton(onClick = onConfirm, modifier = Modifier.testTag("scanDialogConfirm")) { Text("Trigger") }
            },
            dismissButton = {
                TextButton(onClick = onDismiss, modifier = Modifier.testTag("scanDialogCancel")) { Text("Cancel") }
            },
        )
    } else {
        AlertDialog(
            onDismissRequest = onDismiss,
            title = { Text("Scanned code") },
            text = {
                Column(Modifier.verticalScroll(rememberScrollState())) {
                    Text(
                        "Not a recognized event code. Raw scanned content:",
                        modifier = Modifier.testTag("scanDialogBody"),
                    )
                    Text(rawText, fontFamily = FontFamily.Monospace, modifier = Modifier.testTag("scanDialogRawText"))
                }
            },
            confirmButton = {
                TextButton(onClick = onDismiss, modifier = Modifier.testTag("scanDialogCancel")) { Text("Close") }
            },
        )
    }
}
