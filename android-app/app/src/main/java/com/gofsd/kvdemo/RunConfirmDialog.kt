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
 * Shown on every scan that decodes as a [RunCode] (see AppRoot's scan dispatch) -- the sole
 * confirm-then-execute surface for every one of CommandCatalog.kt's specs now that
 * CommandDetailScreen's own Run button is gone. Scanning alone never executes anything, same
 * rule every other decodable code in this app follows ([RecruitConfirmDialog]) -- only a
 * subsequent tap on Execute calls [CommandExecutor.execute].
 */
@Composable
fun RunConfirmDialog(
    category: String,
    name: String,
    params: List<String>,
    onConfirm: () -> Unit,
    onDismiss: () -> Unit,
) {
    LaunchedEffect(category, name, params) {
        Log.w("KVDemo", "ACTION_REQUIRED: waiting for human to tap Execute/Cancel on RunConfirmDialog ($category: $name)")
    }

    AlertDialog(
        onDismissRequest = onDismiss,
        title = { Text("$category: $name") },
        text = {
            Column(Modifier.verticalScroll(rememberScrollState())) {
                Text(
                    if (params.isEmpty()) "No parameters" else params.joinToString("\n"),
                    fontFamily = FontFamily.Monospace,
                    modifier = Modifier.testTag("runConfirmParams"),
                )
            }
        },
        confirmButton = {
            TextButton(onClick = onConfirm, modifier = Modifier.testTag("runConfirmExecute")) { Text("Execute") }
        },
        dismissButton = {
            TextButton(onClick = onDismiss, modifier = Modifier.testTag("runConfirmCancel")) { Text("Cancel") }
        },
    )
}
