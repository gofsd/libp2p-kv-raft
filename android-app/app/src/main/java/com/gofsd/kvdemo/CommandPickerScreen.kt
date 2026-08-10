package com.gofsd.kvdemo

import android.util.Log
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.material3.Button
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.platform.testTag
import androidx.compose.ui.unit.dp
import com.google.zxing.common.BitMatrix

private const val TAG = "KVDemo"

/**
 * "Commands": pick any single command from the app's full flat catalog (every category's every
 * CommandSpec, see CommandCatalog.kt) and Generate a [NavCode.Command] DataMatrix code for it.
 * Another device scanning that code (see AppRoot's scan dispatch) jumps straight to that exact
 * command's own CommandDetailScreen -- with empty inputs, ready to fill in and Run, never
 * auto-submitted -- instead of navigating there by hand through Categories -> that category's
 * command list. Generates nothing and submits nothing itself; this screen's only output is the
 * code.
 */
@Composable
fun CommandPickerScreen() {
    val context = LocalContext.current
    val commands = remember {
        buildCommands(context.filesDir.absolutePath, OutputLog::append)
    }
    var selectedLabel by remember { mutableStateOf<String?>(null) }
    var generatedMatrix by remember { mutableStateOf<BitMatrix?>(null) }
    var error by remember { mutableStateOf<String?>(null) }

    Column(
        modifier = Modifier.fillMaxSize().padding(16.dp).testTag("screen_command_picker"),
    ) {
        Text(
            "Generate a command shortcut",
            style = MaterialTheme.typography.titleLarge,
            modifier = Modifier.padding(bottom = 12.dp),
        )

        SelectDropdown(
            label = "Command",
            options = commands.map { it.label },
            selected = selectedLabel,
            onSelect = { selectedLabel = it },
            testTag = "commandPickerSelect",
        )

        Button(
            enabled = selectedLabel != null,
            onClick = {
                val label = selectedLabel ?: return@Button
                val spec = commands.first { it.label == label }
                Log.i(TAG, "USER_TAP: Generate pressed for command shortcut ${spec.label}")
                error = null
                generatedMatrix = try {
                    val raw = DataMatrixCodec.textToBytes(NavCode.encodeCommand(spec.category, spec.name))
                    DataMatrixCodec.encode(raw, size = 220)
                } catch (e: Exception) {
                    Log.w(TAG, "RESULT: generate command shortcut FAILED: ${e.message}")
                    error = e.message
                    null
                }
            },
            modifier = Modifier.fillMaxWidth().padding(top = 16.dp).testTag("commandPickerGenerate"),
        ) {
            Text("Generate")
        }

        error?.let { Text("Failed to generate code: $it", modifier = Modifier.padding(top = 8.dp)) }

        generatedMatrix?.let { matrix ->
            GeneratedDataMatrixDialog(
                matrix = matrix,
                onDismiss = { generatedMatrix = null },
                title = "Scan this to open \"$selectedLabel\" on the other device",
            )
        }
    }
}
