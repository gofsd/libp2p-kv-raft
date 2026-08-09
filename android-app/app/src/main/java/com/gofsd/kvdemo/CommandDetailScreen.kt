package com.gofsd.kvdemo

import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.text.KeyboardOptions
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.Button
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedButton
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateListOf
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.platform.testTag
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.text.input.ImeAction
import androidx.compose.ui.unit.dp
import com.google.zxing.common.BitMatrix
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kvmobile.Kvmobile
import org.json.JSONObject

/**
 * One CommandSpec's own screen (see CommandCatalog.kt): a labeled input
 * field per parameter, a Run button, and this screen's own local output
 * log of every Run made here -- scoped to this screen alone, not shared
 * with OutputLog/LogScreen (that log is only for the WatchExecute/
 * WatchCommandLog/RunCommandDispatcher notifications CommandCatalog.kt's
 * callbacks deliver on their own schedule, unrelated to which detail
 * screen, if any, is currently open). Fresh each visit -- navigating away
 * and back starts with an empty output log again, the same as the old
 * CommandDetailActivity did every time you switched commands. Replaces
 * that Activity 1:1; the "Generate DataMatrix" button (see
 * CommandCatalog.kt's eventOp/toEventFields) is added alongside Run for
 * specs with a single capnp-event equivalent.
 */
@Composable
fun CommandDetailScreen(category: String, name: String) {
    val context = LocalContext.current
    val spec = remember(category, name) {
        buildCommands(context.filesDir.absolutePath, OutputLog::append)
            .first { it.category == category && it.name == name }
    }

    val paramValues = remember(spec) { mutableStateListOf(*Array(spec.params.size) { "" }) }
    var output by remember(spec) { mutableStateOf("") }
    var running by remember(spec) { mutableStateOf(false) }
    var generating by remember(spec) { mutableStateOf(false) }
    var generatedMatrix by remember(spec) { mutableStateOf<BitMatrix?>(null) }
    val scope = rememberCoroutineScope()
    val scrollState = rememberScrollState()
    val canGenerate = spec.eventOp != null || spec.rawEventJsonParamIndex != null

    LaunchedEffect(output) {
        scrollState.animateScrollTo(scrollState.maxValue)
    }

    Column(
        modifier = Modifier
            .fillMaxSize()
            .padding(16.dp)
            .testTag("screen_command_detail"),
    ) {
        Text(
            spec.label,
            style = MaterialTheme.typography.titleLarge,
            modifier = Modifier.padding(bottom = 12.dp).testTag("commandTitle"),
        )

        Column(modifier = Modifier.testTag("paramsContainer")) {
            spec.params.forEachIndexed { index, hint ->
                OutlinedTextField(
                    value = paramValues[index],
                    onValueChange = { paramValues[index] = it },
                    label = { Text(hint) },
                    singleLine = true,
                    keyboardOptions = KeyboardOptions(imeAction = ImeAction.Next),
                    modifier = Modifier
                        .fillMaxWidth()
                        .padding(vertical = 4.dp)
                        .testTag("param_$index"),
                )
            }
        }

        Button(
            enabled = !running,
            onClick = {
                val args = paramValues.toList()
                running = true
                scope.launch {
                    val line = try {
                        val result = withContext(Dispatchers.IO) { spec.run(args) }
                        "${spec.label}(${args.joinToString(", ")}) ->\n$result"
                    } catch (e: Exception) {
                        "${spec.label}(${args.joinToString(", ")}) FAILED: ${e.message}"
                    }
                    output = if (output.isEmpty()) line else "$output\n\n$line"
                    running = false
                }
            },
            modifier = Modifier.fillMaxWidth().padding(top = 8.dp).testTag("runButton"),
        ) {
            Text("Run")
        }

        OutlinedButton(
            enabled = canGenerate && !generating,
            onClick = {
                val args = paramValues.toList()
                val eventJson = if (spec.rawEventJsonParamIndex != null) {
                    args[spec.rawEventJsonParamIndex]
                } else {
                    JSONObject().apply {
                        put("event", spec.eventOp)
                        put("fields", JSONObject(spec.toEventFields!!(args)))
                    }.toString()
                }
                generating = true
                scope.launch {
                    try {
                        val raw = withContext(Dispatchers.IO) { Kvmobile.encodeEvent(eventJson) }
                        generatedMatrix = withContext(Dispatchers.Default) { DataMatrixCodec.encode(raw) }
                    } catch (e: Exception) {
                        val line = "Generate DataMatrix FAILED: ${e.message}"
                        output = if (output.isEmpty()) line else "$output\n\n$line"
                    }
                    generating = false
                }
            },
            modifier = Modifier.fillMaxWidth().padding(top = 8.dp).testTag("generateDataMatrixButton"),
        ) {
            Text("Generate DataMatrix")
        }
        if (!canGenerate) {
            Text(
                "No single capnp event for this operation -- not generatable as a code.",
                style = MaterialTheme.typography.labelSmall,
                modifier = Modifier.padding(top = 4.dp).testTag("generateDataMatrixUnavailableHint"),
            )
        }

        generatedMatrix?.let { matrix ->
            GeneratedDataMatrixDialog(matrix = matrix, onDismiss = { generatedMatrix = null })
        }

        Column(
            modifier = Modifier
                .fillMaxSize()
                .padding(top = 12.dp)
                .verticalScroll(scrollState)
                .testTag("outputScroll"),
        ) {
            Text(
                output,
                fontFamily = FontFamily.Monospace,
                style = MaterialTheme.typography.bodySmall,
                modifier = Modifier.testTag("outputText"),
            )
        }
    }
}
