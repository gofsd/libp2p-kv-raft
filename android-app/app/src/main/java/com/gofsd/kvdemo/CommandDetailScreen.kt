package com.gofsd.kvdemo

import android.util.Base64
import android.util.Log
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.text.KeyboardOptions
import androidx.compose.foundation.verticalScroll
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
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kvmobile.Kvmobile
import org.json.JSONArray

/**
 * One CommandSpec's own screen (see CommandCatalog.kt): a labeled input field per parameter, a
 * "Generate DataMatrix" button, and a "Scan & Execute" button. There is no Run button -- every
 * command in this app executes exactly one way: generate a [RunCode] here (or on another device),
 * scan it with a real camera, confirm in [RunConfirmDialog], which calls
 * [CommandExecutor.execute]. Generate is always enabled now (a [RunCode] just needs
 * category+name+params -- no `eventOp`/signing key required) except for the one
 * `generateFromResultBase64` spec (CreateJoinRequestTicket), whose generated code has to be the
 * real signed ticket [spec.run] returns, not a RunCode -- see CommandSpec's own doc comment for
 * why. [onNavigateToLog] is only ever invoked for a spec with awaitAdmissionAfterGenerate set,
 * once this device is observed to have actually been admitted to a cluster (see that field's own
 * doc comment on CommandSpec) -- every other spec's Generate flow never calls it.
 */
private const val TAG = "KVDemo"

@Composable
fun CommandDetailScreen(category: String, name: String, onNavigateToLog: () -> Unit = {}) {
    val context = LocalContext.current
    val spec = remember(category, name) {
        buildCommands(context.filesDir.absolutePath, OutputLog::append)
            .first { it.category == category && it.name == name }
    }

    val paramValues = remember(spec) { mutableStateListOf(*Array(spec.params.size) { "" }) }
    var output by remember(spec) { mutableStateOf("") }
    var generating by remember(spec) { mutableStateOf(false) }
    var generatedMatrix by remember(spec) { mutableStateOf<BitMatrix?>(null) }
    val scope = rememberCoroutineScope()
    val scrollState = rememberScrollState()

    LaunchedEffect(output) {
        scrollState.animateScrollTo(scrollState.maxValue)
    }

    // Once a ticket this device generated has been scanned and redeemed
    // elsewhere, this device's own n.raft goes from pending to admitted --
    // there's no push/watch callback for that transition (see
    // CommandSpec.awaitAdmissionAfterGenerate's doc comment), so this
    // polls Kvmobile.listClusterMembers() for this device's own peer id
    // appearing while the popup is up. Keyed on the matrix's own identity:
    // a fresh popup gets a fresh poll loop, and setting generatedMatrix
    // back to null (here, or via the dialog's own Close button) cancels
    // whichever loop was running, since Compose cancels/relaunches
    // LaunchedEffect whenever its key changes.
    LaunchedEffect(generatedMatrix) {
        val matrix = generatedMatrix
        if (matrix == null || !spec.awaitAdmissionAfterGenerate) return@LaunchedEffect
        val ownPeerID = runCatching { withContext(Dispatchers.IO) { Kvmobile.peerID() } }.getOrNull()
        if (ownPeerID.isNullOrEmpty()) return@LaunchedEffect
        Log.i(TAG, "AUTO: polling listClusterMembers for own peer id $ownPeerID to appear (admission)")
        while (true) {
            delay(2000)
            val membersJson = runCatching {
                withContext(Dispatchers.IO) { Kvmobile.listClusterMembers() }
            }.getOrNull() ?: continue
            val admitted = runCatching {
                val members = JSONArray(membersJson)
                (0 until members.length()).any { members.getJSONObject(it).optString("peer_id") == ownPeerID }
            }.getOrDefault(false)
            if (admitted) {
                Log.i(TAG, "RESULT: ${spec.label} admitted to cluster as $ownPeerID")
                val line = "${spec.label}: admitted to cluster as $ownPeerID"
                output = if (output.isEmpty()) line else "$output\n\n$line"
                generatedMatrix = null
                onNavigateToLog()
                return@LaunchedEffect
            }
        }
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

        OutlinedButton(
            enabled = !generating,
            onClick = {
                val args = paramValues.toList()
                Log.i(TAG, "USER_TAP: Generate DataMatrix pressed for ${spec.label}(${args.joinToString(", ")})")
                generating = true
                scope.launch {
                    try {
                        val raw = if (spec.generateFromResultBase64) {
                            val resultB64 = withContext(Dispatchers.IO) { spec.run(args) }
                            Base64.decode(resultB64, Base64.DEFAULT)
                        } else {
                            DataMatrixCodec.textToBytes(RunCode.encode(spec.category, spec.name, args))
                        }
                        generatedMatrix = withContext(Dispatchers.Default) { DataMatrixCodec.encode(raw) }
                        Log.w(TAG, "ACTION_REQUIRED: DataMatrix for ${spec.label} is on screen -- scan it with the other device's camera now")
                    } catch (e: Exception) {
                        Log.w(TAG, "RESULT: Generate DataMatrix FAILED for ${spec.label}: ${e.message}")
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

        OutlinedButton(
            onClick = {
                Log.i(TAG, "USER_TAP: Scan & Execute pressed for ${spec.label} (opening scanner)")
                ScannerCoordinator.expanded = true
            },
            modifier = Modifier.fillMaxWidth().padding(top = 8.dp).testTag("scanAndExecuteButton"),
        ) {
            Text("Scan & Execute")
        }

        generatedMatrix?.let { matrix ->
            GeneratedDataMatrixDialog(
                matrix = matrix,
                onDismiss = { generatedMatrix = null },
                title = if (spec.awaitAdmissionAfterGenerate) {
                    "Scan this on the recruiting device -- closes automatically once admitted"
                } else {
                    "Scan this on another device to run ${spec.label}"
                },
            )
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
