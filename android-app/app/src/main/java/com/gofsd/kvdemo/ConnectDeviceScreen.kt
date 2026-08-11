package com.gofsd.kvdemo

import android.util.Log
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.Button
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedButton
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.testTag
import androidx.compose.ui.unit.dp
import com.google.zxing.common.BitMatrix
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kvmobile.Kvmobile
import org.json.JSONObject

private const val TAG = "KVDemo"

/**
 * Builds and generates a `dial_submit_command` event (capnp
 * api/shmevent.capnp's dialSubmitCommand group, pkg/e2edata op
 * "dial_submit_command") -- see CommandCatalog.kt's Dispatch/
 * DialSubmitCommand doc comment: "this device dials targetAddr directly
 * and submits commandId/inputsJson there ..., provided targetAddr has
 * linked commandId to a public group". That's exactly what makes this a
 * "connect to a device and run a command on it by scanning" flow rather
 * than an ordinary generic event: whichever device *scans and triggers*
 * this code does the dialing, so for the intended "generate on device A,
 * scan on device B, command runs on A" shape, [targetPeer] should name
 * *this* device -- it's pre-filled from Kvmobile.getOwnAddr() (with a
 * Refresh button, since GetOwnAddr's own doc comment warns a relay
 * reservation can finish completing after this device started, so an
 * early call can return a stale private/loopback address) but stays
 * freely editable in case the operator wants to target some other device
 * instead of this one.
 *
 * Nothing about the scanning/receiving side needs to change:
 * AppRoot.kt's existing generic scan dispatch decodes this event like any
 * other and offers it through the ordinary ScanConfirmationDialog, whose
 * Trigger already calls Kvmobile.sendEvent and records the result
 * (including this op's returned instance_id) to OutputLog/LogScreen.
 */
@Composable
fun ConnectDeviceScreen() {
    var targetPeer by remember { mutableStateOf("") }
    var commandID by remember { mutableStateOf("") }
    var inputsJSON by remember { mutableStateOf("") }
    var note by remember { mutableStateOf("") }
    var loadingAddr by remember { mutableStateOf(false) }
    var generating by remember { mutableStateOf(false) }
    var generatedMatrix by remember { mutableStateOf<BitMatrix?>(null) }
    var errorText by remember { mutableStateOf("") }
    val scope = rememberCoroutineScope()

    fun refreshOwnAddr() {
        Log.i(TAG, "AUTO: GetOwnAddr for ConnectDeviceScreen's targetPeer field")
        loadingAddr = true
        scope.launch {
            try {
                targetPeer = withContext(Dispatchers.IO) { Kvmobile.getOwnAddr() }
            } catch (e: Exception) {
                errorText = "GetOwnAddr FAILED: ${e.message}"
            }
            loadingAddr = false
        }
    }

    LaunchedEffect(Unit) { refreshOwnAddr() }

    Column(
        modifier = Modifier
            .fillMaxSize()
            .padding(16.dp)
            .verticalScroll(rememberScrollState())
            .testTag("screen_connect_device"),
    ) {
        Text("Connect to Device", style = MaterialTheme.typography.titleLarge, modifier = Modifier.padding(bottom = 12.dp))

        Row(modifier = Modifier.fillMaxWidth()) {
            OutlinedTextField(
                value = targetPeer,
                onValueChange = { targetPeer = it },
                label = { Text("targetPeer (this device's own address, or another device's)") },
                singleLine = true,
                enabled = !loadingAddr,
                modifier = Modifier.weight(1f).testTag("targetPeerField"),
            )
            OutlinedButton(
                onClick = { refreshOwnAddr() },
                enabled = !loadingAddr,
                modifier = Modifier.padding(start = 4.dp).testTag("refreshOwnAddrButton"),
            ) {
                Text("Refresh")
            }
        }

        OutlinedTextField(
            value = commandID,
            onValueChange = { commandID = it },
            label = { Text("commandID") },
            singleLine = true,
            modifier = Modifier.fillMaxWidth().padding(vertical = 4.dp).testTag("commandIDField"),
        )
        OutlinedTextField(
            value = inputsJSON,
            onValueChange = { inputsJSON = it },
            label = { Text("inputsJSON") },
            singleLine = true,
            modifier = Modifier.fillMaxWidth().padding(vertical = 4.dp).testTag("inputsJSONField"),
        )
        OutlinedTextField(
            value = note,
            onValueChange = { note = it },
            label = { Text("note") },
            singleLine = true,
            modifier = Modifier.fillMaxWidth().padding(vertical = 4.dp).testTag("noteField"),
        )

        if (errorText.isNotEmpty()) {
            Text(errorText, modifier = Modifier.padding(top = 8.dp).testTag("connectDeviceError"))
        }

        Button(
            enabled = !generating,
            onClick = {
                Log.i(TAG, "USER_TAP: Generate DataMatrix pressed for dial_submit_command (target=$targetPeer, command=$commandID)")
                generating = true
                errorText = ""
                scope.launch {
                    try {
                        val raw = withContext(Dispatchers.IO) {
                            val json = JSONObject().apply {
                                put("event", "dial_submit_command")
                                put(
                                    "fields",
                                    JSONObject(
                                        mapOf(
                                            "target_peer" to targetPeer,
                                            "command_id" to commandID,
                                            "inputs_json" to inputsJSON,
                                            "note" to note,
                                        ),
                                    ),
                                )
                            }.toString()
                            Kvmobile.encodeEvent(json)
                        }
                        generatedMatrix = withContext(Dispatchers.Default) { DataMatrixCodec.encode(raw) }
                        Log.w(TAG, "ACTION_REQUIRED: DataMatrix for dial_submit_command is on screen -- scan it with the other device's camera now")
                    } catch (e: Exception) {
                        Log.w(TAG, "RESULT: Generate DataMatrix FAILED for dial_submit_command: ${e.message}")
                        errorText = "Generate DataMatrix FAILED: ${e.message}"
                    }
                    generating = false
                }
            },
            modifier = Modifier.fillMaxWidth().padding(top = 8.dp).testTag("generateButton"),
        ) {
            Text("Generate DataMatrix")
        }
    }

    val matrix = generatedMatrix
    if (matrix != null) {
        GeneratedDataMatrixDialog(
            matrix = matrix,
            onDismiss = { generatedMatrix = null },
            title = "Scan this to run \"$commandID\" on $targetPeer",
        )
    }
}
