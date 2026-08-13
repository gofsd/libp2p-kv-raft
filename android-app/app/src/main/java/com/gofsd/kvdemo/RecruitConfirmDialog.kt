package com.gofsd.kvdemo

import android.util.Log
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.material3.AlertDialog
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.testTag

/**
 * Shown instead of [RunConfirmDialog] when AppRoot's scan dispatch recognizes a decoded event as
 * `join_request_ticket` -- some other device's CreateJoinRequestTicket code (see
 * CommandDetailScreen's awaitAdmissionAfterGenerate handling on that side). This device is
 * already an established raft voter/learner (only an existing member can
 * admit anyone -- see RedeemJoinRequestTicket), so scanning the ticket
 * doesn't run it as an ordinary command the way RunConfirmDialog's
 * "Execute" does: it stops here first, letting the operator optionally
 * give the device being admitted a human-readable station name (see
 * mobile/kvmobile.PutStation -- purely descriptive, independent of the
 * admission itself) before [onApprove] actually redeems the ticket
 * (Kvmobile.redeemJoinRequestTicket), admitting it into this device's
 * cluster.
 */
@Composable
fun RecruitConfirmDialog(
    sourceAddr: String,
    onApprove: (stationName: String) -> Unit,
    onDismiss: () -> Unit,
) {
    var stationName by remember { mutableStateOf("") }

    LaunchedEffect(sourceAddr) {
        Log.w("KVDemo", "ACTION_REQUIRED: waiting for human to tap Approve/Cancel on RecruitConfirmDialog (source=$sourceAddr)")
    }

    AlertDialog(
        onDismissRequest = onDismiss,
        title = { Text("Admit recruited device?") },
        text = {
            Column {
                if (sourceAddr.isNotEmpty()) {
                    Text(sourceAddr, modifier = Modifier.testTag("recruitConfirmSourceAddr"))
                }
                OutlinedTextField(
                    value = stationName,
                    onValueChange = { stationName = it },
                    label = { Text("Station name (optional)") },
                    singleLine = true,
                    modifier = Modifier.fillMaxWidth().testTag("recruitConfirmName"),
                )
            }
        },
        confirmButton = {
            TextButton(
                onClick = { onApprove(stationName) },
                modifier = Modifier.testTag("recruitConfirmApprove"),
            ) { Text("Approve") }
        },
        dismissButton = {
            TextButton(onClick = onDismiss, modifier = Modifier.testTag("recruitConfirmCancel")) { Text("Cancel") }
        },
    )
}
