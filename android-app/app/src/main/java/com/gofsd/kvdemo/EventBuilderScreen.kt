package com.gofsd.kvdemo

import android.util.Log
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.Button
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateListOf
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
 * One field of a hand-constructible event op: [key] is the wire field name
 * pkg/e2edata/file.go's `ToMsg` switch reads (`e.Fields[key]`), [label] is
 * what's shown above the input, [blankValue] is substituted when the user
 * leaves the field empty (default "": most fields are fine as an empty
 * string; a couple of dial_query_command_log's fields are Int64/Int32 on
 * the wire and error on `strconv.ParseInt("")`, so those use "0" -- which,
 * per that op's own doc comment, means "unbounded" and is exactly what
 * leaving the field blank should mean), and [encode] transforms the raw
 * text into the wire value CommandCatalog.kt's own toEventFields closures
 * already use for the same op where one exists (numeric permit "kind"/
 * recruit "suffrage" strings, the CancelJoinRequest "0x" token prefix).
 */
private data class EventField(
    val key: String,
    val label: String,
    val blankValue: String = "",
    val encode: (String) -> String = { it },
)

/**
 * Every hand-constructible pkg/e2edata event op this screen offers, in
 * dropdown order, paired with its own ordered fields. Field keys/encodings
 * for ops already wired to a CommandSpec's eventOp/toEventFields in
 * CommandCatalog.kt are copied verbatim from there so the two stay in
 * sync; the rest (get_field_by_key, log_append, station_put/delete,
 * dial_submit_command, dial_query_command_log) are read directly off
 * pkg/e2edata/file.go's ToMsg switch, since no CommandSpec constructs them
 * today. Purely internal/system ops (setKey/getKey/getFieldByRegistry,
 * getPublicKey/getPrivateKey, the channel-prefixed events, execute/
 * pollExecute, txn, the offline-ticket-only shapes) are deliberately left
 * off this list -- see this project's own CLAUDE.md on why those aren't
 * meant to be hand-built.
 */
private val EVENT_OP_FIELDS: List<Pair<String, List<EventField>>> = listOf(
    "get_own_addr" to emptyList(),
    "join_request_create" to emptyList(),
    "join_request_cancel" to listOf(
        EventField("token", "tokenHex", encode = { "0x$it" }),
    ),
    "recruit" to listOf(
        EventField("ticket", "ticket (addr#tokenHex)"),
        EventField("suffrage", "voter|learner", encode = ::suffrageNumber),
    ),
    "kick" to listOf(EventField("peer_id", "targetPeerID")),
    "set" to listOf(EventField("key", "key"), EventField("value", "value")),
    "get_field_by_key" to listOf(EventField("key", "key")),
    "log_append" to listOf(EventField("key", "key"), EventField("value", "value")),
    "permit_request" to listOf(
        EventField("kind", "kind (bootstrap)", encode = ::permitKindNumber),
        EventField("peer_id", "targetPeerID"),
        EventField("metadata", "metadata"),
    ),
    "permit_confirm" to listOf(
        EventField("kind", "kind (bootstrap|cluster-join)", encode = ::permitKindNumber),
        EventField("peer_id", "targetPeerID"),
    ),
    "permit_revoke" to listOf(
        EventField("kind", "kind (bootstrap)", encode = ::permitKindNumber),
        EventField("peer_id", "targetPeerID"),
    ),
    "group_put" to listOf(
        EventField("id", "id"), EventField("name", "name"), EventField("public", "public (true|false)"),
    ),
    "group_delete" to listOf(EventField("id", "id")),
    "command_put" to listOf(
        EventField("id", "id"), EventField("name", "name"), EventField("peer_id", "targetPeerID"),
    ),
    "command_delete" to listOf(EventField("id", "id")),
    "group_command_put" to listOf(EventField("command_id", "commandID"), EventField("group_id", "groupID")),
    "group_command_delete" to listOf(EventField("command_id", "commandID"), EventField("group_id", "groupID")),
    "peer_group_put" to listOf(EventField("peer_id", "peerID"), EventField("group_id", "groupID")),
    "peer_group_delete" to listOf(EventField("peer_id", "peerID"), EventField("group_id", "groupID")),
    "station_put" to listOf(
        EventField("peer_id", "peerID"), EventField("name", "name"), EventField("attrs", "attrsJSON (blank ok)"),
    ),
    "station_delete" to listOf(EventField("peer_id", "peerID")),
    "dial_submit_command" to listOf(
        EventField("target_peer", "targetPeer (multiaddr or bare peer id)"),
        EventField("command_id", "commandID"),
        EventField("inputs_json", "inputsJSON"),
        EventField("note", "note"),
    ),
    "dial_query_command_log" to listOf(
        EventField("target_peer", "targetPeer"),
        EventField("instance_id", "instanceID"),
        EventField("since", "since (unix nanos, blank=unbounded)", blankValue = "0"),
        EventField("until", "until (unix nanos, blank=unbounded)", blankValue = "0"),
        EventField("limit", "limit (blank=unbounded)", blankValue = "0"),
    ),
)

/**
 * Generic "build any event" screen: pick a [EVENT_OP_FIELDS] op from the
 * dropdown, fill in that op's own labeled fields, Generate builds
 * `{"event":op,"fields":{...}}` and passes it to
 * [kvmobile.Kvmobile.encodeEvent] -- the exact same call
 * CommandDetailScreen's per-command "Generate DataMatrix" button makes,
 * just sourced from this screen's own dropdown+fields instead of one fixed
 * CommandSpec. Generalizes/unifies the scattered per-command Generate
 * buttons (and the "Raw: SendEvent" free-JSON escape hatch) into one
 * discoverable form; nothing about the scanning/receiving side changes --
 * AppRoot.kt's existing generic scan dispatch already decodes and offers
 * to trigger any event this produces via ScanConfirmationDialog.
 */
@Composable
fun EventBuilderScreen() {
    var selectedOp by remember { mutableStateOf(EVENT_OP_FIELDS.first().first) }
    val fields = remember(selectedOp) { EVENT_OP_FIELDS.first { it.first == selectedOp }.second }
    val fieldValues = remember(selectedOp) { mutableStateListOf(*Array(fields.size) { "" }) }
    var generating by remember { mutableStateOf(false) }
    var generatedMatrix by remember { mutableStateOf<BitMatrix?>(null) }
    var errorText by remember { mutableStateOf("") }
    val scope = rememberCoroutineScope()

    Column(
        modifier = Modifier
            .fillMaxSize()
            .padding(16.dp)
            .verticalScroll(rememberScrollState())
            .testTag("screen_event_builder"),
    ) {
        Text("Build Event", style = MaterialTheme.typography.titleLarge, modifier = Modifier.padding(bottom = 12.dp))

        SelectDropdown(
            label = "Event",
            options = EVENT_OP_FIELDS.map { it.first },
            selected = selectedOp,
            onSelect = { selectedOp = it },
            testTag = "eventOpDropdown",
        )

        Column(modifier = Modifier.testTag("eventFieldsContainer")) {
            fields.forEachIndexed { index, field ->
                OutlinedTextField(
                    value = fieldValues[index],
                    onValueChange = { fieldValues[index] = it },
                    label = { Text(field.label) },
                    singleLine = true,
                    modifier = Modifier
                        .fillMaxWidth()
                        .padding(vertical = 4.dp)
                        .testTag("eventField_$index"),
                )
            }
        }

        if (errorText.isNotEmpty()) {
            Text(errorText, modifier = Modifier.padding(top = 8.dp).testTag("eventBuilderError"))
        }

        Button(
            enabled = !generating,
            onClick = {
                Log.i(TAG, "USER_TAP: Generate DataMatrix pressed for event=$selectedOp")
                generating = true
                errorText = ""
                scope.launch {
                    try {
                        val raw = withContext(Dispatchers.IO) {
                            val fieldsMap = fields.mapIndexed { index, field ->
                                field.key to field.encode(fieldValues[index].ifBlank { field.blankValue })
                            }.toMap()
                            val json = JSONObject().apply {
                                put("event", selectedOp)
                                put("fields", JSONObject(fieldsMap))
                            }.toString()
                            Kvmobile.encodeEvent(json)
                        }
                        generatedMatrix = withContext(Dispatchers.Default) { DataMatrixCodec.encode(raw) }
                        Log.w(TAG, "ACTION_REQUIRED: DataMatrix for event=$selectedOp is on screen -- scan it with the other device's camera now")
                    } catch (e: Exception) {
                        Log.w(TAG, "RESULT: Generate DataMatrix FAILED for event=$selectedOp: ${e.message}")
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
            title = "Scan this to trigger \"$selectedOp\"",
        )
    }
}
