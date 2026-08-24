package com.gofsd.kvdemo

import android.util.Base64
import android.util.Log
import androidx.activity.compose.rememberLauncherForActivityResult
import androidx.activity.result.contract.ActivityResultContracts
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.lazy.LazyRow
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.text.KeyboardOptions
import androidx.compose.foundation.verticalScroll
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.QrCodeScanner
import androidx.compose.material3.AssistChip
import androidx.compose.material3.Icon
import androidx.compose.material3.IconButton
import androidx.compose.material3.LocalContentColor
import androidx.compose.material3.LocalTextStyle
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedButton
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.DisposableEffect
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateListOf
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
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
 * One CommandSpec's own screen (see CommandCatalog.kt): an input row per parameter (see
 * [ParamField] for the scan/peer-chip/file affordances each one earns from its own label), a
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
 *
 * [initialParams] seeds the param fields instead of leaving them blank -- used by a log row's
 * Repeat button (see LogScreen.kt), which navigates here with the original run's own args via
 * [commandDetailRoute]'s optional query param.
 *
 * Two of this screen's inputs arrive through the camera rather than the keyboard, and both fill a
 * field *while the screen is already showing* rather than rebuilding it -- which is the whole point,
 * since rebuilding would discard every other field the person had already typed:
 *
 *  - a spec with [CommandSpec.logIdParam] set has that param filled from [ScannedLogRef], the log
 *    reference this device currently considers itself to be looking at, and re-filled every time
 *    another label for the same group is scanned (see [LogRefCode] and the LaunchedEffect below).
 *    Passive: the person scans a label, and whichever form is open reacts.
 *  - any non-multiline param can also be filled *deliberately*, by tapping its own scan button --
 *    which arms that field (see [PendingFieldScan]) so the next scan is diverted into it instead of
 *    being dispatched as a command to run. Active, one scan long, and the two cannot collide: an
 *    armed field is checked before every other branch of AppRoot's scan handling, log references
 *    included.
 */
private const val TAG = "KVDemo"

@Composable
fun CommandDetailScreen(
    category: String,
    name: String,
    initialParams: List<String> = emptyList(),
    onNavigateToLog: () -> Unit = {},
) {
    val context = LocalContext.current
    val spec = remember(category, name) {
        buildCommands(context.filesDir.absolutePath, OutputLog::append)
            .first { it.category == category && it.name == name }
    }

    val paramValues = remember(spec) {
        mutableStateListOf(*Array(spec.params.size) { i -> initialParams.getOrElse(i) { "" } })
    }

    // The log-reference field, filled and re-filled from whatever object the device is currently
    // looking at (see [ScannedLogRef] and [CommandSpec.logIdParam]). Keyed on the reference itself,
    // so it runs twice for two different reasons that look the same from here:
    //
    //  - this screen was just opened while a reference was already in force -- the person scanned
    //    a label, landed in its group, and tapped a command;
    //  - this screen was already open and a *new* label was scanned -- AppRoot leaves the
    //    navigation alone in that case precisely so this fires (see LogRefTarget.RefillOpenForm),
    //    and only this one field changes. Everything else the person has typed stays.
    //
    // Guarded on the group matching this spec's own category: AppRoot only leaves a form standing
    // for a same-group scan, but this composable outlives a single scan and must not seed itself
    // from a reference belonging to somewhere else.
    val scannedLogRef = ScannedLogRef.current
    LaunchedEffect(spec, scannedLogRef) {
        val index = spec.logIdParam ?: return@LaunchedEffect
        val ref = scannedLogRef ?: return@LaunchedEffect
        if (ref.group != spec.category) return@LaunchedEffect
        val value = ref.logId.toString()
        if (paramValues[index] == value) return@LaunchedEffect
        Log.i(TAG, "AUTO: filling ${spec.label} param_$index with scanned log reference $value")
        paramValues[index] = value
    }
    // A scan aimed at one specific field (see [PendingFieldScan] and AppRoot's first scan branch).
    // Applied the moment it arrives and immediately consumed, so recomposition -- or reopening this
    // same form later -- cannot reapply a stale one. Keyed on the event itself rather than on the
    // index, so scanning twice into the same field in a row (correcting a mis-scan) applies both.
    val fieldScan = PendingFieldScan.scanned
    LaunchedEffect(fieldScan) {
        val (index, value) = fieldScan ?: return@LaunchedEffect
        if (index !in paramValues.indices) return@LaunchedEffect
        paramValues[index] = value
        PendingFieldScan.scanned = null
    }
    // Whatever field this form armed stops meaning anything the moment the form goes away. Without
    // this, navigating back mid-scan would leave a field armed and the *next* scan -- on whatever
    // screen happens to be showing then -- would be swallowed into a param nobody can see, instead
    // of running the command it encodes.
    DisposableEffect(Unit) { onDispose { PendingFieldScan.disarm() } }

    // This cluster's live members, offered as one-tap chips on any peer-id param. Fetched only when
    // the spec actually has such a param: for the majority of this catalog that don't, the call
    // would be a pointless cluster read on every screen open.
    var peers by remember(spec) { mutableStateOf<List<String>>(emptyList()) }
    LaunchedEffect(spec) {
        if (spec.params.none(::isPeerParam)) return@LaunchedEffect
        val membersJson = runCatching {
            withContext(Dispatchers.IO) { Kvmobile.listClusterMembers() }
        }.getOrNull() ?: return@LaunchedEffect
        peers = runCatching {
            val members = JSONArray(membersJson)
            (0 until members.length())
                .map { members.getJSONObject(it).optString("peer_id") }
                .filter { it.isNotEmpty() }
        }.getOrDefault(emptyList())
    }

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
                ParamField(
                    index = index,
                    hint = hint,
                    value = paramValues[index],
                    onValueChange = { paramValues[index] = it },
                    multiline = spec.isMultiline(index),
                    peers = peers,
                    onNote = { line -> output = if (output.isEmpty()) line else "$output\n\n$line" },
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

/**
 * One parameter's input row: the text field itself, plus whichever of three affordances this
 * param's own label earns it. All three are ported from object-history-app's `CommandFormScreen`,
 * where they hang off explicit per-field metadata (`CommandFieldSpec.scannable`, `FieldKind.FILE`,
 * the literal `peerId` field name). Here they are derived from [hint] instead, and deliberately so:
 * `CommandCatalog.kt` holds 135 specs that are each one line long, and a change that made every one
 * of them declare a field kind would cost more than it explains -- the labels already say
 * "targetPeerID" and "chunk (base64)" because that is what those arguments are.
 *
 *  - **Scan** (every param but a multiline one -- scanning Lua source is meaningless): arms this
 *    field for the next scan (see [PendingFieldScan]) and opens the scanner. Tinted while armed,
 *    which is the only thing telling the person their next scan fills a field instead of running a
 *    command.
 *  - **Peer chips** ([isPeerParam]): the cluster's actual members, tap to fill. A peer id is 52
 *    characters that nobody types correctly by hand, and this screen has ~17 params that want one.
 *  - **Choose file** ([isBase64Param]): reads a file through the system document picker and
 *    base64-encodes it into the field -- today just `Channel: SendChannelData`'s chunk.
 */
@Composable
private fun ParamField(
    index: Int,
    hint: String,
    value: String,
    onValueChange: (String) -> Unit,
    multiline: Boolean,
    peers: List<String>,
    onNote: (String) -> Unit,
) {
    val context = LocalContext.current
    val scope = rememberCoroutineScope()
    val armed = PendingFieldScan.armedIndex == index

    Row(modifier = Modifier.fillMaxWidth(), verticalAlignment = Alignment.CenterVertically) {
        // A multiline param (Lua source, in practice -- see CommandSpec.multilineParams) gets a
        // tall monospace field: typing a script into a one-line field that scrolls sideways is
        // unusable, and Enter has to insert a newline rather than move to the next field.
        OutlinedTextField(
            value = value,
            onValueChange = onValueChange,
            label = { Text(hint) },
            singleLine = !multiline,
            minLines = if (multiline) 8 else 1,
            textStyle = if (multiline) {
                LocalTextStyle.current.copy(fontFamily = FontFamily.Monospace)
            } else {
                LocalTextStyle.current
            },
            keyboardOptions = KeyboardOptions(
                imeAction = if (multiline) ImeAction.Default else ImeAction.Next,
            ),
            modifier = Modifier
                .weight(1f)
                .padding(vertical = 4.dp)
                .testTag("param_$index"),
        )

        if (!multiline) {
            IconButton(
                onClick = {
                    Log.i(TAG, "USER_TAP: scan armed for param_$index (\"$hint\")")
                    PendingFieldScan.armedIndex = index
                    ScannerCoordinator.expanded = true
                },
                modifier = Modifier.testTag("scan_$index"),
            ) {
                Icon(
                    Icons.Filled.QrCodeScanner,
                    contentDescription = "Scan into $hint",
                    tint = if (armed) MaterialTheme.colorScheme.primary else LocalContentColor.current,
                )
            }
        }
    }

    if (isPeerParam(hint) && peers.isNotEmpty()) {
        LazyRow(
            horizontalArrangement = Arrangement.spacedBy(8.dp),
            modifier = Modifier.padding(bottom = 4.dp).testTag("peerChips_$index"),
        ) {
            items(peers) { peer ->
                AssistChip(
                    onClick = {
                        Log.i(TAG, "USER_TAP: peer chip $peer filled param_$index")
                        onValueChange(peer)
                    },
                    // The tail is what actually distinguishes two peer ids at a glance; they share
                    // a prefix. The chip fills the field with the whole thing regardless.
                    label = { Text("...${peer.takeLast(PEER_CHIP_TAIL)}") },
                    modifier = Modifier.testTag("peer_$peer"),
                )
            }
        }
    }

    if (isBase64Param(hint)) {
        val picker = rememberLauncherForActivityResult(ActivityResultContracts.OpenDocument()) { uri ->
            if (uri == null) return@rememberLauncherForActivityResult
            scope.launch {
                val bytes = runCatching {
                    withContext(Dispatchers.IO) { readAtMost(context, uri, MAX_CHUNK_BYTES + 1) }
                }.getOrElse { e ->
                    Log.w(TAG, "RESULT: reading $uri failed: ${e.message}")
                    onNote("Choose file FAILED: ${e.message}")
                    return@launch
                }
                // Refused rather than truncated: MAX_CHUNK_BYTES is exactly what pkg/chandata's
                // WriteChunk enforces on the other side, so a bigger pick could only ever fail
                // later, further away, and less legibly than it does here.
                if (bytes.size > MAX_CHUNK_BYTES) {
                    Log.w(TAG, "RESULT: picked file is larger than $MAX_CHUNK_BYTES bytes, refused")
                    onNote("Choose file FAILED: file is over ${MAX_CHUNK_BYTES / 1024} KiB, the maximum one chunk can carry")
                    return@launch
                }
                onValueChange(Base64.encodeToString(bytes, Base64.NO_WRAP))
                Log.i(TAG, "RESULT: param_$index filled with ${bytes.size} bytes of base64 from $uri")
            }
        }
        OutlinedButton(
            onClick = { picker.launch(arrayOf("*/*")) },
            modifier = Modifier.padding(bottom = 4.dp).testTag("pick_$index"),
        ) {
            Text("Choose file...")
        }
    }
}

/** Reads at most [limit] bytes of [uri], so an accidentally-picked gigabyte never lands in memory. */
private fun readAtMost(context: android.content.Context, uri: android.net.Uri, limit: Int): ByteArray {
    val stream = context.contentResolver.openInputStream(uri)
        ?: throw IllegalStateException("could not open $uri")
    return stream.use {
        val buf = ByteArray(limit)
        var n = 0
        while (n < limit) {
            val read = it.read(buf, n, limit - n)
            if (read <= 0) break
            n += read
        }
        buf.copyOf(n)
    }
}

/** Whether a param label names a peer id -- covers peerID, targetPeerID, destPeerID, requesterPeerID. */
private fun isPeerParam(hint: String): Boolean = hint.contains("peer", ignoreCase = true)

/** Whether a param label says it wants base64 -- today only Channel: SendChannelData's chunk. */
private fun isBase64Param(hint: String): Boolean = hint.contains("base64", ignoreCase = true)

/** A peer id's tail is what distinguishes two of them at a glance; the prefix is shared. */
private const val PEER_CHIP_TAIL = 8

/**
 * One chunk's ceiling, mirroring `pkg/chandata.MaxChunkSize` (256 KiB) -- the limit `WriteChunk`
 * rejects above, and so the largest file worth base64-ing into a chunk param.
 */
private const val MAX_CHUNK_BYTES = 256 * 1024
