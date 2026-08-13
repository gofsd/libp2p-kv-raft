package com.gofsd.kvdemo

import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.lazy.rememberLazyListState
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.automirrored.filled.List
import androidx.compose.material.icons.filled.QrCode2
import androidx.compose.material.icons.filled.Refresh
import androidx.compose.material.icons.filled.Tag
import androidx.compose.material3.AlertDialog
import androidx.compose.material3.Card
import androidx.compose.material3.CardDefaults
import androidx.compose.material3.Icon
import androidx.compose.material3.IconButton
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.DisposableEffect
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateListOf
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.vector.ImageVector
import androidx.compose.ui.platform.LocalClipboardManager
import androidx.compose.ui.platform.testTag
import androidx.compose.ui.text.AnnotatedString
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.unit.dp
import com.google.zxing.common.BitMatrix
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Locale

/**
 * Shows OutputLog's full history (see that file's doc comment) -- every
 * WatchExecute/WatchCommandLog/RunCommandDispatcher notification delivered
 * since this process started, plus every command actually executed
 * anywhere in the app via CommandExecutor, regardless of which screen, if
 * any, was open when it happened -- as a list of status-colored rows
 * (object-history-app's CommandLogScreen/CommandLogItem styling: SUCCESS
 * green, FAILED red, plain INFO neutral), tap to expand a row's full body.
 * Live updates for anything recorded while this screen itself is composed
 * (see [DisposableEffect] below).
 *
 * [focusedLogId] (set by AppRoot right after a scan-triggered execution
 * completes, see RunConfirmDialog's onConfirm) scrolls to and starts that
 * one row expanded, then is cleared via [onFocusedLogConsumed] -- a direct
 * port of object-history-app's CommandLogScreen focusedLogId handling.
 */
@Composable
fun LogScreen(
    focusedLogId: Long?,
    onFocusedLogConsumed: () -> Unit,
    onRepeat: (LogEntry) -> Unit,
    onEnterGroup: (String) -> Unit,
) {
    val entries = remember { mutableStateListOf<LogEntry>().apply { addAll(OutputLog.snapshot()) } }
    val scope = rememberCoroutineScope()
    val listState = rememberLazyListState()

    DisposableEffect(Unit) {
        // OutputLog's callbacks -- and so this listener -- can land on
        // arbitrary threads (see OutputLog's doc comment), so hop back to
        // Main before touching Compose state.
        OutputLog.setListener { entry ->
            scope.launch(Dispatchers.Main) { entries.add(entry) }
        }
        onDispose { OutputLog.setListener(null) }
    }

    LaunchedEffect(focusedLogId, entries.size) {
        val id = focusedLogId ?: return@LaunchedEffect
        val index = entries.indexOfFirst { it.id == id }
        if (index >= 0) {
            listState.animateScrollToItem(index)
        }
        onFocusedLogConsumed()
    }

    Column(modifier = Modifier.fillMaxSize().padding(16.dp).testTag("screen_log")) {
        Text(
            "Activity Log",
            style = MaterialTheme.typography.titleLarge,
            modifier = Modifier.padding(bottom = 12.dp),
        )
        LazyColumn(state = listState, modifier = Modifier.fillMaxSize().testTag("logList")) {
            items(entries, key = { it.id }) { entry ->
                LogRow(
                    entry = entry,
                    startExpanded = entry.id == focusedLogId,
                    onRepeat = onRepeat,
                    onEnterGroup = onEnterGroup,
                )
            }
        }
    }
}

@Composable
private fun LogRow(
    entry: LogEntry,
    startExpanded: Boolean,
    onRepeat: (LogEntry) -> Unit,
    onEnterGroup: (String) -> Unit,
) {
    var expanded by remember(entry.id) { mutableStateOf(startExpanded) }
    val timeFormat = remember { SimpleDateFormat("HH:mm:ss", Locale.getDefault()) }
    val hasSpec = entry.category.isNotBlank() && entry.name.isNotBlank()

    Card(
        modifier = Modifier
            .fillMaxWidth()
            .padding(vertical = 4.dp)
            .testTag("log_item_${entry.id}"),
        colors = CardDefaults.cardColors(containerColor = statusColor(entry.status)),
        onClick = { expanded = !expanded },
    ) {
        Column(modifier = Modifier.padding(12.dp)) {
            Text(
                "${timeFormat.format(Date(entry.timestamp))}  ${entry.title}",
                color = Color.Black,
                fontFamily = FontFamily.Monospace,
                modifier = Modifier.testTag("log_title_${entry.id}"),
            )
            if (expanded && entry.body.isNotEmpty()) {
                Text(
                    entry.body,
                    color = Color.Black,
                    fontFamily = FontFamily.Monospace,
                    modifier = Modifier.padding(top = 6.dp).testTag("log_body_${entry.id}"),
                )
            }
            if (hasSpec) {
                Row(modifier = Modifier.padding(top = 4.dp)) {
                    LogActionButton(
                        icon = Icons.Filled.Refresh,
                        contentDescription = "Repeat",
                        testTag = "log_repeat_${entry.id}",
                        onClick = { onRepeat(entry) },
                    )
                    ExecutionIdButton(entry)
                    GroupQrButton(entry)
                    LogActionButton(
                        icon = Icons.AutoMirrored.Filled.List,
                        contentDescription = "Group",
                        testTag = "log_group_${entry.id}",
                        onClick = { onEnterGroup(entry.category) },
                    )
                }
            }
        }
    }
}

@Composable
private fun LogActionButton(
    icon: ImageVector,
    contentDescription: String,
    testTag: String,
    onClick: () -> Unit,
) {
    IconButton(onClick = onClick, modifier = Modifier.size(44.dp).testTag(testTag)) {
        Icon(icon, contentDescription = contentDescription)
    }
}

/**
 * Shows this entry's log sequence number (labeled honestly, not a server-side execution/dispatch
 * id -- most commands have no such id at all, only some Dispatch and ExecInvite results carry one
 * inside their own result text), with a Copy action.
 */
@Composable
private fun ExecutionIdButton(entry: LogEntry) {
    var showDialog by remember(entry.id) { mutableStateOf(false) }
    val clipboard = LocalClipboardManager.current

    LogActionButton(
        icon = Icons.Filled.Tag,
        contentDescription = "Execution ID",
        testTag = "log_execid_${entry.id}",
        onClick = { showDialog = true },
    )
    if (showDialog) {
        AlertDialog(
            onDismissRequest = { showDialog = false },
            title = { Text("Execution ID") },
            text = { Text("Log #${entry.id} -- a local log sequence number, not a server-side execution/dispatch id.") },
            confirmButton = {
                TextButton(
                    onClick = {
                        clipboard.setText(AnnotatedString("Log #${entry.id}"))
                        showDialog = false
                    },
                    modifier = Modifier.testTag("log_execid_copy_${entry.id}"),
                ) { Text("Copy") }
            },
            dismissButton = {
                TextButton(onClick = { showDialog = false }) { Text("Close") }
            },
        )
    }
}

/**
 * Generates a [NavCode.Group] DataMatrix for this entry's category -- the only remaining place
 * this code can be generated from, now that GroupPickerScreen's own Submit enters a group instead
 * of generating a code for it.
 */
@Composable
private fun GroupQrButton(entry: LogEntry) {
    var matrix by remember(entry.id) { mutableStateOf<BitMatrix?>(null) }
    var error by remember(entry.id) { mutableStateOf<String?>(null) }

    LogActionButton(
        icon = Icons.Filled.QrCode2,
        contentDescription = "Group QR",
        testTag = "log_groupqr_${entry.id}",
        onClick = {
            matrix = try {
                DataMatrixCodec.encode(DataMatrixCodec.textToBytes(NavCode.encodeGroup(entry.category)), size = 220)
            } catch (e: Exception) {
                error = e.message
                null
            }
        },
    )
    matrix?.let {
        GeneratedDataMatrixDialog(
            matrix = it,
            onDismiss = { matrix = null },
            title = "Scan this to open \"${entry.category}\" on the other device",
        )
    }
    error?.let { msg ->
        AlertDialog(
            onDismissRequest = { error = null },
            title = { Text("Failed to generate code") },
            text = { Text(msg) },
            confirmButton = { TextButton(onClick = { error = null }) { Text("Close") } },
        )
    }
}

private fun statusColor(status: LogStatus): Color = when (status) {
    LogStatus.INFO -> Color(0xFFE0E0E0)
    LogStatus.SUCCESS -> Color(0xFF4CAF50)
    LogStatus.FAILED -> Color(0xFFF44336)
}
