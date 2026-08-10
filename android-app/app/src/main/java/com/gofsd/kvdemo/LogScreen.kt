package com.gofsd.kvdemo

import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.material3.Card
import androidx.compose.material3.CardDefaults
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.DisposableEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateListOf
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.platform.testTag
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.unit.dp
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Locale

/**
 * Shows OutputLog's full history (see that file's doc comment) -- every
 * WatchExecute/WatchCommandLog/RunCommandDispatcher notification delivered
 * since this process started, plus every CommandDetailScreen Run's own
 * result, regardless of which screen, if any, was open when it happened --
 * as a list of status-colored rows (object-history-app's CommandLogScreen/
 * CommandLogItem styling: SUCCESS green, FAILED red, plain INFO neutral),
 * tap to expand a row's full body. Live updates for anything recorded while
 * this screen itself is composed (see [DisposableEffect] below).
 */
@Composable
fun LogScreen() {
    val entries = remember { mutableStateListOf<LogEntry>().apply { addAll(OutputLog.snapshot()) } }
    val scope = rememberCoroutineScope()

    DisposableEffect(Unit) {
        // OutputLog's callbacks -- and so this listener -- can land on
        // arbitrary threads (see OutputLog's doc comment), so hop back to
        // Main before touching Compose state.
        OutputLog.setListener { entry ->
            scope.launch(Dispatchers.Main) { entries.add(entry) }
        }
        onDispose { OutputLog.setListener(null) }
    }

    Column(modifier = Modifier.fillMaxSize().padding(16.dp).testTag("screen_log")) {
        Text(
            "Activity Log",
            style = MaterialTheme.typography.titleLarge,
            modifier = Modifier.padding(bottom = 12.dp),
        )
        LazyColumn(modifier = Modifier.fillMaxSize().testTag("logList")) {
            items(entries, key = { it.id }) { entry -> LogRow(entry) }
        }
    }
}

@Composable
private fun LogRow(entry: LogEntry) {
    var expanded by remember(entry.id) { mutableStateOf(false) }
    val timeFormat = remember { SimpleDateFormat("HH:mm:ss", Locale.getDefault()) }

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
        }
    }
}

private fun statusColor(status: LogStatus): Color = when (status) {
    LogStatus.INFO -> Color(0xFFE0E0E0)
    LogStatus.SUCCESS -> Color(0xFF4CAF50)
    LogStatus.FAILED -> Color(0xFFF44336)
}
