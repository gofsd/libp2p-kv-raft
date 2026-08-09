package com.gofsd.kvdemo

import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.DisposableEffect
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.testTag
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.unit.dp
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch

/**
 * Shows OutputLog's full history (see that file's doc comment) -- every
 * WatchExecute/WatchCommandLog/RunCommandDispatcher notification delivered
 * since this process started, regardless of which CommandDetailScreen, if
 * any, was open when it arrived -- plus live updates for anything that
 * arrives while this screen itself is composed (see [DisposableEffect]
 * below, this route's equivalent of the old ActivityLogActivity's
 * onResume/onPause listener registration). Replaces that Activity 1:1.
 */
@Composable
fun LogScreen() {
    var logText by remember { mutableStateOf(OutputLog.snapshot().joinToString("\n\n")) }
    val scrollState = rememberScrollState()
    val scope = rememberCoroutineScope()

    DisposableEffect(Unit) {
        // OutputLog's callbacks -- and so this listener -- can land on
        // arbitrary threads (see OutputLog's doc comment), so hop back to
        // Main before touching Compose state, the same care the old
        // ActivityLogActivity took via runOnUiThread.
        OutputLog.setListener { line ->
            scope.launch(Dispatchers.Main) {
                logText = if (logText.isEmpty()) line else "$logText\n\n$line"
            }
        }
        onDispose { OutputLog.setListener(null) }
    }

    LaunchedEffect(logText) {
        scrollState.animateScrollTo(scrollState.maxValue)
    }

    Column(modifier = Modifier.fillMaxSize().padding(16.dp).testTag("screen_log")) {
        Text(
            "Activity Log",
            style = MaterialTheme.typography.titleLarge,
            modifier = Modifier.padding(bottom = 12.dp),
        )
        Column(
            modifier = Modifier
                .fillMaxSize()
                .verticalScroll(scrollState)
                .testTag("logScroll"),
        ) {
            Text(logText, fontFamily = FontFamily.Monospace, modifier = Modifier.testTag("logText"))
        }
    }
}
