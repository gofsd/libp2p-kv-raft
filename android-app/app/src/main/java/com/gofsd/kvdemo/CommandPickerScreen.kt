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

private const val TAG = "KVDemo"

/**
 * "Commands": one of the Default group's two pseudo-items (see GroupPageScreen) -- pick any
 * single command from the app's full flat catalog (every category's every CommandSpec, see
 * CommandCatalog.kt) via a search-filterable select, Submit jumps straight to its own
 * CommandDetailScreen. A same-device navigation shortcut, nothing more -- generates no code of
 * its own: CommandDetailScreen's own "Generate DataMatrix" button (a [RunCode], the only way any
 * command executes now) is reached from there, the same as entering that command's group and
 * tapping it in its own command list would leave you. [SearchableSelectDropdown], not the plain
 * [SelectDropdown], since the flat catalog is ~100+ entries -- unusable in an unfilterable menu.
 */
@Composable
fun CommandPickerScreen(onNavigateToDetail: (category: String, name: String) -> Unit) {
    val context = LocalContext.current
    val commands = remember {
        buildCommands(context.filesDir.absolutePath, OutputLog::append)
    }
    var selectedLabel by remember { mutableStateOf<String?>(null) }

    Column(
        modifier = Modifier.fillMaxSize().padding(16.dp).testTag("screen_command_picker"),
    ) {
        Text(
            "Open a command",
            style = MaterialTheme.typography.titleLarge,
            modifier = Modifier.padding(bottom = 12.dp),
        )

        SearchableSelectDropdown(
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
                Log.i(TAG, "USER_TAP: Submit pressed for ${spec.label}")
                onNavigateToDetail(spec.category, spec.name)
            },
            modifier = Modifier.fillMaxWidth().padding(top = 16.dp).testTag("commandPickerOpen"),
        ) {
            Text("Submit")
        }
    }
}
