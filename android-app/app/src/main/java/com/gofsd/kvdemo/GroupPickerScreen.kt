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
 * "Groups": the Default group's other pseudo-item (see GroupPageScreen) -- pick one
 * CommandCatalog.kt category (Cluster, KV, Test, ...) and Submit to *enter* it: [onSubmit] sets
 * the app's current group (hoisted in AppRoot) to that category, so the pager's page 0 now shows
 * that category's own commands instead of the Default group's two pseudo-items. Not kvmobile's
 * daemon-backed Group/Command ACL catalog (see README's "Group/command ACL" section for that,
 * separate, feature); "group" here means a CommandCatalog.kt category, purely a UI grouping.
 *
 * This screen no longer generates a [NavCode.Group] DataMatrix code itself -- that capability now
 * lives only on an Activity Log row's "Group QR" button (see LogScreen.kt), once a log entry
 * exists in that category to hang the button off of.
 */
@Composable
fun GroupPickerScreen(onSubmit: (category: String) -> Unit) {
    val context = LocalContext.current
    val categories = remember {
        buildCommands(context.filesDir.absolutePath, OutputLog::append).map { it.category }.distinct()
    }
    var selectedCategory by remember { mutableStateOf<String?>(null) }

    Column(
        modifier = Modifier.fillMaxSize().padding(16.dp).testTag("screen_group_picker"),
    ) {
        Text(
            "Open a group",
            style = MaterialTheme.typography.titleLarge,
            modifier = Modifier.padding(bottom = 12.dp),
        )

        SelectDropdown(
            label = "Group",
            options = categories,
            selected = selectedCategory,
            onSelect = { selectedCategory = it },
            testTag = "groupPickerSelect",
        )

        Button(
            enabled = selectedCategory != null,
            onClick = {
                val category = selectedCategory ?: return@Button
                Log.i(TAG, "USER_TAP: Submit pressed for group $category")
                onSubmit(category)
            },
            modifier = Modifier.fillMaxWidth().padding(top = 16.dp).testTag("groupPickerSubmit"),
        ) {
            Text("Submit")
        }
    }
}
