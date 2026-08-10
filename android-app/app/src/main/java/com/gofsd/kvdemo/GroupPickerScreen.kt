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
import com.google.zxing.common.BitMatrix

private const val TAG = "KVDemo"

/**
 * "Groups": pick one CommandCatalog.kt category (Cluster, KV, Test, ...) and Generate a
 * [NavCode.Group] DataMatrix code for it. Another device scanning that code (see AppRoot's scan
 * dispatch) jumps straight to that category's own CommandListScreen -- the same screen tapping
 * that category from CategoriesScreen would open -- instead of navigating there by hand. Not
 * kvmobile's daemon-backed Group/Command ACL catalog (see README's "Group/command ACL" section for
 * that, separate, feature); "group" here means a CommandCatalog.kt category, purely a UI grouping.
 */
@Composable
fun GroupPickerScreen() {
    val context = LocalContext.current
    val categories = remember {
        buildCommands(context.filesDir.absolutePath, OutputLog::append).map { it.category }.distinct()
    }
    var selectedCategory by remember { mutableStateOf<String?>(null) }
    var generatedMatrix by remember { mutableStateOf<BitMatrix?>(null) }
    var error by remember { mutableStateOf<String?>(null) }

    Column(
        modifier = Modifier.fillMaxSize().padding(16.dp).testTag("screen_group_picker"),
    ) {
        Text(
            "Generate a group shortcut",
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
                Log.i(TAG, "USER_TAP: Generate pressed for group shortcut $category")
                error = null
                generatedMatrix = try {
                    val raw = DataMatrixCodec.textToBytes(NavCode.encodeGroup(category))
                    DataMatrixCodec.encode(raw, size = 220)
                } catch (e: Exception) {
                    Log.w(TAG, "RESULT: generate group shortcut FAILED: ${e.message}")
                    error = e.message
                    null
                }
            },
            modifier = Modifier.fillMaxWidth().padding(top = 16.dp).testTag("groupPickerGenerate"),
        ) {
            Text("Generate")
        }

        error?.let { Text("Failed to generate code: $it", modifier = Modifier.padding(top = 8.dp)) }

        generatedMatrix?.let { matrix ->
            GeneratedDataMatrixDialog(
                matrix = matrix,
                onDismiss = { generatedMatrix = null },
                title = "Scan this to open \"$selectedCategory\" on the other device",
            )
        }
    }
}
