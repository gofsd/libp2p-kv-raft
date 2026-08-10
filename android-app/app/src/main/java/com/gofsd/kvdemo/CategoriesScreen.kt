package com.gofsd.kvdemo

import android.util.Log
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.material3.Button
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.remember
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.platform.testTag
import androidx.compose.ui.unit.dp

/**
 * Landing screen: lists every CommandCatalog.kt category. Tap one to
 * browse its commands (CommandListScreen -> CommandDetailScreen); tap
 * "Activity Log" to see every WatchExecute/WatchCommandLog/
 * RunCommandDispatcher notification delivered so far regardless of which
 * screen was open when it arrived (see OutputLog). Replaces the old
 * MainActivity 1:1 -- CommandCatalog.kt's CommandSpec list itself is
 * unchanged, just hosted in a NavHost route instead of an Activity.
 */
@Composable
fun CategoriesScreen(
    statusText: String,
    onCategoryClick: (String) -> Unit,
    onLogClick: () -> Unit,
) {
    val context = LocalContext.current
    val categories = remember {
        buildCommands(context.filesDir.absolutePath, OutputLog::append)
            .map { it.category }
            .distinct()
    }

    Column(
        modifier = Modifier
            .fillMaxSize()
            .padding(16.dp)
            .testTag("screen_categories"),
    ) {
        Text(statusText, modifier = Modifier.padding(bottom = 12.dp).testTag("statusText"))

        LazyColumn(modifier = Modifier.weight(1f).testTag("itemList")) {
            items(categories) { category ->
                Text(
                    category,
                    modifier = Modifier
                        .fillMaxWidth()
                        .clickable {
                            Log.i("KVDemo", "USER_TAP: category $category opened")
                            onCategoryClick(category)
                        }
                        .padding(vertical = 12.dp)
                        .testTag("listItem_$category"),
                )
            }
        }

        Button(
            onClick = onLogClick,
            modifier = Modifier.fillMaxWidth().padding(top = 8.dp).testTag("logButton"),
        ) {
            Text("Activity Log")
        }
    }
}
