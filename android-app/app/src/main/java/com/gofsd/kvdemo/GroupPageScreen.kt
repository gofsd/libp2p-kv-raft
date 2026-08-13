package com.gofsd.kvdemo

import android.util.Log
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.remember
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.platform.testTag
import androidx.compose.ui.unit.dp

/**
 * Page 0 of the swipeable pager (see [CommandsPagerScreen]): the current group's own command
 * list. [DEFAULT_GROUP], the app's starting group, isn't a real CommandCatalog.kt category -- it
 * shows exactly two pseudo-command items, "Commands" and "Groups", each opening a generic
 * picker-form (select + submit, the same mechanic a real command's own params use) instead of any
 * command of its own: [CommandPickerScreen] (pick any command in the whole catalog, jump to its
 * form) and [GroupPickerScreen] (pick a category, enter it). Any other group is a real category:
 * its own commands, tap one -> CommandDetailScreen -- the same rendering CommandListScreen had
 * before this pager rewrite, tags preserved on purpose.
 */
@Composable
fun GroupPageScreen(
    group: String,
    onCommandClick: (name: String) -> Unit,
    onOpenCommandsPicker: () -> Unit,
    onOpenGroupsPicker: () -> Unit,
) {
    if (group == DEFAULT_GROUP) {
        DefaultGroupList(onOpenCommandsPicker, onOpenGroupsPicker)
    } else {
        CategoryCommandList(group, onCommandClick)
    }
}

@Composable
private fun DefaultGroupList(onOpenCommandsPicker: () -> Unit, onOpenGroupsPicker: () -> Unit) {
    Column(
        modifier = Modifier.fillMaxSize().padding(16.dp).testTag("screen_default_group"),
    ) {
        Text(
            DEFAULT_GROUP,
            style = MaterialTheme.typography.titleLarge,
            modifier = Modifier.padding(bottom = 12.dp),
        )

        LazyColumn(modifier = Modifier.fillMaxSize().testTag("mainItemList")) {
            item {
                Text(
                    "Commands",
                    modifier = Modifier
                        .fillMaxWidth()
                        .clickable {
                            Log.i("KVDemo", "USER_TAP: Commands opened")
                            onOpenCommandsPicker()
                        }
                        .padding(vertical = 16.dp)
                        .testTag("mainListItem_commands"),
                )
            }
            item {
                Text(
                    "Groups",
                    modifier = Modifier
                        .fillMaxWidth()
                        .clickable {
                            Log.i("KVDemo", "USER_TAP: Groups opened")
                            onOpenGroupsPicker()
                        }
                        .padding(vertical = 16.dp)
                        .testTag("mainListItem_groups"),
                )
            }
        }
    }
}

@Composable
private fun CategoryCommandList(category: String, onCommandClick: (String) -> Unit) {
    val context = LocalContext.current
    val names = remember(category) {
        buildCommands(context.filesDir.absolutePath, OutputLog::append)
            .filter { it.category == category }
            .map { it.name }
    }

    Column(
        modifier = Modifier
            .fillMaxSize()
            .padding(16.dp)
            .testTag("screen_commands"),
    ) {
        Text(
            category,
            style = MaterialTheme.typography.titleLarge,
            modifier = Modifier.padding(bottom = 12.dp).testTag("categoryTitle"),
        )

        LazyColumn(modifier = Modifier.fillMaxSize().testTag("itemList")) {
            items(names) { name ->
                Text(
                    name,
                    modifier = Modifier
                        .fillMaxWidth()
                        .clickable {
                            Log.i("KVDemo", "USER_TAP: command $category: $name opened")
                            onCommandClick(name)
                        }
                        .padding(vertical = 12.dp)
                        .testTag("listItem_$name"),
                )
            }
        }
    }
}
