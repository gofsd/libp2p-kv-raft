package com.gofsd.kvdemo

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
 * Every CommandSpec (see CommandCatalog.kt) in the category
 * CategoriesScreen was tapped for; tap one to open its own
 * CommandDetailScreen. Rebuilds the full command list itself rather than
 * having it passed in -- CommandSpec.run is a closure, which a nav
 * argument can't carry, and buildCommands is cheap/side-effect-free to
 * call again (nothing in it touches kvmobile until a Run button is
 * actually tapped on CommandDetailScreen). Replaces the old
 * CommandListActivity 1:1.
 */
@Composable
fun CommandListScreen(category: String, onCommandClick: (String) -> Unit) {
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
                        .clickable { onCommandClick(name) }
                        .padding(vertical = 12.dp)
                        .testTag("listItem_$name"),
                )
            }
        }
    }
}
