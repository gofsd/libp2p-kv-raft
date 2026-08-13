package com.gofsd.kvdemo

import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.material3.Button
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedButton
import androidx.compose.material3.Text
import androidx.compose.material3.pulltorefresh.PullToRefreshBox
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.testTag
import androidx.compose.ui.unit.dp
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch

/** The two things a pull-to-refresh on this screen can retrieve -- see [MainListScreen]. */
private data class MainListItem(val id: String, val label: String)

private val MAIN_LIST_ITEMS = listOf(
    MainListItem("commands", "Commands"),
    MainListItem("groups", "Groups"),
)

/**
 * The app's start screen: a pull-to-refresh list of this app's navigation shortcuts -- not live
 * data, but pull-to-refresh is still wired up for real (a brief spinner, then the same rows again)
 * since that gesture, not a static menu, is the interaction this screen is modeled on.
 *
 * "Commands" opens [CommandPickerScreen] (pick any single command from the full catalog, jump
 * straight to its own CommandDetailScreen to fill params and Generate a [RunCode] -- the only way
 * any command executes now, see AppRoot's scan dispatch). "Groups" opens [GroupPickerScreen] (pick
 * a CommandCatalog.kt category, Generate a [NavCode.Group] code that jumps to that category's
 * command list when scanned -- pure navigation, executes nothing).
 *
 * The full category browser (CategoriesScreen) is unchanged and still reachable -- just no longer
 * the first thing shown -- via [onBrowseCategories].
 */
@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun MainListScreen(
    onCommandsClick: () -> Unit,
    onGroupsClick: () -> Unit,
    onBrowseCategories: () -> Unit,
    onLogClick: () -> Unit,
) {
    var isRefreshing by remember { mutableStateOf(false) }
    val scope = rememberCoroutineScope()

    Column(modifier = Modifier.fillMaxSize().padding(16.dp).testTag("screen_main")) {
        Text("libp2p-kv-raft", style = MaterialTheme.typography.titleLarge)

        PullToRefreshBox(
            isRefreshing = isRefreshing,
            onRefresh = {
                scope.launch {
                    isRefreshing = true
                    delay(300)
                    isRefreshing = false
                }
            },
            modifier = Modifier.weight(1f).fillMaxWidth(),
        ) {
            LazyColumn(modifier = Modifier.fillMaxSize().testTag("mainItemList")) {
                items(MAIN_LIST_ITEMS) { item ->
                    Text(
                        item.label,
                        modifier = Modifier
                            .fillMaxWidth()
                            .clickable {
                                when (item.id) {
                                    "commands" -> onCommandsClick()
                                    "groups" -> onGroupsClick()
                                }
                            }
                            .padding(vertical = 16.dp)
                            .testTag("mainListItem_${item.id}"),
                    )
                }
            }
        }

        OutlinedButton(
            onClick = onBrowseCategories,
            modifier = Modifier.fillMaxWidth().padding(top = 8.dp).testTag("browseCategories"),
        ) {
            Text("Browse all categories")
        }
        Button(
            onClick = onLogClick,
            modifier = Modifier.fillMaxWidth().padding(top = 8.dp).testTag("logButton"),
        ) {
            Text("Activity Log")
        }
    }
}
