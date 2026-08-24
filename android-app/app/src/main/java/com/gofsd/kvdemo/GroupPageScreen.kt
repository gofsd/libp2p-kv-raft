package com.gofsd.kvdemo

import android.util.Log
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.PaddingValues
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.automirrored.filled.List
import androidx.compose.material.icons.filled.Add
import androidx.compose.material.icons.filled.Folder
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Text
import androidx.compose.material3.pulltorefresh.PullToRefreshBox
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableIntStateOf
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.platform.testTag
import androidx.compose.ui.unit.dp
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kvmobile.Kvmobile
import org.json.JSONArray
import org.json.JSONObject

/**
 * Page 0 of the swipeable pager (see [CommandsPagerScreen]): the current group's own command
 * list. [DEFAULT_GROUP], the app's starting group, isn't a real CommandCatalog.kt category -- it
 * shows exactly two pseudo-command items, "Commands" and "Groups", each opening a generic
 * picker-form (select + submit, the same mechanic a real command's own params use) instead of any
 * command of its own: [CommandPickerScreen] (pick any command in the whole catalog, jump to its
 * form) and [GroupPickerScreen] (pick a category, enter it). Any other group is a real category:
 * its own commands, tap one -> CommandDetailScreen.
 *
 * Both lists fill the page: no heading, no outer padding, rows drawn by [CommandListItem] and
 * spaced by their own cards. The category's name is not repeated here -- it is on the pager's own
 * group context bar, one row above (see [CommandsPagerScreen]), which is also where the
 * `categoryTitle` tag the e2e harness reads now lives.
 */
@Composable
fun GroupPageScreen(
    group: String,
    onCommandClick: (name: String) -> Unit,
    onOpenCommandsPicker: () -> Unit,
    onOpenGroupsPicker: () -> Unit,
    onOpenLuaEditor: () -> Unit = {},
    onOpenLuaRun: (commandID: String) -> Unit = {},
) {
    if (group == DEFAULT_GROUP) {
        DefaultGroupList(onOpenCommandsPicker, onOpenGroupsPicker)
    } else {
        CategoryCommandList(group, onCommandClick, onOpenLuaEditor, onOpenLuaRun)
    }
}

@Composable
private fun DefaultGroupList(onOpenCommandsPicker: () -> Unit, onOpenGroupsPicker: () -> Unit) {
    // Two nested nodes, not one, purely so both tags survive: Modifier.testTag() sets a single
    // semantics property, so chaining two of them on one node silently keeps only the last.
    Column(modifier = Modifier.fillMaxSize().testTag("screen_default_group")) {
        LazyColumn(
            modifier = Modifier.fillMaxSize().testTag("mainItemList"),
            contentPadding = PaddingValues(vertical = 4.dp),
        ) {
            item {
                CommandListItem(
                    title = "Commands",
                    subtitle = "open any command in the catalog",
                    icon = Icons.AutoMirrored.Filled.List,
                    testTag = "mainListItem_commands",
                    onClick = {
                        Log.i("KVDemo", "USER_TAP: Commands opened")
                        onOpenCommandsPicker()
                    },
                )
            }
            item {
                CommandListItem(
                    title = "Groups",
                    subtitle = "enter one of the catalog's categories",
                    icon = Icons.Filled.Folder,
                    testTag = "mainListItem_groups",
                    onClick = {
                        Log.i("KVDemo", "USER_TAP: Groups opened")
                        onOpenGroupsPicker()
                    },
                )
            }
        }
    }
}

/**
 * One category's commands.
 *
 * [refreshKey] is what pull-to-refresh moves. It re-keys both the local spec list and, for the Lua
 * category, [LuaClusterCommands]' own cluster read -- the one list in this app fed by a live
 * `Kvmobile.listCommands()` call, which until now could only be re-run by leaving the group and
 * coming back. Rebuilding the local specs alongside it is nearly free and keeps "pull down to get
 * the current state of this screen" true of the whole screen rather than half of it.
 */
@OptIn(ExperimentalMaterial3Api::class)
@Composable
private fun CategoryCommandList(
    category: String,
    onCommandClick: (String) -> Unit,
    onOpenLuaEditor: () -> Unit = {},
    onOpenLuaRun: (String) -> Unit = {},
) {
    val context = LocalContext.current
    var refreshKey by remember(category) { mutableIntStateOf(0) }
    // Only ever true for Lua: that is the one category whose refresh does real asynchronous work
    // (LuaClusterCommands' cluster read) and so has something for a spinner to wait on.
    // Everywhere else the rebuild below is synchronous and complete by the time this recomposes,
    // so arming the spinner at all would only ever draw one that is already stale.
    var refreshing by remember(category) { mutableStateOf(false) }

    val specs = remember(category, refreshKey) {
        buildCommands(context.filesDir.absolutePath, OutputLog::append).filter { it.category == category }
    }

    PullToRefreshBox(
        isRefreshing = refreshing,
        onRefresh = {
            Log.i("KVDemo", "USER_TAP: pull-to-refresh on group $category")
            refreshing = category == "Lua"
            refreshKey++
        },
        modifier = Modifier.fillMaxSize().testTag("screen_commands"),
    ) {
        LazyColumn(
            modifier = Modifier.fillMaxSize().testTag("itemList"),
            contentPadding = PaddingValues(vertical = 4.dp),
        ) {
            // The Lua group leads with the one thing in this app that cannot be reached by
            // scanning a code: writing a script (see LuaEditorScreen's doc comment on why
            // authoring is the exception to scan-only execution). Everything below it is an
            // ordinary spec, opened and generated like any other.
            if (category == "Lua") {
                item {
                    CommandListItem(
                        title = "New Lua command...",
                        subtitle = "write a script on this device",
                        icon = Icons.Filled.Add,
                        testTag = "listItem_newLuaCommand",
                        onClick = {
                            Log.i("KVDemo", "USER_TAP: Lua editor opened")
                            onOpenLuaEditor()
                        },
                    )
                }
            }
            items(specs, key = { it.name }) { spec ->
                CommandListItem(
                    title = spec.name,
                    subtitle = commandSubtitle(spec),
                    icon = iconForCategory(spec.category),
                    testTag = "listItem_${spec.name}",
                    onClick = {
                        Log.i("KVDemo", "USER_TAP: command $category: ${spec.name} opened")
                        onCommandClick(spec.name)
                    },
                )
            }
            if (category == "Lua") {
                item {
                    LuaClusterCommands(
                        refreshKey = refreshKey,
                        onLoaded = { refreshing = false },
                        onOpenLuaRun = onOpenLuaRun,
                    )
                }
            }
        }
    }
}

/**
 * The Lua commands that actually exist on this cluster, under the local specs that operate on
 * them.
 *
 * The list above this one is the app's own API surface -- Put, Run, Serve and so on, the same on
 * every device. This one is the cluster's: whichever commands somebody has registered, with the
 * device each runs on. Without it there is no way to find out what can be run except by asking
 * another device, which for a feature whose whole point is that anyone can add a command is the
 * wrong way round.
 *
 * Tapping one opens "Lua: Run" with that command id already filled in -- a form, not an
 * execution. Running still means generating a code here and scanning it on another device, the
 * same as every other command in this app (see LuaEditorScreen's doc comment on why authoring is
 * the single exception to that).
 *
 * [refreshKey] re-runs the read; [onLoaded] tells [CategoryCommandList]'s pull-to-refresh spinner
 * that the read it triggered has finished, success or failure.
 */
@Composable
private fun LuaClusterCommands(
    refreshKey: Int,
    onLoaded: () -> Unit,
    onOpenLuaRun: (String) -> Unit,
) {
    var commands by remember { mutableStateOf<List<Pair<String, String>>>(emptyList()) }
    var note by remember { mutableStateOf("") }

    LaunchedEffect(refreshKey) {
        runCatching {
            val raw = withContext(Dispatchers.IO) { Kvmobile.listCommands() }
            val arr = JSONArray(raw)
            (0 until arr.length()).mapNotNull { i ->
                val obj = arr.getJSONObject(i)
                val spec = obj.optString("spec")
                // A Lua command is one whose spec says so -- the same test the runner itself
                // applies (examples/luacmd.ParseSpec). Everything else in the catalog is
                // somebody else's command and none of this screen's business.
                val isLua = spec.isNotBlank() &&
                    runCatching { JSONObject(spec).optString("runtime") == "lua" }.getOrDefault(false)
                if (isLua) obj.optString("id") to obj.optString("target_peer_id") else null
            }
        }.onSuccess {
            commands = it
            note = if (it.isEmpty()) "no Lua commands registered on this cluster yet" else ""
        }.onFailure {
            Log.w("KVDemo", "RESULT: listing this cluster's Lua commands failed: ${it.message}")
            note = "could not read this cluster's commands: ${it.message}"
        }
        onLoaded()
    }

    Column(modifier = Modifier.fillMaxWidth().padding(top = 24.dp).testTag("luaClusterCommands")) {
        Text(
            "On this cluster",
            style = MaterialTheme.typography.titleMedium,
            modifier = Modifier.padding(start = 16.dp, bottom = 4.dp),
        )
        if (note.isNotEmpty()) {
            Text(
                note,
                style = MaterialTheme.typography.bodySmall,
                modifier = Modifier.padding(horizontal = 16.dp).testTag("luaClusterNote"),
            )
        }
        commands.forEach { (id, targetPeerID) ->
            CommandListItem(
                title = id,
                subtitle = "runs on ...${targetPeerID.takeLast(8)}",
                icon = iconForCategory("Lua"),
                testTag = "luaCommand_$id",
                onClick = {
                    Log.i("KVDemo", "USER_TAP: Lua command $id opened for a run")
                    onOpenLuaRun(id)
                },
            )
        }
    }
}
