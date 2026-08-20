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
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
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
 * its own commands, tap one -> CommandDetailScreen -- the same rendering CommandListScreen had
 * before this pager rewrite, tags preserved on purpose.
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
private fun CategoryCommandList(
    category: String,
    onCommandClick: (String) -> Unit,
    onOpenLuaEditor: () -> Unit = {},
    onOpenLuaRun: (String) -> Unit = {},
) {
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
            // The Lua group leads with the one thing in this app that cannot be reached by
            // scanning a code: writing a script (see LuaEditorScreen's doc comment on why
            // authoring is the exception to scan-only execution). Everything below it is an
            // ordinary spec, opened and generated like any other.
            if (category == "Lua") {
                item {
                    Text(
                        "New Lua command...",
                        modifier = Modifier
                            .fillMaxWidth()
                            .clickable {
                                Log.i("KVDemo", "USER_TAP: Lua editor opened")
                                onOpenLuaEditor()
                            }
                            .padding(vertical = 12.dp)
                            .testTag("listItem_newLuaCommand"),
                    )
                }
            }
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
            if (category == "Lua") {
                item { LuaClusterCommands(onOpenLuaRun) }
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
 */
@Composable
private fun LuaClusterCommands(onOpenLuaRun: (String) -> Unit) {
    var commands by remember { mutableStateOf<List<Pair<String, String>>>(emptyList()) }
    var note by remember { mutableStateOf("") }

    LaunchedEffect(Unit) {
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
    }

    Column(modifier = Modifier.fillMaxWidth().padding(top = 24.dp).testTag("luaClusterCommands")) {
        Text(
            "On this cluster",
            style = MaterialTheme.typography.titleMedium,
            modifier = Modifier.padding(bottom = 4.dp),
        )
        if (note.isNotEmpty()) {
            Text(note, style = MaterialTheme.typography.bodySmall, modifier = Modifier.testTag("luaClusterNote"))
        }
        commands.forEach { (id, targetPeerID) ->
            Text(
                "$id  ->  ${targetPeerID.takeLast(8)}",
                modifier = Modifier
                    .fillMaxWidth()
                    .clickable {
                        Log.i("KVDemo", "USER_TAP: Lua command $id opened for a run")
                        onOpenLuaRun(id)
                    }
                    .padding(vertical = 12.dp)
                    .testTag("luaCommand_$id"),
            )
        }
    }
}
