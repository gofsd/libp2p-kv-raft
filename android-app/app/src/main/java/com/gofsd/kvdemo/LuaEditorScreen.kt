package com.gofsd.kvdemo

import android.util.Log
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.Button
import androidx.compose.material3.LocalTextStyle
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.platform.testTag
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.unit.dp
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kvmobile.Kvmobile
import org.json.JSONArray

private const val TAG = "KVDemo"

/**
 * Write a Lua command: an id, a name, the group that decides who may run it, and the script
 * itself in a tall monospace field. Create stores the script in the journal and registers it as
 * a catalog Command targeting *this* device, in one call
 * ([kvmobile.Kvmobile.luaCreateCommand]).
 *
 * # Why this screen has a button when nothing else does
 *
 * Every other command in this app executes exactly one way: generate a [RunCode] here or on
 * another device, scan it with a real camera, confirm in [RunConfirmDialog] (see
 * [CommandExecutor], the single place any spec runs). This screen deliberately breaks that, and
 * the reason is size: a DataMatrix a phone camera can read across a desk holds a few hundred
 * bytes, and a script is measured in kilobytes. A scan-only authoring flow would mean typing the
 * script on some other device anyway, which is the same act with an extra step.
 *
 * It is only *authoring* that is exempt. Running a Lua command needs a command id and small
 * inputs, which fit a code comfortably, so execution keeps the scan-and-confirm path unchanged --
 * "Lua: Run" is an ordinary spec in [buildCommands] like any other.
 *
 * The exemption is also narrower than it looks: Create still goes through
 * [CommandExecutor.execute] against that same "Lua: CreateCommand" spec, so what happens here is
 * recorded in the shared [OutputLog] exactly like a scanned execution, and pkg/e2erun's log
 * verification sees it the same way.
 *
 * # Groups
 *
 * The group dropdown is the cluster's real groups ([kvmobile.Kvmobile.listGroups]), not a local
 * list, because a group is what actually decides who may run the command: public group, anybody;
 * private group, its members. Leaving it blank creates the command unlinked, which means nobody
 * can run it yet -- fine while drafting, and fixable later with "Links: AddCommandToGroup".
 */
@Composable
fun LuaEditorScreen(onCreated: () -> Unit) {
    val context = LocalContext.current
    val spec = remember {
        buildCommands(context.filesDir.absolutePath, OutputLog::append)
            .first { it.category == "Lua" && it.name == "CreateCommand" }
    }

    var id by remember { mutableStateOf("") }
    var name by remember { mutableStateOf("") }
    var code by remember { mutableStateOf("") }
    var selectedGroup by remember { mutableStateOf<String?>(null) }
    var groups by remember { mutableStateOf<List<String>>(emptyList()) }
    var output by remember { mutableStateOf("") }
    var creating by remember { mutableStateOf(false) }
    val scope = rememberCoroutineScope()
    val scrollState = rememberScrollState()

    // The cluster's own groups, read once when the screen opens. A failure here is not fatal --
    // the dropdown is simply empty and the command can be created unlinked -- so it reports
    // itself in the output area rather than blocking the screen.
    LaunchedEffect(Unit) {
        val loaded = runCatching {
            val raw = withContext(Dispatchers.IO) { Kvmobile.listGroups() }
            val arr = JSONArray(raw)
            (0 until arr.length()).map { arr.getJSONObject(it).optString("id") }.filter { it.isNotBlank() }
        }
        loaded.onSuccess { groups = it }
        loaded.onFailure {
            Log.w(TAG, "RESULT: listGroups failed while opening the Lua editor: ${it.message}")
            output = "could not read this cluster's groups: ${it.message}"
        }
    }

    Column(
        modifier = Modifier
            .fillMaxSize()
            .padding(16.dp)
            .verticalScroll(scrollState)
            .testTag("screen_lua_editor"),
    ) {
        Text(
            "New Lua command",
            style = MaterialTheme.typography.titleLarge,
            modifier = Modifier.padding(bottom = 12.dp).testTag("luaEditorTitle"),
        )

        OutlinedTextField(
            value = id,
            onValueChange = { id = it },
            label = { Text("id") },
            singleLine = true,
            modifier = Modifier.fillMaxWidth().padding(vertical = 4.dp).testTag("luaEditorId"),
        )
        OutlinedTextField(
            value = name,
            onValueChange = { name = it },
            label = { Text("name") },
            singleLine = true,
            modifier = Modifier.fillMaxWidth().padding(vertical = 4.dp).testTag("luaEditorName"),
        )
        SelectDropdown(
            label = "group (blank = unlinked)",
            options = groups,
            selected = selectedGroup,
            onSelect = { selectedGroup = it },
            testTag = "luaEditorGroup",
        )
        OutlinedTextField(
            value = code,
            onValueChange = { code = it },
            label = { Text("code") },
            minLines = 12,
            textStyle = LocalTextStyle.current.copy(fontFamily = FontFamily.Monospace),
            modifier = Modifier.fillMaxWidth().padding(vertical = 4.dp).testTag("luaEditorCode"),
        )

        Button(
            enabled = !creating && id.isNotBlank() && name.isNotBlank() && code.isNotBlank(),
            onClick = {
                val args = listOf(id, name, selectedGroup ?: "", code)
                Log.i(TAG, "USER_TAP: Lua editor Create pressed for $id (${code.length} bytes)")
                creating = true
                scope.launch {
                    // Through CommandExecutor, not Kvmobile directly, so this lands in the
                    // shared OutputLog exactly like a scanned execution would -- see this
                    // screen's own doc comment.
                    output = CommandExecutor.execute(spec, args)
                    creating = false
                    if (!output.contains("FAILED")) {
                        onCreated()
                    }
                }
            },
            modifier = Modifier.fillMaxWidth().padding(top = 12.dp).testTag("luaEditorCreate"),
        ) {
            Text(if (creating) "Creating..." else "Create")
        }

        Text(
            output,
            fontFamily = FontFamily.Monospace,
            style = MaterialTheme.typography.bodySmall,
            modifier = Modifier.padding(top = 12.dp).testTag("luaEditorOutput"),
        )
    }
}
