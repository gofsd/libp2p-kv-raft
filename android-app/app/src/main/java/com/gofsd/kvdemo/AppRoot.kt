package com.gofsd.kvdemo

import android.util.Base64
import android.util.Log
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Surface
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.platform.testTag
import androidx.navigation.NavType
import androidx.navigation.compose.NavHost
import androidx.navigation.compose.composable
import androidx.navigation.compose.rememberNavController
import androidx.navigation.navArgument
import java.net.URLDecoder
import java.net.URLEncoder
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kvmobile.Kvmobile
import org.json.JSONObject

/**
 * Single-Activity Compose root, replacing the old MainActivity ->
 * CommandListActivity -> CommandDetailActivity/ActivityLogActivity
 * Activity-per-screen structure with one NavHost -- the screens' own logic
 * (CommandCatalog.kt's data-driven CommandSpec list, OutputLog) is
 * otherwise unchanged, only how they're hosted. Brings up the kvmobile
 * daemon exactly once for this process's whole lifetime here (every screen
 * assumes it's either already up or on its way up, same assumption the old
 * MainActivity's onCreate documented) -- this LaunchedEffect(Unit) lives at
 * the NavHost's root, so it survives navigating between routes, unlike one
 * scoped to a single screen alone (which NavHost disposes/recomposes on
 * every visit).
 *
 * [MainListScreen] ("main") is the start destination: a pull-to-refresh
 * list of "Commands" (-> [CommandPickerScreen]) and "Groups" (->
 * [GroupPickerScreen]), the app's two navigation-shortcut generators (see
 * [NavCode]'s doc comment). The full category browser ([CategoriesScreen],
 * "categories" -> "commands/{category}" -> "commandDetail/{category}/{name}")
 * is unchanged and still reachable from there, just no longer the first
 * screen shown.
 *
 * [ScannerHost] is mounted exactly once here, as a Box sibling of the
 * NavHost, so its camera binds once and stays alive across every screen
 * -- see ScannerHost/MainScannerWidget's own doc comments for why calling
 * it from more than one place would tear the camera down and rebind it
 * on every navigation.
 *
 * The scanner is active on every screen at all times (per its own
 * always-mounted design above); this is also where every scan actually
 * gets acted on, regardless of which screen was showing when it happened
 * -- a second LaunchedEffect(Unit) here (same "lives for the whole
 * NavHost's lifetime" reasoning as the Start() one) collects
 * [ScannerCoordinator.scans] and forks on what it decoded to, in order:
 * a [NavCode] (a "Commands"/"Groups" shortcut code some device's Generate
 * button minted) navigates [navController] straight to that command's
 * detail screen or that category's command list -- no dialog, since it's
 * pure navigation and grants/submits nothing; a `join_request_ticket`
 * event (decoded via [kvmobile.Kvmobile.decodeEvent] -- some other
 * device's CreateJoinRequestTicket code, see CommandDetailScreen's
 * awaitAdmissionAfterGenerate handling) routes to [RecruitConfirmDialog]
 * -- admitting a device into this cluster needs its own confirm-then-redeem
 * step, not a blind "Trigger"; every other decodable event, or nothing
 * decodable at all, keeps going through [ScanConfirmationDialog]: scanning
 * alone never submits anything, only a subsequent tap on "Trigger" calls
 * [kvmobile.Kvmobile.sendEvent] (this class's own `triggerEvent` alias) to
 * actually submit the decoded event against the current cluster.
 */
private const val TAG = "KVDemo"
private class PendingScan(val decodedJson: String?, val rawText: String)
private class PendingRecruitTicket(val ticketB64: String, val sourceAddr: String)
private fun encodeSegment(s: String) = URLEncoder.encode(s, "UTF-8")
private fun decodeSegment(s: String) = URLDecoder.decode(s, "UTF-8")

fun commandListRoute(category: String) = "commands/${encodeSegment(category)}"
fun commandDetailRoute(category: String, name: String) =
    "commandDetail/${encodeSegment(category)}/${encodeSegment(name)}"

@Composable
fun AppRoot() {
    val context = LocalContext.current
    var statusText by remember { mutableStateOf("Connecting to cluster...") }
    var pendingScan by remember { mutableStateOf<PendingScan?>(null) }
    var pendingRecruitTicket by remember { mutableStateOf<PendingRecruitTicket?>(null) }
    val scope = rememberCoroutineScope()
    // Hoisted above both LaunchedEffects (not created down in the Box below) so the scan
    // collector's own NavCode branch can navigate directly -- a nav-shortcut scan (see NavCode's
    // doc comment) is handled entirely here, with no confirmation dialog of its own.
    val navController = rememberNavController()

    LaunchedEffect(Unit) {
        withContext(Dispatchers.IO) {
            Log.i(TAG, "AUTO: Kvmobile.start() beginning")
            try {
                val peerID = Kvmobile.start(context.filesDir.absolutePath)
                statusText = "Connected as $peerID"
                Log.i(TAG, "RESULT: Kvmobile.start() connected as $peerID")
            } catch (e: Exception) {
                statusText = "Failed to start: ${e.message}"
                Log.w(TAG, "RESULT: Kvmobile.start() failed: ${e.message}")
            }
        }
    }

    LaunchedEffect(Unit) {
        ScannerCoordinator.scans.collect { bytes ->
            Log.i(TAG, "AUTO: scan received (${bytes.size} bytes), decoding")
            val navCode = NavCode.decode(DataMatrixCodec.bytesToText(bytes))
            if (navCode != null) {
                Log.i(TAG, "RESULT: nav-shortcut scan decoded: $navCode")
                when (navCode) {
                    is NavCode.Command -> navController.navigate(commandDetailRoute(navCode.category, navCode.name))
                    is NavCode.Group -> navController.navigate(commandListRoute(navCode.category))
                }
                ScannerCoordinator.expanded = false
                return@collect
            }
            val decodedJson = withContext(Dispatchers.IO) {
                runCatching { Kvmobile.decodeEvent(bytes) }.getOrNull()
            }
            val eventName = decodedJson?.let { runCatching { JSONObject(it).optString("event") }.getOrNull() }
            if (eventName == "join_request_ticket") {
                val sourceAddr = runCatching {
                    JSONObject(decodedJson!!).getJSONObject("fields").optString("source_addr")
                }.getOrDefault("")
                pendingRecruitTicket = PendingRecruitTicket(
                    ticketB64 = Base64.encodeToString(bytes, Base64.NO_WRAP),
                    sourceAddr = sourceAddr,
                )
                Log.w(TAG, "ACTION_REQUIRED: RecruitConfirmDialog shown for source=$sourceAddr -- tap Approve or Cancel on this device")
            } else {
                pendingScan = PendingScan(decodedJson, DataMatrixCodec.bytesToText(bytes))
                Log.w(TAG, "ACTION_REQUIRED: ScanConfirmationDialog shown (event=$eventName, decoded=${decodedJson != null}) -- tap Trigger or Cancel on this device")
            }
            // Collapse the fullscreen scanner so the confirmation dialog
            // (drawn above it either way, but this avoids leaving the
            // camera view expanded and pointless behind the dialog) reads
            // as the natural next step, not a random popup mid-scan.
            ScannerCoordinator.expanded = false
        }
    }

    MaterialTheme {
        Surface(modifier = Modifier.fillMaxSize()) {
            Box(modifier = Modifier.fillMaxSize()) {
                NavHost(navController = navController, startDestination = "main") {
                    composable("main") {
                        MainListScreen(
                            onCommandsClick = { navController.navigate("commandPicker") },
                            onGroupsClick = { navController.navigate("groupPicker") },
                            onBrowseCategories = { navController.navigate("categories") },
                            onLogClick = { navController.navigate("log") },
                        )
                    }
                    composable("commandPicker") {
                        CommandPickerScreen()
                    }
                    composable("groupPicker") {
                        GroupPickerScreen()
                    }
                    composable("categories") {
                        CategoriesScreen(
                            statusText = statusText,
                            onCategoryClick = { category ->
                                navController.navigate(commandListRoute(category))
                            },
                            onLogClick = { navController.navigate("log") },
                        )
                    }
                    composable(
                        "commands/{category}",
                        arguments = listOf(navArgument("category") { type = NavType.StringType }),
                    ) { backStackEntry ->
                        val category = decodeSegment(backStackEntry.arguments?.getString("category") ?: "")
                        CommandListScreen(
                            category = category,
                            onCommandClick = { name ->
                                navController.navigate(commandDetailRoute(category, name))
                            },
                        )
                    }
                    composable(
                        "commandDetail/{category}/{name}",
                        arguments = listOf(
                            navArgument("category") { type = NavType.StringType },
                            navArgument("name") { type = NavType.StringType },
                        ),
                    ) { backStackEntry ->
                        val category = decodeSegment(backStackEntry.arguments?.getString("category") ?: "")
                        val name = decodeSegment(backStackEntry.arguments?.getString("name") ?: "")
                        CommandDetailScreen(
                            category = category,
                            name = name,
                            onNavigateToLog = { navController.navigate("log") },
                        )
                    }
                    composable("log") {
                        LogScreen()
                    }
                }

                ScannerHost(modifier = Modifier.align(Alignment.BottomEnd).testTag("scanner_host"))

                val scan = pendingScan
                if (scan != null) {
                    ScanConfirmationDialog(
                        decodedJson = scan.decodedJson,
                        rawText = scan.rawText,
                        onConfirm = {
                            Log.i(TAG, "USER_TAP: ScanConfirmationDialog Trigger pressed")
                            val json = scan.decodedJson ?: return@ScanConfirmationDialog
                            pendingScan = null
                            scope.launch {
                                try {
                                    val result = withContext(Dispatchers.IO) { Kvmobile.sendEvent(json) }
                                    Log.i(TAG, "RESULT: scanned event sent: $result")
                                    OutputLog.record("Scanned event", result, LogStatus.SUCCESS)
                                } catch (e: Exception) {
                                    Log.w(TAG, "RESULT: scanned event FAILED: ${e.message}")
                                    OutputLog.record("Scanned event", "FAILED: ${e.message}", LogStatus.FAILED)
                                }
                            }
                        },
                        onDismiss = {
                            Log.i(TAG, "USER_TAP: ScanConfirmationDialog Cancel pressed")
                            pendingScan = null
                        },
                    )
                }

                val recruit = pendingRecruitTicket
                if (recruit != null) {
                    RecruitConfirmDialog(
                        sourceAddr = recruit.sourceAddr,
                        onApprove = { stationName ->
                            Log.i(TAG, "USER_TAP: RecruitConfirmDialog Approve pressed (stationName=\"$stationName\")")
                            pendingRecruitTicket = null
                            scope.launch {
                                try {
                                    Log.i(TAG, "AUTO: redeemJoinRequestTicket(voter) starting")
                                    val result = withContext(Dispatchers.IO) {
                                        Kvmobile.redeemJoinRequestTicket(recruit.ticketB64, "voter")
                                    }
                                    val admittedPeerID = result.substringBefore(' ')
                                    if (stationName.isNotBlank()) {
                                        withContext(Dispatchers.IO) {
                                            Kvmobile.putStation(admittedPeerID, stationName, "")
                                        }
                                    }
                                    Log.i(TAG, "RESULT: recruit admitted $admittedPeerID: $result")
                                    val named = if (stationName.isNotBlank()) " (named \"$stationName\")" else ""
                                    OutputLog.record("Recruited", "$result$named", LogStatus.SUCCESS)
                                } catch (e: Exception) {
                                    Log.w(TAG, "RESULT: recruit FAILED: ${e.message}")
                                    OutputLog.record("Recruit", "FAILED: ${e.message}", LogStatus.FAILED)
                                }
                            }
                        },
                        onDismiss = {
                            Log.i(TAG, "USER_TAP: RecruitConfirmDialog Cancel pressed")
                            pendingRecruitTicket = null
                        },
                    )
                }
            }
        }
    }
}
