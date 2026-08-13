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
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kvmobile.Kvmobile
import org.json.JSONArray
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
 * The start (and effectively only real-content) destination is `"pager"`
 * ([CommandsPagerScreen]): a 2-page swipeable pager over the current group
 * (hoisted here as [currentGroup], see that file's own doc comment for why
 * it's plain state and not a nav-route argument) and the Activity Log.
 * [DEFAULT_GROUP]'s page 0 shows two pseudo-items, "Commands" ->
 * [CommandPickerScreen] and "Groups" -> [GroupPickerScreen] -- both a
 * generic select-a-thing-then-Submit form, reusing the same mechanic a real
 * command's own params use, just for navigation instead of execution. Every
 * other group's page 0 is that category's own real commands -> ["commandDetail/{category}/{name}"]
 * ([CommandDetailScreen]).
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
 * a [RunCode] (any device's CommandDetailScreen "Generate DataMatrix"
 * button minted this, for any of CommandCatalog.kt's specs) routes to
 * [RunConfirmDialog] -- this is now the *only* way any command executes,
 * see that dialog's own doc comment, and its Execute button lands back on
 * the pager's log page focused on the new entry once it finishes; a
 * [NavCode.Group] shortcut sets [currentGroup] and collapses the back
 * stack to a single `"pager"` instance showing that group -- no dialog,
 * since it's pure navigation and grants/executes nothing; a
 * `join_request_ticket` event (decoded via [kvmobile.Kvmobile.decodeEvent]
 * -- some other device's CreateJoinRequestTicket code, see
 * CommandDetailScreen's awaitAdmissionAfterGenerate handling) routes to
 * [RecruitConfirmDialog] -- admitting a device into this cluster needs its
 * own confirm-then-redeem step, not a plain Execute; anything else (an
 * undecodable scan, or a stray foreign barcode) shows
 * [UnrecognizedScanDialog] instead of silently doing nothing.
 */
private const val TAG = "KVDemo"

/** The app's starting group -- not a real CommandCatalog.kt category, see [GroupPageScreen]. */
const val DEFAULT_GROUP = "Default"

private class PendingRun(val category: String, val name: String, val params: List<String>)
private class PendingRecruitTicket(val ticketB64: String, val sourceAddr: String)
private fun encodeSegment(s: String) = URLEncoder.encode(s, "UTF-8")
private fun decodeSegment(s: String) = URLDecoder.decode(s, "UTF-8")

/**
 * [initialParams], when non-empty, appends a URL-encoded `?args=<JSON array>` query param --
 * mirroring object-history-app's `Routes.form(id, values)` -- so [CommandDetailScreen] can seed
 * its param fields instead of leaving them blank. Used by a log row's Repeat button.
 */
fun commandDetailRoute(category: String, name: String, initialParams: List<String> = emptyList()): String {
    val base = "commandDetail/${encodeSegment(category)}/${encodeSegment(name)}"
    if (initialParams.isEmpty()) return base
    val arr = JSONArray()
    for (p in initialParams) arr.put(p)
    return "$base?args=${encodeSegment(arr.toString())}"
}

@Composable
fun AppRoot() {
    val context = LocalContext.current
    var statusText by remember { mutableStateOf("Connecting to cluster...") }
    var currentGroup by remember { mutableStateOf(DEFAULT_GROUP) }
    var focusedLogId by remember { mutableStateOf<Long?>(null) }
    var pendingRun by remember { mutableStateOf<PendingRun?>(null) }
    var pendingRecruitTicket by remember { mutableStateOf<PendingRecruitTicket?>(null) }
    var pendingUnrecognized by remember { mutableStateOf<String?>(null) }
    val scope = rememberCoroutineScope()
    // Hoisted above both LaunchedEffects (not created down in the Box below) so the scan
    // collector's own NavCode branch can navigate directly -- a nav-shortcut scan (see NavCode's
    // doc comment) is handled entirely here, with no confirmation dialog of its own.
    val navController = rememberNavController()

    LaunchedEffect(Unit) {
        withContext(Dispatchers.IO) {
            OutputLog.init(context.filesDir.absolutePath)
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
        // Must start collecting immediately, not after any delay: ScannerCoordinator.onScanned
        // dedupes by comparing against the *last* decoded payload (see that file's own doc
        // comment), so a code already sitting in front of the camera at Activity launch emits
        // to `scans` exactly once, and every later frame decoding the same still-displayed code
        // is a deliberate no-op -- `scans` itself has no replay cache (replay = 0), so a
        // collector that isn't already subscribed at that one moment misses the emission
        // entirely (confirmed live: delaying the collect{} call itself, an earlier version of
        // this fix, made nav-shortcut scans stop being observed at all rather than fixing
        // anything). The delay that actually matters -- see inside the NavCode branch below --
        // has to be on the *navigate* action, after the value is already safely received here.
        ScannerCoordinator.scans.collect { bytes ->
            Log.i(TAG, "AUTO: scan received (${bytes.size} bytes), decoding")
            val text = DataMatrixCodec.bytesToText(bytes)

            val runCode = RunCode.decode(text)
            if (runCode != null) {
                Log.i(TAG, "RESULT: run-code scan decoded: $runCode")
                pendingRun = PendingRun(runCode.category, runCode.name, runCode.params)
                Log.w(TAG, "ACTION_REQUIRED: RunConfirmDialog shown for ${runCode.category}: ${runCode.name} -- tap Execute or Cancel on this device")
                ScannerCoordinator.expanded = false
                return@collect
            }

            val navCode = NavCode.decode(text)
            if (navCode != null) {
                Log.i(TAG, "RESULT: nav-shortcut scan decoded: $navCode")
                // A code already sitting in front of the camera at the exact moment this
                // Activity launches (this project's own automated optical-scan e2e harness, see
                // pkg/e2erun/android_optical.go -- not something a human scanning by hand could
                // ever produce) can have the collect{} above receive and act on a scan within a
                // few hundred milliseconds of AppRoot's own first composition -- confirmed live
                // via that harness's own logging, well before NavHost (further down this same
                // function) has finished attaching its graph to navController (created above) or
                // even before Kvmobile.start() above has returned. Navigating that early left a
                // NavBackStackEntry stuck at INITIALIZED, crashing the process on next teardown
                // ("State must be at least CREATED to move to DESTROYED, but was INITIALIZED").
                // No real human scan can happen this fast, so delaying the navigate action itself
                // (not the collection above, which must stay immediate -- see this LaunchedEffect's
                // own doc comment) is invisible in practice; it exists purely to let the rest of
                // this Composable's own setup stabilize before acting on an already-received scan.
                delay(1000)
                // Defense in depth alongside that delay: currentDestination stays null until
                // NavHost's own setup completes, so poll for it rather than assume it's ready.
                while (navController.currentDestination == null) {
                    delay(16)
                }
                when (navCode) {
                    is NavCode.Group -> {
                        // Entering a group is a plain state change (see PagerScreen.kt's
                        // currentGroup), not a nav-route push -- so the same "collapse back to
                        // the single existing pager instance" popUpTo("pager")/launchSingleTop
                        // pattern used below for post-execution nav and CreateJoinRequestTicket's
                        // admission nav applies here too, and *not* inclusive=true: that would
                        // destroy and recreate the pager route's own composition (losing e.g. its
                        // HorizontalPager scroll state) even when nothing was pushed on top of it
                        // to begin with, which is the common case since a scan can only ever land
                        // while already on the pager route or on a route pushed on top of it.
                        currentGroup = navCode.category
                        if (navController.currentDestination?.route != "pager") {
                            navController.navigate("pager") {
                                popUpTo("pager")
                                launchSingleTop = true
                            }
                        }
                    }
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
                pendingUnrecognized = text
                Log.w(TAG, "ACTION_REQUIRED: UnrecognizedScanDialog shown -- tap Close on this device")
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
                NavHost(navController = navController, startDestination = "pager") {
                    composable("pager") {
                        CommandsPagerScreen(
                            statusText = statusText,
                            currentGroup = currentGroup,
                            onGroupChange = { currentGroup = it },
                            focusedLogId = focusedLogId,
                            onFocusedLogConsumed = { focusedLogId = null },
                            onCommandClick = { name ->
                                navController.navigate(commandDetailRoute(currentGroup, name))
                            },
                            onOpenCommandsPicker = { navController.navigate("commandPicker") },
                            onOpenGroupsPicker = { navController.navigate("groupPicker") },
                            onRepeat = { entry ->
                                navController.navigate(commandDetailRoute(entry.category, entry.name, entry.args))
                            },
                        )
                    }
                    composable("commandPicker") {
                        CommandPickerScreen(
                            onNavigateToDetail = { category, name ->
                                navController.navigate(commandDetailRoute(category, name))
                            },
                        )
                    }
                    composable("groupPicker") {
                        GroupPickerScreen(
                            onSubmit = { category ->
                                currentGroup = category
                                navController.popBackStack()
                            },
                        )
                    }
                    composable(
                        "commandDetail/{category}/{name}?args={args}",
                        arguments = listOf(
                            navArgument("category") { type = NavType.StringType },
                            navArgument("name") { type = NavType.StringType },
                            navArgument("args") {
                                type = NavType.StringType
                                nullable = true
                                defaultValue = null
                            },
                        ),
                    ) { backStackEntry ->
                        val category = decodeSegment(backStackEntry.arguments?.getString("category") ?: "")
                        val name = decodeSegment(backStackEntry.arguments?.getString("name") ?: "")
                        val initialParams = backStackEntry.arguments?.getString("args")?.let { encoded ->
                            runCatching {
                                val arr = JSONArray(decodeSegment(encoded))
                                (0 until arr.length()).map { arr.getString(it) }
                            }.getOrNull()
                        } ?: emptyList()
                        CommandDetailScreen(
                            category = category,
                            name = name,
                            initialParams = initialParams,
                            onNavigateToLog = {
                                focusedLogId = OutputLog.snapshot().lastOrNull()?.id
                                if (navController.currentDestination?.route != "pager") {
                                    navController.navigate("pager") {
                                        popUpTo("pager")
                                        launchSingleTop = true
                                    }
                                }
                            },
                        )
                    }
                }

                ScannerHost(modifier = Modifier.align(Alignment.BottomEnd).testTag("scanner_host"))

                val run = pendingRun
                if (run != null) {
                    RunConfirmDialog(
                        category = run.category,
                        name = run.name,
                        params = run.params,
                        onConfirm = {
                            Log.i(TAG, "USER_TAP: RunConfirmDialog Execute pressed (${run.category}: ${run.name})")
                            pendingRun = null
                            scope.launch {
                                val spec = withContext(Dispatchers.IO) {
                                    buildCommands(context.filesDir.absolutePath, OutputLog::append)
                                        .firstOrNull { it.category == run.category && it.name == run.name }
                                }
                                if (spec == null) {
                                    Log.w(TAG, "RESULT: no CommandSpec matches ${run.category}: ${run.name}")
                                    OutputLog.record("${run.category}: ${run.name}", "FAILED: unknown command", LogStatus.FAILED)
                                } else {
                                    CommandExecutor.execute(spec, run.params)
                                }
                                focusedLogId = OutputLog.snapshot().lastOrNull()?.id
                                if (navController.currentDestination?.route != "pager") {
                                    navController.navigate("pager") {
                                        popUpTo("pager")
                                        launchSingleTop = true
                                    }
                                }
                            }
                        },
                        onDismiss = {
                            Log.i(TAG, "USER_TAP: RunConfirmDialog Cancel pressed")
                            pendingRun = null
                        },
                    )
                }

                val unrecognized = pendingUnrecognized
                if (unrecognized != null) {
                    UnrecognizedScanDialog(
                        rawText = unrecognized,
                        onDismiss = {
                            Log.i(TAG, "USER_TAP: UnrecognizedScanDialog Close pressed")
                            pendingUnrecognized = null
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
