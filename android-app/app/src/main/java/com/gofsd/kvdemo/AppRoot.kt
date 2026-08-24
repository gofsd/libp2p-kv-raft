package com.gofsd.kvdemo

import android.util.Base64
import android.util.Log
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Surface
import androidx.compose.runtime.Composable
import androidx.compose.runtime.DisposableEffect
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
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.retry
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
 * since it's pure navigation and grants/executes nothing; a [LogRefCode]
 * (api/logref.capnp, some device's "Log records: GenerateLogRef") names
 * the *object* in front of the camera rather than an action, so it is
 * resolved to that object's group and either enters it or, when a form
 * for that same group is already open, leaves the screen alone and
 * simply re-fills its log-id field -- see [logRefTarget], and note that
 * this is the one branch here with no confirming tap and no navigation
 * at all in its common case, which is the entire point of it; a
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
    // Hoisted here, not left as CommandsPagerScreen's own rememberPagerState, specifically so it
    // survives that composable being disposed and recreated -- which Navigation-Compose does to
    // the "pager" route's own composition every time something (commandPicker/groupPicker/
    // commandDetail) gets pushed on top of it, even though the same NavBackStackEntry stays in
    // the back stack the whole time. Popping back (e.g. after a log row's Repeat button opens
    // CommandDetailScreen, then the system Back button returns) used to always land on page 0
    // regardless of which page was actually showing before, because the old
    // LaunchedEffect(currentGroup){ animateScrollToPage(0) } fired on every fresh mount, not just
    // on a genuine group change -- Compose has no way to tell those two apart from state local to
    // the disposed-and-recreated composable alone. See PagerScreen.kt's own doc comment for the
    // full mechanism this state now drives.
    var currentPage by remember { mutableStateOf(0) }
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
        // onEach + retry rather than a bare collect: an exception escaping this body used to escape
        // the LaunchedEffect with it, and an exception out of a LaunchedEffect cancels the whole
        // composition's effect scope -- which takes this collector *and* MainScannerWidget's camera
        // bind down together while the Activity stays alive and keeps drawing its last frame. That
        // is exactly the state this harness spent runs chasing as "AppRoot's scan collector is
        // gone": no dialog, no navigation, no scans, nothing decoding, no exception logged, and no
        // way back short of a fresh process (recreating the Activity was tried and does not rebuild
        // the composition). Nothing in a scan's own handling -- decoding bytes, parsing JSON out of
        // a payload a camera read off a screen, a navigate on a controller mid-transition -- is
        // worth that outcome, so a throw here is logged and the collector resubscribes instead.
        // `catch` alone would not do: it *completes* the flow, leaving the same zero-collector state
        // this exists to prevent.
        ScannerCoordinator.scans.onEach { bytes ->
            Log.i(TAG, "AUTO: scan received (${bytes.size} bytes), decoding")
            val text = DataMatrixCodec.bytesToText(bytes)

            // A form field armed for a scan wins over every dispatch below it, and has to: the
            // person tapped a specific field's scan button and is now holding the camera over a
            // label, so running whatever that label happens to encode is the one thing they did
            // not ask for. Ported from object-history-app's CommandsViewModel.awaitFieldScan,
            // which occupies the same position in its own scan handling.
            //
            // The diversion lasts exactly one scan (armedIndex is cleared here as it is consumed),
            // and CommandDetailScreen disarms on dispose -- between them there is no way to leave
            // the app in a state where scan-to-run has quietly stopped working.
            val armedIndex = PendingFieldScan.armedIndex
            if (armedIndex != null) {
                // A log reference is the one binary code here that already means "an id" (see
                // [LogRefCode]), so unwrap it to the bare number rather than dropping its capnp
                // bytes into a text field as mojibake. Everything else goes in as the text the
                // code literally carries.
                val logRefID = runCatching { Kvmobile.decodeLogRef(bytes) }.getOrNull()
                val value = logRefID?.toString() ?: text
                Log.i(TAG, "RESULT: scan filled the open form's param_$armedIndex with \"$value\"")
                PendingFieldScan.armedIndex = null
                PendingFieldScan.scanned = armedIndex to value
                ScannerCoordinator.expanded = false
                return@onEach
            }

            val runCode = RunCode.decode(text)
            if (runCode != null) {
                Log.i(TAG, "RESULT: run-code scan decoded: $runCode")
                pendingRun = PendingRun(runCode.category, runCode.name, runCode.params)
                Log.w(TAG, "ACTION_REQUIRED: RunConfirmDialog shown for ${runCode.category}: ${runCode.name} -- tap Execute or Cancel on this device")
                ScannerCoordinator.expanded = false
                return@onEach
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
                    is NavCode.Screen -> {
                        // A plain route push, unlike Group's state change below: the editor
                        // is a screen, not a category.
                        navController.navigate(navCode.route)
                    }

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
                        currentPage = 0
                        if (navController.currentDestination?.route != "pager") {
                            navController.navigate("pager") {
                                popUpTo("pager")
                                launchSingleTop = true
                            }
                        }
                    }
                }
                ScannerCoordinator.expanded = false
                return@onEach
            }

            // A log reference -- the code that says what this device is *looking at* rather
            // than what to do with it (see [LogRefCode], api/logref.capnp). Tried after RunCode/
            // NavCode and before a generic event because it is the only binary payload here that
            // is not an Event: decodeLogRef checks the schema's own constant tag, so a foreign
            // barcode falls through rather than being resolved as some object's id.
            //
            // Resolving the id to a group is a real cluster read (kvmobile.LogRefGroup, which
            // retries briefly while a just-registered id replicates), hence Dispatchers.IO --
            // and hence a failure worth telling the person about: a label whose id nobody
            // registered is exactly the case where doing nothing silently would leave them
            // scanning it again forever.
            val logRefID = runCatching { Kvmobile.decodeLogRef(bytes) }.getOrNull()
            if (logRefID != null) {
                Log.i(TAG, "RESULT: log-reference scan decoded: logId=$logRefID")
                val group = runCatching {
                    withContext(Dispatchers.IO) { Kvmobile.logRefGroup(logRefID) }
                }.getOrElse { e ->
                    Log.w(TAG, "RESULT: log reference $logRefID did not resolve to a group: ${e.message}")
                    pendingUnrecognized = "log reference $logRefID: ${e.message}"
                    ScannerCoordinator.expanded = false
                    return@onEach
                }
                val code = LogRefCode(logRefID, group)
                // Back to the main thread before touching the NavController, and it is this
                // branch specifically that needs saying so: it is the only one that navigates
                // *after* a withContext(Dispatchers.IO), and a coroutine resuming from one does
                // not reliably land back on the dispatcher it left. Everything below reads or
                // mutates NavController state, which androidx.navigation asserts is main-thread
                // only.
                //
                // Found on the two-device rig, and worth knowing how well it hides: entering a
                // group from the pager only *assigns Compose state* (thread-safe from anywhere),
                // so the navigate() is skipped and the bug is invisible. It fires only when a
                // form is open -- the one case that has a back stack entry to pop -- where it
                // threw "Method setCurrentState must be called on the main thread" out of
                // popEntryFromBackStack, left the controller inconsistent, and surfaced as a
                // later, unrelated-looking "Cannot transition entry that is not in the back
                // stack".
                withContext(Dispatchers.Main) {
                    // The same launch race the NavCode branch above documents: a code already in front
                    // of the camera when this Activity starts can be decoded and acted on before
                    // NavHost has attached its graph, and navigating then leaves a NavBackStackEntry
                    // stuck at INITIALIZED and crashes the process on the next teardown. Gated on the
                    // controller not being ready rather than paid unconditionally, because this is the
                    // one branch a person triggers over and over in a row -- a fixed second per label
                    // would undo the very thing the feature is for -- and because it also protects the
                    // back-stack read just below, which would otherwise report "no form open" simply
                    // because the graph was not up yet.
                    if (navController.currentDestination == null) {
                        delay(1000)
                        while (navController.currentDestination == null) {
                            delay(16)
                        }
                    }
                    // Which form, if any, is open right now -- the whole of the state this decision
                    // turns on. Read from the back stack rather than tracked separately so it cannot
                    // disagree with what is actually on screen; the category is URL-encoded in the
                    // route, the same as commandDetailRoute wrote it.
                    val openFormCategory = navController.currentBackStackEntry
                        ?.takeIf { it.destination.route?.startsWith("commandDetail/") == true }
                        ?.arguments?.getString("category")
                        ?.let { runCatching { decodeSegment(it) }.getOrNull() }
                    // Set before acting on the target, not after: RefillOpenForm's whole effect *is*
                    // this assignment (CommandDetailScreen is already composed and observing it), and
                    // EnterGroup needs it in place before the group's own commands can be tapped.
                    ScannedLogRef.current = code
                    when (val target = logRefTarget(code, openFormCategory)) {
                        is LogRefTarget.RefillOpenForm -> {
                            Log.i(TAG, "RESULT: log reference $logRefID re-fills the open $openFormCategory form, staying put")
                        }

                        is LogRefTarget.EnterGroup -> {
                            Log.i(TAG, "RESULT: log reference $logRefID belongs to group \"${target.group}\", entering it")
                            currentGroup = target.group
                            currentPage = 0
                            // Same popUpTo("pager")/launchSingleTop, and the same reason not to use
                            // inclusive=true, as the NavCode.Group branch above -- see its comment.
                            if (navController.currentDestination?.route != "pager") {
                                navController.navigate("pager") {
                                    popUpTo("pager")
                                    launchSingleTop = true
                                }
                            }
                        }
                    }
                }
                ScannerCoordinator.expanded = false
                return@onEach
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
        }.retry { t ->
            Log.w(TAG, "AUTO: a scan's own handling threw -- logged and ignored, the scan collector stays subscribed", t)
            true
        }.collect()
    }

    // Says so in the log if AppRoot ever leaves the composition while its Activity lives on -- the
    // one explanation for a vanished scan collector that the guard above cannot rule out, and
    // indistinguishable from every other cause without this line.
    DisposableEffect(Unit) {
        onDispose { Log.w(TAG, "AUTO: AppRoot left the composition -- its scan collector and camera bind are gone with it") }
    }

    MaterialTheme {
        Surface(modifier = Modifier.fillMaxSize()) {
            Box(modifier = Modifier.fillMaxSize()) {
                NavHost(navController = navController, startDestination = "pager") {
                    composable("pager") {
                        CommandsPagerScreen(
                            statusText = statusText,
                            currentGroup = currentGroup,
                            onGroupChange = { currentGroup = it; currentPage = 0; ScannedLogRef.clearUnless(it) },
                            currentPage = currentPage,
                            onPageChange = { currentPage = it },
                            focusedLogId = focusedLogId,
                            onFocusedLogConsumed = { focusedLogId = null },
                            onCommandClick = { name ->
                                navController.navigate(commandDetailRoute(currentGroup, name))
                            },
                            onOpenCommandsPicker = { navController.navigate("commandPicker") },
                            onOpenGroupsPicker = { navController.navigate("groupPicker") },
                            onOpenLuaEditor = { navController.navigate(NavCode.LUA_EDITOR) },
                            // A form with the command id already filled in, not an execution:
                            // running still means generating a code and scanning it elsewhere.
                            onOpenLuaRun = { commandID ->
                                navController.navigate(commandDetailRoute("Lua", "Run", listOf(commandID, "")))
                            },
                            onRepeat = { entry ->
                                navController.navigate(commandDetailRoute(entry.category, entry.name, entry.args))
                            },
                        )
                    }
                    composable(NavCode.LUA_EDITOR) {
                        LuaEditorScreen(
                            onCreated = {
                                focusedLogId = OutputLog.snapshot().lastOrNull()?.id
                                navController.navigate("pager") {
                                    popUpTo("pager")
                                    launchSingleTop = true
                                }
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
                                currentPage = 0
                                ScannedLogRef.clearUnless(category)
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
