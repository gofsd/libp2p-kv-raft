package com.gofsd.kvdemo

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

/**
 * Single-Activity Compose root, replacing the old MainActivity ->
 * CommandListActivity -> CommandDetailActivity/ActivityLogActivity
 * Activity-per-screen structure with one NavHost of four routes -- the
 * screens' own logic (CommandCatalog.kt's data-driven CommandSpec list,
 * OutputLog) is otherwise unchanged, only how they're hosted. Brings up
 * the kvmobile daemon exactly once for this process's whole lifetime here
 * (every screen assumes it's either already up or on its way up, same
 * assumption the old MainActivity's onCreate documented) -- this
 * LaunchedEffect(Unit) lives at the NavHost's root, so it survives
 * navigating between routes, unlike one scoped to CategoriesScreen alone
 * (which NavHost disposes/recomposes on every visit).
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
 * [ScannerCoordinator.scans], decodes each one via
 * [kvmobile.Kvmobile.decodeEvent], and shows [ScanConfirmationDialog]
 * -- scanning alone never submits anything; only a subsequent tap on
 * "Trigger" calls [kvmobile.Kvmobile.sendEvent] (this class's own
 * `triggerEvent` alias) to actually submit the decoded event against the
 * current cluster.
 */
private class PendingScan(val decodedJson: String?, val rawText: String)
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
    val scope = rememberCoroutineScope()

    LaunchedEffect(Unit) {
        withContext(Dispatchers.IO) {
            try {
                val peerID = Kvmobile.start(context.filesDir.absolutePath)
                statusText = "Connected as $peerID"
            } catch (e: Exception) {
                statusText = "Failed to start: ${e.message}"
            }
        }
    }

    LaunchedEffect(Unit) {
        ScannerCoordinator.scans.collect { bytes ->
            val decodedJson = withContext(Dispatchers.IO) {
                runCatching { Kvmobile.decodeEvent(bytes) }.getOrNull()
            }
            pendingScan = PendingScan(decodedJson, DataMatrixCodec.bytesToText(bytes))
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
                val navController = rememberNavController()
                NavHost(navController = navController, startDestination = "categories") {
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
                        CommandDetailScreen(category = category, name = name)
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
                            val json = scan.decodedJson ?: return@ScanConfirmationDialog
                            pendingScan = null
                            scope.launch {
                                val line = try {
                                    val result = withContext(Dispatchers.IO) { Kvmobile.sendEvent(json) }
                                    "Scanned event ->\n$result"
                                } catch (e: Exception) {
                                    "Scanned event FAILED: ${e.message}"
                                }
                                OutputLog.append(line)
                            }
                        },
                        onDismiss = { pendingScan = null },
                    )
                }
            }
        }
    }
}
