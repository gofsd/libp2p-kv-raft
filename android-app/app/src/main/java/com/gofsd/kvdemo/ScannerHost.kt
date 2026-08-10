package com.gofsd.kvdemo

import android.Manifest
import android.content.pm.PackageManager
import android.util.Log
import androidx.compose.foundation.background
import androidx.compose.foundation.border
import androidx.compose.foundation.clickable
import androidx.compose.foundation.interaction.MutableInteractionSource
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.NoPhotography
import androidx.compose.material.icons.filled.QrCodeScanner
import androidx.compose.material3.Card
import androidx.compose.material3.CardDefaults
import androidx.compose.material3.Icon
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.remember
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.draw.shadow
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.platform.testTag
import androidx.compose.ui.text.style.TextAlign
import androidx.compose.ui.unit.dp
import androidx.compose.ui.zIndex
import androidx.core.content.ContextCompat

/**
 * The app's single persistent scanner, composed exactly once in [AppRoot],
 * above the NavHost, so it's never re-created by navigation between
 * screens -- only its expanded/minimized chrome changes; the camera
 * itself binds once. Decoded results go to [ScannerCoordinator.onScanned];
 * the confirmation-dialog collector in [AppRoot] is the sole consumer of
 * [ScannerCoordinator.scans]. Ported from object-history-app's
 * identically-shaped ScannerHost.
 *
 * Unlike object-history-app, camera permission is never mandatory here --
 * this app's actual purpose (raft/libp2p node control) has zero
 * dependency on the camera, so a denied/missing permission degrades to
 * this placeholder card, and the rest of the app stays fully usable; see
 * AndroidManifest.xml's uses-permission comment.
 *
 * [MainScannerWidget] is called exactly once below (not from two
 * different branches of an `if (expanded)`) -- see its own doc comment
 * for why calling it from two source positions would tear down and
 * rebind the camera on every toggle instead of just restyling the same
 * live one.
 */
@Composable
fun ScannerHost(modifier: Modifier = Modifier) {
    val context = LocalContext.current
    val expanded = ScannerCoordinator.expanded
    val accentColor = MaterialTheme.colorScheme.primary

    if (ContextCompat.checkSelfPermission(context, Manifest.permission.CAMERA)
        != PackageManager.PERMISSION_GRANTED
    ) {
        Log.w("KVDemo", "ACTION_REQUIRED: camera permission not granted -- scanner disabled until it is")
        Card(
            modifier = modifier
                .padding(16.dp)
                .size(120.dp)
                .zIndex(30f)
                .testTag("scanner_permission_placeholder"),
            shape = RoundedCornerShape(20.dp),
            colors = CardDefaults.cardColors(
                containerColor = MaterialTheme.colorScheme.surfaceVariant,
            ),
        ) {
            Box(
                modifier = Modifier
                    .fillMaxSize()
                    .padding(12.dp),
                contentAlignment = Alignment.Center,
            ) {
                Column(horizontalAlignment = Alignment.CenterHorizontally) {
                    Icon(
                        Icons.Filled.NoPhotography,
                        contentDescription = null,
                        tint = MaterialTheme.colorScheme.onSurfaceVariant,
                    )
                    Text(
                        "Camera permission required",
                        color = MaterialTheme.colorScheme.onSurfaceVariant,
                        style = MaterialTheme.typography.labelSmall,
                        textAlign = TextAlign.Center,
                    )
                }
            }
        }
        return
    }

    // Only the outer frame differs between the two states; MainScannerWidget itself is called
    // from exactly one source position below (see this function's doc comment).
    val outerModifier = if (expanded) {
        modifier
            .fillMaxSize()
            .zIndex(50f)
    } else {
        modifier
            .padding(16.dp)
            .size(88.dp)
            .zIndex(20f)
    }

    Box(modifier = outerModifier.testTag("scanner_bubble")) {
        val scannerModifier = if (expanded) {
            Modifier
                .fillMaxSize()
                .background(Color.Black)
                .clickable(
                    interactionSource = remember { MutableInteractionSource() },
                    indication = null,
                ) {
                    Log.i("KVDemo", "USER_TAP: scanner collapsed")
                    ScannerCoordinator.expanded = false
                }
        } else {
            Modifier
                .fillMaxSize()
                .shadow(10.dp, CircleShape)
                .clip(CircleShape)
                .border(2.5.dp, accentColor, CircleShape)
                .clickable {
                    Log.i("KVDemo", "USER_TAP: scanner expanded to fullscreen")
                    ScannerCoordinator.expanded = true
                }
        }

        MainScannerWidget(
            isExpanded = expanded,
            onResult = { bytes -> ScannerCoordinator.onScanned(bytes) },
            modifier = scannerModifier,
        )

        if (!expanded) {
            Box(
                modifier = Modifier
                    .align(Alignment.BottomEnd)
                    .size(28.dp)
                    .shadow(4.dp, CircleShape)
                    .background(accentColor, CircleShape)
                    .border(2.dp, MaterialTheme.colorScheme.surface, CircleShape),
                contentAlignment = Alignment.Center,
            ) {
                Icon(
                    Icons.Filled.QrCodeScanner,
                    contentDescription = "Open scanner",
                    modifier = Modifier.size(16.dp),
                    tint = MaterialTheme.colorScheme.onPrimary,
                )
            }
        }
    }
}
