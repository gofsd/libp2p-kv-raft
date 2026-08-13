package com.gofsd.kvdemo

import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.statusBarsPadding
import androidx.compose.foundation.pager.HorizontalPager
import androidx.compose.foundation.pager.rememberPagerState
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.Close
import androidx.compose.material3.Icon
import androidx.compose.material3.IconButton
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.snapshotFlow
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.testTag
import androidx.compose.ui.unit.dp

/**
 * The app's start destination and the only route hosting real content: a 2-page swipeable pager
 * (page 0 = [GroupPageScreen] for [currentGroup], page 1 = [LogScreen]) -- "no tab bar, the swipe
 * is the control," the same HorizontalPager-as-one-nav-destination shape as object-history-app's
 * CommandsScreen.kt, adapted here to a real Group concept instead of a runtime ACL-filter context.
 *
 * [currentGroup] is hoisted in AppRoot, not a nav-route argument -- that's what lets a log row's
 * "Group" shortcut (see LogScreen.kt) switch context with a plain state assignment, no
 * NavController call, since both pages already live in this one composable.
 *
 * [currentPage] is hoisted in AppRoot too, not left as this composable's own `rememberPagerState`
 * state, for a subtler reason: Navigation-Compose disposes and recreates this whole composable's
 * composition every time something (commandPicker/groupPicker/commandDetail) gets pushed on top
 * of the "pager" route and popped back off, even though the same `NavBackStackEntry` stays in the
 * back stack throughout. An earlier version seeded a plain `rememberPagerState(pageCount={2})`
 * and forced page 0 via `LaunchedEffect(currentGroup){ animateScrollToPage(0) }` -- which fires on
 * *every* fresh mount of this composable, not just a genuine group change, since Compose has no
 * way to tell "just recomposed after being disposed" apart from "first ever composition" using
 * only state local to the disposed-and-recreated composable itself. Confirmed live: a log row's
 * Repeat button (on page 1) opens CommandDetailScreen, and pressing Back from there always landed
 * back on page 0 instead of page 1, regardless of `currentGroup` being unchanged the whole time.
 *
 * The fix: [pagerState] seeds its `initialPage` from the hoisted [currentPage] (so a fresh mount
 * starts on the right page with no animation needed), a `snapshotFlow` on `pagerState.settledPage`
 * mirrors every real settle (user swipe or programmatic) back into [onPageChange] (so a *later*
 * fresh mount has the latest value to seed from), and jumping to page 0 on a group change is now
 * driven by whoever actually changes the group explicitly setting `currentPage = 0` alongside it
 * (see AppRoot.kt's `onGroupChange`/`GroupPickerScreen`/`NavCode.Group` call sites) rather than by
 * this composable inferring it from a key change that can't be reliably observed across a
 * dispose+recreate cycle. `LaunchedEffect(currentPage)`'s own guard
 * (`pagerState.currentPage != currentPage`) is what keeps this from re-triggering a redundant
 * animation on that same fresh-mount case, since `initialPage` already seeded the correct value.
 *
 * A just-executed command's log entry (set via [focusedLogId], from AppRoot's RunConfirmDialog)
 * re-scrolls to page 1 ([LaunchedEffect(focusedLogId)]) -- LogScreen itself then scrolls to and
 * expands the matching row and clears [focusedLogId] via [onFocusedLogConsumed] once it has. This
 * one doesn't need the same treatment as the group-change case: [focusedLogId] is only ever
 * non-null right when a fresh mount *should* jump to the log page (it's set immediately before
 * navigating back to "pager"), never as a leftover from an unrelated remount.
 *
 * `testTag("screen_main")` is kept on this composable's root (not renamed to e.g. "screen_pager")
 * deliberately -- it's what the existing android_optical_cases e2e harness waits on to know the
 * app has launched, and keeping the identifier stable shrinks that harness's own migration diff
 * even though the underlying nav route is now `"pager"`, not `"main"`.
 */
@Composable
fun CommandsPagerScreen(
    statusText: String,
    currentGroup: String,
    onGroupChange: (String) -> Unit,
    currentPage: Int,
    onPageChange: (Int) -> Unit,
    focusedLogId: Long?,
    onFocusedLogConsumed: () -> Unit,
    onCommandClick: (name: String) -> Unit,
    onOpenCommandsPicker: () -> Unit,
    onOpenGroupsPicker: () -> Unit,
    onRepeat: (LogEntry) -> Unit,
) {
    val pagerState = rememberPagerState(initialPage = currentPage) { 2 }

    LaunchedEffect(currentPage) {
        if (pagerState.currentPage != currentPage) {
            pagerState.animateScrollToPage(currentPage)
        }
    }
    LaunchedEffect(pagerState) {
        snapshotFlow { pagerState.settledPage }.collect { onPageChange(it) }
    }
    LaunchedEffect(focusedLogId) {
        // Only the page flip happens here -- LogScreen itself owns clearing focusedLogId (via
        // onFocusedLogConsumed), once it's actually scrolled to and expanded the matching row.
        if (focusedLogId != null) {
            pagerState.animateScrollToPage(1)
        }
    }

    // statusBarsPadding() matters here specifically because this is the first screen with
    // interactive content (the leave-group chip's close button) anchored to the very top -- edge-
    // to-edge (the default this project's targetSdk 36 enforces) draws content underneath the
    // system status bar, and a tap landing in that overlap silently never reaches the app (see
    // MainScannerWidget.kt's own statusBarsPadding() calls for the same fix applied earlier).
    Column(modifier = Modifier.fillMaxSize().statusBarsPadding().testTag("screen_main")) {
        if (currentGroup == DEFAULT_GROUP) {
            Text(
                statusText,
                modifier = Modifier.fillMaxWidth().padding(horizontal = 16.dp, vertical = 8.dp).testTag("statusText"),
            )
        } else {
            GroupContextBar(currentGroup, onLeave = { onGroupChange(DEFAULT_GROUP) })
        }
        HorizontalPager(state = pagerState, modifier = Modifier.fillMaxSize()) { page ->
            when (page) {
                0 -> GroupPageScreen(
                    group = currentGroup,
                    onCommandClick = onCommandClick,
                    onOpenCommandsPicker = onOpenCommandsPicker,
                    onOpenGroupsPicker = onOpenGroupsPicker,
                )
                1 -> LogScreen(
                    focusedLogId = focusedLogId,
                    onFocusedLogConsumed = onFocusedLogConsumed,
                    onRepeat = onRepeat,
                    onEnterGroup = onGroupChange,
                )
            }
        }
    }
}

@Composable
private fun GroupContextBar(group: String, onLeave: () -> Unit) {
    Surface(
        color = MaterialTheme.colorScheme.secondaryContainer,
        modifier = Modifier.fillMaxWidth().testTag("groupContextBar"),
    ) {
        Row(
            modifier = Modifier.fillMaxWidth().padding(horizontal = 16.dp, vertical = 8.dp),
            verticalAlignment = Alignment.CenterVertically,
        ) {
            Text("Group: $group", modifier = Modifier.weight(1f).testTag("groupContextLabel"))
            IconButton(onClick = onLeave, modifier = Modifier.testTag("groupContextLeave")) {
                Icon(Icons.Filled.Close, contentDescription = "Leave group")
            }
        }
    }
}
