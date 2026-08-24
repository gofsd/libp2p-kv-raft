package com.gofsd.kvdemo

import androidx.compose.foundation.background
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.automirrored.filled.Send
import androidx.compose.material.icons.filled.Book
import androidx.compose.material.icons.filled.ChevronRight
import androidx.compose.material.icons.filled.Code
import androidx.compose.material.icons.filled.DataObject
import androidx.compose.material.icons.filled.Group
import androidx.compose.material.icons.filled.Hub
import androidx.compose.material.icons.filled.Key
import androidx.compose.material.icons.filled.Link
import androidx.compose.material.icons.filled.PersonAdd
import androidx.compose.material.icons.filled.PlayArrow
import androidx.compose.material.icons.filled.Router
import androidx.compose.material.icons.filled.Schedule
import androidx.compose.material.icons.filled.Science
import androidx.compose.material.icons.filled.SettingsInputAntenna
import androidx.compose.material.icons.filled.Storage
import androidx.compose.material.icons.filled.SwapHoriz
import androidx.compose.material.icons.filled.Terminal
import androidx.compose.material3.Card
import androidx.compose.material3.CardDefaults
import androidx.compose.material3.Icon
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.vector.ImageVector
import androidx.compose.ui.platform.testTag
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp

/**
 * One tappable row in a command list -- object-history-app's
 * `core/ui/components/CommandListItem.kt` ported onto this app's [CommandSpec], and the reason
 * [GroupPageScreen] no longer renders bare `Text` rows. A card with an icon tile, the command's
 * name, a subtitle, and a chevron: enough shape that a ~30-entry category reads at a glance
 * instead of as an undifferentiated column of words.
 *
 * The subtitle is the spec's own **parameter list**, not a description. That is a deliberate
 * departure from the original, which carries a hand-written `description` per command: there are
 * 135 specs here across 17 categories, all of them thin wrappers over one `Kvmobile` call whose
 * name already says what it does, and what is genuinely not visible from the name is what the
 * command is going to *ask you for*. "key, value" before you tap is worth more here than a
 * sentence restating the title, and it costs `CommandCatalog.kt` nothing to keep current -- a spec
 * that gains a parameter gains it in the list automatically.
 *
 * [testTag] is passed in rather than derived, so every tag the existing e2e harness clicks
 * (`listItem_$name`, `mainListItem_commands`, `luaCommand_$id`, ...) lands on this card unchanged.
 */
@Composable
fun CommandListItem(
    title: String,
    subtitle: String?,
    icon: ImageVector,
    testTag: String,
    onClick: () -> Unit,
) {
    Card(
        modifier = Modifier
            .fillMaxWidth()
            .padding(horizontal = 12.dp, vertical = 4.dp)
            .testTag(testTag),
        shape = RoundedCornerShape(14.dp),
        colors = CardDefaults.cardColors(containerColor = MaterialTheme.colorScheme.surfaceVariant),
        elevation = CardDefaults.cardElevation(defaultElevation = 0.dp),
        onClick = onClick,
    ) {
        Row(
            modifier = Modifier.fillMaxWidth().padding(horizontal = 14.dp, vertical = 10.dp),
            verticalAlignment = Alignment.CenterVertically,
        ) {
            Box(
                modifier = Modifier
                    .size(36.dp)
                    .background(MaterialTheme.colorScheme.primaryContainer, RoundedCornerShape(10.dp)),
                contentAlignment = Alignment.Center,
            ) {
                Icon(
                    icon,
                    contentDescription = null,
                    modifier = Modifier.size(18.dp),
                    tint = MaterialTheme.colorScheme.onPrimaryContainer,
                )
            }

            Column(modifier = Modifier.weight(1f).padding(start = 12.dp)) {
                Text(
                    title,
                    style = MaterialTheme.typography.titleSmall,
                    fontWeight = FontWeight.SemiBold,
                    color = MaterialTheme.colorScheme.onSurface,
                    maxLines = 1,
                    overflow = TextOverflow.Ellipsis,
                )
                if (subtitle != null) {
                    Text(
                        subtitle,
                        style = MaterialTheme.typography.bodySmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant,
                        maxLines = 1,
                        overflow = TextOverflow.Ellipsis,
                    )
                }
            }

            Icon(
                Icons.Filled.ChevronRight,
                contentDescription = null,
                modifier = Modifier.width(20.dp),
                tint = MaterialTheme.colorScheme.onSurfaceVariant,
            )
        }
    }
}

/**
 * The subtitle line for a real [CommandSpec] -- its parameters, or an explicit statement that it
 * takes none. Said out loud rather than left blank: an empty second line reads as missing
 * information, whereas "no parameters" is itself the useful fact that tapping this row leads
 * straight to a Generate button.
 */
fun commandSubtitle(spec: CommandSpec): String =
    if (spec.params.isEmpty()) "no parameters" else spec.params.joinToString(", ")

/**
 * One icon per `CommandCatalog.kt` category. Per-category rather than per-command (the original
 * keys off the command id) because a category here holds up to 31 commands that genuinely are all
 * the same *kind* of thing, and 135 distinct glyphs would carry no information anyway -- what the
 * icon is for is telling a Cluster row from a KV row at a glance while scrolling.
 */
fun iconForCategory(category: String): ImageVector = when (category) {
    "Cluster" -> Icons.Filled.Hub
    "KV" -> Icons.Filled.Storage
    "Lua" -> Icons.Filled.Code
    "Channel" -> Icons.Filled.SwapHoriz
    "Journal" -> Icons.Filled.Book
    "Cron" -> Icons.Filled.Schedule
    "Dispatch" -> Icons.AutoMirrored.Filled.Send
    "Command" -> Icons.Filled.Terminal
    "Group" -> Icons.Filled.Group
    "Station" -> Icons.Filled.Router
    "Permits" -> Icons.Filled.Key
    "RelayNode" -> Icons.Filled.SettingsInputAntenna
    "Links" -> Icons.Filled.Link
    "ExecInvite" -> Icons.Filled.PersonAdd
    "Raw" -> Icons.Filled.DataObject
    "Test" -> Icons.Filled.Science
    else -> Icons.Filled.PlayArrow
}
