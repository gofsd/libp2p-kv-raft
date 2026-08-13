package com.gofsd.kvdemo

import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.material3.DropdownMenu
import androidx.compose.material3.DropdownMenuItem
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.ExposedDropdownMenuBox
import androidx.compose.material3.ExposedDropdownMenuDefaults
import androidx.compose.material3.MenuAnchorType
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.testTag

/**
 * A read-only "pick one of these" field -- GroupPickerScreen's ~15-category list uses this
 * (unfilterable is fine at that size) instead of each hand-rolling an ExposedDropdownMenuBox,
 * since it's a plain shape: a label, every option's own item, and a callback for the one the
 * user tapped. See [SearchableSelectDropdown] for the filterable sibling CommandPickerScreen
 * uses instead, given its ~100+ options.
 */
@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun SelectDropdown(
    label: String,
    options: List<String>,
    selected: String?,
    onSelect: (String) -> Unit,
    testTag: String,
) {
    var expanded by remember { mutableStateOf(false) }

    ExposedDropdownMenuBox(
        expanded = expanded,
        onExpandedChange = { expanded = it },
        modifier = Modifier.fillMaxWidth().testTag(testTag),
    ) {
        OutlinedTextField(
            value = selected ?: "",
            onValueChange = {},
            readOnly = true,
            label = { Text(label) },
            trailingIcon = { ExposedDropdownMenuDefaults.TrailingIcon(expanded = expanded) },
            modifier = Modifier.fillMaxWidth().menuAnchor(MenuAnchorType.PrimaryNotEditable),
        )
        DropdownMenu(
            expanded = expanded,
            onDismissRequest = { expanded = false },
        ) {
            options.forEach { option ->
                DropdownMenuItem(
                    text = { Text(option) },
                    onClick = {
                        onSelect(option)
                        expanded = false
                    },
                    modifier = Modifier.testTag("${testTag}_option_$option"),
                )
            }
        }
    }
}

/**
 * [SelectDropdown]'s filterable sibling: the text field is editable, and the dropdown menu below
 * it only ever shows options whose text contains what's been typed so far (case-insensitive) --
 * CommandPickerScreen's flat ~100+-command catalog is unusable in a plain unfilterable menu the
 * way [SelectDropdown] renders it. [onSelect] fires only when an actual menu item is tapped (not
 * on every keystroke), same contract as [SelectDropdown] -- free-typed text that never resolves
 * to a tap just filters the menu, it doesn't itself become a selection.
 */
@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun SearchableSelectDropdown(
    label: String,
    options: List<String>,
    selected: String?,
    onSelect: (String) -> Unit,
    testTag: String,
) {
    var expanded by remember { mutableStateOf(false) }
    var query by remember { mutableStateOf(selected ?: "") }
    val filtered = remember(query, options) {
        if (query.isBlank()) options else options.filter { it.contains(query, ignoreCase = true) }
    }

    ExposedDropdownMenuBox(
        expanded = expanded,
        onExpandedChange = { expanded = it },
        modifier = Modifier.fillMaxWidth().testTag(testTag),
    ) {
        OutlinedTextField(
            value = query,
            onValueChange = {
                query = it
                expanded = true
            },
            label = { Text(label) },
            trailingIcon = { ExposedDropdownMenuDefaults.TrailingIcon(expanded = expanded) },
            modifier = Modifier.fillMaxWidth().menuAnchor(MenuAnchorType.PrimaryEditable),
        )
        DropdownMenu(
            expanded = expanded && filtered.isNotEmpty(),
            onDismissRequest = { expanded = false },
        ) {
            filtered.forEach { option ->
                DropdownMenuItem(
                    text = { Text(option) },
                    onClick = {
                        query = option
                        onSelect(option)
                        expanded = false
                    },
                    modifier = Modifier.testTag("${testTag}_option_$option"),
                )
            }
        }
    }
}
