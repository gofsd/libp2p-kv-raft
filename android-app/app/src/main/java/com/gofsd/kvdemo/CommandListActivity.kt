package com.gofsd.kvdemo

import android.app.Activity
import android.content.Intent
import android.os.Bundle
import android.widget.AdapterView
import android.widget.ArrayAdapter
import android.widget.ListView
import android.widget.TextView

// Every CommandSpec (see CommandCatalog.kt) in the category
// MainActivity's category list was tapped for; tap one to open its own
// CommandDetailActivity. Rebuilds the full command list itself rather
// than having it passed in -- CommandSpec.run is a closure, which an
// Intent extra can't carry, and buildCommands is cheap/side-effect-free
// to call again (nothing in it touches kvmobile until a Run button is
// actually tapped on CommandDetailActivity).
class CommandListActivity : Activity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_command_list)

        val category = intent.getStringExtra(EXTRA_CATEGORY) ?: ""
        findViewById<TextView>(R.id.categoryTitle).text = category

        val names = buildCommands(filesDir.absolutePath, OutputLog::append)
            .filter { it.category == category }
            .map { it.name }

        val list = findViewById<ListView>(R.id.itemList)
        list.adapter = ArrayAdapter(this, android.R.layout.simple_list_item_1, names)
        list.onItemClickListener = AdapterView.OnItemClickListener { _, _, position, _ ->
            startActivity(
                Intent(this, CommandDetailActivity::class.java)
                    .putExtra(CommandDetailActivity.EXTRA_CATEGORY, category)
                    .putExtra(CommandDetailActivity.EXTRA_COMMAND, names[position]),
            )
        }
    }

    companion object {
        const val EXTRA_CATEGORY = "category"
    }
}
