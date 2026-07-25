package com.gofsd.kvdemo

import android.app.Activity
import android.content.Intent
import android.os.Bundle
import android.widget.AdapterView
import android.widget.ArrayAdapter
import android.widget.Button
import android.widget.ListView
import android.widget.TextView
import kvmobile.Kvmobile

// Landing screen: brings up the kvmobile daemon once for this process's
// whole lifetime (every other screen assumes it's either already up or on
// its way up -- none of them re-trigger Start), then lists every
// CommandCatalog.kt category. Tap one to browse its commands
// (CommandListActivity -> CommandDetailActivity); tap "Activity Log" to
// see every WatchExecute/WatchCommandLog/RunCommandDispatcher notification
// delivered so far regardless of which screen was open when it arrived
// (see OutputLog). Replaces the single flat spinner-over-every-command
// screen this app used before with real category -> command -> detail
// navigation; CommandCatalog.kt's ~60-command CommandSpec list itself is
// unchanged, just browsed differently. This device runs a raft follower
// in-process (see kvmobile's package doc) and every Submit is forwarded
// from that follower to the current leader over
// pkg/daemon.ForwardProtocolID, wherever the leader happens to be running.
class MainActivity : Activity() {
    private lateinit var statusText: TextView
    private lateinit var categoryList: ListView

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_main)

        statusText = findViewById(R.id.statusText)
        categoryList = findViewById(R.id.itemList)
        val logButton = findViewById<Button>(R.id.logButton)

        val categories = buildCommands(filesDir.absolutePath, OutputLog::append)
            .map { it.category }
            .distinct()

        categoryList.adapter = ArrayAdapter(this, android.R.layout.simple_list_item_1, categories)
        categoryList.onItemClickListener = AdapterView.OnItemClickListener { _, _, position, _ ->
            startActivity(
                Intent(this, CommandListActivity::class.java)
                    .putExtra(CommandListActivity.EXTRA_CATEGORY, categories[position]),
            )
        }
        logButton.setOnClickListener { startActivity(Intent(this, ActivityLogActivity::class.java)) }

        Thread {
            try {
                val peerID = Kvmobile.start(filesDir.absolutePath)
                runOnUiThread { statusText.text = "Connected as $peerID" }
            } catch (e: Exception) {
                runOnUiThread { statusText.text = "Failed to start: ${e.message}" }
            }
        }.start()
    }
}
