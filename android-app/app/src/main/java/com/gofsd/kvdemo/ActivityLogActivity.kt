package com.gofsd.kvdemo

import android.app.Activity
import android.os.Bundle
import android.view.View
import android.widget.ScrollView
import android.widget.TextView

// Shows OutputLog's full history (see that file's doc comment) -- every
// WatchExecute/WatchCommandLog/RunCommandDispatcher notification delivered
// since this process started, regardless of which CommandDetailActivity
// screen, if any, was open when it arrived -- plus live updates for
// anything that arrives while this screen itself is the foregrounded one.
class ActivityLogActivity : Activity() {
    private lateinit var logText: TextView
    private lateinit var logScroll: ScrollView

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_log)

        logText = findViewById(R.id.logText)
        logScroll = findViewById(R.id.logScroll)
        logText.text = OutputLog.snapshot().joinToString("\n\n")
    }

    override fun onResume() {
        super.onResume()
        OutputLog.setListener { line -> runOnUiThread { appendLine(line) } }
    }

    override fun onPause() {
        super.onPause()
        OutputLog.setListener(null)
    }

    private fun appendLine(line: String) {
        logText.append(if (logText.text.isEmpty()) line else "\n\n$line")
        logScroll.post { logScroll.fullScroll(View.FOCUS_DOWN) }
    }
}
