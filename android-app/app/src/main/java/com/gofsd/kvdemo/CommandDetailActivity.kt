package com.gofsd.kvdemo

import android.app.Activity
import android.os.Bundle
import android.view.View
import android.widget.Button
import android.widget.EditText
import android.widget.LinearLayout
import android.widget.ScrollView
import android.widget.TextView

// One CommandSpec's own screen (see CommandCatalog.kt): a labeled input
// field per parameter, a Run button, and this screen's own local output
// log of every Run made here -- scoped to this screen alone, not shared
// with OutputLog/ActivityLogActivity (that log is only for the
// WatchExecute/WatchCommandLog/RunCommandDispatcher notifications
// CommandCatalog.kt's callbacks deliver on their own schedule, unrelated
// to which detail screen, if any, is currently open). Fresh each visit --
// navigating away and back starts with an empty output log again, the
// same as the old single-screen spinner UI did every time you switched
// commands.
class CommandDetailActivity : Activity() {
    private lateinit var spec: CommandSpec
    private lateinit var paramsContainer: LinearLayout
    private lateinit var runButton: Button
    private lateinit var outputText: TextView
    private lateinit var outputScroll: ScrollView
    private var paramFields: List<EditText> = emptyList()

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_command_detail)

        val category = intent.getStringExtra(EXTRA_CATEGORY) ?: ""
        val name = intent.getStringExtra(EXTRA_COMMAND) ?: ""
        spec = buildCommands(filesDir.absolutePath, OutputLog::append)
            .first { it.category == category && it.name == name }

        title = spec.label
        findViewById<TextView>(R.id.commandTitle).text = spec.label
        paramsContainer = findViewById(R.id.paramsContainer)
        runButton = findViewById(R.id.runButton)
        outputText = findViewById(R.id.outputText)
        outputScroll = findViewById(R.id.outputScroll)

        paramFields = spec.params.map { hint ->
            EditText(this).apply {
                this.hint = hint
                layoutParams = LinearLayout.LayoutParams(
                    LinearLayout.LayoutParams.MATCH_PARENT,
                    LinearLayout.LayoutParams.WRAP_CONTENT,
                )
            }
        }
        paramFields.forEach { paramsContainer.addView(it) }

        runButton.setOnClickListener { onRun() }
    }

    // onRun reads the current parameter field values and calls the
    // command off the UI thread (every kvmobile call may block on a
    // shmring round trip), appending its result or error to this screen's
    // own output log either way.
    private fun onRun() {
        val args = paramFields.map { it.text.toString() }
        runButton.isEnabled = false
        Thread {
            val line = try {
                "${spec.label}(${args.joinToString(", ")}) ->\n${spec.run(args)}"
            } catch (e: Exception) {
                "${spec.label}(${args.joinToString(", ")}) FAILED: ${e.message}"
            }
            runOnUiThread {
                appendOutput(line)
                runButton.isEnabled = true
            }
        }.start()
    }

    private fun appendOutput(line: String) {
        outputText.append(if (outputText.text.isEmpty()) line else "\n\n$line")
        outputScroll.post { outputScroll.fullScroll(View.FOCUS_DOWN) }
    }

    companion object {
        const val EXTRA_CATEGORY = "category"
        const val EXTRA_COMMAND = "command"
    }
}
