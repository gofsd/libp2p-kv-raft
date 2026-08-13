package com.gofsd.kvdemo

import android.util.Log
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext

private const val TAG = "KVDemo"

/**
 * The one place any [CommandSpec] actually runs -- called by [RunConfirmDialog]'s Execute button
 * and nowhere else, now that CommandDetailScreen's own Run button is gone (see that class's own
 * doc comment): every command, local or peer-facing, executes only by being scanned (as a
 * [RunCode]) and confirmed here. Always runs `spec.run(args)` on [Dispatchers.IO], records
 * pass/fail to the persisted [OutputLog] (read back by pkg/e2erun/android_optical.go's
 * verification, same as every command's outcome always has been), and returns the
 * "$label(args) ->\nresult" / "... FAILED: message" line CommandDetailScreen's old Run button
 * used to build, for a caller that wants to show it immediately too.
 */
object CommandExecutor {
    suspend fun execute(spec: CommandSpec, args: List<String>): String {
        Log.i(TAG, "AUTO: executing ${spec.label}(${args.joinToString(", ")})")
        return try {
            val result = withContext(Dispatchers.IO) { spec.run(args) }
            Log.i(TAG, "RESULT: ${spec.label} -> $result")
            OutputLog.record(spec.label, "(${args.joinToString(", ")}) ->\n$result", LogStatus.SUCCESS)
            "${spec.label}(${args.joinToString(", ")}) ->\n$result"
        } catch (e: Exception) {
            Log.w(TAG, "RESULT: ${spec.label} FAILED: ${e.message}")
            OutputLog.record(spec.label, "(${args.joinToString(", ")}) FAILED: ${e.message}", LogStatus.FAILED)
            "${spec.label}(${args.joinToString(", ")}) FAILED: ${e.message}"
        }
    }
}
