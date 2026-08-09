package com.gofsd.kvdemo

import android.os.Bundle
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent

/**
 * Single-Activity host for the whole app -- see AppRoot's doc comment for
 * the NavHost/ScannerHost structure this replaces the old MainActivity/
 * CommandListActivity/CommandDetailActivity/ActivityLogActivity
 * four-Activity structure with. This device runs a raft follower
 * in-process (see kvmobile's package doc) and every Submit is forwarded
 * from that follower to the current leader over
 * pkg/daemon.ForwardProtocolID, wherever the leader happens to be running.
 */
class MainActivity : ComponentActivity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContent { AppRoot() }
    }
}
