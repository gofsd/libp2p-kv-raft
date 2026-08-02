package com.gofsd.kvdemo

import android.util.Base64
import kvmobile.ChannelCallback
import kvmobile.CommandDispatchHandler
import kvmobile.ExecuteCallback
import kvmobile.Kvmobile
import kvmobile.LogCallback

/**
 * One kvmobile call exposed in the command-runner UI (see MainActivity).
 * [params] names the ordered text fields to render for it; [run] performs
 * the call against those raw string values (off the UI thread) and
 * returns the text to append to the output log, or throws to report a
 * failure -- MainActivity renders either outcome the same way.
 */
class CommandSpec(
    val category: String,
    val name: String,
    val params: List<String>,
    val run: (List<String>) -> String,
) {
    val label: String get() = "$category: $name"
}

private fun ok() = "OK"

private fun String.toLongOrThrow(field: String): Long =
    toLongOrNull() ?: throw IllegalArgumentException("$field must be a whole number")

private fun String.toBooleanOrThrow(field: String): Boolean =
    toBooleanStrictOrNull() ?: throw IllegalArgumentException("$field must be \"true\" or \"false\"")

/**
 * The full kvmobile API surface, one CommandSpec per exported function --
 * mirrors README.md's "Follower on Android" section (and, through it,
 * every `mage` target it documents an Android equivalent for) closely
 * enough that this list and that section should stay in sync. [dataDir]
 * is bound automatically to the app's own private storage, the same
 * directory Start has always used, rather than exposed as a field -- it's
 * an internal Android path, not something a user meaningfully types.
 * [appendLog] lets WatchExecute/WatchCommandLog's callbacks post
 * additional output lines after their initial call returns, since both
 * register a standing subscription rather than answering once.
 */
fun buildCommands(dataDir: String, appendLog: (String) -> Unit): List<CommandSpec> {
    val commands = mutableListOf<CommandSpec>()
    fun add(category: String, name: String, params: List<String>, run: (List<String>) -> String) {
        commands += CommandSpec(category, name, params, run)
    }

    // Cluster lifecycle -- see README's "Follower on Android" section for
    // why Start/StartWithKey/Join/JoinWithKey/Stop/Delete/Leave/Rm map
    // onto desktop's addfollower/addfollowerwithkey/join/joinwithkey/
    // use/deletenode/leave/rm the way they do (kvmobile runs exactly one
    // daemon per process and can never bootstrap as a fresh leader).
    add("Cluster", "Start", emptyList()) { Kvmobile.start(dataDir) }
    add("Cluster", "StartWithKey", listOf("keyHex")) { a -> Kvmobile.startWithKey(dataDir, a[0]) }
    add("Cluster", "Join", listOf("leaderAddr")) { a -> Kvmobile.join(dataDir, a[0]) }
    add("Cluster", "JoinWithKey", listOf("keyHex", "leaderAddr")) { a -> Kvmobile.joinWithKey(dataDir, a[0], a[1]) }
    // StartSolo/StartSoloWithKey bootstrap this device as the sole leader
    // of its own brand new single-node cluster, instead of joining
    // leaderMultiaddr -- desktop's `mage addnode` with no leaderPeerID.
    // Only safe to invoke for real against a genuinely fresh device (it's
    // destructive to this device's existing membership, same category as
    // Join/StartPending -- see UiCommandE2ETest.kt's own doc comment).
    add("Cluster", "StartSolo", emptyList()) { Kvmobile.startSolo(dataDir) }
    add("Cluster", "StartSoloWithKey", listOf("keyHex")) { a -> Kvmobile.startSoloWithKey(dataDir, a[0]) }
    // StartSoloWithKeyAndPort/StartPendingWithKeyAndPort (below) pin the
    // libp2p listen port instead of leaving it ephemeral -- only needed
    // by pkg/e2erun/android_pair.go's two-emulator Join/RecruitPeer
    // scenario, see StartSoloWithKeyAndPort's own doc comment for why.
    add("Cluster", "StartSoloWithKeyAndPort", listOf("keyHex", "port")) { a ->
        Kvmobile.startSoloWithKeyAndPort(dataDir, a[0], a[1].toLongOrThrow("port"))
    }

    // Reverse invite ("join-request"): StartPending brings this device up
    // with no cluster yet (instead of Start/Join's immediate leader join);
    // GetOwnAddr returns this device's own current best-advertised address
    // (queried live -- a relay reservation completes asynchronously after
    // StartPending returns, so call it again if an earlier call returned a
    // private/loopback address); CreateJoinRequest mints a ticket to hand
    // some other cluster's voter (combine with GetOwnAddr's address as
    // "<addr>#<tokenHex>" into a barcode); RecruitPeer is the other
    // direction -- this device, already a voter somewhere, admits whatever
    // device scanned/typed a ticket into its own cluster. See README's
    // join-request section and mobile/kvmobile/joinrequest.go.
    add("Cluster", "StartPending", emptyList()) { Kvmobile.startPending(dataDir) }
    add("Cluster", "StartPendingWithKey", listOf("keyHex")) { a -> Kvmobile.startPendingWithKey(dataDir, a[0]) }
    add("Cluster", "StartPendingWithKeyAndPort", listOf("keyHex", "port")) { a ->
        Kvmobile.startPendingWithKeyAndPort(dataDir, a[0], a[1].toLongOrThrow("port"))
    }
    add("Cluster", "GetOwnAddr", emptyList()) { Kvmobile.getOwnAddr() }
    add("Cluster", "CreateJoinRequest", emptyList()) { Kvmobile.createJoinRequest() }
    add("Cluster", "CancelJoinRequest", listOf("tokenHex")) { a -> Kvmobile.cancelJoinRequest(a[0]); ok() }
    add("Cluster", "RecruitPeer", listOf("ticket (addr#tokenHex)", "voter|learner")) { a ->
        Kvmobile.recruitPeer(a[0], a[1])
    }
    // CreateJoinInvite mints a one-time token granting suffrage on THIS
    // device's own cluster, without hand-delivering it the way RecruitPeer
    // does -- combine with GetOwnAddr's address as "<addr>#<tokenHex>" for
    // some other device's own Join/Start to redeem directly (admitted
    // immediately even if this cluster's leader normally requires
    // confirmation). RevokeJoinInvite deletes one before it's ever
    // redeemed. Only take effect if this device is itself a raft voter.
    add("Cluster", "CreateJoinInvite", listOf("voter|learner")) { a -> Kvmobile.createJoinInvite(a[0]) }
    add("Cluster", "RevokeJoinInvite", listOf("tokenHex")) { a -> Kvmobile.revokeJoinInvite(a[0]); ok() }

    add("Cluster", "Stop", emptyList()) { Kvmobile.stop(); ok() }
    add("Cluster", "Delete", emptyList()) { Kvmobile.delete(dataDir); ok() }
    add("Cluster", "Leave", emptyList()) { Kvmobile.leave(); ok() }
    add("Cluster", "Rm", emptyList()) { Kvmobile.rm(); ok() }
    // Force-removes some OTHER peer from this device's cluster, without
    // that peer's cooperation -- desktop's `mage kick`. Unlike Leave/Rm,
    // this device's own membership is untouched; only takes effect if
    // this device is itself a raft voter (or forwards to one), true for
    // any real device build -- see Kvmobile.kick's doc comment.
    add("Cluster", "Kick", listOf("targetPeerID")) { a -> Kvmobile.kick(a[0]); ok() }
    add("Cluster", "ListClusters", emptyList()) { Kvmobile.listClusters() }
    add("Cluster", "ListClusterMembers", emptyList()) { Kvmobile.listClusterMembers() }
    add("Cluster", "PeerID", emptyList()) { Kvmobile.peerID() }
    // This device's own deterministic cmd/kvhttp bearer token -- desktop's
    // `mage accesstoken <peerID>` counterpart, e.g. to hand a desktop
    // operator this device's token for a kvhttp routing rule.
    add("Cluster", "AccessToken", emptyList()) { Kvmobile.accessToken() }

    // KV
    add("KV", "Submit", listOf("key", "value")) { a -> Kvmobile.submit(a[0], a[1]); ok() }
    add("KV", "Get", listOf("key")) { a -> Kvmobile.get(a[0]) }
    add("KV", "RangeScan", listOf("start", "end", "limit (0=unlimited)")) { a ->
        Kvmobile.rangeScan(a[0], a[1], a[2].toLongOrThrow("limit"))
    }

    // Permits
    add("Permits", "RequestPermit", listOf("kind (peer|bootstrap)", "targetPeerID", "metadata")) { a ->
        Kvmobile.requestPermit(a[0], a[1], a[2]); ok()
    }
    add("Permits", "ConfirmPermit", listOf("kind (peer|bootstrap)", "targetPeerID")) { a ->
        Kvmobile.confirmPermit(a[0], a[1]); ok()
    }
    add("Permits", "RevokePermit", listOf("kind (peer|bootstrap)", "targetPeerID")) { a ->
        Kvmobile.revokePermit(a[0], a[1]); ok()
    }
    add("Permits", "RequestLogPermit", listOf("logKind", "targetPeerID", "metadata")) { a ->
        Kvmobile.requestLogPermit(a[0], a[1], a[2]); ok()
    }
    add("Permits", "ConfirmLogPermit", listOf("logKind", "targetPeerID")) { a ->
        Kvmobile.confirmLogPermit(a[0], a[1]); ok()
    }
    add("Permits", "RevokeLogPermit", listOf("logKind", "targetPeerID")) { a ->
        Kvmobile.revokeLogPermit(a[0], a[1]); ok()
    }

    // Execute -- the raft-bypassing peer-to-peer notification.
    add("Execute", "Execute", listOf("destPeerID", "value")) { a -> Kvmobile.execute(a[0], a[1]); ok() }
    add("Execute", "PollExecute", emptyList()) { Kvmobile.pollExecute() }
    add("Execute", "WatchExecute", emptyList()) {
        Kvmobile.watchExecute(object : ExecuteCallback {
            override fun onNotification(senderPeerID: String, value: String) {
                appendLog("Execute from $senderPeerID: $value")
            }
        })
        "Watching -- notifications appear below as they arrive"
    }
    add("Execute", "StopWatchExecute", emptyList()) { Kvmobile.stopWatchExecute(); ok() }

    // Channel -- a raw, persistent, bidirectional byte pipe to another
    // peer, the mobile port of desktop's `mage openchannel`/`listenchannel`
    // (see README's "Raw Channel" section and
    // mobile/kvmobile/channel.go's own doc comments for the wire design).
    // Kvmobile.sendChannelData/ChannelCallback.onData carry raw ByteArray
    // across the gomobile boundary directly (gobind binds Go []byte to
    // Kotlin ByteArray natively -- no base64 needed there); base64 only
    // shows up here, at the UI edge, because a phone keyboard has no way
    // to type arbitrary binary into an EditText -- SendChannelData's own
    // "chunk (base64)" field is decoded before the actual call, and
    // received data is re-encoded only for display in the shared,
    // text-only Activity Log. OpenChannel/ListenChannel both start a
    // standing delivery loop, same "outlives this screen, logged to the
    // shared Activity Log" treatment WatchExecute/WatchCommandLog already
    // get.
    val channelCallback = { label: String ->
        object : ChannelCallback {
            override fun onData(purpose: String, chunk: ByteArray) {
                appendLog("$label [$purpose] data (base64): ${Base64.encodeToString(chunk, Base64.NO_WRAP)}")
            }
            override fun onClosed(reason: String) {
                appendLog(if (reason.isEmpty()) "$label closed" else "$label closed: $reason")
            }
        }
    }
    add("Channel", "OpenChannel", listOf("peerID")) { a ->
        val channelID = Kvmobile.openChannel(a[0], channelCallback("Channel[to ${a[0]}]"))
        "Opened $channelID -- incoming data appears below as it arrives"
    }
    add("Channel", "ListenChannel", emptyList()) {
        val resultJson = Kvmobile.listenChannel(channelCallback("Channel[incoming]"))
        "Claimed $resultJson -- incoming data appears below as it arrives"
    }
    add("Channel", "StopListenChannel", emptyList()) { Kvmobile.stopListenChannel(); ok() }
    add("Channel", "SendChannelData", listOf("channelID", "purpose", "chunk (base64)")) { a ->
        Kvmobile.sendChannelData(a[0], a[1], Base64.decode(a[2], Base64.NO_WRAP)); ok()
    }
    add("Channel", "CloseChannelWrite", listOf("channelID")) { a -> Kvmobile.closeChannelWrite(a[0]); ok() }
    add("Channel", "CloseChannel", listOf("channelID")) { a -> Kvmobile.closeChannel(a[0]); ok() }
    add("Channel", "StopChannel", listOf("channelID")) { a -> Kvmobile.stopChannel(a[0]); ok() }

    // pkg/logrecord read/write.
    add("Log records", "LogAppend", listOf("kind", "unitID", "fieldsJSON", "narrative")) { a ->
        Kvmobile.logAppend(a[0], a[1], a[2], a[3]); ok()
    }
    add(
        "Log records", "LogQuery",
        listOf("kind", "unitID", "since (RFC3339 or blank)", "until (RFC3339 or blank)", "limit (blank=unlimited)"),
    ) { a -> Kvmobile.logQuery(a[0], a[1], a[2], a[3], a[4]) }

    // Group/Command ACL catalog -- daemon-enforced records, see README's
    // "Group/command ACL" section for the model this mirrors exactly.
    add("Group", "CreateGroup", listOf("id", "name", "public (true|false)")) { a ->
        Kvmobile.createGroup(a[0], a[1], a[2].toBooleanOrThrow("public")); ok()
    }
    add("Group", "UpdateGroup", listOf("id", "name", "public (true|false)")) { a ->
        Kvmobile.updateGroup(a[0], a[1], a[2].toBooleanOrThrow("public")); ok()
    }
    add("Group", "DeleteGroup", listOf("id")) { a -> Kvmobile.deleteGroup(a[0]); ok() }
    add("Group", "GetGroup", listOf("id")) { a -> Kvmobile.getGroup(a[0]) }
    add("Group", "ListGroups", emptyList()) { Kvmobile.listGroups() }

    add("Command", "CreateCommand", listOf("id", "name", "targetPeerID")) { a ->
        Kvmobile.createCommand(a[0], a[1], a[2]); ok()
    }
    add("Command", "UpdateCommand", listOf("id", "name", "targetPeerID")) { a ->
        Kvmobile.updateCommand(a[0], a[1], a[2]); ok()
    }
    add("Command", "DeleteCommand", listOf("id")) { a -> Kvmobile.deleteCommand(a[0]); ok() }
    add("Command", "GetCommand", listOf("id")) { a -> Kvmobile.getCommand(a[0]) }
    add("Command", "ListCommands", emptyList()) { Kvmobile.listCommands() }

    add("Links", "AddCommandToGroup", listOf("commandID", "groupID")) { a ->
        Kvmobile.addCommandToGroup(a[0], a[1]); ok()
    }
    add("Links", "RemoveCommandFromGroup", listOf("commandID", "groupID")) { a ->
        Kvmobile.removeCommandFromGroup(a[0], a[1]); ok()
    }
    add("Links", "ListGroupsForCommand", listOf("commandID")) { a -> Kvmobile.listGroupsForCommand(a[0]) }
    add("Links", "AddPeerToGroup", listOf("peerID", "groupID")) { a -> Kvmobile.addPeerToGroup(a[0], a[1]); ok() }
    add("Links", "RemovePeerFromGroup", listOf("peerID", "groupID")) { a ->
        Kvmobile.removePeerFromGroup(a[0], a[1]); ok()
    }
    add("Links", "ListGroupsForPeer", listOf("peerID")) { a -> Kvmobile.listGroupsForPeer(a[0]) }

    // Dispatch -- turns a Command from the catalog into a request/response
    // flow (see README's "Follower on Android" section, dispatch layer).
    add("Dispatch", "SubmitCommand", listOf("commandID", "inputsJSON")) { a -> Kvmobile.submitCommand(a[0], a[1]) }
    add("Dispatch", "GetCommandRequest", listOf("commandID", "instanceID")) { a ->
        Kvmobile.getCommandRequest(a[0], a[1])
    }
    add("Dispatch", "ListCommandRequests", listOf("commandID")) { a -> Kvmobile.listCommandRequests(a[0]) }
    add("Dispatch", "ListExecutionsByPeer", listOf("peerID")) { a -> Kvmobile.listExecutionsByPeer(a[0]) }
    add(
        "Dispatch", "AppendCommandLog",
        listOf("requesterPeerID (blank=no poke)", "instanceID", "fieldsJSON", "narrative"),
    ) { a -> Kvmobile.appendCommandLog(a[0], a[1], a[2], a[3]); ok() }
    add(
        "Dispatch", "QueryCommandLog",
        listOf("instanceID", "since (RFC3339 or blank)", "until (RFC3339 or blank)", "limit (blank=unlimited)"),
    ) { a -> Kvmobile.queryCommandLog(a[0], a[1], a[2], a[3]) }
    add("Dispatch", "LatestCommandLog", listOf("instanceID")) { a -> Kvmobile.latestCommandLog(a[0]) }
    add("Dispatch", "WatchCommandLog", listOf("instanceID")) { a ->
        val instanceID = a[0]
        Kvmobile.watchCommandLog(instanceID, object : LogCallback {
            override fun onRecords(recordsJSON: String) {
                appendLog("CommandLog[$instanceID]: $recordsJSON")
            }
        })
        "Watching -- new records appear below as they arrive"
    }
    add("Dispatch", "StopWatchCommandLog", listOf("instanceID")) { a -> Kvmobile.stopWatchCommandLog(a[0]); ok() }

    // RunCommandDispatcher is the "target device's own application logic"
    // SubmitCommand/dispatch.go's doc comments describe: a standing
    // handler that answers every CommandRequest against commandID as it
    // arrives. This demo handler just echoes commandID/instanceID/inputs
    // back as the recorded result's fields (see
    // kvmobile.CommandDispatchHandler's doc comment for the
    // {"fields":{...},"narrative":"..."} shape it must return) and logs
    // the dispatch below -- a real app would replace Handle's body with
    // whatever that command actually does.
    add("Dispatch", "RunCommandDispatcher", listOf("commandID")) { a ->
        val commandID = a[0]
        Kvmobile.runCommandDispatcher(commandID, object : CommandDispatchHandler {
            override fun handle(instanceID: String, commandID: String, requestedBy: String, inputs: String): String {
                appendLog("Dispatching $commandID/$instanceID from $requestedBy (inputs: $inputs)")
                return """{"fields":{"handled_by":"android-demo"},"narrative":"echoed by CommandCatalog demo handler"}"""
            }
        })
        "Dispatching -- requests for $commandID are handled as they arrive, logged below"
    }
    add("Dispatch", "StopCommandDispatcher", listOf("commandID")) { a -> Kvmobile.stopCommandDispatcher(a[0]); ok() }

    // One-time execution invites -- see execinvite.go's doc comment for
    // the full design (mirrors desktop's pkg/kvctl/execinvite.go). No
    // printexecinvitedatamatrix equivalent here: CreateExecInvite returns
    // the raw tokenHex, and a real app combines it with its own
    // advertised multiaddr and renders/scans the barcode itself.
    add("ExecInvite", "CreateExecInvite", listOf("commandID", "inputsJSON")) { a ->
        Kvmobile.createExecInvite(a[0], a[1])
    }
    add("ExecInvite", "RevokeExecInvite", listOf("tokenHex")) { a -> Kvmobile.revokeExecInvite(a[0]); ok() }
    add("ExecInvite", "RedeemExecInvite", listOf("sourceAddr#tokenHex")) { a -> Kvmobile.redeemExecInvite(a[0]) }

    // Raw escape hatch -- the same one E2ETest uses, see its own doc
    // comment and README's "Follower on Android" section.
    add("Raw", "SendEvent", listOf("eventJSON")) { a -> Kvmobile.sendEvent(a[0]) }

    // Test-only utility, not a kvmobile call: keeps this instrumentation
    // invocation's process (and so whatever daemon an earlier op in the
    // same invocation just resumed) alive for millis. Only needed by
    // pkg/e2erun/android_pair.go's runDialStep -- a cross-device dial
    // needs the *receiving* device to still be up and listening at the
    // exact moment the *other* device dials in, which a device's own
    // single `adb shell am instrument` invocation can't otherwise
    // guarantee once its own ops finish (confirmed live via a tcpdump
    // capture on the host loopback interface: the adb-forwarded TCP
    // handshake completed, but the far end sent a bare FIN within ~2.5ms,
    // before any bytes were exchanged, because that receiving device's
    // prior instrumentation process -- and with it, its daemon's listen
    // socket -- had already exited).
    add("Test", "SleepMillis", listOf("millis")) { a -> Thread.sleep(a[0].toLongOrThrow("millis")); ok() }

    return commands
}
