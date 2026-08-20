package com.gofsd.kvdemo

import android.util.Base64
import kvmobile.ChannelCallback
import kvmobile.CommandDispatchHandler
import kvmobile.CronListener
import kvmobile.ExecuteCallback
import kvmobile.Kvmobile
import kvmobile.LogCallback
import kvmobile.LuaListener

/**
 * One kvmobile call exposed in the command-runner UI (see
 * CommandDetailScreen). [params] names the ordered text fields to render
 * for it; [run] performs the call against those raw string values (off
 * the UI thread) and returns the text to append to the output log, or
 * throws to report a failure. [run] executes exactly one way now: a real
 * camera scan of a RunCode (category+name+params), confirmed via
 * RunConfirmDialog, which calls [run] directly through CommandExecutor --
 * CommandDetailScreen's own Run button is gone. Every spec (bar the one
 * exception below) shares this uniformly, needing no eventOp/per-spec
 * wiring at all.
 *
 * [generateFromResultBase64] is the one spec-specific exception to that
 * uniform RunCode generation: for a command whose [run] already returns
 * one self-contained, signed, base64-encoded ticket (CreateJoinRequestTicket,
 * and structurally CreateExecInviteTicket/CreateJoinInviteTicket too,
 * though only the first is wired to it today) the generated code has to
 * be that real capnp ticket, not a RunCode -- a receiving device scanning
 * it needs to redeem *this* device's ticket (RecruitConfirmDialog), not
 * re-run CreateJoinRequestTicket on itself. base64-decoding [run]'s own
 * result IS the payload to render in that one case.
 * [awaitAdmissionAfterGenerate], set only for CreateJoinRequestTicket,
 * additionally has CommandDetailScreen poll Kvmobile.listClusterMembers()
 * for this device's own peer id appearing while the generated code's
 * popup is showing -- the only externally observable sign this device's
 * `n.raft` went from pending to admitted (see mobile/kvmobile's own doc
 * comments -- there is no push/watch callback for this transition) --
 * and, once it does, auto-closes the popup and navigates to the activity
 * log, since a recruited device has nothing further to do with the code
 * once someone else has scanned it.
 */
class CommandSpec(
    val category: String,
    val name: String,
    val params: List<String>,
    val run: (List<String>) -> String,
    val generateFromResultBase64: Boolean = false,
    val awaitAdmissionAfterGenerate: Boolean = false,
    val multilineParams: Set<Int> = emptySet(),
) {
    val label: String get() = "$category: $name"

    /** Whether param [index] holds something with newlines in it -- Lua source, in practice --
     *  and so wants a tall field rather than [CommandDetailScreen]'s usual single line. */
    fun isMultiline(index: Int): Boolean = index in multilineParams
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
    fun add(
        category: String,
        name: String,
        params: List<String>,
        generateFromResultBase64: Boolean = false,
        awaitAdmissionAfterGenerate: Boolean = false,
        multilineParams: Set<Int> = emptySet(),
        run: (List<String>) -> String,
    ) {
        commands += CommandSpec(
            category, name, params, run,
            generateFromResultBase64, awaitAdmissionAfterGenerate, multilineParams,
        )
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
    // CreateJoinRequestTicket/RedeemJoinRequestTicket are
    // CreateJoinRequest/RecruitPeer's signed-ticket counterpart (see
    // README's "Signed tickets" section): the ticket already carries this
    // device's own address (no separate GetOwnAddr combine step), signed
    // with this device's own key, so a recruiting peer can verify it
    // really came from this device before ever dialing it.
    // RedeemJoinRequestTicket verifies then recruits in one call, exactly
    // like RecruitPeer does for a plain ticket. This is the "recruit me"
    // entry point end to end: generateFromResultBase64 lets
    // CommandDetailScreen's "Generate DataMatrix" button render the
    // signed ticket itself, not a RunCode (there's no "re-run this on the
    // scanning device" meaning for a compound, already-signed call like
    // this), and awaitAdmissionAfterGenerate has it watch for this
    // device actually being admitted -- the persistent scanner's own
    // AppRoot dispatch recognizes a scanned join_request_ticket code and
    // routes the *other* device straight to RecruitConfirmDialog instead
    // of the generic scan-confirmation flow (see AppRoot.kt).
    add(
        "Cluster", "CreateJoinRequestTicket", emptyList(),
        generateFromResultBase64 = true, awaitAdmissionAfterGenerate = true,
    ) { Kvmobile.createJoinRequestTicket() }
    add("Cluster", "RedeemJoinRequestTicket", listOf("ticketB64", "voter|learner")) { a ->
        Kvmobile.redeemJoinRequestTicket(a[0], a[1])
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
    // CreateJoinInviteTicket/VerifyJoinInviteTicket are CreateJoinInvite's
    // signed-ticket counterpart (see README's "Signed tickets" section).
    // Unlike RedeemJoinRequestTicket above, VerifyJoinInviteTicket doesn't
    // redeem anything itself -- join-invite redemption is baked into
    // Join/JoinWithKey's own daemon-startup flow -- it only verifies and
    // hands back the identical plain "<addr>#<tokenHex>" string
    // CreateJoinInvite's caller has always hand-assembled, ready to pass
    // to Join/JoinWithKey unchanged.
    add("Cluster", "CreateJoinInviteTicket", listOf("voter|learner")) { a -> Kvmobile.createJoinInviteTicket(a[0]) }
    add("Cluster", "VerifyJoinInviteTicket", listOf("ticketB64")) { a -> Kvmobile.verifyJoinInviteTicket(a[0]) }

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
    // Self-service escalation: asks the relay baked in at build time (see
    // relayMultiaddr) for Channel + circuit-relay standing under this
    // device's own identity -- see Kvmobile.requestRelayAccess's own doc
    // comment for why a freshly installed app needs this at all (its own
    // relay reservation attempts are unconditionally ACL-denied without
    // it). Safe to call more than once; each call just re-asserts the
    // grant.
    add("Cluster", "RequestRelayAccess", listOf("note")) { a -> Kvmobile.requestRelayAccess(a[0]) }
    // RequestPublicAccess is RequestRelayAccess's general form: the same
    // self-service escalation, but against any cluster address, not just
    // the relay baked in at build time.
    add("Cluster", "RequestPublicAccess", listOf("targetAddr", "note")) { a ->
        Kvmobile.requestPublicAccess(a[0], a[1])
    }
    // This device's own build/version info -- desktop's `mage version`.
    add("Cluster", "Version", emptyList()) { Kvmobile.version() }

    // RelayNode -- shmevent.KindBootstrapNode's request/confirm/revoke
    // lifecycle exposed as CRUD (desktop's mage addrelaynode/
    // confirmrelaynode/removerelaynode/listrelaynodes/getrelaynode), the
    // pool pkg/daemon's relayCandidates draws its failover list from.
    add("RelayNode", "AddRelayNode", listOf("multiaddr", "priority (0-255)")) { a ->
        Kvmobile.addRelayNode(a[0], a[1].toLongOrThrow("priority")); ok()
    }
    add("RelayNode", "ConfirmRelayNode", listOf("multiaddr")) { a -> Kvmobile.confirmRelayNode(a[0]); ok() }
    add("RelayNode", "RemoveRelayNode", listOf("multiaddr")) { a -> Kvmobile.removeRelayNode(a[0]); ok() }
    add("RelayNode", "GetRelayNode", listOf("multiaddr")) { a -> Kvmobile.getRelayNode(a[0]) }
    add("RelayNode", "ListRelayNodes", emptyList()) { Kvmobile.listRelayNodes() }

    // Station -- a human-readable name/attrs for a peer id (desktop's mage
    // createstation/updatestation/deletestation/getstation/liststations;
    // PutStation covers both create and update on the Go side). Only a
    // current raft voter may Put/Delete one.
    add("Station", "PutStation", listOf("peerID", "name", "attrsJSON")) { a ->
        Kvmobile.putStation(a[0], a[1], a[2]); ok()
    }
    add("Station", "DeleteStation", listOf("peerID")) { a -> Kvmobile.deleteStation(a[0]); ok() }
    add("Station", "GetStation", listOf("peerID")) { a -> Kvmobile.getStation(a[0]) }
    add("Station", "ListStations", emptyList()) { Kvmobile.listStations() }

    // KV
    add("KV", "Submit", listOf("key", "value")) { a -> Kvmobile.submit(a[0], a[1]); ok() }
    add("KV", "Get", listOf("key")) { a -> Kvmobile.get(a[0]) }
    // skip/order make this a real paging call rather than "everything from
    // the start of the range": skip drops that many pairs from the front of
    // the *requested* order, so desc+skip counts back from the end of the
    // range. An empty order means asc.
    add("KV", "RangeScan", listOf("start", "end", "limit (0=unlimited)", "skip", "order (asc|desc)")) { a ->
        Kvmobile.rangeScan(a[0], a[1], a[2].toLongOrThrow("limit"), a[3].toLongOrThrow("skip"), a[4])
    }
    // CompareAndSwap/CompareAndSwapAbsent are the safe read-modify-write
    // primitive Get+Submit alone can't express -- a false result means
    // another writer got there first, not an error; desktop's `mage cas`/
    // `mage casabsent`.
    add("KV", "CompareAndSwap", listOf("key", "expected", "value")) { a ->
        "swapped=${Kvmobile.compareAndSwap(a[0], a[1], a[2])}"
    }
    add("KV", "CompareAndSwapAbsent", listOf("key", "value")) { a ->
        "swapped=${Kvmobile.compareAndSwapAbsent(a[0], a[1])}"
    }
    // Atomic multi-key write -- desktop's `mage txn "<op> [op...]"`; see
    // shmevent.ParseTxnOpsString's grammar (k=v, del:k, if:k=v, ifabsent:k).
    add("KV", "Txn", listOf("ops (\"k=v del:k if:k=v ifabsent:k\")")) { a -> Kvmobile.txn(a[0]); ok() }

    // Permits -- shmevent.KindBootstrapNode's request/confirm/revoke
    // lifecycle; ConfirmPermit alone also accepts "cluster-join" (see
    // permitKindFromName's doc comment: "peer"/log-permit kinds were
    // removed when this project's ACL model moved to the unconditional
    // Group/Command catalog below).
    add("Permits", "RequestPermit", listOf("kind (bootstrap)", "targetPeerID", "metadata")) { a ->
        Kvmobile.requestPermit(a[0], a[1], a[2]); ok()
    }
    add("Permits", "ConfirmPermit", listOf("kind (bootstrap|cluster-join)", "targetPeerID")) { a ->
        Kvmobile.confirmPermit(a[0], a[1]); ok()
    }
    add("Permits", "RevokePermit", listOf("kind (bootstrap)", "targetPeerID")) { a ->
        Kvmobile.revokePermit(a[0], a[1]); ok()
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
    // *WithSpec carry the command's own form definition (opaque JSON,
    // replicated verbatim) alongside it -- desktop's `mage
    // createcommandspec`; targetPeerID may be "" here, unlike plain
    // CreateCommand/UpdateCommand above.
    add("Command", "CreateCommandWithSpec", listOf("id", "name", "targetPeerID (may be blank)", "specJSON")) { a ->
        Kvmobile.createCommandWithSpec(a[0], a[1], a[2], a[3]); ok()
    }
    add("Command", "UpdateCommandWithSpec", listOf("id", "name", "targetPeerID (may be blank)", "specJSON")) { a ->
        Kvmobile.updateCommandWithSpec(a[0], a[1], a[2], a[3]); ok()
    }

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
    // Non-final progress update from inside a RunCommandDispatcher handler
    // -- stamps status "running" so a dead process's instance still looks
    // retryable; desktop's pkg/kvctl.ReportProgress / `mage reportprogress`.
    add(
        "Dispatch", "ReportProgress",
        listOf("requesterPeerID", "instanceID", "fieldsJSON (blank ok)", "narrative"),
    ) { a -> Kvmobile.reportProgress(a[0], a[1], a[2], a[3]); ok() }
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

    // Dial* generalize public-access (RequestPublicAccess above) from one
    // hardcoded command to any commandID: this device dials targetAddr
    // directly and submits/reads back a dispatch on a cluster it has no
    // local replica of at all -- only that commandID is linked to a public
    // group there. No local exec-index/Execute-poke bookkeeping, since
    // there's no local replica to index against.
    add("Dispatch", "DialSubmitCommand", listOf("targetAddr", "commandID", "inputsJSON", "note")) { a ->
        Kvmobile.dialSubmitCommand(a[0], a[1], a[2], a[3])
    }
    add(
        "Dispatch", "DialQueryCommandLog",
        listOf("targetAddr", "instanceID", "since (RFC3339 or blank)", "until (RFC3339 or blank)", "limit (blank=unlimited)"),
    ) { a -> Kvmobile.dialQueryCommandLog(a[0], a[1], a[2], a[3], a[4]) }
    add("Dispatch", "DialWatchCommandLog", listOf("targetAddr", "instanceID")) { a ->
        val instanceID = a[1]
        Kvmobile.dialWatchCommandLog(a[0], instanceID, object : LogCallback {
            override fun onRecords(recordsJSON: String) {
                appendLog("DialCommandLog[$instanceID]: $recordsJSON")
            }
        })
        "Watching -- new records appear below as they arrive"
    }
    add("Dispatch", "StopDialWatchCommandLog", listOf("instanceID")) { a -> Kvmobile.stopDialWatchCommandLog(a[0]); ok() }

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
    // ttlSeconds is how long the invite stays redeemable, 0 meaning no
    // expiry (the default).
    add("ExecInvite", "CreateExecInvite", listOf("commandID", "inputsJSON", "ttlSeconds (0=never)")) { a ->
        Kvmobile.createExecInvite(a[0], a[1], a[2].toLongOrThrow("ttlSeconds"))
    }
    add("ExecInvite", "RevokeExecInvite", listOf("tokenHex")) { a -> Kvmobile.revokeExecInvite(a[0]); ok() }
    add("ExecInvite", "RedeemExecInvite", listOf("sourceAddr#tokenHex")) { a -> Kvmobile.redeemExecInvite(a[0]) }
    // CreateExecInviteTicket/RedeemExecInviteTicket are CreateExecInvite/
    // RedeemExecInvite's signed-ticket counterpart (see README's "Signed
    // tickets" section): the ticket already carries this device's own
    // address, signed with this device's own key, so a redeeming peer can
    // verify it really came from this device before ever dialing it.
    // RedeemExecInviteTicket verifies then redeems in one call, exactly
    // like RedeemExecInvite does for a plain ticket.
    add("ExecInvite", "CreateExecInviteTicket", listOf("commandID", "inputsJSON", "ttlSeconds (0=never)")) { a ->
        Kvmobile.createExecInviteTicket(a[0], a[1], a[2].toLongOrThrow("ttlSeconds"))
    }
    add("ExecInvite", "RedeemExecInviteTicket", listOf("ticketB64")) { a -> Kvmobile.redeemExecInviteTicket(a[0]) }

    // Journal -- the log book examples/relations/journalcmd serves behind a
    // Command (see examples/relations/README.md). Two roles live here, and
    // the split is worth reading before using them:
    //
    //   - Serve/StopServe/Define/Vocabulary/Verify are for a device that
    //     *owns* a book: they write its schema and run the dispatcher that
    //     answers everybody else. Define/Vocabulary write journal keys
    //     directly, which only the owning device can do.
    //   - Form/Append/Correct/Void/Page/Identity/Countersign/SignOff are
    //     for a device that only writes *into* somebody else's book. They
    //     submit a Command and touch no journal key at all, so a device
    //     granted command standing and nothing else can keep a log without
    //     being able to write to the store behind it -- which is the whole
    //     reason to drive a log this way from a phone.
    //
    // Form is what a form-driven screen would call first: it returns the
    // book's columns, what each holds, and the values a closed vocabulary
    // admits, generated from the log's own declarations so what a device
    // draws and what the log enforces cannot drift apart.
    add("Journal", "Serve", listOf("commandID", "log")) { a ->
        Kvmobile.journalServe(a[0], a[1].toLongOrThrow("log")); ok()
    }
    add("Journal", "StopServe", listOf("commandID")) { a -> Kvmobile.stopJournalServe(a[0]); ok() }
    add("Journal", "Define", listOf("log", "columnsJSON")) { a ->
        Kvmobile.journalDefine(a[0].toLongOrThrow("log"), a[1])
    }
    add("Journal", "Vocabulary", listOf("log", "column", "valuesJSON", "close (true|false)")) { a ->
        Kvmobile.journalVocabulary(a[0].toLongOrThrow("log"), a[1], a[2], a[3].toBooleanOrThrow("close"))
    }
    add("Journal", "Verify", listOf("log")) { a -> Kvmobile.journalVerify(a[0].toLongOrThrow("log")) }
    add("Journal", "Form", listOf("commandID")) { a -> Kvmobile.journalForm(a[0]) }
    add("Journal", "Append", listOf("commandID", "cellsJSON")) { a -> Kvmobile.journalAppend(a[0], a[1]) }
    add("Journal", "Correct", listOf("commandID", "line", "cellsJSON")) { a ->
        Kvmobile.journalCorrect(a[0], a[1], a[2])
    }
    add("Journal", "Void", listOf("commandID", "line", "reason (blank ok)")) { a ->
        Kvmobile.journalVoid(a[0], a[1], a[2]); ok()
    }
    add("Journal", "Page", listOf("commandID", "page (0=current)")) { a ->
        Kvmobile.journalPage(a[0], a[1].toLongOrThrow("page"))
    }
    add("Journal", "Identity", listOf("commandID")) { a -> Kvmobile.journalIdentity(a[0]) }
    // Countersign/SignOff are this device's own signature, made here with
    // a key that never leaves it: the record is built and signed on this
    // device and handed over as bytes, and the device that owns the book
    // checks it and writes it verbatim. It could not produce this
    // signature itself, which is exactly what makes an endorsement worth
    // having. A device may not endorse a line it wrote itself.
    add("Journal", "Countersign", listOf("commandID", "line")) { a ->
        Kvmobile.journalCountersign(a[0], a[1]); ok()
    }
    add("Journal", "SignOff", listOf("commandID", "page (0=current)")) { a ->
        Kvmobile.journalSignOff(a[0], a[1].toLongOrThrow("page")); ok()
    }

    // Cron -- examples/croncmd, a schedule saying "submit command C at
    // these times" and a scheduler that turns that into ordinary
    // SubmitCommand dispatches (see examples/croncmd/README.md).
    //
    // Two things are worth knowing before running Serve on a phone.
    //
    // The first is that it is safe to. Every fire is claimed through a
    // raft-committed compare-and-swap before it is submitted, so this
    // device, a laptop and three bootstrap nodes all running schedulers
    // produce one dispatch per fire between them -- running one here is a
    // redundancy decision, not a duplication bug. Which in turn is why a
    // phone is a reasonable place for one despite being the worst clock
    // in any cluster: a fire missed while asleep is someone else's if
    // anyone else is up, and this device's own to catch up if not, while
    // anything older than the catch-up window is skipped rather than
    // dispatched in a burst on waking.
    //
    // The second is that a timer grants nothing. The scheduler submits
    // under *this device's* peer id, and the FSM checks it against the
    // command's groups exactly as it would for a tap on Dispatch:
    // SubmitCommand above.
    //
    // Next is what a form calls first: it answers "when would this
    // expression actually fire" from the expression alone, needing no
    // daemon and writing nothing -- a cron expression's usual failure is
    // parsing cleanly and meaning something else.
    add("Cron", "Next", listOf("spec", "count", "location (blank=UTC)")) { a ->
        Kvmobile.cronNext(a[0], a[1].toLongOrThrow("count"), a[2])
    }
    add("Cron", "Put", listOf("id", "spec", "commandID", "inputsJSON (blank ok)", "location (blank=UTC)")) { a ->
        Kvmobile.cronPut(a[0], a[1], a[2], a[3], a[4]); ok()
    }
    add("Cron", "List", emptyList()) { Kvmobile.cronList() }
    add("Cron", "Get", listOf("id")) { a -> Kvmobile.cronGet(a[0]) }
    add("Cron", "SetEnabled", listOf("id", "enabled (true|false)")) { a ->
        Kvmobile.cronSetEnabled(a[0], a[1].toBooleanOrThrow("enabled")); ok()
    }
    add("Cron", "Delete", listOf("id")) { a -> Kvmobile.cronDelete(a[0]); ok() }
    // Fires is the durable view, and shows what *any* node's scheduler
    // dispatched rather than only this one's: it is read back out of the
    // claim keys themselves, so it reaches back exactly as far as those
    // are retained (a fortnight by default). A row with no instance id is
    // a fire that was claimed but whose submission then failed.
    add("Cron", "Fires", listOf("scheduleID (blank=all)", "limit (0=all)")) { a ->
        Kvmobile.cronFires(a[0], a[1].toLongOrThrow("limit"))
    }
    // Serve deliberately outlives Stop, the way WatchExecute and
    // WatchCommandLog above do: a torn-down daemon makes a tick fail and
    // be reported rather than making the loop exit, and a later Start
    // picks it straight back up. StopServe is what actually ends it.
    add("Cron", "Serve", listOf("intervalSeconds (0=default)", "catchUpSeconds (0=default)")) { a ->
        Kvmobile.cronServeWithListener(
            a[0].toLongOrThrow("intervalSeconds"),
            a[1].toLongOrThrow("catchUpSeconds"),
            object : CronListener {
                override fun onFire(scheduleID: String, commandID: String, fire: String, instanceID: String) {
                    appendLog("Cron fired $scheduleID -> $commandID at $fire: $instanceID")
                }

                override fun onSkip(scheduleID: String, fire: String, reason: String) {
                    appendLog("Cron skipped $scheduleID at $fire: $reason")
                }

                override fun onError(message: String) {
                    appendLog("Cron: $message")
                }
            },
        )
        "Scheduling -- fires appear below as they happen"
    }
    add("Cron", "StopServe", emptyList()) { Kvmobile.stopCronServe(); ok() }
    add("Cron", "Serving", emptyList()) { Kvmobile.cronServing().toString() }

    // Lua -- examples/luacmd, a command whose body is a Lua script stored
    // in the journal and registered in the catalog like any other command
    // (see examples/luacmd's package doc).
    //
    // Three things are worth knowing before writing one.
    //
    // The first is that a script grants nothing. It runs on whichever
    // device the command targets and submits under *that* device's peer
    // id, and the FSM checks that peer against the command's groups
    // exactly as it would for a tap on Dispatch: SubmitCommand. A command
    // written in Lua is permissioned like every other command: by the
    // group it is linked to -- public group, anybody; private group, its
    // members.
    //
    // The second is that a Command pins the hash of the script it was
    // registered with. Put alone stores a new revision but does not change
    // what an existing command runs; that takes CreateCommand again, which
    // only a raft voter may do. Which is the point: the journal is
    // writable by anything local, the catalog is not.
    //
    // The third is Serve. A Lua command names one target device, and only
    // that device should run a runner for it -- unlike Cron: Serve above,
    // where more schedulers mean more redundancy, two runners for one
    // command can both decide a request is unhandled. Serve is what makes
    // this device actually execute the commands that name it; without one
    // running somewhere, a submitted command simply stays pending.
    add("Lua", "CreateCommand", listOf("id", "name", "groupID (blank ok)", "code"), multilineParams = setOf(3)) { a ->
        Kvmobile.luaCreateCommand(a[0], a[1], a[2], a[3]); ok()
    }
    add("Lua", "Put", listOf("id", "name", "code"), multilineParams = setOf(2)) { a ->
        Kvmobile.luaPut(a[0], a[1], a[2]); ok()
    }
    add("Lua", "Get", listOf("id")) { a -> Kvmobile.luaGet(a[0]) }
    add("Lua", "List", emptyList()) { Kvmobile.luaList() }
    add("Lua", "History", listOf("id")) { a -> Kvmobile.luaHistory(a[0]) }
    add("Lua", "Delete", listOf("id")) { a -> Kvmobile.luaDelete(a[0]); ok() }
    // Run submits and returns the instance id immediately: the script's own
    // lines arrive afterwards, written to the replicated command log by
    // whichever device is running it. So this also starts watching that
    // instance (LuaWatch, an ordinary Dispatch: WatchCommandLog under the
    // hood) -- which is what makes a Lua command read in the log list the
    // way every other command does, lines appearing as they happen instead
    // of one "submitted" row and then silence. The watch stops itself when
    // the run records a terminal entry.
    add("Lua", "Run", listOf("commandID", "inputsJSON (blank ok)")) { a ->
        val instanceID = Kvmobile.luaRun(a[0], a[1])
        LuaWatch.start(a[0], instanceID)
        instanceID
    }
    add("Lua", "Logs", listOf("instanceID")) { a -> Kvmobile.luaLogs(a[0]) }
    // LastLog answers "what did the most recent run of this command do",
    // for when the instance id went to whoever submitted it and not to
    // you -- which is every dispatch that came from another device.
    add("Lua", "LastLog", listOf("commandID")) { a -> Kvmobile.luaLastLog(a[0]) }
    add("Lua", "Serve", listOf("intervalSeconds (0=default)", "concurrency (0=default)")) { a ->
        Kvmobile.luaServeWithListener(
            a[0].toLongOrThrow("intervalSeconds"),
            a[1].toLongOrThrow("concurrency"),
            object : LuaListener {
                override fun onStart(commandID: String, instanceID: String) {
                    appendLog("Lua started $commandID/$instanceID")
                }

                override fun onLog(commandID: String, instanceID: String, narrative: String) {
                    appendLog("Lua[$commandID] $narrative")
                }

                override fun onFinish(commandID: String, instanceID: String, status: String, narrative: String) {
                    appendLog("Lua $status $commandID/$instanceID: $narrative")
                }

                override fun onError(message: String) {
                    appendLog("Lua: $message")
                }
            },
        )
        "Serving -- runs of this device's Lua commands appear below as they happen"
    }
    add("Lua", "StopServe", emptyList()) { Kvmobile.stopLuaServe(); ok() }
    add("Lua", "Serving", emptyList()) { Kvmobile.luaServing().toString() }

    // Raw escape hatch -- the same one E2ETest uses, see its own doc
    // comment and README's "Follower on Android" section. Generatable and
    // scannable like every other spec now (a RunCode naming "Raw:
    // SendEvent" with this one eventJSON param) -- no special-casing
    // needed here anymore.
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
