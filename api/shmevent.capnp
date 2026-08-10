# shmevent.capnp defines the single wire structure used for every message
# exchanged between a raft node instance and a local "user" (the desktop
# CLI, the in-process Android UI, or a browser tab's main thread) over
# shmring shared memory -- and, since the same relationship holds for a
# remote browser learner talking to a node over libp2p, also over
# pkg/daemon.ClientProtocolID's network stream. One struct, one encoding,
# every hop.
#
# # Design: a real union, one variant per operation
#
# id/crc32/signature are the only fields common to every message -- a
# request/response correlation nonce (id, dual-purpose as a stable handle
# a caller can cite later via a variant's own sourceId/destinationId
# field, e.g. execute's), and the integrity/authenticity pair (crc32 a
# cheap corruption check, signature a real Ed25519 signature checked
# against the sender's public key -- getPublicKey/getPrivateKey are the
# only two variants a node accepts without a valid signature, since
# there's no key yet to check one against).
#
# Everything else is `body`, a union with one variant per logical
# operation, each a `group` (inline, sharing the parent struct's data
# section -- no extra allocation per variant) carrying only the named,
# typed fields that operation actually needs. This replaces an earlier
# design (still recognizable in this project's git history) where every
# operation shared one raw `value :Data` field and hand-packed its own
# ad-hoc binary layout inside it -- workable, but meant hand-crafting a
# single event (e.g. for a Data Matrix trigger via `kvctl-cli
# printeventdatamatrix`) required already knowing that private layout.
# Named fields are now self-describing instead.
#
# Most variants serve both directions of one request/response exchange
# rather than getting separate variants for each -- a caller populates
# whichever fields its direction needs (e.g. listRange's request sets
# start/end, its response fills key/value; getKey's request sets
# sourceId, its response fills key). `error` is the one variant every
# operation's failed response uses instead of its own variant, carrying
# a plain UTF-8 message.
#
# A few operations that shared one wire byte in the old design because
# their meaning depended on whether some other field was zero are split
# into two real variants here instead (e.g. the old combined bootstrap/
# join/learner-join event is now bootstrapOrJoinCluster + addLearner; the
# old dual-mode field read is now getFieldByRegistry + getFieldByKey).
# Two more variant families (groupPut/groupDelete/commandPut/... and
# permitRequest/permitConfirm/permitRevoke/joinInviteCreate/...) replace
# what used to be two generic envelope events whose own value carried a
# further kind/action selector byte plus an opaque inner blob -- each of
# those ~17 logical operations is now its own top-level variant the same
# way as everything else.
#
# Before adding a new variant: every raft node in the mesh -- including
# long-lived bootstrap nodes and Android devices nobody force-upgrades --
# has to agree on what it means, forever, and it needs a hand-duplicated
# dispatch/wrapper added across pkg/daemon and pkg/kvctl/mobile/kvmobile
# (and, once ported, web-app/src/shmevent -- a hand-maintained mirror,
# not generated). If what's being added is a new *task* -- typed inputs,
# ACL, durable request/response logging, a low-latency "ready" poke --
# register it as a Command through pkg/kvctl/dispatch.go's
# SubmitCommand/Group-Command catalog instead (see that file's doc
# comment): it already dispatches generically over getFieldByKey/
# logAppend/listRange/execute, with zero new wire variants needed per
# task type. Reserve new variants for genuinely new wire-level
# primitives -- a new relational shape, a new stream/session protocol --
# not new task types.
@0x907f33b2bf56870e;

using Go = import "go.capnp";
$Go.package("shmevent");
$Go.import("github.com/gofsd/libp2p-kv-raft/pkg/shmevent");

# One op of an atomic multi-key write (txn's ops list) -- op is
# TxnOpSet/TxnOpDelete/TxnOpCompare/TxnOpCompareAbsent (see
# pkg/shmevent's TxnOp* constants); value is unused for Delete/
# CompareAbsent.
struct TxnOp {
  op @0 :UInt8;
  key @1 :Data;
  value @2 :Data;
}

struct Event {
  # Request/response correlation nonce, chosen by whichever side
  # originates the message that starts an exchange (always the caller,
  # for every operation defined today) -- a response always echoes the
  # request's own id.
  id @0 :UInt16;

  # CRC-32 (IEEE polynomial) over every other field in wire order,
  # excluding itself and signature -- corruption check only, not a
  # security boundary.
  crc32 @1 :UInt32;

  # Ed25519 signature (64 bytes) over the same bytes crc32 covers plus
  # the crc32 value itself, checked against the sender's public key.
  signature @2 :Data;

  union {
    # Registers value under this message's own id in the node's key registry -- used both for an actual KV key (ahead of setField) and a peer id (ahead of addLearner/bootstrapOrJoinCluster).
    setKey :group {
      value @3 :Data;
    }

    # store.Set(registry[sourceId], value).
    setField :group {
      sourceId @4 :UInt16;
      value @5 :Data;
    }

    # Reverse registry lookup. Request sets sourceId; response fills key.
    getKey :group {
      sourceId @6 :UInt16;
      key @7 :Data;
    }

    # store.Get(registry[sourceId]). Response fills value.
    getFieldByRegistry :group {
      sourceId @8 :UInt16;
      value @9 :Data;
    }

    # One-shot store.Get(key), no prior registry entry needed. Request sets key; response fills value.
    getFieldByKey :group {
      key @10 :Data;
      value @11 :Data;
    }

    # Unsigned-allowed bootstrap event. Response fills pubKey with this node's own Ed25519 public key.
    getPublicKey :group {
      pubKey @12 :Data;
    }

    # Unsigned-allowed bootstrap event. Response fills privKey -- how a caller with no key yet bootstraps into the node's own identity key.
    getPrivateKey :group {
      privKey @13 :Data;
    }

    # leaderAddr empty: bootstrap as sole leader. Non-empty: join that leader's cluster (may carry a "<multiaddr>#<tokenHex>" invite, parsed downstream same as today).
    bootstrapOrJoinCluster :group {
      leaderAddr @14 :Text;
    }

    # claimedPeerId is a prior setKey registry reference to this node's own peer id (cross-checked against the connection's authenticated identity); addr is this node's own reachable address.
    addLearner :group {
      claimedPeerId @15 :UInt16;
      addr @16 :Text;
    }

    # Ordinary replicated store write, rejected if key starts with the reserved SystemKeyPrefix.
    set :group {
      key @17 :Data;
      value @18 :Data;
    }

    # Direct peer-to-peer notification, bypassing the store/raft entirely. Two distinct wire shapes share this one variant: the local IPC request (sourceId/destinationId, prior setKey registrations for the sender's own peer id and the receiver's; senderPeerId unset) that a caller's own daemon resolves and re-sends as the second shape -- the ExecuteProtocolID network message actually placed on the wire between two daemon processes with no shared registry (senderPeerId set directly instead; sourceId/destinationId unset).
    execute :group {
      sourceId @19 :UInt16;
      destinationId @20 :UInt16;
      senderPeerId @21 :Text;
      value @22 :Data;
    }

    # Drains the local execute inbox. Request carries no fields; response is empty if nothing queued, else the oldest notification's sender and payload.
    pollExecute :group {
      senderPeerId @23 :Text;
      value @24 :Data;
    }

    # Bounded key-range scan, answered directly by whichever node receives it (no leader forwarding). Request sets start/end; response fills key/value (empty if nothing left in range).
    listRange :group {
      start @25 :Data;
      end @26 :Data;
      key @27 :Data;
      value @28 :Data;
    }

    # pkg/logrecord append -- key must start with the reserved log-key prefix. CommandRequestLogKind keys additionally run the command/log carve-out ACL.
    logAppend :group {
      key @29 :Data;
      value @30 :Data;
    }

    # Local-only: gracefully raft.RemoveServer's this node out of its own cluster, resuming its own solo store.
    leave @31 :Void;

    # Two distinct wire shapes share this one variant, the same way execute's does: the local IPC request (sourceAddr/token; response fills instanceId) that a caller's own daemon resolves and re-sends as the second shape -- the ExecInviteRedeemProtocolID network message actually placed on the wire (redeemerPeerId/token set instead, naming this node's own identity directly since there's no shared registry across processes; the response on that leg is a bare text line, not a shmevent Msg at all).
    execInviteRedeem :group {
      sourceAddr @32 :Text;
      token @33 :Data;
      instanceId @34 :Text;
      redeemerPeerId @35 :Text;
    }

    # Local-only: mints a fresh correlation token held purely in this daemon's process memory (never persisted -- this node may have no raft instance yet). Response fills token.
    joinRequestCreate :group {
      token @36 :Data;
    }

    # Local-only: invalidates a still-pending join-request token.
    joinRequestCancel :group {
      token @37 :Data;
    }

    # Local-only: splits a scanned "<ownAddr>#<tokenHex>" join-request ticket, mints a normal join invite, and dials the device directly to hand it over.
    recruit :group {
      ticket @38 :Text;
      suffrage @39 :UInt8;
    }

    # Response fills addr with this node's own current best-advertised address (public, then relay circuit, then anything else, loopback last).
    getOwnAddr :group {
      addr @40 :Text;
    }

    # Opens a new data-plane Channel to peerId. Two distinct wire shapes share this one variant, the same way execute's does: the local IPC request (peerId, the dial target; senderPeerId unset) that a caller's own daemon resolves and re-sends as the second shape -- the ChannelProtocolID network handshake actually placed on the wire (senderPeerId set instead, naming this node's own identity directly; peerId unused on that leg).
    channelOpen :group {
      peerId @41 :Text;
      senderPeerId @42 :Text;
    }

    # Sends one framed chunk on an already-open channel.
    channelSend :group {
      channelId @43 :Text;
      purpose @44 :UInt8;
      chunk @45 :Data;
    }

    # Request sets channelId; response fills status/purpose/chunk.
    channelPoll :group {
      channelId @46 :Text;
      status @47 :UInt8;
      purpose @48 :UInt8;
      chunk @49 :Data;
    }

    # Request carries no fields; response fills channelId/remotePeerId once an inbound channel is accepted.
    channelListen :group {
      channelId @50 :Text;
      remotePeerId @51 :Text;
    }

    # Closes a channel outright.
    channelClose :group {
      channelId @52 :Text;
    }

    # Half-closes a channel's write side.
    channelCloseWrite :group {
      channelId @53 :Text;
    }

    # Notification that new data is available to poll on channelId.
    channelDataReady :group {
      channelId @54 :Text;
    }

    # Force raft.RemoveServer's peerId out of this cluster. Remote caller must be a current raft voter.
    kick :group {
      peerId @55 :Text;
    }

    # Applies an ordered op list as one raft log entry -- every op lands or none do.
    txn :group {
      ops @56 :List(TxnOp);
    }

    # Always open even to a remote caller. Request carries no fields.
    getVersion :group {
      commit @57 :Text;
      dirty @58 :Bool;
      buildTime @59 :Text;
      goVersion @60 :Text;
      libp2pVersion @61 :Text;
    }

    # Local-only: dials targetPeer (multiaddr or bare peer id, see resolveDialAddr) and self-services this node's own public-group/relay standing there. Response fills instanceId.
    publicAccess :group {
      targetPeer @62 :Text;
      note @63 :Text;
      instanceId @64 :Text;
    }

    # Offline signed-ticket wire format (never dispatched live) -- pkg/kvctl.CreateExecInviteTicket/RedeemExecInviteTicket build/verify this directly, entirely client-side.
    execTicket :group {
      sourceAddr @65 :Text;
      token @66 :Data;
    }

    # Offline signed-ticket wire format, join-invite's counterpart to execTicket.
    joinTicket :group {
      sourceAddr @67 :Text;
      token @68 :Data;
    }

    # Offline signed-ticket wire format, join-request's counterpart to execTicket.
    joinRequestTicket :group {
      sourceAddr @69 :Text;
      token @70 :Data;
    }

    # Local-only: dials targetPeer (multiaddr or bare peer id) and submits commandId/inputsJson there as a CommandRequestLogKind write, provided targetPeer has linked commandId to a public group. Response fills instanceId.
    dialSubmitCommand :group {
      targetPeer @71 :Text;
      commandId @72 :Text;
      inputsJson @73 :Text;
      note @74 :Text;
      instanceId @75 :Text;
    }

    # Local-only: dials targetPeer and reads back instanceId's own CommandExecLogKind entries -- needs no standing at all, possessing instanceId is the credential. since/until are UnixNano (0 = unbounded); limit 0 = unbounded. Response fills records (each an encoded logrecord.Record).
    dialQueryCommandLog :group {
      targetPeer @76 :Text;
      instanceId @77 :Text;
      since @78 :Int64;
      until @79 :Int64;
      limit @80 :Int32;
      records @81 :List(Data);
    }

    # Response-only: any failed request answers with this instead of its own variant.
    error :group {
      message @82 :Text;
    }

    # Group catalog upsert.
    groupPut :group {
      id @83 :Text;
      name @84 :Text;
      public @85 :Bool;
    }

    # Group catalog delete.
    groupDelete :group {
      id @86 :Text;
    }

    # Command catalog upsert. spec absent (HasSpec()==false) preserves any existing spec unchanged; present-empty clears it; present-nonempty sets it -- the same three-state convention the old CommandPayloadWithSpec/ClearingSpec pair encoded.
    commandPut :group {
      id @87 :Text;
      name @88 :Text;
      peerId @89 :Text;
      spec @90 :Text;
    }

    # Command catalog delete.
    commandDelete :group {
      id @91 :Text;
    }

    # Station catalog upsert. attrs is caller-defined opaque JSON.
    stationPut :group {
      peerId @92 :Text;
      name @93 :Text;
      attrs @94 :Text;
    }

    # Station catalog delete.
    stationDelete :group {
      peerId @95 :Text;
    }

    # Links commandId into groupId.
    groupCommandPut :group {
      commandId @96 :Text;
      groupId @97 :Text;
    }

    # Unlinks commandId from groupId.
    groupCommandDelete :group {
      commandId @98 :Text;
      groupId @99 :Text;
    }

    # Adds peerId to groupId.
    peerGroupPut :group {
      peerId @100 :Text;
      groupId @101 :Text;
    }

    # Removes peerId from groupId.
    peerGroupDelete :group {
      peerId @102 :Text;
      groupId @103 :Text;
    }

    # Lodges a pending permit record. kind stays a real open-ended parameter (bootstrap-node vs. cluster-join today, more reserved for later) -- not the ambiguity this redesign removes; metadata is that kind's own small JSON blob. Any raft node may receive/relay this.
    permitRequest :group {
      kind @104 :UInt8;
      peerId @105 :Text;
      metadata @106 :Text;
    }

    # Promotes a pending permit record to confirmed. Only honored if the true originator (checked via the forwarding connection's own authenticated identity, never anything inside this message) is currently a raft voter.
    permitConfirm :group {
      kind @107 :UInt8;
      peerId @108 :Text;
    }

    # Deletes an already-confirmed permit record outright. Same raft-voter-only restriction as permitConfirm.
    permitRevoke :group {
      kind @109 :UInt8;
      peerId @110 :Text;
    }

    # Mints a one-time join-invite token, redeemable by any device presenting it.
    joinInviteCreate :group {
      token @111 :Data;
      suffrage @112 :UInt8;
    }

    # Invalidates a still-unredeemed join-invite token.
    joinInviteRevoke :group {
      token @113 :Data;
    }

    # Mints a one-time execution-invite token bound to commandId/inputsJson.
    # ttlSeconds is how long the invite stays redeemable from the moment
    # this create is applied, 0 meaning no expiry (the default) -- see
    # shmevent.KindExecInvite's doc comment on how it's stored/checked.
    execInviteCreate :group {
      token @114 :Data;
      commandId @115 :Text;
      inputsJson @116 :Text;
      ttlSeconds @118 :UInt64;
    }

    # Invalidates a still-unredeemed execution-invite token.
    execInviteRevoke :group {
      token @117 :Data;
    }

  }
}

