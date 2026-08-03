# libp2p-kv-raft

A distributed key-value store: [hashicorp/raft](https://github.com/hashicorp/raft) consensus
running over [libp2p](https://github.com/libp2p/go-libp2p) transport, with
[SQLite](https://sqlite.org/) (via the pure-Go [modernc.org/sqlite](https://pkg.go.dev/modernc.org/sqlite)
driver, so no CGO toolchain is needed) as the on-disk store for the replicated state
machine. Nodes can run on separate machines (including behind NAT/cellular, e.g. an Android
phone) and are driven locally over `github.com/gofsd/shmring` shared-memory IPC rather than a
network-facing RPC port.

Integrating this repo as a dependency from another Go project (or driving it as a subprocess from
any language)? See [`docs/library-usage.md`](docs/library-usage.md) for the API-focused guide;
this file covers this repo's own architecture and `mage` workflow instead.

## Architecture

- `pkg/daemon` — the long-running node process (`cmd/kvnode`): a libp2p host, a raft instance
  backed by `pkg/kvfsm`/`pkg/store`, and a `pkg/ipc` server for local control.
- `api/shmevent.capnp` — the single [Cap'n Proto](https://capnproto.org/)-encoded wire struct every
  "user"-to-"raft node instance" hop speaks: `pkg/ipc`'s local shared memory, and
  `pkg/daemon.ClientProtocolID`'s network hop for a remote browser learner. One `event` byte,
  `sourceId`/`destinationId` relational references, a raw `value`, a CRC32, an Ed25519 `signature`,
  and a correlation `id` — a Set decomposes into a linked `SetKey`+`SetField` pair, a Get is a
  one-shot `GetField`, and `GetPublicKey`/`GetPrivateKey` are how a caller with no key yet
  bootstraps into the same key the node itself holds. `pkg/shmevent` (Go) and `web-app/src/shmevent.rs`
  (Rust) are both generated from this identical schema. See its doc comment for the full design.
- `pkg/ipc` — request/response IPC between a short-lived CLI process and the daemon, over shmring
  ring buffers carrying `pkg/shmevent.Msg`. `ipc.go` is the desktop (named shared-memory) transport;
  `ipc_android.go` is the Android transport (`ASharedMemory`, no named rendezvous, so client and
  daemon must share a process — see that file's doc comment). `pkg/shmclient` implements the
  caller-side SetKey+SetField/GetField orchestration and the `GetPrivateKey` bootstrap on top of it.
  See [Local IPC token](#local-ipc-token) for how the desktop transport authenticates a caller.
- `pkg/kvctl` / `cmd/kvctl-cli` — client logic for spawning/bootstrapping nodes and issuing
  set/get requests. `kvctl-cli` is a no-Go-toolchain-required binary meant to run next to an
  already-built `kvnode` binary on a remote deployment target (e.g. a VPS reached over SSH).
- `mobile/kvmobile` — the `gomobile`-bindable entry point that runs the follower daemon
  in-process inside an Android app (see `android-app/`).
- `magefile.go` — desktop convenience targets (`mage addnode`, `mage set`, ...) that wrap
  `pkg/kvctl` for local development.
- `web-app/` — a browser client, in Rust compiled to `wasm32-unknown-unknown` over `rust-libp2p`
  (see [Client in a browser](#client-in-a-browser)); unlike every other client here it never
  *votes*, but it does run a real hashicorp/raft non-voter (learner), reimplementing
  `NetworkTransport`'s msgpack wire protocol to receive genuine `AppendEntries` replication.

A node has no leader/follower role until it receives an `EventAdd` request (`pkg/shmevent`):
bootstrap as the cluster's sole leader, or join an existing leader (given as a bare peer ID
registered on the same machine, or a full multiaddr for a leader on another machine).

## Running a cluster

### Leader on a remote machine (over SSH)

The remote machine needs no Go toolchain — cross-compile (or build natively) `kvnode` and
`kvctl-cli`, copy them over, then bootstrap:

```bash
GOOS=linux GOARCH=amd64 go build -o kvnode ./cmd/kvnode
GOOS=linux GOARCH=amd64 go build -o kvctl-cli ./cmd/kvctl-cli
scp kvnode kvctl-cli user@remote:/opt/kvstore/bin/

ssh user@remote 'KVSTORE_HOME=/opt/kvstore /opt/kvstore/bin/kvctl-cli addnode \
  -bin /opt/kvstore/bin/kvnode -listen-port 4001 -relay-service'
```

`-relay-service` makes this node act as a circuit-relay v2 point (needed for followers with no
directly-dialable address of their own, e.g. a phone on cellular) and forces it to advertise
itself as publicly reachable. `-listen-port` pins the port so it survives restarts.
`KVSTORE_HOME` controls where the registry/node data lives (defaults to
`~/.libp2p-kv-raft`); set it explicitly and pass it on every subsequent `kvctl-cli` call
against that install.

**Breaking change:** a `-relay-service` node used to let *any* peer reserve a slot or open a
relayed circuit through it by default, with `-require-permit-for-relay` as an opt-in restriction;
the generic remote (`ClientProtocolID`) `Set`/`Get`/etc. surface and `EventExecute` delivery had
their own similar opt-in flags (`-require-permit-for-remote`/`-require-permit-for-execute`), each
gated on a confirmed `KindPermitPeer` record. None of that exists anymore. There is no longer an
opt-out for any of the four: relay admission, Channel admission, the generic remote RPC surface,
and Execute delivery are now all unconditionally gated by the same group-ACL mechanism —
`pkg/daemon.relayACL`'s `AllowReserve`/`AllowConnect`, `handleChannelStream`,
`handleShmEvent`'s top-of-function gate, and `handleExecuteStream` each admit a peer only if it's a
current raft cluster member, in the resource's own reserved group (`shmevent.ReservedGroupRelay`/
`ReservedGroupChannel`/`ReservedGroupRemote`/`ReservedGroupExecute` respectively — see
[Reserved cluster/voter/learner/channel/relay/remote/execute groups](#reserved-clustervoterlearnerchannelrelayremoteexecute-groups)
below), or individually granted access via this node's own personal group (`isAuthorizedForGatedAccess`).
`KindPermitPeer` itself is gone — it served exactly those three purposes (relay/remote/execute),
all replaced by groups — along with `mage requestpermit peer`/`confirmpermit peer`/`revokepermit peer`.
`KindBootstrapNode` (below, relay/bootstrap-node registration) still uses the same underlying
`EventPermitRequest`/`EventPermitConfirm`/`EventPermitRevoke` machinery under kind `"bootstrap"` — that
part is unaffected.

The **one deliberate exception** to the generic remote gate (besides Join/Recruit, which are their own
separate, already-gated protocols) is submitting a command linked to a public `Group`
(`SubmitCommand`'s actual write, `EventLogAppend` targeting a `shmevent.CommandRequestLogKind` key)
and reading back that same dispatch's own `CommandRequest`/execution-index/execution-log records
(`pkg/daemon.isCommandLogCarveOut`) — this is the *only* door a peer with no other standing has
into an otherwise closed cluster. See
[One-time execution invites](#one-time-execution-invites) and
[Group/command ACL](#groupcommand-acl) for how a public command is set up; a public command's own
execution logic can widen that peer's access further from there (e.g.
`mage addpeertogroup <peerID> remote`), but nothing does so automatically.

Upgrading a node that relied on the old open-by-default relay, or that never set any of the
`-require-permit-for-*` flags (today's actual default in every existing deployment), means every
peer that needs generic remote/execute/relay/channel access must now be explicitly admitted:

```bash
mage addpeertogroup <peerID> relay          # (on a current raft voter of this cluster) grant it
mage addpeertogroup <peerID> remote         # generic Set/Get/etc. over ClientProtocolID
mage addpeertogroup <peerID> execute        # EventExecute delivery
mage removepeerfromgroup <peerID> <group>   # revoke any of the above again
```

A `-relay-service` node's resource limits (per-peer circuit/reservation caps) are still
flag-configurable exactly as before: `-relay-max-circuits-per-peer` (default 1) bounds concurrent
open relayed circuits a single peer may hold; `-relay-limit-data-bytes` (default 1GB) and
`-relay-limit-duration` (default 720h/30 days) bound a circuit's data/lifetime before it's
reset; `-relay-max-reservations-per-ip`/`-relay-max-reservations-per-peer` (defaults 5/1) bound
active relay-slot reservations from one IP/peer. All five default to
`pkg/shmevent`'s `DefaultRelay*` constants when left at 0. go-libp2p's circuit-relay v2 applies
these as one uniform `v2relay.Resources` value to every peer alike — there's no way to give one
peer a bigger allotment than another without forking that package.

**Usage quotas** are the separate, cumulative counterpart to those static per-circuit/
per-reservation caps above: `-quota-relay-events-per-peer-per-sec`/`-quota-relay-burst-per-peer`
and `-quota-relay-events-per-ip-per-sec`/`-quota-relay-burst-per-ip` bound a sustained *rate* of
reservation/connect events from one peer id or one IP address (a token bucket,
`pkg/daemon.quotaTracker`), independent of whether that peer clears the group-ACL check above —
both gates must pass. Relay quota is metered in events, not bytes: go-libp2p's circuit-relay v2
never reports actual per-circuit byte usage back to `relayACL`, only reservation/connect calls
happen at a point this node can see. The identical mechanism also gates
[Raw Channel](#raw-channel) traffic, there metered in real bytes instead:
`-quota-channel-bytes-per-peer-per-sec`/`-quota-channel-burst-per-peer` and
`-quota-channel-bytes-per-ip-per-sec`/`-quota-channel-burst-per-ip`. All eight flags default to 0
(unlimited) — a peer/IP that exceeds its quota has an in-progress `channelSession` closed, or a
relay reservation/connect simply denied, same as failing the group-ACL check.

`EventExecute` (`mage execute <destPeerID> <value>` / `mage pollexecute`, a direct unreplicated
peer-to-peer notification between two node processes — see `pkg/shmevent`'s `EventExecute` doc
comment) is now gated by `shmevent.ReservedGroupExecute` exactly the way relay/Channel are: a raft
cluster member (voter or learner) can always send one, any other peer needs
`mage addpeertogroup <peerID> execute` (or a pairwise personal-group grant) first, and
`mage removepeerfromgroup <peerID> execute` revokes it again, immediately, on whichever node's
store the removal replicates to.

Print the leader's multiaddr for followers to join against:

```bash
ssh user@remote 'cat /opt/kvstore/registry.json'   # listen_addrs includes the public multiaddr
```

### Follower on the local machine

```bash
mage addfollower "/ip4/<remote-ip>/tcp/4001/p2p/<leader-peer-id>"
mage set mykey myvalue
mage get mykey
```

`mage resumenode <peerID>` restarts an existing node in place from its persisted raft state (no
leader coordination needed, as long as its address hasn't changed). `mage rejoinnode <leaderAddr>
<peerID>` restarts it *and* re-sends the join request — use this if the node's address changed or
a new leader needs to know about it. Note a 2-voter cluster has no fault tolerance: if either side
is down for a while, the other cannot commit and eventually can't win an election either;
bringing the down side back with `resumenode`/`rejoinnode` lets them re-elect on their own.

`mage addnodewithkey <keyFile>` / `mage addfollowerwithkey <keyFile> <leaderAddr>` are the
`addnode`/`addfollower` equivalents for provisioning a node under a specific, already-known Ed25519
identity — `<keyFile>` is a file in `identity.key`'s own hex-encoded format (e.g. a backed-up copy
of a previous node's key) — instead of always minting a fresh one. The resulting peer id is
whatever that key derives to, not a new random one.

`mage deletenode <peerID>` permanently removes a node's on-disk state (identity key, sqlite store,
raft log/snapshots — its whole data directory under the registry) and its entry in
`registry.json`. It refuses while that node's daemon process still appears to be running — stop it
first — since deleting files out from under a live process would corrupt them; unlike
`e2e:deletenode`, it never kills anything itself.

`mage listclusters` lists every raft cluster known to this machine's registry, grouped by
whichever peer id originally bootstrapped it (`registry.NodeInfo.ClusterPeerID` if a node joined
elsewhere, otherwise its own peer id) — a pure local read, no daemon needs to be running, but for
the same reason it only ever shows clusters this machine has itself created or joined a node into,
never a network-wide view. `mage listnodes <peerID>` instead queries that *already-running* node
for its raft cluster's full **live** membership — every current voter/learner/leader, including
peers this machine never created and so has no registry entry for at all — read from that node's
own locally-replicated `shmevent.KindClusterMember` records (kept current by every member whenever
a peer joins/leaves or its own leadership status changes). Both are also exposed on `kvctl-cli`
(`listclusters` / `listnodes <peerID>`), printing one JSON object per line the same way `logquery`
does.

`mage rangescan <start> <end> [limit]` is the generic counterpart to `set`/`get`: instead of one
key at a time, it lists every key/value pair in `[start, end]` (both inclusive, lexicographic byte
order over the raw key bytes) on the current node, one JSON object per line. It isn't scoped to
just ordinary data — it can see this project's own reserved namespaces too
(`shmevent.SystemKeyPrefix`, `pkg/logrecord`'s prefix) — but that's not a new privilege: every
`kvctl`/`kvctl-cli` call only ever reaches the *local* daemon over shmring IPC, the same
same-machine trust boundary `set`/`get` already operate under (see `pkg/shmevent`'s package doc
comment), so a local caller already had unrestricted read access to its own node's entire store;
this just exposes it conveniently instead of requiring a raw `sendevent` call. Also exposed on
`kvctl-cli` as `rangescan <start> <end> [-limit N]`.

`kvctl-cli sendrawevent <peerID> <base64Payload>` and `kvctl-cli printeventdatamatrix <peerID>
<eventJSON> <outFile.png>` extend `sendevent` for one-time, pre-signed events. `sendevent` always
signs fresh with whoever it's calling; `printeventdatamatrix` instead builds and signs the event
(same `eventJSON` shape, same peerID-key-fetch-if-needed signing step) but writes the resulting
bytes as a Data Matrix barcode to `outFile.png` and prints the base64 payload, without sending it
yet. `sendrawevent` is the reverse: it delivers a base64 payload to `peerID` completely
unchanged, over `pkg/ipc.CallRaw` — no re-signing, so whatever signature was baked in earlier (by
this peer's own key, at whatever time `printeventdatamatrix` ran) survives intact. Together
they're what a "one-time join ticket" is: a current raft voter runs `printeventdatamatrix` once,
in advance, to pre-sign an `EventPermitConfirm` for a specific not-yet-arrived `KindClusterJoin`
request; replaying that payload later via `sendrawevent` (still locally, on that same voter's own
node — shmring IPC is same-machine only, see `pkg/shmevent`'s package doc comment) completes the
confirm without the operator composing/signing anything at redemption time. No new server-side
ticket-tracking was needed for the "one-time" part: `kvfsm`'s existing `OpConfirm` already deletes
the pending record it consumes (see `SystemKey`'s doc comment), so replaying the same payload
twice just fails the second time on its own. The identical pair works for any other event too
(e.g. a pre-signed `EventExecute`, for a one-time command-execution ticket) — neither command is
tied to one event type.

#### Changing which cluster a node belongs to: `join`/`leave`/`rm`

`addnode`/`addfollower` above always mint a *new* identity. `mage join <targetPeerID>` instead
lets the *current* node (`mage use`) — already running its own default, solo single-node cluster
— join a different cluster under that same identity, switching its active data directory to
`<own-peer-id>-<target-peer-id>` (stopping and restarting the local daemon process as needed; the
same directory-naming scheme `rejoinnode` already uses for an existing identity). Whether this is
admitted immediately or requires a separate approval step depends entirely on the target
daemon's own `-require-confirm-for-join` flag (`Config.RequireConfirmForJoin`, default off):

```bash
mage join <targetPeerID>                    # ask targetPeerID's cluster to admit the current node
mage confirmpermit cluster-join <peerID>    # (only if the target requires it) admit a pending join
```

With `-require-confirm-for-join` unset, `join` behaves exactly like `addfollower` under an existing
identity — immediate `raft.AddVoter`/`AddNonvoter`, no separate step. With it set, a join request
only lodges a pending `shmevent.KindClusterJoin` record (the same pending/confirmed system-record
machinery `requestpermit`/`confirmpermit` already use, just a different `kind`); the joining node
is *not* yet a raft member. **Any current raft voter — not just the leader** — can then run `mage
confirmpermit cluster-join <peerID>` against that cluster to actually admit it; `mage
revokepermit cluster-join <peerID>` deletes a still-pending or already-confirmed record outright
(same voter-only restriction as every other permit kind).

`mage leave <peerID>` asks that cluster to remove peerID via `raft.RemoveServer` — a graceful
shrink; the remaining voters keep operating normally, exactly like hashicorp/raft already
tolerates any minority of members going offline — then restarts peerID's daemon back on its own
default solo data directory. The composite cluster directory is left on disk untouched, so a
later `join`/`rejoinnode` back to the same cluster picks its local state back up.

`mage rm <peerID>` does everything `leave` does, plus revokes peerID's `cluster-join` standing
with that cluster (so a *later* `join` attempt against it starts genuinely pending again, not
auto-admitted by a stale confirmed record) and deletes the composite cluster directory outright —
`deletenode`'s counterpart for "leave a cluster and don't keep its local data around", as opposed
to `deletenode`'s "erase this identity entirely."

#### One-time join invites: admitting a device the cluster has never seen before

`cluster-join`'s pending/confirmed flow above always addresses a specific, already-known peer id —
fine for `join`/`rejoinnode` reconnecting an identity that already exists, but no help for a brand
new device (nothing to name until it shows up). `shmevent.KindJoinInvite` is a different
mechanism for exactly that case: a random, unguessable token stands in for a peer id, and *any*
device presenting a still-valid one gets admitted immediately — with `-require-confirm-for-join`
on or off — with no live voter approving anything at the moment it actually joins.

```bash
mage createjoininvite <voter|learner>              # (on a current voter) prints a fresh tokenHex
mage addfollower "<leaderMultiaddr>#<tokenHex>"     # any new device, redeems it by joining
mage revokejoininvite <tokenHex>                    # invalidate one before it's ever redeemed
```

The token rides along inside the same `leaderPeerID`/`leaderAddr` string every join path already
threads through unchanged (`EventAdd`'s own wire payload, `kvctl.AddNode`, `mobile/kvmobile.Join`)
— `#` never appears in a multiaddr or peer id, so `pkg/daemon`'s `splitInviteToken` just strips it
back off before resolving the address, no new parameter anywhere above `handleAdd`. Redemption
(`kvfsm.OpConsumeInvite`) reads and deletes the invite record in one atomic raft `Apply` call, the
same way `OpConfirm` already guarantees a pending record is only ever promoted once — so a second
device presenting an already-consumed token is rejected outright, not silently downgraded to the
slower pending-confirm path. `kvctl-cli printjoininvitedatamatrix <leaderMultiaddr> <tokenHex>
<outFile.png>` barcodes the plain `"<leaderMultiaddr>#<tokenHex>"` string (not a signed event —
the token itself is the credential, there's nothing to sign) for a device to scan and pass
straight to its own `addfollower`/`addnode` call.

#### Reverse invite: a device requests to join ("join-request")

Join-invite above is generator-is-already-a-voter, scanner-is-the-joiner by construction — the
token only makes sense as something *a cluster* mints for *an outsider*. `shmevent.KindJoinInvite`'s
mirror image, `EventJoinRequestCreate`/`EventRecruit` (no new `Kind` — this ticket is never
raft-backed, see below), swaps those roles: the device that will end up admitted generates the
ticket, and an existing cluster voter scans it and pushes the join through, with no further action
on the device's side at all beyond having minted the ticket.

```bash
mage addpending                                          # spawn a fresh node -- no cluster yet
mage getownaddr                                          # this node's own current best-advertised addr
mage createjoinrequest                                   # (on that pending node) prints a fresh tokenHex
kvctl-cli printjoinrequestdatamatrix <ownAddr> <tokenHex> <outFile.png>   # barcode it
mage recruitpeer "<ownAddr>#<tokenHex>" <voter|learner>   # some other cluster's voter, scans & admits it
mage canceljoinrequest <tokenHex>                         # invalidate one before it's ever redeemed
```

This only works for a device that hasn't bootstrapped or joined anywhere yet: `addpending` spawns
the daemon (host + IPC alive) exactly like `addnode`'s bootstrap case, except it never sends the
usual bootstrap-or-join `EventAdd` afterward, so the node comes up with no raft instance at all
(`registry.RolePending`). That's a deliberate scope limit, not an oversight — every other "switch
which cluster this identity belongs to" path in this project (`join`/`leave`/`rm`) works by
stopping the daemon process and respawning it against a different data directory, because
`raft.NewRaft` is bound to one data dir/log for a process's lifetime; there is no live, in-process
way to move an *already-clustered* node to a different cluster. Only a node's first join (no raft
instance yet) can be admitted live, in-process — the same `handleAdd`/`join` path an ordinary
`addfollower` already uses. If a device is already solo-bootstrapped or clustered elsewhere, reset
it with `leave`/`rm` first before generating a join-request ticket.

`getownaddr` (`shmevent.EventGetOwnAddr`) returns this node's own current best-advertised address —
public first, then a `/p2p-circuit` relay reservation, then anything else, loopback last (the same
priority `pkg/daemon`'s `advertisedAddrs` already applies everywhere else it picks "the" address to
use). Queried live, never cached: a node started with `-relay-peer` only gets its actual circuit
reservation asynchronously in the background sometime after startup, so a device behind NAT/cellular
should re-run `getownaddr` a moment later if an earlier call returned a private/loopback address
instead of the relayed one — there is no other way to learn that address; unlike a leader's, it
isn't known ahead of time and isn't derivable from anything else this device can print.

`createjoinrequest` mints a fresh, cryptographically random 16-byte token and holds it purely in
that daemon's own process memory (never persisted, never a raft record — unlike `KindJoinInvite`,
this device may have no raft instance to write one through in the first place).
`printjoinrequestdatamatrix` barcodes the plain string `"<ownAddr>#<tokenHex>"` — the device's *own*
address (`getownaddr`) this time, not a leader's. `recruitpeer`, run on the recruiting voter's own node,
splits that string, mints a normal `KindJoinInvite` on its own cluster exactly like
`createjoininvite` does, and dials the device directly over a new libp2p protocol
(`pkg/daemon.RecruitProtocolID`) to hand it that invite plus the ticket's own correlation token, all
in one plain-text line (mirroring `JoinProtocolID`'s own `reqLine` — there's nothing to sign, the
correlation token is itself the credential the device already trusts, having minted it and handed it
only to whoever physically scanned the resulting barcode).

The device's own daemon (`handleRecruitStream`) checks the correlation token against its pending
ticket — consumed either way, so a replayed or wrong token can never match again afterward — and on
a match calls `handleAdd` directly, in-process, exactly as if its own operator had just run `mage
addfollower "<leaderAddr>#<tokenHex>"` themselves: the same relay-address wait, one-hop
leader-forwarding, and `-require-confirm-for-join` handling as any other invite-based join, entirely
unchanged. `recruitpeer` blocks until that join actually completes (or fails), printing the same
`"<peerID> ok"`/`"<peerID> pending"` result `addfollower` itself would. On success the device's own
`pkg/registry` entry is updated to reflect the new membership (`ClusterPeerID`/`LeaderPeerID`/
`Role`) — normally `bootUp` does this right after an ordinary join, but no `kvctl` process is
involved in this leg at all, so the daemon does it itself.

`mobile/kvmobile` mirrors this one-for-one (`StartPending`/`GetOwnAddr`/`CreateJoinRequest`/
`CancelJoinRequest`/`RecruitPeer`, wired into `CommandCatalog.kt`'s Cluster category) — the same
"brand new device with no cluster yet, admitted by whoever scans its ticket" flow, just triggered
from the Android UI instead of `mage`. `GetOwnAddr` matters even more here than on desktop: a phone
is exactly the "no other way to learn my own address" case this exists for.

### Follower on Android

The Android app (`android-app/`) runs the same follower daemon in-process via
`mobile/kvmobile`, bound as a `gomobile` AAR. The leader to join is baked in at build time (a
mobile app has no operator to type a peer ID at runtime):

```bash
export ANDROID_NDK_HOME=<path-to-ndk>   # e.g. $ANDROID_HOME/ndk/<version>
LEADER_ADDR="/ip4/<remote-ip>/tcp/4001/p2p/<leader-peer-id>"

gomobile bind -target=android -androidapi 26 \
  -ldflags "-X github.com/gofsd/libp2p-kv-raft/mobile/kvmobile.leaderMultiaddr=$LEADER_ADDR" \
  -o android-app/app/libs/kvmobile.aar ./mobile/kvmobile

cd android-app && ./gradlew assembleDebug
adb install -r app/build/outputs/apk/debug/app-debug.apk
```

`-androidapi` **must be 26 or higher** — the shmring Android backend uses `ASharedMemory_create`,
which the NDK headers only declare from API 26 onward; building against a lower target silently
hides the declaration and fails with a confusing `could not determine what
C.ASharedMemory_create refers to` linker error rather than a clear availability error.

The app's UI browses the full `Kvmobile` command surface rather than exposing just a few calls:
`MainActivity` brings the daemon up once via `Start` (joining the build-time-baked-in leader) and
lists every category from `CommandCatalog.kt`'s ~60-entry `CommandSpec` table (Cluster, KV,
Permits, Execute, Log records, Group, Command, Links, Dispatch, ExecInvite, Raw); tapping one opens
`CommandListActivity` for that category's commands, and tapping a command opens
`CommandDetailActivity`, which renders one labeled input field per parameter, a Run button, and
that screen's own scrollable output log. Every call goes through the daemon's IPC exactly like the
desktop CLI, just over the Android shared-memory transport instead of named shared memory, off the
UI thread; `submit`/dispatch calls are forwarded from this (never-leader) follower to whichever
peer is currently leader, over `pkg/daemon.ForwardProtocolID`. The three standing-subscription
calls (`WatchExecute`, `WatchCommandLog`, `RunCommandDispatcher`) post their callback notifications
to a process-wide `OutputLog` instead of that one screen's output, since the subscription keeps
running after you navigate away — `ActivityLogActivity` (the main screen's "Activity Log" button)
shows that full history plus live updates while it's the foregrounded screen.

`Kvmobile` runs exactly one daemon per process. `Start(dataDir)`/`StartWithKey(dataDir, keyHex)`
bring it up joined to the build-time-baked-in leader — the Android equivalent of desktop's
`addfollower`/`addfollowerwithkey`, not `addnode`/`addnodewithkey`: a phone can never bootstrap a
*fresh* cluster as leader (`Start` errors out if no `leaderMultiaddr` was compiled in), so there's
no kvmobile equivalent of `addnode` at all. `Start` also folds in what `resumenode`/`rejoinnode` do
on desktop: it re-sends the join every call regardless of whether `dataDir`'s identity is fresh or
already-persisted, since `raft.AddVoter` is a no-op-ish update for a peer id already in the
configuration (see `Start`'s own doc comment for the same reasoning `pkg/kvctl.RejoinNode` uses).
`StartWithKey` provisions `dataDir`'s identity from an existing `identity.key`-format hex string at
runtime instead of always minting a fresh one (refusing if `dataDir` already holds a *different*
identity, same guard `addnodewithkey` applies). `Stop()` shuts the current daemon down so a
different `dataDir`/identity can be started next — there's no multi-node registry to pick a
"current" one from the way desktop's `mage use <peerID>` does, so switching is just `Stop()` then
`Start`/`StartWithKey` against the target directory. `Delete(dataDir)` wipes a `dataDir` outright
and refuses while a daemon is running against it, same safety rule as `mage deletenode`.

`Join(dataDir, leaderAddr)`/`JoinWithKey(dataDir, keyHex, leaderAddr)` are the Android equivalent of
desktop's `mage join <targetPeerID>`: switching this device onto a *different* cluster at runtime
(`leaderAddr` a full multiaddr, e.g. typed by an operator or scanned from a QR code) rather than the
one baked in at build time. Unlike `Start`, which no-ops if a daemon is already running, `Join`
always stops whatever's currently running first and starts a fresh one against `leaderAddr` —
switching clusters is the whole point of calling it. Joining back into a cluster this identity has
belonged to before (matching leader peer id) picks its local raft state back up and lets raft's own
snapshot/log replication catch it up on everything committed while it was away, exactly like
`join`/`rejoinnode` do on desktop (see `pkg/daemon.TestOriginRejoinsClusterCatchesUpOnMissedWrites`).

`Leave()` asks the cluster the currently-running device is joined to to remove it
(`raft.RemoveServer` — a graceful shrink, the remaining voters keep operating normally) and then
stops the daemon, mirroring desktop's `mage leave`. Unlike desktop, there's no solo/default
cluster to fall back to afterward — `kvmobile`'s daemon always joins the build-time-baked-in
`leaderMultiaddr` — so a subsequent `Start`/`StartWithKey` just attempts to rejoin the very same
cluster. `Rm()` does everything `Leave` does, plus revokes this device's `cluster-join` standing
(so that later rejoin attempt needs a fresh confirmation rather than being silently re-admitted,
if the leader requires one) and deletes the joined cluster's local data subdirectory specifically
— never the identity key at `dataDir` itself, same distinction desktop's `mage rm` draws against
`mage deletenode`.

`CreateJoinInvite(suffrage)` is the Android counterpart of desktop's `mage createjoininvite
<voter|learner>`: mints a fresh, random `KindJoinInvite` token (returned hex-encoded) granting the
given suffrage on this device's own cluster, without hand-delivering it the way `RecruitPeer` does
— append it to this device's own address (`GetOwnAddr`) as `"<addr>#<tokenHex>"` for some other
device's `Join`/`Start` to redeem directly, admitted immediately even if this cluster's leader
normally requires confirmation. `RevokeJoinInvite(tokenHex)` deletes one before it's ever redeemed.
Both only take effect if this device is itself a raft voter, same as desktop.

`Kick(targetPeerID)` is the Android counterpart of desktop's `mage kick`: force-removes some
*other* peer from the currently-joined cluster (`raft.RemoveServer`) without that peer's own
cooperation, for a voter that's gone down for good and isn't coming back to gracefully `Leave` on
its own. Unlike `Leave`/`Rm`, it never touches this device's own membership or restarts anything —
this device stays exactly where it is; `targetPeerID` is who leaves. It only takes effect if this
device is itself a raft voter (or forwards to one, exactly like desktop) — true for any real device
build, since `Start`/`StartWithKey`'s automatic join always requests full voter suffrage (only
`pkg/e2erun`'s own test harness ever builds a learner-suffrage variant). Like `mage kick`, it can't
help once the cluster has already lost quorum outright — that needs desktop's offline `cmd/kvrecover`
instead, which has no Android equivalent (it operates on a *stopped* node's raw on-disk raft files, a
concept that doesn't fit `Kvmobile`'s always-running in-process daemon).

`AccessToken()` is the Android counterpart of desktop's `mage accesstoken <peerID>`: this device's
own deterministic `cmd/kvhttp` bearer token (an HMAC over `identity.key`'s raw private key bytes —
see `registry.AccessTokenForKeyFile`'s doc comment), e.g. to hand a desktop operator this device's
token for a `kvhttp` routing rule. Desktop resolves `peerID` through its multi-node registry first;
`Kvmobile` runs exactly one daemon and needs no such lookup, so this just derives straight from
whatever `identity.key` already sits at the running device's own data directory.

`ListClusters()` and `ListClusterMembers()` are the Android counterparts of desktop's
`listclusters`/`listnodes`, adapted to `Kvmobile` running exactly one daemon at a time:
`ListClusters()` returns a JSON array with 0 or 1 entries — whichever cluster, if any, this device
is currently joined to — since unlike desktop's `registry.json` there's no persistent history of
every cluster this identity has ever joined to enumerate; `ListClusterMembers()` needs no peer-id
argument (there's only ever one running daemon to ask) and returns that one cluster's full live
membership the same way desktop's `listnodes` does.

`RangeScan(start, end, limit)` is the Android counterpart of desktop's `rangescan`: a JSON array of
every key/value pair in `[start, end]` on this device's own locally replicated state, up to `limit`
results (`0` = unlimited) — the same generic complement to `Submit`/`Get` desktop's version is to
`set`/`get`, under the same "a local caller already has full read access to its own daemon" scope.

`Kvmobile` also binds the permit and direct-notification desktop commands, against whichever
device is currently running (Start's session, same as Submit/Get): `RequestPermit`/`ConfirmPermit`/
`RevokePermit(kind, targetPeerID[, metadata])` (`kind` is `"bootstrap"`, or `"cluster-join"` for
`ConfirmPermit`, mirroring `mage requestpermit`/`confirmpermit`/`revokepermit` — the old `"peer"`
kind and its `*LogPermit` counterparts were removed along with `KindPermitPeer`/`KindLogPermit`,
folded into the group-based ACL mechanism instead, see
[Reserved cluster/voter/learner/channel/relay/remote/execute groups](#reserved-clustervoterlearnerchannelrelayremoteexecute-groups));
`Execute`/
`PollExecute` for the raft-bypassing peer-to-peer `EventExecute` notification (`mage execute`/
`pollexecute`) — `PollExecute` returns a JSON envelope (`{"pending":true,"sender_peer_id":"...",
"value":"..."}` or `{"pending":false}`) since gomobile bindings only support one non-error return
value, unlike `pkg/kvctl.PollExecute`'s 4-value Go signature. `LogAppend(kind, unitID, fieldsJSON,
narrative)`/`LogQuery(kind, unitID, since, until, limit)` are the `pkg/logrecord` read/write
counterparts (`mage logappend`/`logquery`) — `LogQuery` likewise returns a single JSON array string
(`"[]"` when nothing matches, never `null`) instead of `pkg/kvctl.LogQuery`'s `[]logrecord.Record`.

`WatchExecute(cb ExecuteCallback)`/`StopWatchExecute()` push `EventExecute` notifications to the
caller instead of requiring a `PollExecute` timer: `ExecuteCallback` is a Go interface Kotlin
implements (gomobile's reverse-binding direction — the only `kvmobile` API that isn't plain
string-in/string-out), and `WatchExecute` runs a background loop calling `cb.OnNotification`
per notification, in delivery order, on its own goroutine (Kotlin implementations must marshal
back onto the UI thread themselves before touching views). A single registration survives a
`Stop`/`Start` identity switch with no need to call `WatchExecute` again — the loop just waits
whenever no daemon is currently running rather than exiting. `PollExecute` still exists for a
one-shot manual drain; `WatchExecute` is the continuous-delivery alternative, e.g. to drive a
live "command execution log" view fed by whichever peer is running the command (see that peer's
own `LogAppend` calls, watched for and re-fetched via `LogQuery` on each poke).

`OpenChannel(peerID, cb)`/`ListenChannel(cb)`/`StopListenChannel()`/`SendChannelData(channelID,
purposeName, chunk)`/`CloseChannelWrite(channelID)`/`CloseChannel(channelID)`/
`StopChannel(channelID)` (`channel.go`) are the mobile port of desktop's `mage openchannel`/
`listenchannel` raw byte pipe (see "Raw Channel" above). `CloseChannelWrite` is the half-close a
bulk sender should call once done — unlike `CloseChannel`, it doesn't return until every chunk
already handed to `SendChannelData` has actually reached the wire, and it doesn't stop this
device's own delivery loop, so a reply the remote peer still has in flight is never cut short.
`OpenChannel`/`ListenChannel` each start a background pump loop delivering incoming data to
`cb.OnData`/`cb.OnClosed` (the `ChannelCallback` reverse-binding interface, mirroring
`ExecuteCallback`) until `StopChannel` is called or the channel ends on its own — same
outlives-this-screen, "replace, don't stack" treatment `WatchExecute`/
`RunCommandDispatcher` already get, including recovering a panicking callback so one misbehaving
Kotlin implementation can't take the loop down. Chunks cross the gomobile boundary as raw bytes in
both directions (`SendChannelData`'s `chunk` argument, `OnData`'s callback argument are both plain
`[]byte`/`ByteArray`, not base64 text) — gobind binds a Go `[]byte` parameter directly to a Kotlin
`ByteArray`, so unlike `ExecuteCallback`'s deliberately text-only `OnNotification` (Execute's
payloads are genuinely text), there was no boundary limitation forcing base64 here; an earlier
version of this interface used it anyway, on a mistaken assumption about gomobile's own
capabilities, and a real desktop↔android bulk-transfer benchmark confirmed removing it recovered a
substantial share of this path's own throughput. `StopChannel`/`StopListenChannel` are local-only,
like `StopWatchExecute` — they stop this device's own delivery loop without necessarily ending the
channel/pending listen
itself; `CloseChannel` ends the session server-side too.

`kvmobile` also has a `Group`/`Command` catalog layer (`catalog.go`), built on the same
daemon-enforced ACL records desktop's `mage`/`pkg/kvctl` catalog uses (`KindGroup`/`KindCommand`/
`KindGroupCommand`/`KindPeerGroup` — see "Group/command ACL" above for the full model; this is not
a separate implementation, just a gomobile-bound wrapper around the identical
`EventGroupPut`/`EventCommandPut`/`EventGroupCommandPut`/`EventPeerGroupPut` calls). `CreateGroup(id,
name)`/`UpdateGroup(id, name)`/`DeleteGroup(id)`/`GetGroup(id)`/`ListGroups()` and the `Command`
counterparts `CreateCommand(id, name, targetPeerID)`/`UpdateCommand`/`DeleteCommand(id)`/
`GetCommand(id)`/`ListCommands()` mirror `mage creategroup`/`createcommand`/etc. exactly, each
returning its result as a JSON string (gomobile bindings only support one non-error return value).
`AddCommandToGroup(commandID, groupID)`/`RemoveCommandFromGroup`/`ListGroupsForCommand(commandID)`
and `AddPeerToGroup(peerID, groupID)`/`RemovePeerFromGroup`/`ListGroupsForPeer(peerID)` mirror the
matching `mage` targets — a `Command` no longer names a single owning group directly (it may belong
to several via `AddCommandToGroup`), and there is no participation permit anymore: any current raft
voter may write any of these four kinds unilaterally, enforced by `pkg/daemon` itself, not by
`kvmobile` client-side. Not carried over from the pre-rewrite version: `Group.Description` and
`Command.Description`/`FormSchema` (the new daemon-enforced records have no room for free-form
metadata), and `ResolveQRGroup`/`GroupView` — `GroupCommand`'s key is commandID-first, so there's no
efficient "every command linked to this group" primitive to build a QR-resolved command list from
anymore; a caller decodes the scanned group id itself and calls `GetGroup` + `ListCommands`
(the full catalog) instead.

`kvmobile`'s dispatch layer (`dispatch.go`) turns a `Command` from the catalog into an actual
request/response flow, still with no new capnp wire schema — mirrors desktop's
`pkg/kvctl/dispatch.go` closely enough that the two files carry near-identical doc comments.
`SubmitCommand(commandID, inputsJSON)` — gated by the same `isPermittedForCommand` join described
above, evaluated client-side in `kvmobile` itself — writes a durable `CommandRequest` under a
per-command log kind and sends the command's `TargetPeerID` a best-effort `Execute` poke, returning
an `instanceID` the caller tracks the dispatch by; `GetCommandRequest(commandID,
instanceID)`/`ListCommandRequests(commandID)` read it back (the latter is a target device's
catch-up path for a poke it might have missed). The target reports progress with
`AppendCommandLog(requesterPeerID, instanceID, fieldsJSON, narrative)`, read back via
`QueryCommandLog`/`WatchCommandLog` (a 1.5s poll, accelerated but not replaced by
`AppendCommandLog`'s own poke back to the requester) or `LatestCommandLog(instanceID)` for just the
newest entry.

`ListExecutionsByPeer(peerID)` answers "every command execution touching this peer" without
iterating `ListCommandRequests` per command: `SubmitCommand` writes a small per-peer index entry
(`commandExecIndexKind`) alongside the `CommandRequest` itself, once for the requester and once for
the target (skipped if they're the same peer), and `ListExecutionsByPeer` walks just that one
peer's index, most-recent-first, capped at 200. The index is deliberately thin — it stores only
`command_id`/a one-byte role code, not `requested_by` (already the record's own `AuthorPeerID`) or
`target_peer_id` (redundant when the role is target; looked up via `GetCommand` for a
requester-role entry instead) — because every `pkg/logrecord` write shares
`pkg/shmevent.ValueSize`'s 512-byte budget across its *key* (which already embeds a full peer id
once for this index, via `commandExecIndexKind`) and value combined; an earlier version of this
index stored both peer ids directly and blew that budget the moment two real ~52-byte peer ids
were involved in the same dispatch.

`CreateExecInvite(commandID, inputsJSON)`/`RevokeExecInvite(tokenHex)`/`RedeemExecInvite(sourceAddr#tokenHex)`
are `kvmobile`'s port of desktop's one-time execution invites (see "One-time execution invites"
above for the full design — identical daemon-side ACL/consume guarantees, this is purely the
gomobile-bindable client wrapper). Unlike desktop's `kvctl-cli printexecinvitedatamatrix`, there's no
barcode-rendering call here: `CreateExecInvite` just returns the raw `tokenHex`, and the app combines
it with its own advertised multiaddr and renders the barcode itself (e.g. a Kotlin QR/Data-Matrix
library) — the same "this Go layer hands back data, presentation is the app's job" reasoning
`catalog.go`'s doc comment gives for why `ResolveQRGroup` wasn't carried over either.

`RunCommandDispatcher(commandID, handler)`/`StopCommandDispatcher(commandID)` are `kvmobile`'s port of
desktop's `pkg/kvctl.RunCommandDispatcher` — the first real implementation of the "target device's own
application logic" this section's own dispatch-layer description above leaves unspecified. Since
gomobile has no func-parameter support, `handler` is a `CommandDispatchHandler` interface Kotlin
implements (the same reverse-binding pattern `ExecuteCallback`/`LogCallback` already use):
`Handle(instanceID, commandID, requestedBy, inputs string) string` runs at most once per instance id
(deduped via `QueryCommandLog`, a panic-safe call) and returns a JSON
`{"fields":{...},"narrative":"..."}` object naming the `AppendCommandLog` result to record. A second
`RunCommandDispatcher` call for the same `commandID` replaces the first — independent `commandID`s run
concurrently, the same "replace, don't stack" shape `WatchCommandLog` already uses, keyed the same way.
One real difference from the desktop port: this has **no** `PollExecute`-based fast path at all, purely
timer-based (`watchCommandLogPollInterval`, 1.5s) — `pkg/daemon`'s `executeInbox` is a single-consumer
queue per device (see `WatchCommandLog`'s own doc comment on this identical constraint), so a second
independent `PollExecute` drainer here would race a real app's own `WatchExecute` callback — the normal
shape a `kvmobile` app already runs — and silently steal notifications meant for it.

`Kvmobile.sendEvent` (not used by `MainActivity`, only by the e2e pipeline's `E2ETest`
instrumented test) exposes the same raw `pkg/shmevent` event dispatch `submit`/`get` are themselves
built on, for tests that need the exact event kvctl-cli's `sendevent` can send on desktop/remote
rather than only the higher-level Set/Get shape.

**MIUI/Xiaomi devices**: `adb install` can fail with `INSTALL_FAILED_USER_RESTRICTED` even with
"Unknown sources" allowed — there's a separate Developer Options toggle, **"Install via USB"**,
that must also be enabled.

**Relay reservations for NAT'd followers**: `pkg/daemon.Config.RelayPeers` (and the mirroring
`mobile/kvmobile.relayMultiaddr` build-time var, comma-separated for more than one) let a follower
with no directly-dialable address (e.g. a phone behind carrier-grade NAT) proactively reserve a
circuit-relay v2 slot through one or more relays, so a raft voter that nothing can dial directly
can still be reached. This previously failed the join handshake with a libp2p
stream-protocol-negotiation error (`0x1001`): the relay reservation wait was sitting between
opening the join stream and writing to it, easily outlasting the remote's negotiation timeout.
It's fixed — `join()` now waits for the reservation before opening the stream at all, and the node
also forces itself privately reachable when any relay candidate is configured rather than leaving
that judgment to AutoNAT — and covered by a real relay+leader+follower cluster test
(`pkg/daemon.TestJoinThroughRelay`). A plain direct join (no `relayMultiaddr`) has also been tested
working from a phone on cellular data, joining a publicly reachable leader.

**Relay list and failover**: `RelayPeers`/`relayMultiaddr` are only the *seed* list — enough to
reach the cluster on a device's very first join, before it has any replicated data of its own yet.
Once running, `pkg/daemon`'s `relayCandidates` (called from `newHost`) merges that seed list with
every currently-*confirmed* `shmevent.KindBootstrapNode` record already in the node's own local
store, sorted by ascending priority (lower tried first), and hands the whole ordered set to
`libp2p.EnableAutoRelayWithStaticRelays` — which already accepts, and fails over between, more than
one candidate, so a node isn't stuck if its first-choice relay goes down. `KindBootstrapNode`
uses the generic `EventPermitRequest`/`EventPermitConfirm`/`EventPermitRevoke` request/confirm/revoke
lifecycle (`mage requestpermit`/`confirmpermit`/`revokepermit bootstrap`, above -- any node may
request, only a current raft voter's confirm actually activates one), extended with a priority
byte and a proper read side:

```bash
mage addrelaynode "<multiaddr>" 0       # request (pending) -- multiaddr's own /p2p/<peerID> is the key
mage confirmrelaynode "<multiaddr>"     # (on a current voter) activate -- now picked up by every member
mage listrelaynodes                     # every confirmed relay, ascending priority
mage getrelaynode "<multiaddr>"
mage removerelaynode "<multiaddr>"      # (on a current voter) delete
```

`mobile/kvmobile.AddRelayNode`/`ConfirmRelayNode`/`GetRelayNode`/`ListRelayNodes`/`RemoveRelayNode`
are the Android-bound equivalents, same request/confirm/revoke shape, `priority` as a plain `int`
clamped into `0-255`. Because this is ordinary raft-replicated state, a newly confirmed relay
reaches every cluster member's own candidate list the next time each one calls `relayCandidates`
(startup, or the next `join()`) — no coordinated restart needed.

**Which node to point `RelayPeers`/`relayMultiaddr` at**: `configs/bootstrap-nodes.json` (read via
`mage bootstrapnodes`) is the catalog of already-deployed `-relay-service` nodes -- any node that
can't guarantee it's directly dialable (a phone, a browser, a dev laptop on a LAN/firewall/dynamic
IP) should reserve its relay slot through one of those rather than assume direct dial-back will
work; `addrelaynode`/`confirmrelaynode` above is how to register one of them (or any other
`-relay-service` node) as an ongoing failover candidate for the whole cluster, not just this one
node's own seed list. See `CLAUDE.md`'s "Node connectivity policy" for a real gap this surfaced
and its fix: a follower's forwarded `Set` used to dial the current leader directly with no relay
fallback at all, so a relay-only leader broke every follower's writes even though join/replication
kept working — every `forward*` protocol now dials through `(*Node).dialForward` instead, so a
relay-only leader no longer breaks forwarded writes outright. It's still sensible to keep raft
leadership on a bootstrap-nodes.json host when one is available (direct dials are cheaper/faster
than a relay hop), just no longer a correctness requirement.

### Client in a browser

Unlike the desktop CLI and the Android app, a browser tab can never be a raft *voter*: a voter's
transport must be independently dialable by any other voter at any time, and a browser sandbox has
no way to accept a raw inbound connection. But it turns out a tab *can* be a raft **non-voter
(learner)** once it holds a circuit-relay v2 reservation — the same mechanism a phone behind
carrier-grade NAT already relies on (see [Relay reservations for NAT'd
followers](#follower-on-android) above) — so `web-app/` is a real (if non-voting) member of the
cluster, in Rust compiled to `wasm32-unknown-unknown` over `rust-libp2p`: it reimplements
`hashicorp/raft`'s `NetworkTransport` msgpack wire protocol to receive genuine `AppendEntries`
replication, backed by real SQLite (`sqlite-wasm-rs`) for the replicated log and kv table. Joining
happens over `pkg/daemon.ClientProtocolID`, speaking `pkg/shmevent`'s capnp struct: the browser
first fetches the target's Ed25519 key (`EventGetPrivateKey`, unsigned — the one bootstrap
exception), then sends a signed `EventSetKey`+`EventAdd` pair (own peer id, then own reserved
address) to `handleAddLearner`, which calls `raft.AddNonvoter` — forwarding to the real leader
server-side if the dialed node isn't it, one hop, mirroring how a voter's own join request forwards
(`pkg/daemon.ForwardJoinProtocolID`). A Set still forwards to the leader the same way (as a signed
`EventSetKey`+`EventSetField` pair); a Get reads this tab's own locally-replicated state.

Every node already listens on a browser-reachable WebTransport address (`newHost` adds it
alongside the existing TCP/QUIC listeners); `advertisedAddrs()`/`ready.json` include it
automatically, with its `/certhash` component already appended:

```bash
cat ~/.libp2p-kv-raft/registry.json   # listen_addrs includes .../quic-v1/webtransport/certhash/.../p2p/<peer-id>
```

```bash
cd web-app
npm install
npm run dev   # builds the wasm bundle, then serves on a cross-origin-isolated origin
```

Paste that WebTransport multiaddr into the running page's "Node multiaddr" field and Connect —
unlike Android's build-time-baked leader address (a phone has no operator to type one in), the web
UI takes it at runtime, closer to desktop's `mage addfollower <addr>`. See `web-app/README.md` for
the full architecture and its currently-unverified-in-CI gaps (needs a wasm32 C toolchain for
SQLite, and a real browser + live cluster to exercise end to end).

### HTTP command bridge (`kvhttp`)

`web-app/` above needs a cross-origin-isolated origin (SharedArrayBuffer/WebTransport) that not
every embedding allows — e.g. a third-party sandbox that only permits plain `fetch()`. `cmd/kvhttp`
is a thin local HTTP front door for exactly that case: one endpoint, `POST /command`, accepting and
returning the same human-readable event JSON `kvctl-cli sendevent` already speaks (the `set_key`/
`set_field`/`get_field` shape used throughout this README). It never touches shmring/libp2p/raft
itself — it just shells out to `kvctl-cli sendevent` so the real signing/IPC logic stays in one
place.

```bash
mage kvhttp                 # foreground, listens on 127.0.0.1:8787 by default
mage kvhttp 127.0.0.1:9000  # or a specific -addr
```

One running `kvhttp` serves *every* node currently in the local registry rather than being pinned
to one at startup: each request must carry `Authorization: Bearer <token>` naming which node it
targets. `<token>` is that node's own deterministic access token — `mage addnode`/`addnodewithkey`
print it automatically once the node is up, and it can be recovered again any time (it's re-derived
from `identity.key`, `registry.AccessTokenForKeyFile`, nothing separate is stored) via:

```bash
mage accesstoken <peerID>
```

A request whose token doesn't match any registered node's `401`s before `kvctl-cli` ever runs, so
holding one node's token drives *that* node exactly as if running `kvctl-cli sendevent` yourself,
and says nothing about any other node this machine happens to also have registered — e.g. a
single-node cluster bootstrapped from an operator-supplied identity via `mage addnodewithkey
<keyFile>` gets its own token the same way, usable immediately against the same running `kvhttp`:

```bash
curl -X POST http://127.0.0.1:8787/command \
  -H "Authorization: Bearer $(mage accesstoken <peerID>)" \
  -d '{"event":"set_key","value":"greeting","id":100}'
```

Still meant for a trusted localhost network path, not a substitute for TLS/real network-level
access control if exposed beyond that — token comparison is constant-time, but request bodies and
tokens themselves travel in the clear otherwise.

### Local IPC token

Every local caller reaching a node over `pkg/ipc` (`mage`/`kvctl-cli`/`kvctl`/`kvmobile` — not
`kvhttp`'s own Bearer token above; the two are unrelated) is now also gated by a second, different
secret: a random per-node token, persisted alongside `identity.key` in the node's own data
directory as `ipc.token` (`0600`, same permission discipline as that file). Unlike
`AccessTokenForKeyFile`'s token above — deterministic, re-derivable from `identity.key` at any
time — this one is generated once, the first time either side asks for it via `pkg/ipc`'s
`loadOrGenerateToken`: whichever side gets there first (the daemon starting up, or the first
client to dial in) writes it, and every other caller, on either side, just reads the same bytes
back off disk afterward. That makes ordering harmless: `pkg/kvctl`'s `bootUp` already has the
daemon write `ready.json` before any client's `waitForReady` returns and issues its first call, so
in practice the daemon always wins the race, but nothing depends on that.

This closes a real gap the shmring transport otherwise had: `pkg/ipc`'s request/response segment
names used to be derived from a node's peer id alone — public, printed to stdout the moment a node
starts, and readable by anyone with access to `registry.json` — and shmring's POSIX backend
creates its shared-memory segments with owner **and group** read/write
([`github.com/hidez8891/shm`](https://github.com/hidez8891/shm)), not owner-only. Any process
running as the same user, or merely the same group, could attach to a node's channel just by
knowing (or guessing) its public peer id: race the real client to create the segment, or read/
forge a request or response on it. Folding the token into the segment name itself
(`kvipc-<peerID>-<token>-req`/`-resp-<id>`, not just checked after the fact once already attached)
closes that outright: a process with no read access to `ipc.token` cannot even construct the right
name to attach to the channel, let alone forge traffic on it — the same "tight file, loose
directory" split `identity.key` already relies on (`0600` inside an otherwise-listable, `0755`
data directory).

A client resolves a target peer's token the same way it resolves everything else about a node it
didn't spawn itself: through the local registry (`registry.json`'s `DataDir` entry for that peer
id, `pkg/ipc`'s `tokenForPeer`) — so, like every other `pkg/ipc` caller, this only ever works
against a peer id already known to this machine's own registry, and fails closed (a hard error,
never a silent fallback to the old tokenless name) for one that isn't. There is no way to reach a
node's local IPC channel without also having filesystem access to read its `ipc.token`.

This only protects `pkg/ipc`'s desktop transport (`ipc.go`) — Android's (`ipc_android.go`) has no
separate process to protect against in the first place: client and daemon already share one
process there (see that file's doc comment), rendezvousing through an in-process Go channel with
no OS-level name lookup at all, so it takes an unused `dataDir` parameter purely so its `Serve`
call site compiles identically on both platforms, not because it performs a real check.
`mobile/kvmobile`'s own `Start`/`StartSolo` still best-effort register themselves in the local
registry after spawning their in-process follower — harmless, and needed only because this
package's own test suite (built without the `android` tag) exercises the desktop transport as a
stand-in for Android's; a real device's `registry.Open` call failing (no usable `$HOME` in an app
sandbox) is silently ignored rather than blocking startup.

## Log records

`pkg/logrecord` is a generic, replicated structured-record store built on top of the
same raft-backed KV path ordinary `set`/`get` use — for staff journals, situation
reports, casualty reports, or any other append-heavy structured record type an operator
wants to keep. It's deliberately generic: `kind` (e.g. `"sitrep"`, `"journal"`,
`"casrep"`, anything) is a plain string chosen at call time, not a fixed list baked into
the code, and `Record`'s `Fields`/`Narrative` are an open envelope — this package makes
no claim to implementing any report format's real standardized field layout (those vary
by service, nation, and doctrine); populate them however your own reporting standard
requires.

```bash
mage logappend sitrep 1BCT '{"posture":"green"}' "no significant activity"
mage logappend sitrep 1BCT '{"posture":"amber"}' "increased patrol activity, sector 4"
mage logquery sitrep 1BCT             # every sitrep record for unit 1BCT, oldest first
mage logquery sitrep 1BCT "" "" 10    # same, capped at 10 records
```

Every record's key packs `kind` + `unitID` + a nanosecond timestamp so that "every
record of this kind and unit, in a time window, in order" is a plain ordered range scan
(`pkg/store.Store.ScanRange`, exposed remotely as `pkg/shmevent.EventListRange`) —
`logquery`'s optional third/fourth arguments are RFC3339 `since`/`until` bounds. Writing
a record goes through the same raft-replicated `handleSetForward` path an ordinary `Set`
does, under a key inside a reserved namespace (`logrecord.LogKeyPrefix`, alongside the
existing `shmevent.SystemKeyPrefix` reserved for permits/cluster membership) that an
ordinary caller-supplied key can never collide with — reached through its own
`shmevent.EventLogAppend` event rather than plain `Set`, since `Set`/`SetField`
themselves reject that reserved namespace outright.

Two accepted v1 limits, not oversights: a record's JSON encoding shares the same
512-byte `Set` payload budget as everything else (`shmevent.ValueSize`), leaving roughly
400-470 bytes for `Fields`+`Narrative` combined — tight for a long narrative; and there's
no entry cap or rotation policy, since silently dropping old journal entries once a
count limit is hit would be actively wrong for a logbook. Both are left for a future pass
if they turn out to matter in practice.

### Log access control

A current raft cluster member (voter or learner) may `logappend`/`logquery` records of *any*
kind unconditionally, same as every other event this project unconditionally gates by cluster
membership — see [Leader on a remote machine](#leader-on-a-remote-machine-over-ssh) above.
There is no per-kind permit system anymore (`KindLogPermit`/`-require-permit-for-log`/`mage
requestlogpermit`/`confirmlogpermit`/`revokelogpermit` were removed entirely): a non-member
remote caller gets no log access at all, *except* the narrow command-log carve-out below, which
is scoped to one specific dispatch's own records, not an arbitrary kind such as `"sitrep"`. A
local `mage`/`kvctl-cli` caller is, as always, trusted unconditionally as this node's own
operator regardless of raft membership.

The one door a non-member remote peer has into the log namespace at all is
`pkg/daemon.isCommandLogCarveOut`, the same exception described under
[Leader on a remote machine](#leader-on-a-remote-machine-over-ssh): submitting a command linked
to a public `Group` (`EventLogAppend` targeting `shmevent.CommandRequestLogKind(commandID)`,
still raft-authoritatively enforced by `kvfsm.OpAppendCommandRequest`'s `IsPermittedForCommand`
regardless of this carve-out) and reading back that exact dispatch's own records: the same
`CommandRequestLogKind(commandID)` queue for a command the caller is permitted for, its own
`shmevent.CommandExecIndexKind(peerID)` execution index (self-scoped by peer id — another peer's
index is never readable this way), and `shmevent.CommandExecLogKind` itself for any instance id
at all (unrelated to command/group standing — "possessing the instance id is the credential",
the same design `GetCommandRequest`'s own doc comment already described before this carve-out
existed). Nothing else in the log namespace is reachable by a non-member remote caller.

The read-side check covers the *entire* scanned range, not just where it starts: a `ListRange`
query is only admitted if both `start` and `end` resolve to the identical, allowed kind
(`pkg/daemon.logKindOfBound`) — `pkg/store.Store.ScanRange` is a raw byte range with no concept
of namespaces on its own, so a `start` chosen from inside an allowed kind paired with an `end`
reaching into an unrelated one would otherwise still return data outside what the carve-out
means to admit.

## Group/command ACL

Desktop's `mage`/`pkg/kvctl` and `mobile/kvmobile` (see `kvmobile`'s own section above for its
gomobile-bound equivalents) share one group-based command ACL, not two separate implementations:
`Group` (`id`, `name`, `public`) and `Command` (`id`, `name`, `peer_id` — the command's
`TargetPeerID`) are direct records, and `GroupCommand`/`PeerGroup` are many-to-many relations
linking commands to groups and peers to groups respectively. All four are real
`shmevent.SystemKeyPrefix` records (`KindGroup`/`KindCommand`/`KindGroupCommand`/`KindPeerGroup`),
daemon-enforced rather than a client-side convention. Only `id` is a storage key and therefore
inherently unique on its own; `name` gets its own explicit enforcement (`kvfsm`'s
`checkGroupNameUnique`, evaluated inside the same raft `Apply` call that performs the write, not by
a separate pre-check — a client-side check ahead of time would leave a TOCTOU race between two
concurrent `creategroup` calls picking the same name) so two different groups can never share one:

```bash
mage creategroup g1 infantry false      # or 'updategroup g1 <newname> <public>' -- same call, Put semantics
mage createcommand c1 resupply <peerID> # Command's peer_id is who may execute it
mage addcommandtogroup c1 g1            # link: c1 is now reachable through group g1
mage addpeertogroup <peerID> g1         # membership: peerID may now submit/execute c1
mage submitcommand c1 '{"qty":"20"}'    # dispatches c1, gated by GroupCommand+PeerGroup below
```

CRUD (`creategroup`/`updategroup`/`deletegroup`, `createcommand`/`updatecommand`/`deletecommand`,
`addcommandtogroup`/`removecommandfromgroup`, `addpeertogroup`/`removepeerfromgroup`) is
single-step and voter-gated — any one current raft voter may write any of these four kinds
unilaterally, no second-voter confirmation, reusing the same voter-gated forwarding path
`confirmpermit` uses (`handleForwardConfirmStream`, widened to also accept a
plain `kvfsm.OpSet`) rather than a separate pending→confirmed dance — except `creategroup`/
`updategroup`, which additionally fail outright if `name` is already used by a *different* `id`
(re-Putting a group under its own `id`, unchanged or renamed to something not otherwise taken, is
never a collision with itself). `submitcommand`'s
authorization check (`isPermittedForCommand`) is a real join over the two relation kinds: it walks
the command's linked groups (`GroupCommand`, capped small — see below) and, for each one, either
finds that group's own `public` flag set — in which case *any* peer is permitted, no `PeerGroup`
record needed at all — or falls back to point-checking the submitting peer's membership
(`PeerGroup`) in it, refusing if the command isn't linked to any group the caller belongs to or
that is marked public. A group's `public` flag is meant for commands any peer should be able to
trigger regardless of standing (e.g. a status/health check) — `addpeertogroup`/
`removepeerfromgroup` become no-ops for authorization purposes on a public group, since membership
was never what was granting access.

Deleting a `Group` or `Command` cascades in the same raft `Apply` (`pkg/kvfsm.OpCascadeDelete`): every
`GroupCommand`/`PeerGroup` record referencing the deleted id is removed alongside it, so a delete
never leaves a dangling relation behind. `Group` and `Command` records each have their own list cap
— 200 groups, 2000 commands (`pkg/kvfsm`'s `systemListLimits`) — tighter than the 65000-entry
default every other `SystemKeyPrefix` kind (including `GroupCommand`/`PeerGroup` themselves) gets.

### Reserved cluster/voter/learner/channel/relay/remote/execute groups

Every cluster auto-creates seven reserved `Group` records the moment it's bootstrapped
(`pkg/daemon.ensureReservedGroups`, run once inside the bootstrapping node's own `handleAdd`, right
after its self-election completes): `cluster`, `voter`, `learner`, `channel`, `relay`, `remote`,
and `execute` (`shmevent.ReservedGroupCluster`/`ReservedGroupVoter`/`ReservedGroupLearner`/
`ReservedGroupChannel`/`ReservedGroupRelay`/`ReservedGroupRemote`/`ReservedGroupExecute`). Their
own `Group` records are protected outright — `creategroup`/`updategroup`/`deletegroup` against any
of the seven ids is rejected (`EventGroupPut`/`EventGroupDelete`, `shmevent.IsReservedGroupID`),
the same way every other reserved-namespace write in this project is refused at the daemon
boundary rather than left to convention.

`cluster` and `voter`/`learner` additionally keep their *membership* in lockstep with the raft
cluster's actual live composition, automatically, with no operator action at all
(`pkg/daemon.syncMemberGroups`/`clearMemberGroups`, called alongside every place that already
maintains a `KindClusterMember` record — `addServerLine` on join, `watchLeadership` on this node's
own leadership/suffrage transitions, `removeServerLine` on leave/kick): every current voter or
learner is a member of `cluster`, and of exactly one of `voter`/`learner` matching its current
suffrage (a raft leader counts as a voter for this purpose, since a leader is itself always one of
the voters); a peer that leaves or is kicked is removed from all three. Because membership is
daemon-derived, not an operator grant, `addpeertogroup`/`removepeerfromgroup` against `cluster`,
`voter`, or `learner` is rejected too (`shmevent.IsAutoManagedGroupID`) — there is no way to manually
add or remove a peer from these three; only actual raft membership changes them, and only ever to
the set of peers *currently* in the cluster (unlike `KindClusterMember` itself, whose own doc comment
notes nothing used to delete it — `removeServerLine` deletes both records together now).

`channel`, `relay`, `remote`, and `execute` are the deliberate exception: their `Group` records are
equally protected, but their *membership* remains an ordinary, operator-editable grant via the same
`addpeertogroup`/`removepeerfromgroup` already used for every non-reserved group — the mechanism
for letting a peer that isn't a cluster member (e.g. a short-lived tool on someone's laptop) open a
[Raw Channel](#raw-channel) to this cluster's peers, reserve/use this node's relay service, issue
generic `Set`/`Get`/etc. requests over `ClientProtocolID`, or send `EventExecute` notifications,
anyway. `handleChannelStream`'s incoming-channel gate, `relayACL`'s `AllowReserve`/`AllowConnect`,
`handleShmEvent`'s top-of-function gate, and `handleExecuteStream` (see
[Leader on a remote machine](#leader-on-a-remote-machine-over-ssh) above) all call the identical
`pkg/daemon.isAuthorizedForGatedAccess` check, just against their own respective group — `cluster`
plus `channel`/`relay`/`remote`/`execute` respectively — or a pairwise personal grant into the
gating node's own peer-id group either way. None of the four has a separate `Config` opt-out flag;
all four are always gated, unconditionally, on the sender belonging to one of those (`remote` also
has the narrow command-log carve-out described above and under
[Leader on a remote machine](#leader-on-a-remote-machine-over-ssh)).

### One-time execution invites

`submitcommand` above always runs against the *caller's own* node, dispatching on behalf of whichever
peer id that node already is. `shmevent.KindExecInvite` is a different mechanism for handing a
specific, one-time "run this command with these inputs" ticket to another peer out-of-band (e.g. a
Data Matrix barcode) — modeled closely on `KindJoinInvite` above, but for triggering an execution
instead of admitting a device:

```bash
mage createexecinvite c1 '{"qty":"20"}'                      # (on a current voter) prints a fresh tokenHex
kvctl-cli printexecinvitedatamatrix <sourceMultiaddr> <tokenHex> <outFile.png>   # barcode it
mage redeemexecinvite "<sourceMultiaddr>#<tokenHex>"          # the redeeming peer, elsewhere, scans & runs this
mage revokeexecinvite <tokenHex>                              # invalidate one before it's ever redeemed
```

`createexecinvite` binds `commandID`+`inputsJSON` to a random 16-byte token (only a current raft
voter may do this); `printexecinvitedatamatrix` barcodes the plain string
`"<sourceMultiaddr>#<tokenHex>"` (not a signed event — the token itself is the credential, same
reasoning as `printjoininvitedatamatrix`). `redeemexecinvite`, run on the *redeeming* peer's own
node, splits that string, then has its own daemon sign a small self-contained message with its own
key (`shmevent.EncodeExecuteNotification`, the same self-contained-signature recipe `EventExecute`
already uses — verified by whoever receives it against the *claimed* sender peer id's own extracted
pubkey, not against the connection that happened to carry it, so it verifies identically whether it
lands directly or after one internal forward to the real raft leader) and dials it straight at
`sourceMultiaddr`.

The receiving cluster's raft leader then runs `kvfsm.OpConsumeExecInvite`: in one atomic `Apply`, it
re-checks the redeeming peer's *real* `GroupCommand`/`PeerGroup` ACL standing against the invite's
commandID and only then deletes the invite record — so execution happens only if the peer is
authorized **and** the invite hasn't already been redeemed. This is strictly stronger than
`submitcommand`'s own `isPermittedForCommand` check above (documented there as evaluated
client-side, "only as strong as every caller actually going through `SubmitCommand`"): here the ACL
check is raft-authoritative, since the caller is a genuinely untrusted remote peer rather than a
locally-driven client. An unauthorized redemption attempt is rejected *without* consuming the
invite, so a legitimate peer can still redeem it afterward; only a successful, permitted redemption
burns the ticket. On success, `redeemexecinvite` prints a new instance id — track it with
`getcommandrequest`/`querycommandlog`/`latestcommandlog` against the target's own node, same as any
other `submitcommand` dispatch.

## Raw Channel

`EventExecute` above is a one-shot, fire-and-forget notification. `EventChannelOpen` (and its
`Send`/`Poll`/`Listen`/`Close`/`CloseWrite`/`DataReady` counterparts, `pkg/daemon.ChannelProtocolID`)
is its persistent-session sibling: an unreplicated, bidirectional, **multipurpose** stream directly
to another peer's process — usable for a plain data transfer, a control link, a video stream, or
any mix of those interleaved on the same session, each chunk tagged with a purpose
(`shmevent.ChannelPurposeData`/`ChannelPurposeControl`/`ChannelPurposeVideo`, or any other byte a
caller chooses — the set is open-ended). Every message the channel carries over the network, in
both directions, including every data chunk after the initial handshake, is a real signed frame —
not raw unframed bytes the way earlier versions of this feature worked — so a chunk is
authenticated per-message, not just once at handshake time. Traffic on it never touches raft or
the store, exactly like Execute's.

```bash
mage listenchannel <purpose>                    # (on the receiving peer) blocks until a channel arrives
mage openchannel <peerID> <purpose>             # (on the opening peer) dials in
```

`<purpose>` may be `""` for the default data purpose, or `"control"`/`"video"`/a plain decimal
number for anything else (`shmevent.ChannelPurposeName`/`ChannelPurposeFromName`) — it tags every
chunk *this* process sends; the purpose of what it receives is whatever the remote peer tagged it
with, per chunk. Both commands pump the current process's own stdin/stdout through the channel
once one is open: `openchannel`/`listenchannel` are effectively `nc`/`socat` for this cluster's
own transport — everything piped into stdin on one side comes out stdout on the other, and vice
versa, until stdin reaches EOF, the remote side closes the channel, or the process gets
SIGINT/SIGTERM. For example, to send a file from one peer to another:

```bash
mage listenchannel "" > received.bin              # receiver
mage openchannel <receiverPeerID> "" < send.bin    # sender
```

The two directions are independent: reaching stdin EOF only half-closes this side's *outgoing*
direction (`EventChannelCloseWrite`, mirroring a TCP `shutdown(SHUT_WR)`) so that whatever the
remote peer still has in flight the other way is never cut short — `pkg/kvctl.pumpChannel` waits
for both directions to finish (or an explicit signal) before sending `EventChannelClose` to end
the session outright.

### Data plane: `pkg/chandata`

The commands, purpose tagging, half-close semantics and `kvmobile` bindings above are the whole
user-facing surface, and none of it changed to get here — what moved is *how* a chunk actually gets
from a local caller's `SendChannel`/`PollChannel` call onto (and off) the wire. `EventChannelOpen`/
`Listen`/`Close`/`CloseWrite` remain ordinary `pkg/shmevent` capnp `Msg` round trips over `pkg/ipc`,
same as every other control operation in this document — opening/claiming/ending a session is rare
and latency-insensitive, exactly what that request/response transport is for. Bulk chunk traffic is
not: `pkg/ipc.Call` pays a real cost per round trip (on desktop, a fresh named `shmring` segment
pair — a real `shm_open`+`mmap` and later `shm_unlink`+`munmap` — plus a capnp encode/CRC/sign, all
to move one chunk capped at `shmevent.ChannelValueSize`, 16KB), and since each call only returns
once the daemon has fully consumed it, a sender's stdin-reading loop could never get more than one
chunk ahead of the network write it was waiting on — throughput was bounded by round-trip latency
divided into chunk size, not by the libp2p connection's real bandwidth.

`pkg/chandata` replaces that per-chunk round trip with a pair of long-lived
[`github.com/gofsd/shmring`](https://github.com/gofsd/shmring) ring buffers set up once per channel
and drained continuously for its whole lifetime: an *upload* ring (`chandata.DirUp`) the local
caller writes into and the daemon drains onto the libp2p stream, and a *download* ring
(`chandata.DirDown`) the daemon writes into as it reads the stream and the local caller drains.
`Writer.TryWrite`/`Reader.TryRead` are non-blocking memory operations bridged by a poll-with-backoff
loop (there's no cross-process wakeup primitive to block on — see `shmring`'s own doc comment), so a
producer can run arbitrarily far ahead of its consumer, bounded only by `chandata.Capacity` (1MiB) —
exactly the pipelining a bulk transfer needs to actually saturate the connection, instead of the old
design's inherent stop-and-wait. A ring is a raw byte stream with no message boundaries of its own,
so each `WriteChunk`/`ReadChunk` call frames its chunk with a small purpose+length header — this
framing is local-only (never sent over the network, no CRC/signature of its own), safe because it
never crosses the same-machine trust boundary `pkg/shmevent`'s own doc comment describes.

Wiring a channel's rings up is `shmevent.EventChannelDataReady`: sent once by
`pkg/shmclient.Session.OpenChannel`/`ListenChannel` immediately after each obtains a channelID,
*after* that session has already created its own upload ring and opened the daemon's download ring
(both by then guaranteed to exist — the daemon always creates the download ring synchronously,
before `EventChannelOpen`/`Listen`'s own response ever reaches a caller with a channelID to open it
by, so there is nothing to race there). The daemon's own handler
(`(*Node).dispatchChannelDataReady`) opens that same upload ring as a reader and starts
`pumpChannelUpload`, a goroutine that drains it and forwards each chunk over the wire through the
exact same signed-frame path (`channelSession.write`) `EventChannelSend`'s legacy per-chunk path
still uses — sending is deliberately not an either/or: a raw caller that never sends
`EventChannelDataReady` stays on the plain `EventChannelSend`/`EventChannelPoll` IPC path exactly as
before (still exercised end to end by `pkg/daemon`'s own `channel_test.go`), while `pkg/shmclient`
(and so every `mage openchannel`/`listenchannel`/`kvmobile` caller) always uses the ring. On the
receive side, `pumpChannelReads` delivers every verified chunk both ways unconditionally — into the
legacy in-memory inbox *and* into the download ring — so neither path needs to know which one a
caller is actually using.

Because a chunk's real length is no longer ambiguous the way a capnp `Msg.Value` zero-padded to a
fixed per-event-type width is (see `ValueSize`/`ChannelValueSize` above), the wire frame itself
moved off that scheme too: `shmevent.SignChannelChunk`/`VerifyChannelChunk`/`EncodeChannelFrame`/
`DecodeChannelFrame` sign purpose+chunk at their actual, variable length instead of a fixed
ceiling — raising `ChannelValueSize` itself to fit a much bigger chunk would have forced *every*
frame, including a tiny control ping, to pay CRC32/Ed25519 cost over however big that new ceiling
was, defeating the point. `channelMaxChunkSize` (`pkg/daemon`, equal to `chandata.MaxChunkSize`,
256KB) is this frame's own independent ceiling instead — bigger chunks mean fewer, larger signed
frames per byte transferred, which matters far more for throughput than per-chunk latency does for
a bulk transfer, since signing and the network write syscall both have a mostly-fixed per-frame
cost. This is a wire-incompatible change from the previous `shmevent.Encode`/`Msg`-based per-message
framing, so `ChannelProtocolID` bumped to `3.0.0` (see that constant's own doc comment for the full
version history) — an old peer expecting the fixed-width scheme and a new peer speaking the
variable-length one must never silently negotiate a stream together.

One behavioral consequence worth calling out: because the download ring has a real, bounded
capacity and `pumpChannelReads` blocks (briefly, `downRingWriteTimeout`) writing into it, a local
caller that stops draining `PollChannel` for a sustained period now applies genuine backpressure
all the way back through libp2p's own flow control to the sending peer, rather than the old
design's silent oldest-entry eviction once its in-memory inbox filled up — a slow receiver now
slows the sender down instead of silently losing data. `EventChannelCloseWrite`'s guarantee is
preserved across this rewrite too, just relocated: `Session.CloseChannelWrite` closes the local
upload ring writer (visible to the daemon's `pumpChannelUpload` across the shared memory
immediately) and only *then* sends `EventChannelCloseWrite`, whose handler now deliberately blocks
until `pumpChannelUpload` reports every already-buffered chunk has actually been forwarded before
half-closing the underlying stream — so a caller that sent its last chunk and immediately called
`CloseChannelWrite`, with no chunk yet drained by the daemon, still gets the same "this call
returning means everything I sent is genuinely on the wire" guarantee the old fully-synchronous
per-chunk design had for free.

Like Execute, a channel is local-only to operate (`EventChannelOpen/Send/Poll/Listen/Close/
CloseWrite/DataReady` all reject a remote/`ClientProtocolID` caller — only this node's own operator
drives its own sessions) but gated on the *receiving* side, unconditionally, by the sender belonging
to the reserved `cluster` or `channel` group (see
[Reserved cluster/voter/learner/channel/relay/remote/execute groups](#reserved-clustervoterlearnerchannelrelayremoteexecute-groups)
above): `handleChannelStream` only accepts an incoming channel from a current raft voter/learner
(`cluster` group membership) or a peer an operator has explicitly granted `channel` group membership
(`mage addpeertogroup <peerID> channel`) — no off switch, no permit record, same as every other
gate in this package now; a channel is always authorization-checked. Every chunk in either
direction is also metered against this node's
`-quota-channel-*` flags (see "Leader on a remote machine" above) — a peer/IP over its configured
byte-rate budget has the session closed outright, independent of the group-ACL check. An idle
established channel and an accepted-but-unclaimed incoming one are both reaped opportunistically
after a timeout (`channelIdleTimeout`/`channelPendingTimeout` in `pkg/daemon`) rather than by a
dedicated background goroutine, the same "simplest thing that could work" reasoning
`executeInbox` already uses; reaping cancels the session's own context, which promptly unblocks
`pumpChannelUpload`/any in-flight ring wait rather than leaving it stuck until its own timeout.

`kvmobile`'s `OpenChannel(peerID, cb)`/`ListenChannel(cb)`/`StopListenChannel()`/
`SendChannelData(channelID, purposeName, chunk)`/`CloseChannel(channelID)`/
`StopChannel(channelID)` are the Android port — see the "`kvmobile`" section below — and, like
`pkg/kvctl`'s desktop commands, needed no *existing* method's signature changed to move onto the
ring: both sit on top of the same `pkg/shmclient.Session` methods, so `pkg/chandata`'s Android
backend
(`chandata_android.go`, `ASharedMemory`-backed with an in-process fd registry — mirroring
`pkg/ipc/ipc_android.go`'s own reasoning for why Android needs a different rendezvous than desktop's
named `CreateShm`/`OpenShm`, since ASharedMemory has no OS-level naming, only fd handoff, which only
works within one process) is what changes, invisibly, underneath them. One new method,
`CloseChannelWrite(channelID)`, *was* added by that same rewrite — see the equivalent desktop
paragraph above for why: a bulk sender needs some way to ask "has everything I sent actually
reached the wire yet," a question that only exists because the ring made `SendChannelData`
asynchronous in the first place. Separately, `chunk`/`OnData`'s own callback argument moved from
base64 text to raw `[]byte`/`ByteArray` — see `ChannelCallback.OnData`'s own doc comment in
`channel.go` for why that was a correctness-neutral throughput fix, not something the ring rewrite
itself required; `purposeName` is the same string convention `mage
openchannel`/`listenchannel` uses (`""`/`"control"`/`"video"`/a decimal number).
`ChannelCallback.OnData(purpose, chunk)`/`OnClosed(reason)` is the reverse-binding interface Kotlin
implements to receive incoming data, mirroring `ExecuteCallback`'s shape — `purpose` is the sender's
tag as a string (`shmevent.ChannelPurposeName`). Because `SendChannelData`/`PollChannel`-driving
code/`CloseChannel` may be called from different Kotlin threads concurrently (unlike `pkg/kvctl`'s
own single-threaded pump loops), `pkg/shmclient`'s per-channel ring handles are synchronized against
a concurrent close (cancel-then-wait, not a plain mutex, so a `CloseChannel` never has to wait out
some other blocked call's own full timeout) rather than assuming one goroutine drives a channel
end to end.

## Vendored dependency patch

`thirdparty/anet` is a local, patched copy of `github.com/wlynxg/anet` (pinned via a `replace`
directive in `go.mod`). Upstream's Android network-interface code links (`//go:linkname`)
against `net.zoneCache`, a private stdlib symbol; its layout no longer matches Go 1.25's `net`
package, and there is no newer upstream release fixing it. The patch drops the link and keeps the
cache local-only — harmless here, since libp2p only calls `Interfaces()`/`InterfaceAddrs()` for
listing, never anything relying on the standard library's own zone cache being warmed as a side
effect.

`thirdparty/libc` is a local, trimmed and patched copy of `modernc.org/libc` v1.73.4 (pinned via a
`replace` directive in `go.mod`; the pinned version must match what `modernc.org/sqlite`'s own
`go.mod` expects, per that module's own compatibility note) -- the pure-Go runtime that
`modernc.org/sqlite`'s ccgo-transpiled C code runs on. Its musl-derived `_fstatat_kstat` fast path
(in `ccgo_linux_{amd64,arm64,arm,386}.go`) opportunistically issues the raw legacy
`stat`/`lstat`/`fstat` syscalls for absolute paths/`AT_FDCWD`, exactly like real musl does on real
x86_64/32-bit Linux -- but Android's seccomp-bpf filter blocks those legacy syscalls on 64-bit,
which crashes the process with `SIGSYS` the instant SQLite opens its database file. Confirmed
2026-07-23 running the Android app on this machine's only available AVD, an x86_64 image (a real
arm64 phone doesn't hit this at all -- arm64 never had those legacy syscalls in the first place, so
musl's own arm64 `_fstatat_kstat` never had the risky fast-path branches to begin with). The patch
makes all four architectures' `_fstatat_kstat` unconditionally take the safe `newfstatat`/
`fstatat64` path instead -- the same universally-available syscall Go's own standard library
already uses for `Stat`/`Lstat` on every architecture, so this is a no-behavior-change fix on real
Linux too, not an Android-only special case. The vendored copy is trimmed relative to upstream
(dropped `testdata/`, its own `_test.go` files, and the generated per-arch files for
architectures this repo never targets: `s390x`/`riscv64`/`ppc64le`/`loong64`/`mips*`, plus the
hand-written `darwin`/`freebsd`/`netbsd`/`openbsd`/`illumos` ports) to keep the checked-in size
down; this is still one of the larger things in `thirdparty/` because the musl-to-Go transpilation
it ships is inherently large, not because of anything specific to this patch. The `windows` port
(`libc_windows*.go`/`capi_windows_{386,amd64,arm64}.go`/`musl_windows_{386,amd64,arm64}.go`) was
restored from the same upstream v1.73.4 unmodified -- `mage buildwindows` needs it, and the
`_fstatat_kstat` patch above is Linux/Android-only (musl's `fstatat`/`stat`/`lstat` fast path has no
Windows equivalent), so there's nothing to re-apply to it. Compiles clean
(`go build ./cmd/kvnode ./cmd/kvctl-cli ./cmd/kvhttp ./cmd/kvrecover` under `GOOS=windows
GOARCH=amd64 CGO_ENABLED=1 CC=x86_64-w64-mingw32-gcc`) but hasn't been run on a real Windows
machine -- there wasn't one available to validate against, so treat the resulting `.exe`s as
untested until someone runs them for real.

## Testing

```bash
mage test          # unit tests
mage integration    # integration tests
mage testall        # all of the above, plus every e2e:all row (see below)
```

### End-to-end tests / deploy pipeline

`test/e2e/testdata.json` is the single source of truth for the e2e suite, and is meant to be read
by a human, not just tooling: a version history stamped with this repo's own semver (one shared
version across every platform, from the same git tags `mage patch`/`minor`/`major` manage),
deterministic Ed25519 identities per platform (desktop/android/web/remote), and a recorded log of
test rows -- each one raw `pkg/shmevent.Msg` sent to a node, printed with a human-readable event
name and a plain-text value rather than the wire bytes (see `pkg/e2edata.Event`'s doc comment for
exactly how, without changing the underlying capnp structure at all) -- with the last run's
pass/fail status and error message. See `pkg/e2edata` for the file format and `pkg/e2erun` for what
running a row actually does per platform: a real locally-spawned `kvnode` for desktop, the SSH
bootstrap leader itself for remote, a real Playwright-driven browser check for web, and for android
a real `gomobile bind` (baking that row's node identity and the live bootstrap address into the
AAR, via the same `kvmobile.SendEvent` raw-event entry point kvctl-cli's `sendevent` exposes on
desktop/remote) + `./gradlew installDebug installDebugAndroidTest` + `adb shell am instrument`
against whatever device/emulator is connected, pulling back a real per-row results file (see
`android-app/app/src/androidTest/.../E2ETest.kt`), then a second, separate `adb shell am instrument`
invocation of `UiCommandE2ETest.kt` on the same install -- unlike `E2ETest`, which drives
`Kvmobile.sendEvent` directly and never touches a single screen, this one clicks through the real
app (`MainActivity`'s category list -> `CommandListActivity`'s command list ->
`CommandDetailActivity`'s dynamically-rendered param fields and Run button) for literally every
`CommandCatalog.kt` entry, so the command surface exposed through the UI can never silently drift
out of coverage. A command failure here fails every row in that node's batch, the same as a
build/install failure would, since none of them can be trusted to have run against a working app --
degrading to a clear Skipped status if `gomobile`/`adb`/a connected device aren't available at all,
and a clear Failed status with the real diagnostic (not just "exit status 1") if the build/install/
instrument step itself fails --
e.g. the exact `INSTALL_FAILED_USER_RESTRICTED` MIUI/Xiaomi restriction noted under [Follower on
Android](#follower-on-android) blocks the *instrumented test* APK's install the same way it can
block a plain `adb install`, needing that same device-side "Install via USB" toggle enabled before
`e2e:current`/`e2e:all` can drive that node for real.

```bash
mage e2e:newversion                                                     # stamp a new version with the current semver
mage e2e:addnode desktop                                                # generate a deterministic identity
mage e2e:addtest <nodeID> <eventName> <id> <sourceID> <destID> <value>  # record a row against it
mage e2e:bootstrap                                                      # deploy/confirm the shared leader (SSH)
mage e2e:bootstrapall                                                   # start the leader, plus every desktop node -- no test rows run
mage e2e:current                                                        # run only rows newer than the last published version
mage e2e:all                                                            # run every recorded row
mage e2e:deletenode <nodeID>                                            # tear down a node's real process/data and remove it
mage e2e:destroyall                                                     # tear down every node at once
```

`eventName` is one of `set_key`, `set_field`, `get_key`, `get_field`, `get_public_key`,
`get_private_key`, `add` (see `pkg/shmevent.EventName`). Deployed nodes are never torn down
automatically -- by `e2e:current`, `e2e:all`, or anything else -- specifically so a human can poke
at them after a run; `e2e:deletenode`/`e2e:destroyall` are the explicit, deliberate commands for
when a node (or every node) is no longer wanted. `e2e:destroyall` tears every node down the same
real way `e2e:deletenode` does (one at a time, continuing past any single node's failure rather than
stopping), then saves the file -- so partial teardown from a failure partway through still sticks
for whichever nodes it did reach.

An `add` row (a raft join) is inherently a one-time operation, same as `mage addnode` itself: once a
node has actually joined, re-running `e2e:all` sends that same join again to an already-voting
member, which `pkg/daemon.handleAdd` correctly rejects ("leader rejected join: ERR: not leader" --
the join target no longer being who to ask). That's an expected re-run artifact, not a pipeline bug
-- a genuinely clean pass needs either a fresh node (`e2e:deletenode` first) or accepting that row
as the one exception on a repeat `e2e:all`. It doesn't affect `e2e:current`/the push gate, since that
only re-runs rows newer than `published_version`.

`mage e2e:current` is what runs before every push once installed:

```bash
mage githooks:install   # one-time: points core.hooksPath at scripts/git-hooks/pre-push
```

The shared bootstrap/leader node these tests join against is deployed over SSH to a single,
already-provisioned VPS, into its own isolated directory/port (`pkg/e2erun.BootstrapRemoteDir`,
distinct from any other node manually running on that same host) -- `mage e2e:bootstrap` (or the
first `e2e:current`/`e2e:all` run) is idempotent: it deploys and starts it only if not already up,
and otherwise just confirms it's reachable.
