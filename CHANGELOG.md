# Changelog

All notable changes to this project are documented here. Format loosely
follows [Keep a Changelog](https://keepachangelog.com/); this project
doesn't yet follow strict semver pre-1.0 (see `README.md`/`CLAUDE.md` for
current architecture, which is the authoritative reference -- this file
tracks *changes*, not a full feature description).

## [Unreleased]

### Added
- **`mage backup <peerID> <destArchive>` / `mage restore <archive> <destDir>`**, a `tar.gz` of a
  stopped node's entire data directory (identity key, sqlite store, raft log/snapshots) and its
  exact inverse (`pkg/kvctl/backup.go`). `restore` never touches the registry — extracting into the
  same deterministic path `registry.NodeDataDir`/`ClusterDataDir` would compute for a peer id is
  what lets a plain `mage resumenode` afterward pick the data back up as that node again. See
  README's new "Backup and restore" section for the full runbook, including the live-voter and
  whole-cluster-quorum-loss (`kvrecover`) cases this doesn't cover.
- **`mage e2e:gc <keepVersions> ""|yes`**, a dry-run-by-default prune for desktop/android/web e2e
  nodes no row from the last N versions still references (never the shared `PlatformRemote`
  bootstrap leader) — `mage e2e:deletenode`/`destroyall` already existed but left "which nodes are
  actually stale" entirely to a human reading rows by hand.
- **CI `check-e2e-gate` job** (`scripts/ci/check-e2e-gate.sh`): fails a PR that touches e2e-relevant
  paths (wire protocol, daemon, transport/relay, client bridges) without also touching
  `test/e2e/testdata.json` — a credential-free backstop for the real `mage e2e:current` gate being
  local-only (`scripts/git-hooks/pre-push`) and therefore skippable if never installed or bypassed
  with `SKIP_E2E=1`. Escape hatch: `SKIP_E2E_CHECK` in the PR title or HEAD commit message.

- **Compare-and-swap**, as preconditions inside the existing atomic
  transaction rather than a separate mechanism: `shmevent.TxnOpCompare`
  ("key holds exactly this") and `TxnOpCompareAbsent` ("key does not
  exist"), the `if:<key>=<value>` / `ifabsent:<key>` ops in
  `ParseTxnOpsString`'s shared grammar, `shmclient.CompareAndSwap`,
  `kvmobile.CompareAndSwap`/`CompareAndSwapAbsent`, `mage cas` /
  `mage casabsent`. Every precondition is evaluated in `kvfsm.Apply`
  *before any write in the same transaction lands*, so a failed compare
  leaves the store exactly as it was -- and because raft has already
  serialized the log entry, there is no window between the read and the
  write, which no amount of client-side care could close for a
  `Get`-then-`Set`. A failed compare crosses the IPC boundary as
  `shmevent.CompareFailedMarker` and comes back as a typed result
  (`shmclient.ErrCompareFailed`, `CompareAndSwap`'s plain `false`), since
  losing a race is an expected outcome to retry, not an error to report.
- **`shmevent.KVValueSize` (4KB)**, a third value ceiling between
  `ValueSize` (512B) and `ChannelValueSize` (16KB), applied to the
  plain-KV data events plus `EventCommandPut`/`EventStationPut` -- the
  events whose `Value` is caller-authored data rather than fields this
  project defines. 512 bytes had become the binding constraint on anything
  layered over the KV store, since a `Set` replaces a value outright. The
  Android IPC ring (`pkg/ipc/ipc_android.go`) grew 4096 -> 16384 to carry
  it.
- **A `Command` record's `spec`** -- an opaque, never-parsed form
  definition replicated with the command, so a cluster gains a new command
  without any device gaining new code (`mage createcommandspec`,
  `kvmobile.CreateCommandWithSpec`, `shmclient.PutCommandWithSpec`).
  Carried in a versioned payload marked by an impossible v1 name length
  (`0xFFFF`); a command with no spec is still written as v1 byte for byte,
  so existing records and readers that predate the field -- including
  `web-app`'s Rust decoder -- are untouched. A spec-less `Put` preserves an
  existing spec (`kvfsm.preserveCommandSpec`) instead of clearing it, so a
  rename or a startup re-registration can't silently delete a form
  definition; `mage clearcommandspec` removes one deliberately.
- **`shmevent.KindStation`** (`0x0C`) plus `EventStationPut`/
  `EventStationDelete` (51/52), `mage createstation`/`updatestation`/
  `deletestation`/`getstation`/`liststations`, `kvctl.PutStation` and
  `kvmobile.PutStation`/`ListStations`: a device's operational description
  (name + opaque attributes) keyed by its peer id, so records that name a
  device by a 52-character peer id can be shown as something readable.
  Voter-gated like the rest of the catalog, so a device cannot name
  itself; deliberately *not* cluster membership, so a device can be
  described before it joins and keep its description after it leaves.

### Changed
- **A device may now be the target of many commands.** `kvfsm` no longer
  rejects a second `Command` naming a peer that another command already
  targets, and `peer_id` may be empty for a command not yet bound to a
  station. The old rule capped a cluster's command list at its device
  count, and nothing depended on it -- every consumer looks up command id
  -> target peer, never the reverse. `SubmitCommand` now rejects an
  unbound command by name, which is where a missing target genuinely
  matters.

### Added (earlier in this release)
- `shmevent.EventPublicAccess` (byte 47) plus `pkg/daemon`'s
  `dialAndSubmitPublicAccess`, `shmclient.Session.PublicAccess`,
  `kvctl.RequestPublicAccess`, `kvmobile.RequestPublicAccess`/
  `RequestRelayAccess`, `mage requestpublicaccess <targetAddr> [note]`, and
  `kvctl-cli requestpublicaccess` -- the client side of
  `shmevent.DefaultPublicCommandID`'s self-service escalation, which until
  now existed only server-side. A node can now submit the always-public
  `public-access` command to a cluster it has *no standing in at all*, over
  that cluster's `ClientProtocolID`, and be granted real
  `ReservedGroupChannel`/`ReservedGroupRelay` membership there
  (`kvfsm.grantChannelRelayAccess`) in one raft-committed write. This is
  what makes a relay usable by a device that isn't a member of the relay's
  own cluster: relay admission is unconditionally group-gated
  (`relayACL`, no opt-out by design), so before this the only way in was an
  operator running `mage addpeertogroup <peerID> relay` by hand, once per
  device. Local-only, for the same reason `EventExecInviteRedeem`/
  `EventRecruit` are: it spends the receiving node's own identity.
- `mage enablepublicaccess` / `kvctl-cli enablepublicaccess`
  (`kvctl.EnablePublicAccess`) -- seeds the `public` Group,
  `public-access` Command and their link on an *existing* cluster.
  `pkg/daemon.ensureDefaultPublicCommand` only ever runs at first cluster
  bootstrap, and deliberately not on later leadership transitions (they're
  ordinary mutable records; an operator who closed self-service enrollment
  must not have that undone by an election), so a cluster bootstrapped by
  an older build never gains them and refuses every request with `is not
  permitted to submit command public-access`. Idempotent; also the way to
  re-open enrollment on a cluster where it was closed deliberately.

### Security
- Removed the opt-in permit-based ACL for the generic remote
  (`ClientProtocolID`) RPC surface, `EventExecute` delivery, and per-log-kind
  access (`Config.RequirePermitForRemote`/`RequirePermitForExecute`/
  `RequirePermitForLog`, `KindPermitPeer`, `KindLogPermit`, `mage
  requestlogpermit`/`confirmlogpermit`/`revokelogpermit`, the `"peer"` kind
  on `requestpermit`/`confirmpermit`/`revokepermit`) -- all three previously
  defaulted to *open* (any signed caller allowed) unless an operator
  explicitly turned them on. Replaced with the same unconditional group-ACL
  mechanism Channel/relay already used: two new reserved groups, `remote`
  and `execute` (`shmevent.ReservedGroupRemote`/`ReservedGroupExecute`),
  gate the generic RPC surface and `EventExecute` respectively, with no
  opt-out -- a current raft cluster member always passes, anyone else needs
  an explicit `mage addpeertogroup <peerID> remote`/`execute` grant (or a
  pairwise personal-group grant). `KindBootstrapNode`'s own request/
  confirm/revoke lifecycle (`mage addrelaynode`/etc.) is unaffected -- it
  never used `KindPermitPeer`.
- The one deliberate exception for a peer with no other standing at all:
  `pkg/daemon.isCommandLogCarveOut` lets any peer submit a command linked
  to a *public* `Group` (`SubmitCommand`, still raft-authoritatively
  enforced by `kvfsm.OpAppendCommandRequest`'s `IsPermittedForCommand`) and
  read back that exact dispatch's own `CommandRequest`/execution-index/
  execution-log records -- nothing else in the log namespace or generic RPC
  surface is reachable this way. See README's "Log access control"/
  "Reserved cluster/voter/learner/channel/relay/remote/execute groups"
  sections.
- A new per-instance local-IPC token (`pkg/ipc/token.go`,
  `<dataDir>/ipc.token`, `0600`) closes a real gap in the desktop shmring
  transport: request/response segment names used to be derived from a
  node's public peer id alone, and shmring's POSIX backend grants owner
  **and group** read/write on its shared-memory segments -- any co-resident
  process in the same user or group could attach to a node's channel just
  by knowing its peer id. The token is now folded directly into the
  segment name itself, so a process with no read access to `ipc.token`
  can't even construct the right name to attach. See README's new "Local
  IPC token" section.
- Per-peer/per-IP quota (`pkg/daemon/quota.go`, a token-bucket rate
  limiter) now gates Channel byte throughput and relay-service reservation/
  connect events, independent of the group-ACL check above -- both gates
  must pass. `-quota-channel-*`/`-quota-relay-*` flags configure it; left
  at 0, each now substitutes a real non-zero default (`DefaultQuotaChannel*`/
  `DefaultQuotaRelay*`) instead of silently disabling enforcement,
  mirroring how `RelayLimits` already substitutes defaults for
  `-relay-max-*`. Channel defaults are deliberately generous (sized well
  above this project's own ~300+ MiB/s desktop channel-throughput
  benchmark) since bulk transfer is a first-class use case; relay defaults
  are tight, since a reservation event is inherently rare regardless of
  workload.

### Fixed
- A `Join` through a relay circuit (`connect to leader ...: error opening relay circuit:
  NO_RESERVATION (204)`) could fail outright even though the leader was genuinely reachable,
  reproduced twice pairing two Android devices over the deployed relay. `NO_RESERVATION` means
  the relay had no active reservation on file for the leader at the moment of the dial -- which
  happens whenever the leader's own AutoRelay is still re-establishing its reservation (observed
  taking 24-46s against the same relay, in the same run, via the equivalent `RequestRelayAccess`
  step), not evidence the leader is actually unreachable. `connectWithRetry`'s existing budget
  (`connectRetryAttempts`/`connectRetryDelay`, ~1.5s total) is deliberately short -- sized for
  ordinary packet loss, not a real in-progress reconnect on the *other* end -- so it gave up long
  before that reservation could land. `connectWithRetry` now falls back to a dedicated, much
  longer retry budget (`connectRetryReservationAttempts`/`connectRetryReservationDelay`, 90s --
  matching `recruitJoinTimeout`, this codebase's existing budget for the identical class of wait)
  specifically when every attempt in the short budget fails with `NO_RESERVATION`
  (`isNoReservationError`), leaving every other dial-failure case's fast-fail behavior unchanged.
  A first pass at ~48s fixed the reported `Join` failure outright (previously reproduced 3/3
  times) but lost the same race once more at a sibling call site (`dialAndPushRecruit`) in the
  very next verification run -- 24-46s was the observed *range*, not a ceiling, so a 90s budget
  replaced it. Covered by `TestIsNoReservationErrorMatchesRelayStatusText` in
  `pkg/daemon/nat_edge_cases_test.go`.

- A dead first relay candidate stalled failover to a working second one for
  a full **3 minutes**. `newHost` wires `Config.RelayPeers`/confirmed
  `KindBootstrapNode` records through `libp2p.EnableAutoRelayWithStaticRelays`,
  which sets go-libp2p autorelay's `minCandidates` to the length of that
  list -- but a candidate only counts toward it once it actually answers a
  live connect-and-probe (`relay_finder.go`'s `handleNewNode`/`tryNode`), so
  an unreachable entry (exactly the case a multi-candidate list exists to
  tolerate) never clears that bar, and the real candidate count permanently
  fell short of `minCandidates`. AutoRelay's response to that shortfall is
  to wait out `bootDelay` -- 3 minutes by default, never overridden here --
  before trying the reservations it already has anyway, so a node with one
  dead relay and one perfectly good one got zero relay connectivity for
  those 3 minutes on every startup. Reproduced directly (measured exactly
  3m0s) while writing `pkg/daemon/nat_edge_cases_test.go`'s
  `TestRelayFailoverToSecondCandidateWhenFirstIsDown`; fixed by adding
  `autorelay.WithBootDelay(relayReserveBackoff)` alongside the existing
  backoff/interval overrides, matching the ~10s cadence those already use.
  That same test file also adds direct coverage for `forgetTransientPeer`'s
  relay-candidate exemption, `clearDialBackoff`'s relay-hop clearing,
  `hasPublicAddr`, and `dialAndRedeemExecInvite` over an actual
  `/p2p-circuit` hop -- none of which had a test anywhere in the package
  before.

### Added
- A default bootstrapped public `Group`/`Command` pair
  (`shmevent.DefaultPublicGroupID`/`DefaultPublicCommandID`,
  `pkg/daemon.ensureDefaultPublicCommand`, created once at cluster
  bootstrap): the concrete self-service escalation path for a peer with no
  standing at all -- submitting this specific command (already reachable
  via the public-command carve-out above) atomically also grants the
  submitting peer real Channel and relay access, in the same
  raft-committed write (`kvfsm`'s new `grantChannelRelayAccess`, special-
  cased inside `OpAppendCommandRequest`). Not reserved/protected like the
  seven groups above -- an operator can `deletegroup`/`updategroup
  public=false` it to close open enrollment, the same way they control
  every other ACL decision in this catalog.

### Removed
- `KindPermitPeer`/`KindLogPermit` and everything built only for them:
  `EventLogPermitRequest`/`Confirm`/`Revoke`, `shmevent.LogPermitKey`,
  `EncodePermitPeerPayload`/`DecodePermitPeerPayload`, `mage
  requestlogpermit`/`confirmlogpermit`/`revokelogpermit`, and the `"peer"`
  kind on `requestpermit`/`confirmpermit`/`revokepermit`/
  `kvmobile.RequestPermit` et al. `EventPermitRequest`/`Confirm`/`Revoke`
  themselves are unaffected -- `KindBootstrapNode` still uses that same
  generic lifecycle under kind `"bootstrap"`.

### Fixed
- `pkg/e2erun/android.go`'s `runUICommandTest` (the harness behind `mage
  e2e:all`'s "android UI command test" check, walking all 73 live
  `CommandCatalog.kt` entries) was producing false "PASS" results: its
  `-e cases <json>` argument to `adb shell am instrument` reliably broke
  `am`'s own argument parsing the instant the JSON contained quotes/braces
  (confirmed by hand -- reproducible with even a single-entry cases
  object; `am instrument` fails outright with "Error: Invalid userId -2"
  before `UiCommandE2ETest` ever runs), and the harness ignored that
  failure (`_ = cmd.Run()`) and never cleared the device's previous
  results file first -- so a broken run silently read stale data left
  over from an earlier run and reported it as a fresh pass. Fixed both
  sides: `casesJSON` is now base64-encoded before being passed as the
  instrumentation argument (`UiCommandE2ETest.kt`'s `buildCasesFromArg`
  decodes it), sidestepping the argument-parsing problem entirely instead
  of trying to out-escape it; and the on-device results file is deleted
  before each run, so a future regression in this path fails loudly (a
  missing results file) instead of silently reading stale data.

### Removed
- `cmd/magefile.go`'s `Relay` demo target and `pkg/raft.StartRelay`, the only
  code anywhere in this module that used `github.com/libp2p/go-libp2p-kad-dht`
  -- which carries an unfixed vulnerability (GO-2024-3218, no patched version
  exists). `go mod tidy` drops the dependency entirely now that nothing
  references it. The rest of `pkg/raft`'s demo/test-helper surface
  (`StartRelayNode`/`NewP2PNode`/`LoadOrGenerateKey`, still used by
  `pkg/daemon`'s own test suite, and `cmd/magefile.go`'s other demo targets)
  is unaffected -- this was surgical, not a removal of the whole package.

### Added
- `LICENSE` (Apache-2.0), `NOTICE`, `CONTRIBUTING.md`, `SECURITY.md` --
  first steps toward a publishable, production-ready release.
- `mage buildLinux`/`buildWindows` now actually cross-compile
  `kvnode`/`kvctl-cli`/`kvhttp`/`kvrecover` into `dist/<goos>_<goarch>/`
  (previously no-op stubs -- `mage build` silently built nothing). Windows
  requires restoring `thirdparty/libc`'s `windows/*` files from upstream
  `modernc.org/libc` v1.73.4 (trimmed away along with the other
  non-Linux/Android ports; unrelated to the `_fstatat_kstat` patch, which
  is Linux/Android-only) -- see README's "Vendored dependency patch"
  section. Windows binaries are cross-compiled and compile clean but
  haven't been run on a real Windows machine yet.
- A `release.yml` GitHub Actions workflow: on any `v*` tag push, runs
  `mage buildLinux`/`buildWindows` and publishes the resulting binaries as
  GitHub Release assets. `ci.yml` also gained a `release-build-smoke` job
  that runs both on every push/PR, so a regression in either (like the
  stub/Windows gaps above) shows up in CI instead of silently again.
- A `## Stability` section in `docs/library-usage.md` stating what semver
  covers starting at `v1.0.0` (`pkg/shmclient`, `pkg/daemon.Config`/`Run`,
  `pkg/kvctl`, `api/shmevent.capnp`'s wire format) and what it doesn't
  (everything else, plus CLI flags/output).
- `SECURITY.md`'s "Supported versions" section now states a real policy
  (latest minor line only, no LTS branch yet) instead of a placeholder,
  and its audit-status bullet is now an explicit decision (v1.0.0 ships
  without a paid third-party audit) rather than an open-ended "ongoing".
- `kvhttp` now supports real CA-trusted TLS via Let's Encrypt/ACME
  (`-domain` flag, `golang.org/x/crypto/acme/autocert`), with a
  self-signed fallback (`mage tls:genselfsigned`, new `pkg/tlscert`
  package) when no domain is available. Plain HTTP is no longer
  supported at all.
- Per-resolved-peer-id rate limiting on `kvhttp`'s `/command` endpoint.
- `test/e2e/testdata.json` now also holds the Android `UiCommandE2ETest`
  command-catalog test plan (`android_ui_cases`), merging what used to be
  a hardcoded Kotlin map into the same file as the cross-platform
  wire-protocol rows -- one source of truth for e2e coverage instead of
  two. `E2E_TYPES` env var lets `mage e2e:all`/`e2e:current` run only
  some test types instead of everything.
- An end-to-end test for the Raw Channel feature driven through kvhttp's
  HTTP bridge (`cmd/kvhttp/channel_e2e_test.go`), complementing
  `pkg/daemon`'s existing in-process channel tests.
- `web-app/fuzz/`'s cargo-fuzz targets (`decode_append_entries_request`,
  `decode_request_vote_request`, `decode_request_pre_vote_request`) are now
  tracked in git -- they existed on disk but were untracked.

### Removed
- `docs/getting-started.md`, `docs/linux.md`, `docs/android.md` -- stale
  docs describing a prototype architecture (`pkg/raft.NewP2PNode`,
  `cmd/client`) that predates `pkg/daemon`/`shmevent`/`mobile/kvmobile` and
  was already explicitly disclaimed as not-authoritative in three separate
  places. Deleted rather than left as permanent caveats; `docs/web.md`,
  `docs/library-usage.md`, and `CLAUDE.md` references updated accordingly.

### Fixed
- Three real memory-leak sources found on a 14-day-uptime production
  node (4.95GB RSS against ~265MB of real on-disk data): no read deadline
  on inbound libp2p streams, no bound on libp2p connections/peerstore
  growth, and a `channelTable.reap()` gap on the channel-opening path.
  See `pkg/daemon/daemon.go`'s `streamRequestTimeout`,
  `connManagerLowWater`/`connManagerHighWater`, and `forgetTransientPeer`.

## Earlier history

Everything before this file existed is in `git log` -- notable highlights
include the Group/command ACL system, join-invite/join-request flows,
circuit-relay v2 support for NAT'd followers, the Android
(`mobile/kvmobile`) and browser/wasm (`web-app/`) clients joining the same
raft cluster as real (non-)voting members, and `cmd/kvrecover`'s offline
quorum-loss recovery path.
