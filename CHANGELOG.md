# Changelog

All notable changes to this project are documented here. Format loosely
follows [Keep a Changelog](https://keepachangelog.com/); this project
doesn't yet follow strict semver pre-1.0 (see `README.md`/`CLAUDE.md` for
current architecture, which is the authoritative reference -- this file
tracks *changes*, not a full feature description).

## [Unreleased]

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
