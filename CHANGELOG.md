# Changelog

All notable changes to this project are documented here. Format loosely
follows [Keep a Changelog](https://keepachangelog.com/); this project
doesn't yet follow strict semver pre-1.0 (see `README.md`/`CLAUDE.md` for
current architecture, which is the authoritative reference -- this file
tracks *changes*, not a full feature description).

## [Unreleased]

### Added
- `LICENSE` (Apache-2.0), `NOTICE`, `CONTRIBUTING.md`, `SECURITY.md` --
  first steps toward a publishable, production-ready release.
- `-require-permit-for-remote` CLI flag on `kvnode`, exposing
  `daemon.Config.RequirePermitForRemote` (which already existed and was
  fully wired into the daemon, just unreachable from the CLI).
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
