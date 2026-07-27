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
