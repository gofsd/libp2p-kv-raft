# Security Policy

## Reporting a vulnerability

Please email **madi.nickname@gmail.com** with details rather than opening
a public issue. Include reproduction steps if possible. We'll acknowledge
within a few days and aim to have a fix or mitigation plan before any
public disclosure.

This is a self-directed security process (internal review, fuzzing,
dependency scanning) rather than one backed by a paid external audit --
see the threat model below for what that does and doesn't cover.

## Threat model

### Trust boundaries

- **shmring IPC (same machine only)**: `pkg/ipc`/`pkg/shmclient` connect a
  local CLI/mobile-app process to its own daemon over shared memory, never
  the network. A caller reaching this is assumed to already have
  unrestricted access to that daemon's own store (`set`/`get`/`rangescan`
  all rely on this -- see `pkg/shmevent`'s own package doc comment). This
  is a deliberate design boundary, not an oversight: anything reachable
  over shmring is "as trusted as a local shell on this box already is."

- **libp2p network protocols**: every wire-level event
  (`api/shmevent.capnp`) is Ed25519-signed by the caller's own key and
  verified against the *claimed* sender peer id before any authorization
  check runs (never `stream.Conn().RemotePeer()` -- see
  `handleExecuteStream`/`handleChannelStream`'s own doc comments for why
  that distinction matters). Raft cluster membership (voter/learner) and
  the permit system (`KindPermitPeer`, `KindLogPermit`, etc.) gate which
  signed requests actually take effect; an unsigned or forged-signature
  request is rejected before reaching any of that logic.

- **kvhttp (`cmd/kvhttp`)**: bridges a local daemon's shmring-only
  interface onto HTTPS for callers that can only do a `fetch()` (e.g. a
  browser sandbox). TLS is required outright (self-signed or real
  Let's-Encrypt-via-ACME, see that command's own doc comment) -- there is
  no plain-HTTP mode. Auth is a per-node bearer token
  (`registry.AccessTokenForKeyFile`, constant-time comparison); a request
  is rate-limited per *resolved* peer id (not source IP, which is
  trivially spoofable) to bound abuse of the kvctl-cli-subprocess-per-request
  design. Holding one node's token grants exactly that node's own access,
  nothing about any other node the same machine happens to also host.

### Known accepted risks / out of scope

- A self-signed `kvhttp` cert (the `-domain` flag's fallback when no real
  domain is available) requires each caller to explicitly trust that
  exact certificate -- expected friction for a known-caller deployment,
  not usable by an arbitrary browser user without a real CA-issued cert.
- `kvhttp`'s rate limiting protects against a runaway caller/bug, not a
  distributed multi-attacker flood -- it assumes a small set of trusted
  token holders, matching the rest of this project's trust model.
- No third-party security audit has been performed. Internal hardening
  (fuzzing the wire-format decoders in `pkg/shmevent` and
  `web-app/src/raft_wire.rs`, `govulncheck` dependency scanning, manual
  review of the Group/Command ACL edge cases) is tracked as ongoing work,
  not a completed guarantee.
- Vendored/patched third-party code (`thirdparty/anet`, `thirdparty/libc`
  -- see `CLAUDE.md`'s "Vendored dependency patch" section) carries
  whatever risk the patch itself introduces; both are narrowly scoped
  syscall-level fixes, not general-purpose rewrites.

## Supported versions

Pre-1.0: only the latest commit on `main` is supported. Once a `v1.0.0`
tag exists, this section will be updated with a real support window.
