# libp2p-kv-raft web client

A browser client for [libp2p-kv-raft](..), in Rust compiled to `wasm32-unknown-unknown`. Unlike
the earlier TypeScript/js-libp2p version of this client, a browser tab here is meant to be a
**real hashicorp/raft non-voter (learner)**: it joins the cluster's actual raft configuration,
receives genuine `AppendEntries` replication over the wire-compatible protocol
`pkg/rafttransport` speaks, and answers `Get` from its own locally-applied state -- not a request
forwarded to a voter on every read. It still never votes in an election (a browser tab can't
accept a raw inbound connection the way a real voter's transport requires -- see
[Architecture](#architecture)), which is what "learner" means here and in the Raft paper itself.

## Architecture

- `src/msgpack.rs` / `src/raft_wire.rs` -- a from-scratch msgpack codec and RPC framing
  reimplementing `hashicorp/raft@v1.7.3`'s `NetworkTransport` wire format (see `net_transport.go`)
  byte-for-byte: `AppendEntries`/`RequestVote`/`RequestPreVote`/`InstallSnapshot`/`TimeoutNow`,
  including the embedded-struct field promotion, `[]byte`-as-raw-string encoding, and legacy
  15-byte time format `go-msgpack/v2`'s default `MsgpackHandle{}` uses. Every struct's decoder is
  unit-tested against real hex fixtures copied verbatim from `go-msgpack/v2`'s own
  `codec/internal/testdata/raft_v116.go` -- genuine encoder output the upstream codec pins itself
  against for backward compatibility, not hand-derived -- so `cargo test` here is real evidence of
  wire compatibility, not just internal self-consistency.
- `src/fsm.rs` -- byte-compatible with `pkg/kvfsm`'s log command encoding (`EncodeCommand`) and
  `pkg/store`'s `DumpAll`/`LoadAll` snapshot framing.
- `src/sqlite_store.rs` -- the learner's durable storage (replicated log, `currentTerm`/`votedFor`,
  commit/apply indices, and the kv table itself), on real SQLite via
  [`sqlite-wasm-rs`](https://crates.io/crates/sqlite-wasm-rs) (compiles the actual SQLite C
  amalgamation to `wasm32-unknown-unknown`).
- `src/learner.rs` -- the raft follower state machine itself: term checks, the
  `PrevLogEntry`/`PrevLogTerm` consistency check with conflicting-suffix truncation, commit-index
  advancement, in-order FSM apply, and real (if intentionally inert -- see its doc comment)
  `RequestVote`/`RequestPreVote` handling.
- `src/p2p.rs` -- the `rust-libp2p` `Swarm`: dials a cluster node over WebTransport
  (`pkg/daemon.newHost`'s WebTransport listener), reserves a circuit-relay v2 slot through it so a
  Go leader can `Dial()` back to this tab -- a browser can never accept a raw inbound connection,
  but *can* be dialed through a relay it already holds a reservation on, exactly the mechanism an
  Android device behind carrier-grade NAT already relies on (see
  `pkg/daemon.Config.RelayPeer`'s doc comment) -- and serves `rafttransport.ProtocolID` (the raft
  RPC stream) and `pkg/daemon.ClientProtocolID` (the join handshake and forwarded Set/Get).
- `src/shmevent/` -- Rust port of `pkg/shmevent` (see `api/shmevent.capnp`): the single
  capnp-encoded struct every "user"-to-"raft node instance" hop in this project speaks --
  main-thread/Worker here, and `pkg/daemon.ClientProtocolID`'s network hop too. `mod.rs` is the
  codec itself: three fields common to every message (`id`, `crc32`, `signature`) plus a union with
  one variant per logical operation, each carrying its own named typed fields, mirroring the
  schema's groups one-for-one. A Set decomposes into a linked `SetKey`+`SetField` pair, a Get is a
  one-shot `GetField` (or, addressed by a prior `SetKey`'s id, via `source_id`), and
  `GetPublicKey`/`GetPrivateKey` are how a caller with no key yet bootstraps into one. `system.rs`
  and `catalog_keys.rs` mirror `pkg/shmevent`'s own `system.go`/`catalog.go` split -- the
  `SystemKeyPrefix` *store* key layout and stored-record payload framing, which is a separate
  encoding from the wire events, since permit and catalog *writes* are ordinary union variants
  (`Body::PermitRequest`, `Body::GroupPut`, ...) rather than hand-packed blobs in a generic
  envelope. See `api/shmevent.capnp`'s doc comment for the full design and `mod.rs`'s own for the
  two rules (canonical-form CRC/signature payload, and null vs present-but-empty pointers) that
  make this a byte-compatible mirror of the Go twin rather than a plausible one; both sides are
  generated from that identical schema (`build.rs` runs `capnp compile` at build time) and pinned
  against each other by `api/shmevent_wire_fixture.json` (see "Known gaps" below). Replaces the old
  fixed-size `ipcproto.rs`.
- `src/shmring_ipc.rs` -- the main-thread/Worker channel carrying `shmevent::Msg`, using
  [`shmring`](https://crates.io/crates/shmring) `0.3.0`'s Rust API directly (both sides of this
  channel are this same crate compiled to wasm, so there's no need for shmring's separate
  JS-facing `wasm_api`/`@gofsd/shmring` package -- see that crate's README on using
  `backend::SharedArrayBufferStorage` "exactly like the native backend"). Mirrors
  `pkg/ipc/ipc_android.go`'s Call/Serve pattern: each round trip gets a fresh, single-use pair of
  rings, and only one call is ever in flight at a time.
- `src/app.rs` -- wires all of the above into the `wasm-bindgen` entry points a page actually
  loads: `worker_main()` (run inside the Worker; owns the `p2p::Node`, the `Learner`, its own
  `shmevent` key registry, and answers every request from the main thread), `worker_main_with_seed(seed_hex)`
  (the same, but with a deterministic identity decoded from `seed_hex` instead of a freshly random
  one -- the same 128-hex-char raw stdlib `crypto/ed25519` private key format `pkg/e2edata.Node.PrivateKey`
  and `mobile/kvmobile`'s `identitySeedHex` ldflag use, so the e2e test pipeline's recorded node
  identities work unmodified here too), and `MainHandle` (run on
  the main thread; `connect`/`set`/`get`). Two separate Ed25519 keys are in play -- the Worker's own
  identity key secures the main-thread/Worker hop, and a key fetched from the remote leader during
  `connect` secures the `ClientProtocolID` hop -- see `app.rs`'s doc comment for why both exist.
  `EVENT_ADD`'s `SourceID` referencing a prior `EVENT_SET_KEY` (this tab's own peer id) is what
  turns a plain connect into a real `AddNonvoter` learner-join, the same sequence
  `pkg/daemon.TestAddLearnerThroughRelay` exercises from the Go side.
- `main.js` / `worker.js` / `index.html` -- the JS glue and UI, playing the role `MainActivity.kt`
  plays for the Android app. `main.js` is also this crate's worked example of driving it from
  plain JS: `new Worker("./worker.js")` plus `new MainHandle(worker)` is the entire surface --
  `MainHandle` handles the `shmevent` key bootstrap and the SetKey/SetField pairing internally, so
  callers only ever see `connect(multiaddr)`/`set(key, value)`/`get(key)`.

## Building

```sh
rustup target add wasm32-unknown-unknown   # once
cargo install wasm-pack                     # once
npm install
npm run build:wasm   # wasm-pack build --target web --out-dir pkg
npm run dev
```

Two things need to be on `PATH` beyond the usual Rust toolchain, neither of which needs root if
your package manager doesn't have them:

- The Cap'n Proto schema compiler (`capnp`, for `build.rs`'s `capnp compile` -- see
  `api/shmevent.capnp`; `capnpc`, the Rust codegen backend, is a normal Cargo build-dependency and
  needs nothing extra). Builds cleanly from source in a user-local prefix with just
  `cmake`/`g++`/`pkg-config` (`cmake .. -DCMAKE_INSTALL_PREFIX=$HOME/.local && cmake --build . &&
  cmake --install .` from a `capnproto` release tarball's `c++/` directory) if it isn't packaged
  wherever this runs.
- A C compiler that can target `wasm32-unknown-unknown`, for `sqlite-wasm-rs`'s build script
  (compiles SQLite's C amalgamation). `.cargo/config.toml` points `CC_wasm32_unknown_unknown` at
  `.cargo/wasm-cc.sh`, which prefers a real `clang` already on `PATH` and otherwise falls back to
  [`zig cc`](https://ziglang.org/) (a full clang, bundled in a single ~55 MB download with no root
  needed -- `tar xf zig-*.tar.xz` and put the extracted directory's `zig` binary on `PATH`; no
  further setup). Verified working end to end this way: `wasm-pack build --target web --out-dir
  pkg` and `npx vite build` both produce a real, complete bundle.

## Running it

Needs a `kvnode` already running with a browser-reachable WebTransport listener (every node has
one automatically -- see `pkg/daemon.newHost`) *and* a relay-capable node it can reserve a circuit
through (a leader started with `-relay-service`, or any node with `Config.RelayPeer` set to one --
this tab needs the same kind of reservation an Android device behind carrier-grade NAT already
gets). Find the target's multiaddr via:

```bash
cat ~/.libp2p-kv-raft/registry.json   # listen_addrs includes .../quic-v1/webtransport/certhash/.../p2p/<peer-id>
```

`SharedArrayBuffer` (what `shmring_ipc.rs`'s main-thread/Worker channel is built on) is only
available on a [cross-origin-isolated](https://developer.mozilla.org/en-US/docs/Web/API/crossOriginIsolated)
page; `vite.config.js` sets the required `Cross-Origin-Opener-Policy`/`Cross-Origin-Embedder-Policy`
headers for both `dev` and `preview` -- a production deployment needs to send them too.

Open the dev server URL, paste the target's multiaddr into "Node multiaddr", click Connect (this
dials the target, reserves a relay slot, and asks it -- forwarding to the real leader server-side
if needed -- to `AddNonvoter` this tab at that reserved address), then Set/Get. A Set is forwarded
to the real leader over `ClientProtocolID` exactly like a follower's own forwarded Set
(`pkg/daemon.ForwardProtocolID`); a Get reads this tab's own locally-replicated state, the same
possibly-slightly-lagging-behind-a-just-committed-Set caveat any raft follower's local read
already carries.

`npx playwright test` runs `tests/set_get.spec.js`, a real headless-browser Connect/Set/Get round
trip against a running `kvnode` -- point it at one with `KVNODE_MULTIADDR=<multiaddr>` (see that
file's doc comment); it's skipped with no cluster running.

## Known gaps / what to verify before relying on this

This crate was originally built and its core logic verified in a sandboxed environment with
`cargo`/`rustc`, `capnp` (built from source into a user prefix), and a wasm32-capable C compiler
(via `zig cc` -- see "Building" above), with no ability to launch a real browser or a live cluster;
the `api/shmevent.capnp` union port has since been run against both. Verified:

- `cargo test` (native): the entire msgpack/raft-wire codec, `pkg/kvfsm` byte compatibility, and
  the `shmevent` capnp codec (encode/decode of every union variant, CRC32 corruption detection,
  Ed25519 sign/verify including tamper detection, and the two-bootstrap-event unsigned exception)
  -- all passing, and the `pkg/shmevent` Go twin generated from the same `api/shmevent.capnp`
  schema has its own equivalent passing suite (`go test ./pkg/shmevent/...`).
- **Cross-language, not just self-consistent:** `api/shmevent_wire_fixture.json` holds one real
  Go-encoded message per interesting union variant, signed with a fixed test key. Go's
  `TestWireFixture` and this crate's `shmevent::tests::go_fixture` both read it: each side decodes
  the other's bytes, verifies the Ed25519 signature over them, and re-encodes to byte-identical
  output. That is what pins the two implementations to the same wire format -- a Rust round trip
  alone would pass just as happily on a wire shape the daemon doesn't speak. Regenerate it with
  `go test ./pkg/shmevent -run TestWireFixture -update-wire-fixture` when the schema legitimately
  changes, and re-run `cargo test` here: a diff in either direction is the point of the fixture.
- `wasm-pack build --target web --out-dir pkg` succeeds for real (not just `cargo check`) and
  produces a genuine, complete bundle (`kv_raft_web_bg.wasm`, ~2.7 MB, plus its JS glue) --
  `sqlite-wasm-rs`'s SQLite-to-wasm32 compile included, no stub needed. `npx vite build` then
  packages that bundle with `main.js`/`worker.js`/`index.html` into a working `dist/` with no
  errors.
- Real Go-side integration tests: `pkg/daemon.TestAddLearnerThroughRelay` exercises the *exact*
  wire sequence `app.rs`'s `do_connect` performs -- a signed `SetKey`+`EventAdd` pair, signed with
  the tab's own key (`pkg/daemon` verifies it against the connection's own libp2p-authenticated
  identity, not a key borrowed from the leader -- see `app.rs`'s "One key, two hops" doc comment) --
  against a real leader+relay, proving `AddNonvoter` over a relay-reserved address lands in the
  raft configuration and that the leader's `rafttransport.NetworkTransport` really dials it and
  delivers an `AppendEntries` stream.

- **A real browser tab against the real deployed cluster**, since the union port: `E2E_TYPES=web
  mage e2e:current` (version 6 in `test/e2e/testdata.json`) drives `tests/e2e.spec.js` in a real
  Chromium tab, dialing the deployed bootstrap node's WebTransport address, and passes all six
  rows -- `bootstrap_or_join_cluster` (the tab joins as a genuine raft learner and starts
  receiving `AppendEntries`), `get_public_key`, `set_key`, `set_field`, `get_field_by_key` and
  `get_field_by_registry`. That is the port's real proof: every one of those messages crosses
  `pkg/daemon.ClientProtocolID` in the new union encoding and is decoded, CRC-checked and
  signature-verified by the Go daemon on the other side.

Not yet verified (needs a real browser and a live cluster to drive):

- `SqliteStore`'s OPFS-backed persistence path (`sahpool` VFS) -- `sqlite_store.rs` currently
  opens with the default in-memory VFS; see `SqliteStore::open`'s doc comment for how to switch.
