//! Wires [`crate::p2p::Node`]/[`crate::p2p::Handle`] +
//! [`crate::learner::Learner`] + [`crate::shmring_ipc`] +
//! [`crate::shmevent`] together into the two `wasm-bindgen` entry points a
//! page actually loads: [`worker_main`] (run inside the Worker that owns
//! this tab's daemon-equivalent state, mirroring `mobile/kvmobile.go`'s
//! Start/Submit/Get answering `pkg/ipc` requests from Android's
//! `MainActivity`) and [`MainHandle`] (run on the main thread, driving the
//! Worker exclusively through [`shmring_ipc::MainChannel`], the same shape
//! Android's UI drives its in-process daemon through `pkg/ipc.Call`).
//!
//! # One key, two hops
//!
//! Every hop in this project speaks the same [`crate::shmevent`] struct.
//! This tab has exactly one signing key -- the Worker's own libp2p identity
//! key, generated in [`worker_main`] -- and uses it for both hops:
//!
//!  1. **Main thread <-> Worker** (this file's `MainHandle`/`handle_request`):
//!     `MainHandle::ensure_key` fetches it via `EVENT_GET_PRIVATE_KEY`
//!     before signing anything else, mirroring `pkg/shmclient.Session`'s
//!     bootstrap. Same-origin/same-JS-realm as this relationship is,
//!     there's no realistic attacker who could forge a message here
//!     without already having arbitrary code execution in the page (at
//!     which point far worse is possible directly) -- this hop is signed
//!     anyway for protocol consistency (one struct, one verification path,
//!     everywhere) and because it costs little given the Worker already
//!     has a real key on hand.
//!  2. **This tab <-> the remote leader** (`do_connect`/`do_set`, over
//!     `p2p::Handle::call_client_protocol`): every SetKey/SetField/Add to
//!     the leader is signed with this same Worker key -- not a key
//!     borrowed from the leader. `pkg/daemon.handleShmEvent` verifies a
//!     remote caller's signature against its own libp2p-authenticated
//!     identity (the connection's Noise handshake, not anything the
//!     message claims), so this tab needs no bootstrap round trip to the
//!     leader at all before it can sign -- and `pkg/daemon` refuses to
//!     serve `EVENT_GET_PRIVATE_KEY`/`EVENT_GET_PUBLIC_KEY` to a remote
//!     caller in the first place, since a remote caller always has its own
//!     key already.
//!
//! `ActionAdd`'s old meaning ("connect to the target node and join as a
//! non-voting learner") is now `EVENT_ADD`, `SourceID` referencing a prior
//! `EVENT_SET_KEY` holding this tab's own peer id -- the exact sequence
//! `pkg/daemon.TestAddLearnerThroughRelay` exercises from the Go side.
#![cfg(target_arch = "wasm32")]

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::Rc;

use ed25519_dalek::{SigningKey, VerifyingKey};
use futures::channel::oneshot;
use libp2p::{identity, multiaddr::Protocol, Multiaddr, PeerId};
use wasm_bindgen::prelude::*;
use web_sys::DedicatedWorkerGlobalScope;

use crate::client;
use crate::learner::Learner;
use crate::main_ops;
use crate::p2p::{self, Node};
use crate::shmevent::{self, Msg};
use crate::shmring_ipc;
use crate::sqlite_store::SqliteStore;

/// Random non-zero id for a new message -- 0 is reserved meaning
/// "SourceID/DestinationID not used" (see `api/shmevent.capnp`), so a real
/// message's own id avoids it too. Mirrors `pkg/shmclient.newID`.
pub(crate) fn new_id() -> u16 {
    loop {
        let v = (js_sys::Math::random() * 65536.0) as u32 as u16;
        if v != 0 {
            return v;
        }
    }
}

pub(crate) struct WorkerState {
    pub(crate) handle: p2p::Handle,
    pub(crate) learner: Option<Rc<Learner>>,
    pub(crate) leader: Option<PeerId>,
    /// Backs `EVENT_SET_KEY`/`EVENT_GET_KEY` for the main-thread<->Worker
    /// hop -- this Worker's own mirror of `pkg/shmevent.Registry`.
    registry: HashMap<u16, Vec<u8>>,
    /// This Worker's own identity key, doubling as the main-thread<->Worker
    /// hop's signing key -- see this module's doc comment, key
    /// relationship 1.
    pub(crate) signing_key: SigningKey,
    verifying_key: VerifyingKey,
}

/// Runs forever inside the Worker script (see `web-app/README.md`'s
/// `worker.js`): brings up this tab's own `p2p::Node`, then answers every
/// request the main thread sends over the shmring channel. Generates a
/// fresh random identity every time -- see `worker_main_with_seed` for a
/// deterministic-identity variant.
#[wasm_bindgen]
pub async fn worker_main() {
    run_worker(identity::Keypair::generate_ed25519()).await
}

/// Like `worker_main`, but with a deterministic identity instead of a
/// freshly random one: `seed_hex` is 128 hex chars decoding to the 64 raw
/// stdlib `crypto/ed25519` private key bytes (32-byte seed + 32-byte public
/// key) -- exactly `pkg/e2edata.Node.PrivateKey`'s own format, so a
/// recorded node's key can be passed straight through with no conversion.
/// This is what the e2e test pipeline needs so a build against a recorded
/// identity reliably comes up as that exact peer id (mirrors
/// `mobile/kvmobile`'s `identitySeedHex` ldflag and
/// `pkg/e2edata.WriteDesktopKeyFile`'s desktop equivalent). Returns an
/// error string via `Err` if seed_hex doesn't decode to a valid key,
/// instead of panicking -- a page driving this from JS can then report it
/// instead of silently hanging.
#[wasm_bindgen]
pub async fn worker_main_with_seed(seed_hex: String) -> Result<(), JsValue> {
    let raw = shmevent::hex_decode(&seed_hex)
        .map_err(|e| JsValue::from_str(&format!("worker_main_with_seed: decode seed_hex: {e}")))?;
    if raw.len() != 64 {
        return Err(JsValue::from_str(&format!(
            "worker_main_with_seed: seed_hex must decode to 64 bytes (32-byte seed + 32-byte public key), got {}",
            raw.len()
        )));
    }
    let mut seed: [u8; 32] = raw[..32].try_into().expect("checked len above");
    let keypair = identity::Keypair::ed25519_from_bytes(&mut seed[..])
        .map_err(|e| JsValue::from_str(&format!("worker_main_with_seed: {e}")))?;
    run_worker(keypair).await;
    Ok(())
}

/// Shared body of `worker_main`/`worker_main_with_seed`: brings up this
/// tab's own `p2p::Node` under keypair, then answers every request the
/// main thread sends over the shmring channel.
async fn run_worker(keypair: identity::Keypair) {
    console_error_panic_hook::set_once();

    let global: DedicatedWorkerGlobalScope = js_sys::global().unchecked_into();

    // libp2p_identity::ed25519::Keypair wraps its own ed25519-dalek
    // SigningKey, not necessarily the same major version this crate
    // depends on directly -- to_bytes()/from_bytes() round-trips through
    // the portable raw byte format both understand identically (the same
    // "seed + public key" layout stdlib crypto/ed25519 and pkg/shmevent
    // use on the Go side), rather than trying to share the type itself.
    let ed25519_kp = keypair
        .clone()
        .try_into_ed25519()
        .expect("keypair is always constructed as ed25519 by worker_main/worker_main_with_seed");
    let kp_bytes = ed25519_kp.to_bytes();
    let seed: [u8; 32] = kp_bytes[..32]
        .try_into()
        .expect("ed25519 keypair bytes are 64 bytes: 32-byte seed + 32-byte public key");
    let signing_key = SigningKey::from_bytes(&seed);
    let verifying_key = signing_key.verifying_key();

    let (node, handle) = match Node::new(keypair) {
        Ok(v) => v,
        Err(e) => {
            web_sys::console::error_1(&format!("kv-raft-web: create node: {e}").into());
            return;
        }
    };
    // Node::run becomes this Node's sole, permanent owner -- see p2p.rs's
    // "Task ownership" doc comment.
    wasm_bindgen_futures::spawn_local(node.run());

    let state = Rc::new(RefCell::new(WorkerState {
        handle,
        learner: None,
        leader: None,
        registry: HashMap::new(),
        signing_key,
        verifying_key,
    }));

    shmring_ipc::serve(global, move |req: Msg, crc: u32, sig: Vec<u8>| {
        let state = state.clone();
        async move { handle_request(state, req, crc, sig).await }
    });
}


/// Dispatches one decoded main-thread request the same way
/// `pkg/daemon.handleShmEvent` dispatches a local `pkg/ipc` request --
/// using this Worker's own registry/key for the main-thread<->Worker hop
/// (see this module's doc comment) -- and returns the already-encoded,
/// already-signed response bytes `shmring_ipc::serve` expects.
async fn handle_request(
    state: Rc<RefCell<WorkerState>>,
    req: Msg,
    crc: u32,
    sig: Vec<u8>,
) -> Vec<u8> {
    let req_id = req.id;
    if shmevent::requires_signature(req.event_type) {
        let vk = state.borrow().verifying_key;
        if let Err(e) = shmevent::verify(&vk, &req, crc, &sig) {
            return encode_local_response(&state, Msg::error(req_id, e.to_string()));
        }
    }

    let resp = match req.event_type {
        shmevent::EVENT_SET_KEY => {
            state
                .borrow_mut()
                .registry
                .insert(req.id, req.value.clone());
            Msg {
                event_type: shmevent::EVENT_SET_KEY,
                id: req_id,
                value: req.value,
                ..Default::default()
            }
        }

        shmevent::EVENT_GET_KEY => match state.borrow().registry.get(&req.source_id).cloned() {
            Some(value) => Msg {
                event_type: shmevent::EVENT_GET_KEY,
                id: req_id,
                value,
                ..Default::default()
            },
            None => Msg::error(
                req_id,
                format!("no entry registered under id {}", req.source_id),
            ),
        },

        shmevent::EVENT_SET_FIELD => {
            let key = state.borrow().registry.get(&req.source_id).cloned();
            match key {
                Some(key) => {
                    let key = String::from_utf8_lossy(&key).into_owned();
                    let value = String::from_utf8_lossy(&req.value).into_owned();
                    match do_set(&state, &key, &value).await {
                        Ok(()) => Msg {
                            event_type: shmevent::EVENT_SET_FIELD,
                            id: req_id,
                            ..Default::default()
                        },
                        Err(e) => Msg::error(req_id, e.to_string()),
                    }
                }
                None => Msg::error(
                    req_id,
                    format!(
                        "no key registered under id {} -- send SetKey first",
                        req.source_id
                    ),
                ),
            }
        }

        shmevent::EVENT_GET_FIELD => {
            let key = if req.source_id != 0 {
                match state.borrow().registry.get(&req.source_id).cloned() {
                    Some(k) => k,
                    None => {
                        return encode_local_response(
                            &state,
                            Msg::error(
                                req_id,
                                format!(
                                    "no key registered under id {} -- send SetKey first",
                                    req.source_id
                                ),
                            ),
                        );
                    }
                }
            } else {
                req.value.clone()
            };
            let key = String::from_utf8_lossy(&key).into_owned();
            match do_get(&state, &key).await {
                Ok(value) => Msg {
                    event_type: shmevent::EVENT_GET_FIELD,
                    id: req_id,
                    value: value.into_bytes(),
                    ..Default::default()
                },
                Err(e) => Msg::error(req_id, e.to_string()),
            }
        }

        shmevent::EVENT_GET_PUBLIC_KEY => {
            let vk = state.borrow().verifying_key;
            Msg {
                event_type: shmevent::EVENT_GET_PUBLIC_KEY,
                id: req_id,
                value: vk.to_bytes().to_vec(),
                ..Default::default()
            }
        }

        shmevent::EVENT_GET_PRIVATE_KEY => {
            let sk_bytes = state.borrow().signing_key.to_bytes();
            Msg {
                event_type: shmevent::EVENT_GET_PRIVATE_KEY,
                id: req_id,
                value: sk_bytes.to_vec(),
                ..Default::default()
            }
        }

        shmevent::EVENT_ADD => {
            let target_addr = String::from_utf8_lossy(&req.value).into_owned();
            match do_connect(&state, &target_addr).await {
                Ok(peer_id) => Msg {
                    event_type: shmevent::EVENT_ADD,
                    id: req_id,
                    value: peer_id.to_string().into_bytes(),
                    ..Default::default()
                },
                Err(e) => Msg::error(req_id, e.to_string()),
            }
        }

        // Generic proxy for every remaining single-round-trip event that's
        // actually reachable from a non-voting learner (see
        // crate::client's doc comment for the twelve voter-gated events
        // deliberately absent here): the Worker forwards req.value to the
        // leader unchanged and hands the response straight back, exactly
        // the shape RemoteSession::call already is.
        shmevent::EVENT_SET
        | shmevent::EVENT_PERMIT_REQUEST
        | shmevent::EVENT_LOG_APPEND
        | shmevent::EVENT_LOG_PERMIT_REQUEST
        | shmevent::EVENT_POLL_EXECUTE => {
            let event_type = req.event_type;
            let result = async {
                let mut sess = client::RemoteSession::from_worker_state(&state)?;
                sess.call(event_type, req.value.clone()).await
            }
            .await;
            match result {
                Ok(resp) => Msg {
                    event_type,
                    id: req_id,
                    value: resp.value,
                    ..Default::default()
                },
                Err(e) => Msg::error(req_id, e.to_string()),
            }
        }

        // Main-thread-facing EVENT_EXECUTE packs dest_peer_id and payload
        // together via encode_set_payload -- a private convention of this
        // hop only (do_execute unpacks it and drives the real two-
        // EventSetKey-then-EventExecute wire sequence against the leader
        // itself, see RemoteSession::execute).
        shmevent::EVENT_EXECUTE => {
            match shmevent::decode_set_payload(&req.value) {
                Ok((dest_peer_id, payload)) => {
                    let dest_peer_id = String::from_utf8_lossy(dest_peer_id).into_owned();
                    match do_execute(&state, &dest_peer_id, payload).await {
                        Ok(()) => Msg {
                            event_type: shmevent::EVENT_EXECUTE,
                            id: req_id,
                            ..Default::default()
                        },
                        Err(e) => Msg::error(req_id, e.to_string()),
                    }
                }
                Err(e) => Msg::error(req_id, e.to_string()),
            }
        }

        // main_ops's own op-code band (100-127) -- multi-step/local-read
        // operations that don't reduce to one shmevent wire event. See
        // main_ops's doc comment.
        op if op >= main_ops::OP_BASE => match main_ops::dispatch(&state, op, &req.value).await {
            Ok(value) => Msg {
                event_type: op,
                id: req_id,
                value,
                ..Default::default()
            },
            Err(e) => Msg::error(req_id, e),
        },

        other => Msg::error(req_id, format!("unknown event {other}")),
    };

    encode_local_response(&state, resp)
}

/// Signs `resp` with this Worker's own identity key -- the main thread
/// already fetched the matching public key via `EVENT_GET_PRIVATE_KEY`
/// before sending anything else (see `MainHandle::ensure_key`), mirroring
/// `pkg/ipc.Serve`'s responses always being signed with the daemon's real
/// key (never the `None`-key bootstrap exception, which only ever applies
/// to requests).
fn encode_local_response(state: &Rc<RefCell<WorkerState>>, resp: Msg) -> Vec<u8> {
    let signing_key = state.borrow().signing_key.clone();
    shmevent::encode(&resp, Some(&signing_key)).unwrap_or_default()
}

/// Dials `target_addr` (any cluster member's WebTransport multiaddr, per
/// `pkg/daemon.newHost`), reserves a circuit-relay v2 slot through it, and
/// asks it (forwarding to the real leader server-side if needed -- see the
/// learner-join handling in `pkg/daemon`) to add this tab as a raft
/// non-voter at that reserved address via a signed `EVENT_SET_KEY` +
/// `EVENT_ADD` pair, signed with this tab's own key (see this module's doc
/// comment) -- the same sequence `pkg/daemon.TestAddLearnerThroughRelay`
/// exercises from the Go side. Returns this tab's own peer id, mirroring
/// `kvmobile.Start`'s return value.
async fn do_connect(
    state: &Rc<RefCell<WorkerState>>,
    target_addr: &str,
) -> Result<PeerId, p2p::Error> {
    let addr: Multiaddr = target_addr
        .parse()
        .map_err(|e: libp2p::multiaddr::Error| p2p::Error(e.to_string()))?;
    let target_peer = addr
        .iter()
        .find_map(|p| match p {
            Protocol::P2p(id) => Some(id),
            _ => None,
        })
        .ok_or_else(|| p2p::Error("multiaddr missing /p2p/<peer-id>".into()))?;

    let mut handle = state.borrow().handle.clone();
    crate::p2p::debug_log("kv-raft-web: do_connect: reserving relay slot");
    let self_addr = handle.reserve_relay_slot(addr).await?;
    let self_id = handle.local_peer_id();
    crate::p2p::debug_log(&format!(
        "kv-raft-web: do_connect: relay slot reserved, self_addr={self_addr}"
    ));

    let signing_key = state.borrow().signing_key.clone();

    // A tab replays every recorded row for its identity across however many
    // e2e versions it has history in (see `do_get`'s doc comment on the
    // shared leader's ever-growing log), so `do_connect` can legitimately
    // run more than once in the same session -- e.g. a later version's own
    // "add" row. `libp2p_stream::Control::accept` errors if the same
    // protocol is registered twice, and recreating the `Learner`/
    // `SqliteStore` here would silently orphan the first `serve_raft` loop
    // (still delivering `AppendEntries` to the *first* learner) while
    // `state.learner` pointed at a second one nothing ever drives -- caught
    // directly: a second `add` in one tab left every following `get_field`
    // unable to find a key `set_field` had just reported success for.
    // Reusing the existing learner (same `self_id`, same store filename
    // either way) and skipping the second `serve_raft` spawn keeps the
    // original, still-working accept loop as the single source of truth.
    let already_connected = state.borrow().learner.is_some();
    let learner = if let Some(existing) = state.borrow().learner.clone() {
        existing
    } else {
        let store = SqliteStore::open(&format!("kv-raft-web-{self_id}.sqlite3"), None)
            .map_err(|e| p2p::Error(e.to_string()))?;
        Rc::new(Learner::new(
            store,
            self_id.to_bytes(),
            self_addr.to_string().into_bytes(),
        ))
    };

    {
        let mut guard = state.borrow_mut();
        guard.learner = Some(learner.clone());
        guard.leader = Some(target_peer);
    }

    if !already_connected {
        // Accept inbound raft-protocol streams forever -- spawned once, using
        // its own cloned Handle (see this module's doc comment).
        let mut handle = handle.clone();
        wasm_bindgen_futures::spawn_local(async move {
            if let Err(e) = handle.serve_raft(learner).await {
                web_sys::console::error_1(&format!("kv-raft-web: serve_raft: {e}").into());
            }
        });
    }

    let set_key_id = new_id();
    let set_key_resp = call_remote(
        &mut handle,
        target_peer,
        Msg {
            event_type: shmevent::EVENT_SET_KEY,
            value: self_id.to_string().into_bytes(),
            id: set_key_id,
            ..Default::default()
        },
        Some(&signing_key),
    )
    .await?;
    reject_if_error(&set_key_resp)?;

    let add_resp = call_remote(
        &mut handle,
        target_peer,
        Msg {
            event_type: shmevent::EVENT_ADD,
            source_id: set_key_id,
            value: self_addr.to_string().into_bytes(),
            id: new_id(),
            ..Default::default()
        },
        Some(&signing_key),
    )
    .await?;
    reject_if_error(&add_resp)?;

    Ok(self_id)
}

async fn call_remote(
    handle: &mut p2p::Handle,
    target_peer: PeerId,
    req: Msg,
    priv_key: Option<&SigningKey>,
) -> Result<Msg, p2p::Error> {
    handle
        .call_client_protocol(target_peer, &req, priv_key)
        .await
}

pub(crate) fn reject_if_error(resp: &Msg) -> Result<(), p2p::Error> {
    if resp.event_type == shmevent::EVENT_ERROR {
        return Err(p2p::Error(
            String::from_utf8_lossy(&resp.value).into_owned(),
        ));
    }
    Ok(())
}

/// Applies key=value through raft on the connected leader: `EVENT_SET_KEY`
/// registers key, then `EVENT_SET_FIELD` (referencing it via `SourceID`)
/// applies the value -- see `pkg/shmevent`'s doc comment for why a Set
/// needs two linked messages rather than one.
async fn do_set(
    state: &Rc<RefCell<WorkerState>>,
    key: &str,
    value: &str,
) -> Result<(), p2p::Error> {
    let (mut handle, leader, signing_key) = {
        let guard = state.borrow();
        let leader = guard
            .leader
            .ok_or_else(|| p2p::Error("do_connect has not completed yet".into()))?;
        (guard.handle.clone(), leader, guard.signing_key.clone())
    };

    let set_key_id = new_id();
    let set_key_resp = call_remote(
        &mut handle,
        leader,
        Msg {
            event_type: shmevent::EVENT_SET_KEY,
            value: key.as_bytes().to_vec(),
            id: set_key_id,
            ..Default::default()
        },
        Some(&signing_key),
    )
    .await?;
    reject_if_error(&set_key_resp)?;

    let set_field_resp = call_remote(
        &mut handle,
        leader,
        Msg {
            event_type: shmevent::EVENT_SET_FIELD,
            source_id: set_key_id,
            value: value.as_bytes().to_vec(),
            id: new_id(),
            ..Default::default()
        },
        Some(&signing_key),
    )
    .await?;
    reject_if_error(&set_field_resp)
}

/// Direct, unreplicated peer-to-peer notification to `dest_peer_id` -- see
/// `crate::client::RemoteSession::execute`'s doc comment for the two
/// `EVENT_SET_KEY` registrations this hides on the Worker<->leader hop.
/// Not voter-gated.
async fn do_execute(
    state: &Rc<RefCell<WorkerState>>,
    dest_peer_id: &str,
    payload: &[u8],
) -> Result<(), p2p::Error> {
    let mut sess = client::RemoteSession::from_worker_state(state)?;
    sess.execute(dest_peer_id, payload).await
}

/// This tab's own locally replicated read (via the raft learner applied up
/// through its own `commit_index`) -- may lag a moment behind a Set that
/// just committed on the leader, same caveat any raft follower's local
/// read already carries. Purely local: no round trip to the leader at all.
///
/// Retries for up to `GET_RETRY_BUDGET_MS`. The e2e pipeline's deploy
/// target is a single long-lived VPS leader that is deliberately never
/// torn down between runs (so a human can keep poking at it -- see
/// `test/e2e/testdata.json`'s doc comment), so its raft log keeps growing
/// across every past `mage e2e:all` invocation ever run against it. A tab
/// that just joined (see `do_connect`) starts catching up from index 1, not
/// from wherever the log happens to be "recent" -- confirmed directly:
/// after joining against a leader whose log was already past index 1400,
/// this tab received well over a thousand individual `AppendEntries`
/// (mostly old no-op/heartbeat entries) before ever reaching the one
/// actually containing the Set this row just made. Android/desktop's own
/// equivalent rows join the exact same shared leader and are just as
/// exposed to this in principle, but never hit it in practice: their build/
/// install/ADB round trips before a row even runs are already far longer
/// than the catch-up takes. This budget will need to grow as the shared
/// leader's log keeps accumulating, until raft's own snapshotting kicks in
/// and collapses a fresh join back down to one `InstallSnapshot` again.
async fn do_get(state: &Rc<RefCell<WorkerState>>, key: &str) -> Result<String, p2p::Error> {
    // Was 30_000 -- too short against the shared e2e leader's log as of
    // 2026-07-24 (see this fn's doc comment): every `get_field` row failed
    // outright after burning the full 30s retrying, not just running
    // close to the edge. Still comfortably under tests/e2e.spec.js's
    // per-row 200s backstop and the outer 300s test timeout even with two
    // such calls in one session (a node's history replaying two versions).
    const GET_RETRY_BUDGET_MS: u32 = 90_000;
    const GET_RETRY_INTERVAL_MS: u32 = 100;

    let learner = state
        .borrow()
        .learner
        .clone()
        .ok_or_else(|| p2p::Error("do_connect has not completed yet".into()))?;

    let mut waited_ms = 0;
    loop {
        let found = learner.get(key.as_bytes()).map_err(|e| p2p::Error(e.to_string()))?;
        match found {
            Some(value) => return String::from_utf8(value).map_err(|e| p2p::Error(e.to_string())),
            None if waited_ms < GET_RETRY_BUDGET_MS => {
                gloo_timers::future::TimeoutFuture::new(GET_RETRY_INTERVAL_MS).await;
                waited_ms += GET_RETRY_INTERVAL_MS;
            }
            None => return Err(p2p::Error(format!("key {key:?} not found"))),
        }
    }
}

/// Main-thread handle: the UI's only entry point (see `web-app/README.md`'s
/// `main.js`), wrapping [`shmring_ipc::MainChannel`] with the operations a
/// page actually needs, matching how `MainActivity.kt` drives Android's
/// in-process daemon through `pkg/ipc.Call`. A thin `Rc<MainHandleInner>`
/// wrapper -- required so [`MainHandle::watch_execute`]/
/// [`MainHandle::watch_command_log`] can clone a `'static` handle into a
/// [`wasm_bindgen_futures::spawn_local`] background loop, which a bare
/// `&self`-borrowing method can't do (nothing would keep the borrow alive
/// once the initiating call returns). Every other method still just calls
/// straight through `self.inner`, unaffected by the indirection.
#[wasm_bindgen]
pub struct MainHandle {
    inner: Rc<MainHandleInner>,
}

struct MainHandleInner {
    channel: shmring_ipc::MainChannel,
    signing_key: RefCell<Option<SigningKey>>,
    /// `shmring_ipc::MainChannel` is single-in-flight by design (one
    /// `SharedArrayBuffer`+`onmessage` slot, see that module's own doc
    /// comment) -- unlike `pkg/ipc`'s Android transport, which genuinely
    /// supports concurrent callers (what lets Android's `WatchExecute`
    /// background poll run alongside a foreground `Submit` safely). A
    /// background watch-loop tick calling `channel.call()` while a
    /// foreground call is also in flight would silently corrupt one of
    /// them (the second call's pending-response slot clobbers the
    /// first). This mutex, acquired by every call site that reaches
    /// `channel.call`, serializes all of them -- a correctness fix for
    /// the pre-existing `connect`/`set`/`get`/`send_event` methods too,
    /// not just the new watch loops, none of which were previously
    /// guarded against a concurrent second call either.
    call_lock: futures::lock::Mutex<()>,
    /// This tab's own peer id, cached once [`MainHandle::connect`]
    /// succeeds -- exposed synchronously via [`MainHandle::peer_id`].
    peer_id: RefCell<Option<String>>,
}

impl MainHandleInner {
    /// [`shmring_ipc::MainChannel::call`], serialized through
    /// [`MainHandleInner::call_lock`] -- see that field's doc comment.
    async fn call(&self, req: &Msg, priv_key: Option<&SigningKey>) -> Result<Msg, JsValue> {
        let _guard = self.call_lock.lock().await;
        self.channel.call(req, priv_key).await.map_err(js_err)
    }

    async fn ensure_key(&self) -> Result<SigningKey, JsValue> {
        if let Some(k) = self.signing_key.borrow().clone() {
            return Ok(k);
        }
        let resp = self
            .call(
                &Msg {
                    event_type: shmevent::EVENT_GET_PRIVATE_KEY,
                    id: new_id(),
                    ..Default::default()
                },
                None,
            )
            .await?;
        if resp.event_type == shmevent::EVENT_ERROR {
            // Not routed through into_js_result: that lossy-UTF8-decodes
            // `value`, which is correct for an actual error message but
            // would corrupt the raw key bytes on the success path below.
            return Err(JsValue::from_str(&String::from_utf8_lossy(&resp.value)));
        }
        let seed: [u8; 32] = resp
            .value
            .get(..32)
            .and_then(|s| s.try_into().ok())
            .ok_or_else(|| JsValue::from_str("invalid private key length in response"))?;
        let key = SigningKey::from_bytes(&seed);
        *self.signing_key.borrow_mut() = Some(key.clone());
        Ok(key)
    }

    /// Signs and sends `Msg{event_type, value, id: new_id()}` (the shape
    /// every single-round-trip call below reduces to), rejecting an
    /// `EVENT_ERROR` response.
    async fn call_signed(&self, event_type: u8, value: Vec<u8>) -> Result<Msg, JsValue> {
        let signing_key = self.ensure_key().await?;
        let resp = self
            .call(
                &Msg {
                    event_type,
                    value,
                    id: new_id(),
                    ..Default::default()
                },
                Some(&signing_key),
            )
            .await?;
        if resp.event_type == shmevent::EVENT_ERROR {
            return Err(JsValue::from_str(&String::from_utf8_lossy(&resp.value)));
        }
        Ok(resp)
    }

    /// [`MainHandleInner::call_signed`] with a JSON-encoded request body
    /// against one of `main_ops`'s `OP_*` codes, returning the raw
    /// response bytes (JSON, decoded by the caller into whatever shape
    /// that op returns).
    async fn call_op<Req: serde::Serialize>(&self, op: u8, req: &Req) -> Result<Vec<u8>, JsValue> {
        let value = serde_json::to_vec(req).map_err(|e| JsValue::from_str(&e.to_string()))?;
        Ok(self.call_signed(op, value).await?.value)
    }

    async fn call_op_str<Req: serde::Serialize>(&self, op: u8, req: &Req) -> Result<String, JsValue> {
        let bytes = self.call_op(op, req).await?;
        String::from_utf8(bytes).map_err(|e| JsValue::from_str(&e.to_string()))
    }
}

#[wasm_bindgen]
impl MainHandle {
    #[wasm_bindgen(constructor)]
    pub fn new(worker: web_sys::Worker) -> MainHandle {
        MainHandle {
            inner: Rc::new(MainHandleInner {
                channel: shmring_ipc::MainChannel::new(worker),
                signing_key: RefCell::new(None),
                call_lock: futures::lock::Mutex::new(()),
                peer_id: RefCell::new(None),
            }),
        }
    }

    /// This tab's own peer id, if [`MainHandle::connect`] has succeeded --
    /// `None` beforehand. Synchronous: purely a cached read, no round
    /// trip. Matches `mobile/kvmobile.PeerID`.
    #[wasm_bindgen(js_name = peerId)]
    pub fn peer_id(&self) -> Option<String> {
        self.inner.peer_id.borrow().clone()
    }

    /// Connects to `target_multiaddr` (any cluster member's WebTransport
    /// multiaddr) and joins this tab as a non-voting learner. Resolves to
    /// this tab's own peer id.
    pub async fn connect(&self, target_multiaddr: String) -> Result<String, JsValue> {
        let key = self.inner.ensure_key().await?;
        let resp = self
            .inner
            .call(
                &Msg {
                    event_type: shmevent::EVENT_ADD,
                    value: target_multiaddr.into_bytes(),
                    id: new_id(),
                    ..Default::default()
                },
                Some(&key),
            )
            .await?;
        let peer_id = into_js_result(resp)?;
        *self.inner.peer_id.borrow_mut() = Some(peer_id.clone());
        Ok(peer_id)
    }

    pub async fn set(&self, key: String, value: String) -> Result<String, JsValue> {
        let signing_key = self.inner.ensure_key().await?;
        let set_key_id = new_id();
        let set_key_resp = self
            .inner
            .call(
                &Msg {
                    event_type: shmevent::EVENT_SET_KEY,
                    value: key.into_bytes(),
                    id: set_key_id,
                    ..Default::default()
                },
                Some(&signing_key),
            )
            .await?;
        into_js_result(set_key_resp)?;

        let set_field_resp = self
            .inner
            .call(
                &Msg {
                    event_type: shmevent::EVENT_SET_FIELD,
                    source_id: set_key_id,
                    value: value.into_bytes(),
                    id: new_id(),
                    ..Default::default()
                },
                Some(&signing_key),
            )
            .await?;
        into_js_result(set_field_resp)
    }

    pub async fn get(&self, key: String) -> Result<String, JsValue> {
        let signing_key = self.inner.ensure_key().await?;
        let resp = self
            .inner
            .call(
                &Msg {
                    event_type: shmevent::EVENT_GET_FIELD,
                    value: key.into_bytes(),
                    id: new_id(),
                    ..Default::default()
                },
                Some(&signing_key),
            )
            .await?;
        into_js_result(resp)
    }

    /// Sends one raw event to the Worker and returns its JSON response --
    /// the same human-readable JSON shape `pkg/e2edata.Event`/kvctl-cli
    /// sendevent/`kvmobile.SendEvent` use (e.g.
    /// `{"event":"get_field","value":"hello"}`, see `shmevent::msg_to_json`'s
    /// doc comment for the exact field names and how binary values are
    /// represented). Not used by `main.js`'s UI -- `connect`/`set`/`get`
    /// cover that -- only by the e2e pipeline's Playwright driver, which
    /// needs the same raw-event fidelity kvctl-cli's `sendevent` already
    /// has on desktop/remote and `kvmobile.SendEvent` has on Android,
    /// rather than only this handle's higher-level Set/Get shape.
    ///
    /// Signs with this hop's key (see this module's doc comment, key
    /// relationship 1) unless the event is one of the two bootstrap
    /// exceptions (`get_public_key`/`get_private_key`) -- the same
    /// per-event-type decision `cmd/kvctl-cli`'s `cmdSendEvent` and
    /// `kvmobile.SendEvent` make, since a caller may legitimately want an
    /// unsigned bootstrap fetch through this same entry point.
    pub async fn send_event(&self, event_json: String) -> Result<String, JsValue> {
        web_sys::console::log_1(&format!("kv-raft-web: [main thread] send_event: {event_json}").into());
        let mut req = shmevent::msg_from_json(&event_json).map_err(js_shmevent_err)?;
        if req.id == 0 {
            req.id = new_id();
        }
        let signing_key = if shmevent::requires_signature(req.event_type) {
            Some(self.inner.ensure_key().await?)
        } else {
            None
        };
        let resp = self.inner.call(&req, signing_key.as_ref()).await?;
        shmevent::msg_to_json(&resp).map_err(js_shmevent_err)
    }

    // --- Permits (RequestPermit/RequestLogPermit only -- Confirm/Revoke
    // are voter-gated server-side and permanently unreachable from a
    // non-voting learner, see crate::client's doc comment) ---

    /// Lodges a pending permit record. `kind` is `"peer"` or
    /// `"bootstrap"` (see `shmevent::system::kind_from_name`). Matches
    /// `mobile/kvmobile.RequestPermit`.
    #[wasm_bindgen(js_name = requestPermit)]
    pub async fn request_permit(
        &self,
        kind: String,
        target_peer_id: String,
        metadata: String,
    ) -> Result<(), JsValue> {
        let kind_byte = shmevent::system::kind_from_name(&kind)
            .ok_or_else(|| JsValue::from_str(&format!("unknown permit kind {kind:?}")))?;
        let payload = shmevent::system::encode_permit_request_payload(
            kind_byte,
            target_peer_id.as_bytes(),
            metadata.as_bytes(),
        )
        .map_err(js_shmevent_err)?;
        self.inner
            .call_signed(shmevent::EVENT_PERMIT_REQUEST, payload)
            .await?;
        Ok(())
    }

    /// Lodges a pending log-permit record, scoped by an arbitrary
    /// `log_kind` string. Matches `mobile/kvmobile.RequestLogPermit`.
    #[wasm_bindgen(js_name = requestLogPermit)]
    pub async fn request_log_permit(
        &self,
        log_kind: String,
        target_peer_id: String,
        metadata: String,
    ) -> Result<(), JsValue> {
        let payload = shmevent::logpermit::encode_log_permit_request_payload(
            &log_kind,
            target_peer_id.as_bytes(),
            metadata.as_bytes(),
        )
        .map_err(js_shmevent_err)?;
        self.inner
            .call_signed(shmevent::EVENT_LOG_PERMIT_REQUEST, payload)
            .await?;
        Ok(())
    }

    // --- Catalog reads (no Create/Update/Delete/AddXToY -- voter-gated
    // server-side, see crate::catalog's doc comment) ---

    #[wasm_bindgen(js_name = getGroup)]
    pub async fn get_group(&self, id: String) -> Result<String, JsValue> {
        self.inner
            .call_op_str(main_ops::OP_GET_GROUP, &serde_json::json!({ "id": id }))
            .await
    }

    #[wasm_bindgen(js_name = listGroups)]
    pub async fn list_groups(&self) -> Result<String, JsValue> {
        self.inner
            .call_op_str(main_ops::OP_LIST_GROUPS, &serde_json::json!({}))
            .await
    }

    #[wasm_bindgen(js_name = getCommand)]
    pub async fn get_command(&self, id: String) -> Result<String, JsValue> {
        self.inner
            .call_op_str(main_ops::OP_GET_COMMAND, &serde_json::json!({ "id": id }))
            .await
    }

    #[wasm_bindgen(js_name = listCommands)]
    pub async fn list_commands(&self) -> Result<String, JsValue> {
        self.inner
            .call_op_str(main_ops::OP_LIST_COMMANDS, &serde_json::json!({}))
            .await
    }

    #[wasm_bindgen(js_name = listGroupsForCommand)]
    pub async fn list_groups_for_command(&self, command_id: String) -> Result<String, JsValue> {
        self.inner
            .call_op_str(
                main_ops::OP_LIST_GROUPS_FOR_COMMAND,
                &serde_json::json!({ "command_id": command_id }),
            )
            .await
    }

    #[wasm_bindgen(js_name = listGroupsForPeer)]
    pub async fn list_groups_for_peer(&self, peer_id: String) -> Result<String, JsValue> {
        self.inner
            .call_op_str(
                main_ops::OP_LIST_GROUPS_FOR_PEER,
                &serde_json::json!({ "peer_id": peer_id }),
            )
            .await
    }

    // --- Dispatch ---

    #[wasm_bindgen(js_name = submitCommand)]
    pub async fn submit_command(&self, command_id: String, inputs_json: String) -> Result<String, JsValue> {
        let resp = self
            .inner
            .call_op(
                main_ops::OP_SUBMIT_COMMAND,
                &serde_json::json!({ "command_id": command_id, "inputs_json": inputs_json }),
            )
            .await?;
        let parsed: serde_json::Value =
            serde_json::from_slice(&resp).map_err(|e| JsValue::from_str(&e.to_string()))?;
        parsed
            .get("instance_id")
            .and_then(|v| v.as_str())
            .map(str::to_string)
            .ok_or_else(|| JsValue::from_str("submit_command: malformed response"))
    }

    #[wasm_bindgen(js_name = getCommandRequest)]
    pub async fn get_command_request(&self, command_id: String, instance_id: String) -> Result<String, JsValue> {
        self.inner
            .call_op_str(
                main_ops::OP_GET_COMMAND_REQUEST,
                &serde_json::json!({ "command_id": command_id, "instance_id": instance_id }),
            )
            .await
    }

    #[wasm_bindgen(js_name = listCommandRequests)]
    pub async fn list_command_requests(&self, command_id: String) -> Result<String, JsValue> {
        self.inner
            .call_op_str(
                main_ops::OP_LIST_COMMAND_REQUESTS,
                &serde_json::json!({ "command_id": command_id }),
            )
            .await
    }

    #[wasm_bindgen(js_name = listExecutionsByPeer)]
    pub async fn list_executions_by_peer(&self, peer_id: String) -> Result<String, JsValue> {
        self.inner
            .call_op_str(
                main_ops::OP_LIST_EXECUTIONS_BY_PEER,
                &serde_json::json!({ "peer_id": peer_id }),
            )
            .await
    }

    #[wasm_bindgen(js_name = appendCommandLog)]
    pub async fn append_command_log(
        &self,
        requester_peer_id: String,
        instance_id: String,
        fields_json: String,
        narrative: String,
    ) -> Result<(), JsValue> {
        self.inner
            .call_op(
                main_ops::OP_APPEND_COMMAND_LOG,
                &serde_json::json!({
                    "requester_peer_id": requester_peer_id,
                    "instance_id": instance_id,
                    "fields_json": fields_json,
                    "narrative": narrative,
                }),
            )
            .await?;
        Ok(())
    }

    #[wasm_bindgen(js_name = queryCommandLog)]
    pub async fn query_command_log(
        &self,
        instance_id: String,
        since: String,
        until: String,
        limit: String,
    ) -> Result<String, JsValue> {
        self.inner
            .call_op_str(
                main_ops::OP_QUERY_COMMAND_LOG,
                &serde_json::json!({ "instance_id": instance_id, "since": since, "until": until, "limit": limit }),
            )
            .await
    }

    #[wasm_bindgen(js_name = latestCommandLog)]
    pub async fn latest_command_log(&self, instance_id: String) -> Result<String, JsValue> {
        self.inner
            .call_op_str(
                main_ops::OP_LATEST_COMMAND_LOG,
                &serde_json::json!({ "instance_id": instance_id }),
            )
            .await
    }

    // --- Generic log records ---

    #[wasm_bindgen(js_name = logAppend)]
    pub async fn log_append(
        &self,
        kind: String,
        unit_id: String,
        fields_json: String,
        narrative: String,
    ) -> Result<(), JsValue> {
        self.inner
            .call_op(
                main_ops::OP_LOG_APPEND,
                &serde_json::json!({ "kind": kind, "unit_id": unit_id, "fields_json": fields_json, "narrative": narrative }),
            )
            .await?;
        Ok(())
    }

    #[wasm_bindgen(js_name = logQuery)]
    pub async fn log_query(
        &self,
        kind: String,
        unit_id: String,
        since: String,
        until: String,
        limit: String,
    ) -> Result<String, JsValue> {
        self.inner
            .call_op_str(
                main_ops::OP_LOG_QUERY,
                &serde_json::json!({ "kind": kind, "unit_id": unit_id, "since": since, "until": until, "limit": limit }),
            )
            .await
    }

    // --- Execute ---

    /// Direct, unreplicated peer-to-peer notification to `dest_peer_id`.
    /// Matches `mobile/kvmobile.Execute`.
    pub async fn execute(&self, dest_peer_id: String, value: String) -> Result<(), JsValue> {
        let payload = shmevent::encode_set_payload(dest_peer_id.as_bytes(), value.as_bytes())
            .map_err(js_shmevent_err)?;
        self.inner.call_signed(shmevent::EVENT_EXECUTE, payload).await?;
        Ok(())
    }

    /// Drains one queued [`MainHandle::execute`] notification addressed to
    /// this node, JSON-encoded as `{"pending":bool,"sender_peer_id":...,
    /// "value":...}` (matching `mobile/kvmobile.PollExecute`'s
    /// `pollExecuteResult` shape exactly).
    #[wasm_bindgen(js_name = pollExecute)]
    pub async fn poll_execute(&self) -> Result<String, JsValue> {
        let resp = self
            .inner
            .call_signed(shmevent::EVENT_POLL_EXECUTE, Vec::new())
            .await?;
        let out = if resp.value.is_empty() {
            serde_json::json!({ "pending": false })
        } else {
            let (sender_peer_id, payload) =
                shmevent::decode_execute_notification(&resp.value).map_err(js_shmevent_err)?;
            serde_json::json!({
                "pending": true,
                "sender_peer_id": String::from_utf8_lossy(sender_peer_id),
                "value": String::from_utf8_lossy(payload),
            })
        };
        serde_json::to_string(&out).map_err(|e| JsValue::from_str(&e.to_string()))
    }

    /// Polls [`MainHandle::poll_execute`] in the background (tight loop
    /// while draining a backlog, `WATCH_EXECUTE_POLL_INTERVAL_MS` between
    /// empty polls -- matches `mobile/kvmobile`'s
    /// `watchExecutePollInterval`), invoking `cb(sender_peer_id, value)`
    /// for each notification found, until the returned [`WatchHandle`] is
    /// stopped. Matches `mobile/kvmobile.WatchExecute`.
    #[wasm_bindgen(js_name = watchExecute)]
    pub fn watch_execute(&self, cb: js_sys::Function) -> WatchHandle {
        spawn_watch_loop(self.inner.clone(), WatchKind::Execute, cb)
    }

    /// Polls [`MainHandle::query_command_log`] for `instance_id` every
    /// `WATCH_COMMAND_LOG_POLL_INTERVAL_MS` (matches `mobile/kvmobile`'s
    /// `watchCommandLogPollInterval`), invoking `cb(records_json)` with
    /// whatever's new since the last poll, until the returned
    /// [`WatchHandle`] is stopped. Matches
    /// `mobile/kvmobile.WatchCommandLog`.
    #[wasm_bindgen(js_name = watchCommandLog)]
    pub fn watch_command_log(&self, instance_id: String, cb: js_sys::Function) -> WatchHandle {
        spawn_watch_loop(self.inner.clone(), WatchKind::CommandLog(instance_id), cb)
    }
}

fn js_err(e: shmring_ipc::Error) -> JsValue {
    JsValue::from_str(&e.to_string())
}

fn js_shmevent_err(e: shmevent::Error) -> JsValue {
    JsValue::from_str(&e.to_string())
}

fn into_js_result(resp: Msg) -> Result<String, JsValue> {
    if resp.event_type == shmevent::EVENT_ERROR {
        Err(JsValue::from_str(&String::from_utf8_lossy(&resp.value)))
    } else {
        Ok(String::from_utf8_lossy(&resp.value).into_owned())
    }
}

// --- watch_execute / watch_command_log ---

/// How often `watch_execute` polls when nothing's queued -- tight-loops
/// (no wait) instead while draining a backlog, to catch up fast. Matches
/// `mobile/kvmobile`'s `watchExecutePollInterval`.
const WATCH_EXECUTE_POLL_INTERVAL_MS: u32 = 200;

/// How often `watch_command_log` re-queries -- a real replicated-store
/// read each tick (unlike `watch_execute`'s in-memory queue drain), so
/// deliberately longer. Matches `mobile/kvmobile`'s
/// `watchCommandLogPollInterval`.
const WATCH_COMMAND_LOG_POLL_INTERVAL_MS: u32 = 1_500;

enum WatchKind {
    Execute,
    CommandLog(String),
}

/// A background [`MainHandle::watch_execute`]/
/// [`MainHandle::watch_command_log`] loop's stop handle -- mirrors
/// `mobile/kvmobile`'s `StopWatchExecute`/`StopWatchCommandLog`, which
/// block until the loop has actually exited rather than
/// firing-and-forgetting.
#[wasm_bindgen]
pub struct WatchHandle {
    stop_flag: Rc<Cell<bool>>,
    done: Rc<RefCell<Option<oneshot::Receiver<()>>>>,
}

#[wasm_bindgen]
impl WatchHandle {
    /// Signals the loop to stop and returns a `Promise` that resolves
    /// once it actually has. Not cosmetic: without waiting, a caller
    /// could `stop()` and immediately start a new watch for the same
    /// thing, and the old loop's very next tick could still be in
    /// flight, racing the new one for `MainHandleInner::call_lock`.
    pub fn stop(&self) -> js_sys::Promise {
        self.stop_flag.set(true);
        let done = self.done.clone();
        wasm_bindgen_futures::future_to_promise(async move {
            if let Some(rx) = done.borrow_mut().take() {
                let _ = rx.await;
            }
            Ok(JsValue::UNDEFINED)
        })
    }
}

fn spawn_watch_loop(inner: Rc<MainHandleInner>, kind: WatchKind, cb: js_sys::Function) -> WatchHandle {
    let stop_flag = Rc::new(Cell::new(false));
    let (tx, rx) = oneshot::channel();

    let loop_stop_flag = stop_flag.clone();
    wasm_bindgen_futures::spawn_local(async move {
        match kind {
            WatchKind::Execute => run_execute_watch(inner, &loop_stop_flag, &cb).await,
            WatchKind::CommandLog(instance_id) => {
                run_command_log_watch(inner, &instance_id, &loop_stop_flag, &cb).await
            }
        }
        let _ = tx.send(());
    });

    WatchHandle {
        stop_flag,
        done: Rc::new(RefCell::new(Some(rx))),
    }
}

/// [`MainHandle::watch_execute`]'s loop body -- drains
/// `EVENT_POLL_EXECUTE` in a tight loop while notifications keep being
/// found, sleeping [`WATCH_EXECUTE_POLL_INTERVAL_MS`] between polls once
/// the queue is empty.
async fn run_execute_watch(inner: Rc<MainHandleInner>, stop_flag: &Cell<bool>, cb: &js_sys::Function) {
    loop {
        if stop_flag.get() {
            return;
        }
        let found = match inner.call_signed(shmevent::EVENT_POLL_EXECUTE, Vec::new()).await {
            Ok(msg) if !msg.value.is_empty() => match shmevent::decode_execute_notification(&msg.value) {
                Ok((sender_peer_id, payload)) => {
                    invoke_callback2(
                        cb,
                        &String::from_utf8_lossy(sender_peer_id),
                        &String::from_utf8_lossy(payload),
                    );
                    true
                }
                Err(_) => false,
            },
            _ => false,
        };
        if stop_flag.get() {
            return;
        }
        if !found {
            gloo_timers::future::TimeoutFuture::new(WATCH_EXECUTE_POLL_INTERVAL_MS).await;
        }
    }
}

/// [`MainHandle::watch_command_log`]'s loop body -- re-queries
/// `instance_id`'s command log every [`WATCH_COMMAND_LOG_POLL_INTERVAL_MS`],
/// advancing `since` past the newest record already delivered so each
/// round only asks for what's new.
async fn run_command_log_watch(
    inner: Rc<MainHandleInner>,
    instance_id: &str,
    stop_flag: &Cell<bool>,
    cb: &js_sys::Function,
) {
    let mut since: Option<time::OffsetDateTime> = None;
    loop {
        gloo_timers::future::TimeoutFuture::new(WATCH_COMMAND_LOG_POLL_INTERVAL_MS).await;
        if stop_flag.get() {
            return;
        }

        let since_str = since
            .and_then(|t| t.format(&time::format_description::well_known::Rfc3339).ok())
            .unwrap_or_default();
        let resp = inner
            .call_op(
                main_ops::OP_QUERY_COMMAND_LOG,
                &serde_json::json!({ "instance_id": instance_id, "since": since_str, "until": "", "limit": "" }),
            )
            .await;
        let Ok(bytes) = resp else { continue };
        let Ok(records) = serde_json::from_slice::<Vec<crate::logrecord::Record>>(&bytes) else {
            continue;
        };
        if records.is_empty() {
            continue;
        }

        invoke_callback1(cb, &String::from_utf8_lossy(&bytes));
        since = records.last().map(|r| r.timestamp + time::Duration::nanoseconds(1));
    }
}

fn invoke_callback1(cb: &js_sys::Function, arg: &str) {
    if let Err(e) = cb.call1(&JsValue::NULL, &JsValue::from_str(arg)) {
        web_sys::console::error_1(&e);
    }
}

fn invoke_callback2(cb: &js_sys::Function, arg1: &str, arg2: &str) {
    if let Err(e) = cb.call2(&JsValue::NULL, &JsValue::from_str(arg1), &JsValue::from_str(arg2)) {
        web_sys::console::error_1(&e);
    }
}
