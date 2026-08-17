//! Worker-side session/helper layer -- the analog of `pkg/shmclient`'s
//! job, split explicitly into local vs. remote per the architecture
//! finding that drives this whole port (see the plan this module was
//! built from): `mobile/kvmobile` always targets its own peer id, so its
//! reads are answered by its own already-raft-replicated local store,
//! never over the network. This tab's direct equivalent of that local
//! store is [`crate::learner::Learner`] -- reads go through
//! [`local_get`]/[`local_scan_range`]/[`local_scan_all`], never
//! `call_client_protocol`. Only writes and `Execute`/`PollExecute` need
//! [`RemoteSession`].
//!
//! [`RemoteSession`] only wraps the calls that genuinely need Worker-side
//! state to build (`log_append`/`append_record`'s author peer id,
//! `execute`'s two-`EventSetKey`-registrations-then-notify sequence) --
//! `request_permit`/`poll_execute` have no such need
//! (their wire payload is fully determined by the caller's own arguments),
//! so `MainHandle` on the main-thread side builds those directly and
//! sends them through `app.rs::handle_request`'s generic proxy arm rather
//! than through a redundant Worker-side wrapper here.
//!
//! `permitConfirm`/`permitRevoke`, the two join-invite and two exec-invite
//! lifecycle variants, and all ten Group/Command/Station/GroupCommand/
//! PeerGroup Put/Delete variants are refused by `pkg/daemon.handleShmEvent`
//! for any remote caller that isn't a current raft voter -- web-app can
//! never satisfy that, so no wrapper is provided for any of the sixteen
//! (see `pkg/daemon/daemon.go`'s dispatch switch, each of those sixteen
//! cases calling `requireVoter(n, caller)` before touching the store).
#![cfg(target_arch = "wasm32")]

use std::cell::RefCell;
use std::collections::HashMap;
use std::ops::ControlFlow;
use std::rc::Rc;

use ed25519_dalek::SigningKey;
use libp2p::PeerId;
use time::OffsetDateTime;

use crate::app::{new_id, reject_if_error, WorkerState};
use crate::learner::Learner;
use crate::logrecord;
use crate::p2p::{self, Error};
use crate::shmevent::{self, Body, Msg};

/// Bounds Group/Command ids and every `pkg/logrecord` unit_id this crate
/// writes -- [`kind_prefix_bounds`]'s fixed-width upper bound is built
/// from this same constant. Matches `pkg/kvctl/catalog.go`'s
/// `maxCatalogIDLen`.
pub(crate) const MAX_CATALOG_ID_LEN: usize = 256;

/// Current wall-clock time -- `wasm32-unknown-unknown` has no
/// `SystemTime`, so "now" comes from `js_sys::Date::now()` (millisecond
/// resolution; `logrecord`'s own 8-byte random suffix already exists to
/// disambiguate same-timestamp records, so the precision loss versus
/// Go's nanosecond `time.Now()` is harmless).
pub(crate) fn now() -> OffsetDateTime {
    let millis = js_sys::Date::now();
    OffsetDateTime::from_unix_timestamp_nanos((millis as i128) * 1_000_000)
        .unwrap_or(OffsetDateTime::UNIX_EPOCH)
}

fn parse_rfc3339(s: &str) -> Result<OffsetDateTime, Error> {
    OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339)
        .map_err(|e| Error(format!("{s:?}: {e}")))
}

/// Converts a [`shmevent::Error`] (payload encode/decode failures) into
/// this layer's [`p2p::Error`] -- both are a plain `String` message, this
/// is purely a type-boundary conversion, not a semantic one.
fn se(e: shmevent::Error) -> Error {
    Error(e.0)
}

/// This tab's own locally replicated read -- see [`Learner::get`]'s doc
/// comment for the "may lag a moment behind a just-committed remote Set"
/// caveat every raft follower's local read carries.
pub fn local_get(learner: &Learner, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
    learner.get(key).map_err(|e| Error(e.to_string()))
}

/// This tab's own locally replicated range read -- one page at a time
/// (see [`crate::sqlite_store::SqliteStore::kv_scan_range`]'s doc
/// comment). [`local_scan_all`] is almost always what a caller actually
/// wants; this is exposed separately for callers that want to control
/// their own scan loop (e.g. an ACL point-check that only needs to know
/// whether a range is non-empty).
pub fn local_scan_range(
    learner: &Learner,
    start: &[u8],
    end: &[u8],
) -> Result<Option<(Vec<u8>, Vec<u8>)>, Error> {
    learner.scan_range(start, end).map_err(|e| Error(e.to_string()))
}

/// Walks every `(key, value)` pair in `[start, end]`, ascending, calling
/// `on_pair` for each -- the generic "loop, narrow `start` past the last
/// returned key each round" helper every Go `List*`/`scanRevisions`/
/// `listUnitIDs` function reduces to (see e.g. `pkg/kvctl/catalog.go`'s
/// `scanRevisions`). Unlike the Go original, this has no per-page network
/// cost (a plain local `SqliteStore` read), so it can be a plain
/// synchronous loop rather than an async one making repeated
/// `EventListRange` round trips. `on_pair` returns
/// [`ControlFlow::Break`] to stop early (e.g. once a caller has found
/// what it's looking for).
pub fn local_scan_all(
    learner: &Learner,
    start: &[u8],
    end: &[u8],
    mut on_pair: impl FnMut(&[u8], &[u8]) -> ControlFlow<()>,
) -> Result<(), Error> {
    let mut lo = start.to_vec();
    loop {
        match local_scan_range(learner, &lo, end)? {
            None => return Ok(()),
            Some((key, value)) => {
                if on_pair(&key, &value).is_break() {
                    return Ok(());
                }
                lo = key;
                lo.push(0x00);
            }
        }
    }
}

/// A single-use handle for talking to the leader this tab is connected
/// to, cloned out of [`WorkerState`] in one short borrow (never held
/// across an `.await` -- see this crate's `Rc<RefCell<WorkerState>>`
/// reentrancy discipline). Mirrors `pkg/shmclient.Session`, restricted to
/// the subset of events actually reachable from a non-voting learner (see
/// this module's doc comment).
pub struct RemoteSession {
    handle: p2p::Handle,
    leader: PeerId,
    signing_key: SigningKey,
}

impl RemoteSession {
    /// Clones what's needed for a round of remote calls out of `state` in
    /// one short borrow. Fails if `do_connect` hasn't completed yet (no
    /// leader to call).
    pub fn from_worker_state(state: &Rc<RefCell<WorkerState>>) -> Result<Self, Error> {
        let guard = state.borrow();
        let leader = guard
            .leader
            .ok_or_else(|| Error("do_connect has not completed yet".into()))?;
        Ok(RemoteSession {
            handle: guard.handle.clone(),
            leader,
            signing_key: guard.signing_key.clone(),
        })
    }

    /// This tab's own peer id, as advertised to the leader -- e.g. the
    /// sender identity [`RemoteSession::execute`] registers.
    pub fn local_peer_id(&self) -> PeerId {
        self.handle.local_peer_id()
    }

    /// Sends `msg` to the leader over `call_client_protocol`, signed with
    /// this tab's own key, and rejects an `EVENT_ERROR` response. The
    /// shared primitive every typed wrapper below reduces to.
    pub async fn call_msg(&mut self, msg: Msg) -> Result<Msg, Error> {
        let resp = self
            .handle
            .call_client_protocol(self.leader, &msg, Some(&self.signing_key))
            .await?;
        reject_if_error(&resp)?;
        Ok(resp)
    }

    /// [`RemoteSession::call_msg`] for the common case: `body` under a
    /// fresh correlation id.
    pub async fn call(&mut self, body: Body) -> Result<Msg, Error> {
        self.call_msg(Msg::with_id(new_id(), body)).await
    }

    /// Writes one `pkg/logrecord` record. `key` must already carry
    /// `logrecord::LOG_KEY_PREFIX` (see `crate::logrecord::build_key`) --
    /// matches `pkg/shmevent.EventLogAppend`'s own requirement, enforced
    /// server-side (`pkg/daemon` rejects any other key here). Not
    /// voter-gated -- see `pkg/shmevent.EventLogAppend`'s doc comment.
    pub async fn log_append(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        self.call(Body::LogAppend {
            key: Some(key.to_vec()),
            value: Some(value.to_vec()),
        })
        .await?;
        Ok(())
    }

    /// Direct, unreplicated peer-to-peer notification to `dest_peer_id` --
    /// see `api/shmevent.capnp`'s `execute` doc comment. Registers this
    /// tab's own peer id and `dest_peer_id` under fresh correlation ids
    /// first (`setKey` x2), mirroring `pkg/shmclient.Session.Execute`'s same
    /// two-registration-then-notify sequence: the `execute` variant's
    /// `sourceId`/`destinationId` are references to those registrations,
    /// which is the local-IPC shape of that variant (the network leg names
    /// `senderPeerId` directly instead -- see its doc comment).
    pub async fn execute(&mut self, dest_peer_id: &str, payload: &[u8]) -> Result<(), Error> {
        let own_id = self.local_peer_id().to_string();

        let source_id = new_id();
        self.call_msg(Msg::with_id(
            source_id,
            Body::SetKey {
                value: Some(own_id.into_bytes()),
            },
        ))
        .await?;

        let destination_id = new_id();
        self.call_msg(Msg::with_id(
            destination_id,
            Body::SetKey {
                value: Some(dest_peer_id.as_bytes().to_vec()),
            },
        ))
        .await?;

        self.call(Body::Execute {
            source_id,
            destination_id,
            sender_peer_id: String::new(),
            value: Some(payload.to_vec()),
        })
        .await?;
        Ok(())
    }

}

/// [`scan_revisions`]' result: a `unit_id`'s latest revision, plus
/// who/when first created it (kept separately since "latest" overwrites
/// `timestamp`/`author_peer_id` on every update). Matches
/// `pkg/kvctl/catalog.go`'s `revisionHistory`.
pub struct RevisionHistory {
    pub latest: logrecord::Record,
    pub created_at: OffsetDateTime,
    pub created_by: String,
    pub found: bool,
}

/// Folds every [`logrecord::Record`] for `(kind, unit_id)` down to its
/// latest revision. A local read (see this module's doc comment) --
/// unlike Go's `scanRevisions`, this needs no per-page network round
/// trip, so it's a plain synchronous loop via [`local_scan_all`]. Matches
/// `pkg/kvctl/catalog.go`'s `scanRevisions`.
pub fn scan_revisions(learner: &Learner, kind: &str, unit_id: &str) -> Result<RevisionHistory, Error> {
    let (lo, hi) = logrecord::scan_bounds(kind, unit_id, OffsetDateTime::UNIX_EPOCH, now());
    let mut h = RevisionHistory {
        latest: logrecord::Record {
            kind: String::new(),
            unit_id: String::new(),
            timestamp: OffsetDateTime::UNIX_EPOCH,
            author_peer_id: String::new(),
            fields: HashMap::new(),
            narrative: String::new(),
        },
        created_at: OffsetDateTime::UNIX_EPOCH,
        created_by: String::new(),
        found: false,
    };
    let mut err = None;
    local_scan_all(learner, &lo, &hi, |_key, value| match logrecord::Record::decode(value) {
        Ok(rec) => {
            if !h.found {
                h.created_at = rec.timestamp;
                h.created_by = rec.author_peer_id.clone();
            }
            h.latest = rec;
            h.found = true;
            ControlFlow::Continue(())
        }
        Err(e) => {
            err = Some(se(e));
            ControlFlow::Break(())
        }
    })?;
    if let Some(e) = err {
        return Err(e);
    }
    Ok(h)
}

/// `[lo, hi]` key range covering every record of `kind`, across every
/// `unit_id` and timestamp -- shared bound construction behind
/// [`list_unit_ids`] and `dispatch::list_executions_by_peer`'s per-kind
/// prefix scans. Matches `pkg/kvctl/catalog.go`'s `kindPrefixBounds`.
pub fn kind_prefix_bounds(kind: &str) -> (Vec<u8>, Vec<u8>) {
    let prefix = logrecord::kind_prefix(kind);
    let lo = prefix.clone();
    let mut hi = prefix;
    hi.extend(std::iter::repeat(0xFFu8).take(2 + MAX_CATALOG_ID_LEN + 8 + 8));
    (lo, hi)
}

/// Enumerates every distinct `unit_id` that has ever logged a record of
/// `kind`, in ascending key order -- multiple revisions of the same
/// `unit_id` are deduplicated, keeping first-seen order. Matches
/// `pkg/kvctl/catalog.go`'s `listUnitIDs`.
pub fn list_unit_ids(learner: &Learner, kind: &str) -> Result<Vec<String>, Error> {
    let (lo, hi) = kind_prefix_bounds(kind);
    let mut seen = std::collections::HashSet::new();
    let mut ids = Vec::new();
    let mut err = None;
    local_scan_all(learner, &lo, &hi, |key, _value| match logrecord::parse_key(key) {
        Ok((_kind, unit_id, _ts)) => {
            if seen.insert(unit_id.clone()) {
                ids.push(unit_id);
            }
            ControlFlow::Continue(())
        }
        Err(e) => {
            err = Some(se(e));
            ControlFlow::Break(())
        }
    })?;
    if let Some(e) = err {
        return Err(e);
    }
    Ok(ids)
}

/// Builds and appends one [`logrecord::Record`], attributed to
/// `author_peer_id` -- the shared tail end every dispatch write reduces
/// to. Matches `pkg/kvctl/catalog.go`'s `appendRecord`.
pub async fn append_record(
    sess: &mut RemoteSession,
    kind: &str,
    unit_id: &str,
    author_peer_id: &str,
    fields: HashMap<String, String>,
    narrative: &str,
) -> Result<(), Error> {
    let rnd = logrecord::new_rand().map_err(se)?;
    let ts = now();
    let key = logrecord::build_key(kind, unit_id, ts, rnd).map_err(se)?;
    let rec = logrecord::Record {
        kind: kind.to_string(),
        unit_id: unit_id.to_string(),
        timestamp: ts,
        author_peer_id: author_peer_id.to_string(),
        fields,
        narrative: narrative.to_string(),
    };
    let value = rec.encode().map_err(se)?;
    sess.log_append(&key, &value).await
}

/// This tab's own peer id, `fields_json`-decoded, then
/// [`append_record`]-written -- the generic `LogAppend(kind, unit_id,
/// fields_json, narrative)` operation `MainHandle::log_append` and
/// `dispatch::append_command_log` both reduce to. Matches
/// `mobile/kvmobile.LogAppend`.
pub async fn log_append_json(
    state: &Rc<RefCell<WorkerState>>,
    kind: &str,
    unit_id: &str,
    fields_json: &str,
    narrative: &str,
) -> Result<(), Error> {
    let fields: HashMap<String, String> = if fields_json.is_empty() {
        HashMap::new()
    } else {
        serde_json::from_str(fields_json).map_err(|e| Error(format!("decode fieldsJSON: {e}")))?
    };
    let mut sess = RemoteSession::from_worker_state(state)?;
    let author_peer_id = sess.local_peer_id().to_string();
    append_record(&mut sess, kind, unit_id, &author_peer_id, fields, narrative).await
}

/// Lists every [`logrecord::Record`] for `(kind, unit_id)` with a
/// timestamp in `[since, until]`, oldest first, up to `limit` records.
/// `since`/`until` are RFC3339 or `""` (`since` `""` = unbounded, `until`
/// `""` = now); `limit` is a count or `""` (no limit). Matches
/// `mobile/kvmobile.LogQuery`.
pub fn log_query_json(
    learner: &Learner,
    kind: &str,
    unit_id: &str,
    since: &str,
    until: &str,
    limit: &str,
) -> Result<Vec<logrecord::Record>, Error> {
    let start = if since.is_empty() {
        OffsetDateTime::UNIX_EPOCH
    } else {
        parse_rfc3339(since)?
    };
    let end = if until.is_empty() { now() } else { parse_rfc3339(until)? };
    let n: usize = if limit.is_empty() {
        0
    } else {
        limit
            .parse()
            .map_err(|_| Error(format!("limit: invalid number {limit:?}")))?
    };

    let (lo, hi) = logrecord::scan_bounds(kind, unit_id, start, end);
    let mut records = Vec::new();
    let mut err = None;
    local_scan_all(learner, &lo, &hi, |_key, value| {
        if n > 0 && records.len() >= n {
            return ControlFlow::Break(());
        }
        match logrecord::Record::decode(value) {
            Ok(rec) => {
                records.push(rec);
                ControlFlow::Continue(())
            }
            Err(e) => {
                err = Some(se(e));
                ControlFlow::Break(())
            }
        }
    })?;
    if let Some(e) = err {
        return Err(e);
    }
    Ok(records)
}
