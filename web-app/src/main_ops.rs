//! A small op-code band (`u8` 100-127) on the main-thread<->Worker
//! `shmring_ipc` hop only -- **never** sent to a remote peer, unlike
//! `shmevent::EVENT_*` (1-27, 255). No Go analog: `mobile/kvmobile`'s
//! Kotlin/Java FFI boundary calls straight into Go functions with real
//! typed arguments, so it never needed an operation-code-plus-JSON-body
//! encoding the way this crate's `Msg`-shaped main-thread<->Worker
//! channel does. Everything here is a multi-step operation (a local scan
//! loop, or a local read plus a remote write) that doesn't reduce to one
//! `shmevent` wire event the way `app.rs`'s generic proxy arm handles
//! `EVENT_SET`/`EVENT_PERMIT_REQUEST`/etc.
//!
//! Each op's request/response body is small JSON (reusing `serde_json`,
//! already a dependency) -- [`dispatch`] is the one entry point
//! `app.rs::handle_request` calls into.
#![cfg(target_arch = "wasm32")]

use std::cell::RefCell;
use std::rc::Rc;

use crate::app::WorkerState;
use crate::catalog;
use crate::dispatch;

pub const OP_GET_GROUP: u8 = 100;
pub const OP_LIST_GROUPS: u8 = 101;
pub const OP_GET_COMMAND: u8 = 102;
pub const OP_LIST_COMMANDS: u8 = 103;
pub const OP_LIST_GROUPS_FOR_COMMAND: u8 = 104;
pub const OP_LIST_GROUPS_FOR_PEER: u8 = 105;
pub const OP_SUBMIT_COMMAND: u8 = 106;
pub const OP_GET_COMMAND_REQUEST: u8 = 107;
pub const OP_LIST_COMMAND_REQUESTS: u8 = 108;
pub const OP_LIST_EXECUTIONS_BY_PEER: u8 = 109;
pub const OP_QUERY_COMMAND_LOG: u8 = 110;
pub const OP_LATEST_COMMAND_LOG: u8 = 111;
pub const OP_LOG_QUERY: u8 = 112;
pub const OP_APPEND_COMMAND_LOG: u8 = 113;
pub const OP_LOG_APPEND: u8 = 114;

/// The op-code band's own lower bound, so `app.rs::handle_request` can
/// route anything `>= OP_BASE` here in one match arm instead of listing
/// every op individually.
pub const OP_BASE: u8 = 100;

fn learner_or_err(state: &Rc<RefCell<WorkerState>>) -> Result<Rc<crate::learner::Learner>, String> {
    state
        .borrow()
        .learner
        .clone()
        .ok_or_else(|| "do_connect has not completed yet".to_string())
}

fn decode<T: serde::de::DeserializeOwned>(value: &[u8]) -> Result<T, String> {
    serde_json::from_slice(value).map_err(|e| format!("decode request: {e}"))
}

fn encode<T: serde::Serialize>(v: &T) -> Result<Vec<u8>, String> {
    serde_json::to_vec(v).map_err(|e| format!("encode response: {e}"))
}

#[derive(serde::Deserialize)]
struct IdReq {
    id: String,
}
#[derive(serde::Deserialize)]
struct CommandIdReq {
    command_id: String,
}
#[derive(serde::Deserialize)]
struct PeerIdReq {
    peer_id: String,
}
#[derive(serde::Deserialize)]
struct SubmitCommandReq {
    command_id: String,
    inputs_json: String,
}
#[derive(serde::Serialize)]
struct InstanceIdResp {
    instance_id: String,
}
#[derive(serde::Deserialize)]
struct CommandRequestReq {
    command_id: String,
    instance_id: String,
}
#[derive(serde::Deserialize)]
struct AppendCommandLogReq {
    requester_peer_id: String,
    instance_id: String,
    fields_json: String,
    narrative: String,
}
#[derive(serde::Deserialize)]
struct CommandLogQueryReq {
    instance_id: String,
    since: String,
    until: String,
    limit: String,
}
#[derive(serde::Deserialize)]
struct InstanceIdReq {
    instance_id: String,
}
#[derive(serde::Deserialize)]
struct LogQueryReq {
    kind: String,
    unit_id: String,
    since: String,
    until: String,
    limit: String,
}
#[derive(serde::Deserialize)]
struct LogAppendReq {
    kind: String,
    unit_id: String,
    fields_json: String,
    narrative: String,
}

/// Routes one main-thread<->Worker op to its handler, returning the
/// encoded JSON response body. Errors are plain strings -- the caller
/// (`app.rs::handle_request`) wraps them into an `EVENT_ERROR` `Msg` the
/// same way every other failure on this hop already is.
pub async fn dispatch(state: &Rc<RefCell<WorkerState>>, op: u8, value: &[u8]) -> Result<Vec<u8>, String> {
    match op {
        OP_GET_GROUP => {
            let req: IdReq = decode(value)?;
            let learner = learner_or_err(state)?;
            encode(&catalog::get_group(&learner, &req.id)?)
        }
        OP_LIST_GROUPS => {
            let learner = learner_or_err(state)?;
            encode(&catalog::list_groups(&learner)?)
        }
        OP_GET_COMMAND => {
            let req: IdReq = decode(value)?;
            let learner = learner_or_err(state)?;
            encode(&catalog::get_command(&learner, &req.id)?)
        }
        OP_LIST_COMMANDS => {
            let learner = learner_or_err(state)?;
            encode(&catalog::list_commands(&learner)?)
        }
        OP_LIST_GROUPS_FOR_COMMAND => {
            let req: CommandIdReq = decode(value)?;
            let learner = learner_or_err(state)?;
            encode(&catalog::list_groups_for_command(&learner, &req.command_id)?)
        }
        OP_LIST_GROUPS_FOR_PEER => {
            let req: PeerIdReq = decode(value)?;
            let learner = learner_or_err(state)?;
            encode(&catalog::list_groups_for_peer(&learner, &req.peer_id)?)
        }
        OP_SUBMIT_COMMAND => {
            let req: SubmitCommandReq = decode(value)?;
            let instance_id = dispatch::submit_command(state, &req.command_id, &req.inputs_json).await?;
            encode(&InstanceIdResp { instance_id })
        }
        OP_GET_COMMAND_REQUEST => {
            let req: CommandRequestReq = decode(value)?;
            let learner = learner_or_err(state)?;
            encode(&dispatch::get_command_request(&learner, &req.command_id, &req.instance_id)?)
        }
        OP_LIST_COMMAND_REQUESTS => {
            let req: CommandIdReq = decode(value)?;
            let learner = learner_or_err(state)?;
            encode(&dispatch::list_command_requests(&learner, &req.command_id)?)
        }
        OP_LIST_EXECUTIONS_BY_PEER => {
            let req: PeerIdReq = decode(value)?;
            let learner = learner_or_err(state)?;
            encode(&dispatch::list_executions_by_peer(&learner, &req.peer_id)?)
        }
        OP_APPEND_COMMAND_LOG => {
            let req: AppendCommandLogReq = decode(value)?;
            dispatch::append_command_log(
                state,
                &req.requester_peer_id,
                &req.instance_id,
                &req.fields_json,
                &req.narrative,
            )
            .await?;
            encode(&())
        }
        OP_QUERY_COMMAND_LOG => {
            let req: CommandLogQueryReq = decode(value)?;
            let learner = learner_or_err(state)?;
            encode(&dispatch::query_command_log(
                &learner,
                &req.instance_id,
                &req.since,
                &req.until,
                &req.limit,
            )?)
        }
        OP_LATEST_COMMAND_LOG => {
            let req: InstanceIdReq = decode(value)?;
            let learner = learner_or_err(state)?;
            encode(&dispatch::latest_command_log(&learner, &req.instance_id)?)
        }
        OP_LOG_QUERY => {
            let req: LogQueryReq = decode(value)?;
            let learner = learner_or_err(state)?;
            encode(&crate::client::log_query_json(
                &learner,
                &req.kind,
                &req.unit_id,
                &req.since,
                &req.until,
                &req.limit,
            )
            .map_err(|e| e.to_string())?)
        }
        OP_LOG_APPEND => {
            let req: LogAppendReq = decode(value)?;
            crate::client::log_append_json(state, &req.kind, &req.unit_id, &req.fields_json, &req.narrative)
                .await
                .map_err(|e| e.to_string())?;
            encode(&())
        }
        other => Err(format!("unknown op {other}")),
    }
}
