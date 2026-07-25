//! Worker-side, mirrors `mobile/kvmobile/dispatch.go` 1:1: ties the
//! catalog -> dispatch -> execution-log flow together. `submit_command`
//! dispatches a `catalog::Command` as a durable, replicated
//! `CommandRequest` plus a low-latency `Execute` poke to whoever executes
//! it; `AppendCommandLog`/`QueryCommandLog`/`LatestCommandLog` read back
//! the execution log the target device writes as it works. Like
//! `catalog.rs`, every operation here is a plain local read or
//! `EventLogAppend`/`EventExecute` remote call -- no new wire event.
//!
//! This crate only dispatches and records; it never interprets or runs a
//! Command itself -- that's the target device's own application logic.
#![cfg(target_arch = "wasm32")]

use std::cell::RefCell;
use std::collections::HashMap;
use std::ops::ControlFlow;
use std::rc::Rc;

use time::OffsetDateTime;

use crate::app::WorkerState;
use crate::catalog;
use crate::client::{self, RemoteSession};
use crate::learner::Learner;
use crate::logrecord;

/// Fixed `pkg/logrecord` kind every `append_command_log` entry is stored
/// under, keyed by instance id (globally unique, not scoped to a
/// command). Matches `mobile/kvmobile`'s `logCommandExecKind`.
const LOG_COMMAND_EXEC_KIND: &str = "cmdlog";

/// `commandExecIndexKind` entries' "role" field values. Matches
/// `mobile/kvmobile`'s `execIndexRoleRequester`/`execIndexRoleTarget`.
const EXEC_INDEX_ROLE_REQUESTER: &str = "r";
const EXEC_INDEX_ROLE_TARGET: &str = "t";

/// Bounds [`list_executions_by_peer`]'s result to the most recent
/// executions touching a peer. Matches `mobile/kvmobile`'s
/// `maxExecutionsByPeer`.
const MAX_EXECUTIONS_BY_PEER: usize = 200;

/// `pkg/logrecord` kind every `submit_command` dispatch of `command_id`
/// is stored under. Matches `mobile/kvmobile`'s `commandRequestLogKind`.
fn command_request_log_kind(command_id: &str) -> String {
    format!("cmdreq:{command_id}")
}

/// `pkg/logrecord` kind `submit_command` indexes a dispatch under for
/// `peer_id`'s sake. Matches `mobile/kvmobile`'s `commandExecIndexKind`.
fn command_exec_index_kind(peer_id: &str) -> String {
    format!("cmdexec:{peer_id}")
}

/// `submit_command`'s durable record of one dispatch. Matches
/// `mobile/kvmobile.CommandRequest`'s JSON shape.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CommandRequest {
    pub instance_id: String,
    pub command_id: String,
    pub requested_by: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub inputs: String,
    #[serde(with = "time::serde::rfc3339")]
    pub requested_at: OffsetDateTime,
}

fn revision_to_command_request(h: &client::RevisionHistory) -> CommandRequest {
    CommandRequest {
        instance_id: h.latest.unit_id.clone(),
        command_id: h.latest.fields.get("command_id").cloned().unwrap_or_default(),
        requested_by: h.latest.author_peer_id.clone(),
        inputs: h.latest.fields.get("inputs").cloned().unwrap_or_default(),
        requested_at: h.latest.timestamp,
    }
}

/// One `submit_command` dispatch as it appears from `peer_id`'s point of
/// view (see [`list_executions_by_peer`]). Matches
/// `mobile/kvmobile.CommandExecution`'s JSON shape.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CommandExecution {
    pub instance_id: String,
    pub command_id: String,
    pub requested_by: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub target_peer_id: String,
    pub role: String,
    #[serde(with = "time::serde::rfc3339")]
    pub requested_at: OffsetDateTime,
}

/// The small JSON envelope `submit_command`/`append_command_log` send
/// over `Execute` as an optional low-latency nudge. Matches
/// `mobile/kvmobile`'s `executePoke`.
#[derive(Debug, Clone, serde::Serialize)]
struct ExecutePoke {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    command_id: String,
    instance_id: String,
}

fn new_instance_id() -> Result<String, String> {
    let mut b = [0u8; 16];
    getrandom::fill(&mut b).map_err(|e| format!("generate instance id: {e}"))?;
    Ok(crate::shmevent::hex_encode(&b))
}

async fn append_command_exec_index(
    sess: &mut RemoteSession,
    peer_id: &str,
    instance_id: &str,
    command_id: &str,
    requester_peer_id: &str,
    role: &str,
) -> Result<(), String> {
    let mut fields = HashMap::new();
    fields.insert("command_id".to_string(), command_id.to_string());
    fields.insert("role".to_string(), role.to_string());
    client::append_record(
        sess,
        &command_exec_index_kind(peer_id),
        instance_id,
        requester_peer_id,
        fields,
        "",
    )
    .await
    .map_err(|e| e.to_string())
}

/// Dispatches `command_id` (which must already exist) with `inputs_json`
/// (caller-defined, opaque here) as a durable, replicated
/// [`CommandRequest`], then sends the command's `target_peer_id` a
/// low-latency `Execute` poke naming the new instance id (best-effort: a
/// failed poke doesn't fail the dispatch, since the durable request is
/// the real source of truth). Returns the instance id.
///
/// Requires this tab's own current peer id to be permitted for
/// `command_id` ([`catalog::is_permitted_for_command`]). Matches
/// `mobile/kvmobile.SubmitCommand`.
pub async fn submit_command(
    state: &Rc<RefCell<WorkerState>>,
    command_id: &str,
    inputs_json: &str,
) -> Result<String, String> {
    let mut sess = RemoteSession::from_worker_state(state).map_err(|e| e.to_string())?;
    let requester_peer_id = sess.local_peer_id().to_string();

    let learner = {
        let guard = state.borrow();
        guard
            .learner
            .clone()
            .ok_or_else(|| "do_connect has not completed yet".to_string())?
    };

    let permitted = catalog::is_permitted_for_command(&learner, &requester_peer_id, command_id)?;
    if !permitted {
        return Err(format!(
            "{requester_peer_id} is not permitted to submit command {command_id}"
        ));
    }

    let cmd = catalog::get_command(&learner, command_id)?;
    let target_peer_id = cmd.target_peer_id;

    let instance_id = new_instance_id()?;

    let mut fields = HashMap::new();
    fields.insert("command_id".to_string(), command_id.to_string());
    if !inputs_json.is_empty() {
        fields.insert("inputs".to_string(), inputs_json.to_string());
    }
    client::append_record(
        &mut sess,
        &command_request_log_kind(command_id),
        &instance_id,
        &requester_peer_id,
        fields,
        "",
    )
    .await
    .map_err(|e| e.to_string())?;

    append_command_exec_index(
        &mut sess,
        &requester_peer_id,
        &instance_id,
        command_id,
        &requester_peer_id,
        EXEC_INDEX_ROLE_REQUESTER,
    )
    .await?;
    if target_peer_id != requester_peer_id {
        append_command_exec_index(
            &mut sess,
            &target_peer_id,
            &instance_id,
            command_id,
            &requester_peer_id,
            EXEC_INDEX_ROLE_TARGET,
        )
        .await?;
    }

    let poke = ExecutePoke {
        kind: "cmd_req".to_string(),
        command_id: command_id.to_string(),
        instance_id: instance_id.clone(),
    };
    if let Ok(poke_json) = serde_json::to_vec(&poke) {
        let _ = sess.execute(&target_peer_id, &poke_json).await; // best-effort
    }

    Ok(instance_id)
}

/// Returns `instance_id`'s dispatch record for `command_id`, or an error
/// if it doesn't exist. Matches `mobile/kvmobile.GetCommandRequest`.
pub fn get_command_request(
    learner: &Learner,
    command_id: &str,
    instance_id: &str,
) -> Result<CommandRequest, String> {
    let h = client::scan_revisions(learner, &command_request_log_kind(command_id), instance_id)
        .map_err(|e| e.to_string())?;
    if !h.found {
        return Err(format!(
            "command request {instance_id} not found for command {command_id}"
        ));
    }
    Ok(revision_to_command_request(&h))
}

/// Returns every dispatch request currently recorded for `command_id`
/// (empty if none), oldest first -- how a target device catches up on
/// requests it might have missed an `Execute` poke for. Matches
/// `mobile/kvmobile.ListCommandRequests`.
pub fn list_command_requests(
    learner: &Learner,
    command_id: &str,
) -> Result<Vec<CommandRequest>, String> {
    let kind = command_request_log_kind(command_id);
    let ids = client::list_unit_ids(learner, &kind).map_err(|e| e.to_string())?;
    let mut requests = Vec::new();
    for id in ids {
        let h = client::scan_revisions(learner, &kind, &id).map_err(|e| e.to_string())?;
        if !h.found {
            continue;
        }
        requests.push(revision_to_command_request(&h));
    }
    Ok(requests)
}

fn target_peer_id_for_command(learner: &Learner, command_id: &str) -> String {
    catalog::get_command(learner, command_id)
        .map(|c| c.target_peer_id)
        .unwrap_or_default()
}

/// Returns up to [`MAX_EXECUTIONS_BY_PEER`] most recent `submit_command`
/// dispatches touching `peer_id`, as either requester or target, most
/// recent first. There is no reverse-scan primitive anywhere in this
/// stack, so this costs walking `peer_id`'s whole index ascending and
/// keeping a sliding window. Matches `mobile/kvmobile.ListExecutionsByPeer`.
pub fn list_executions_by_peer(learner: &Learner, peer_id: &str) -> Result<Vec<CommandExecution>, String> {
    if peer_id.is_empty() {
        return Err("ListExecutionsByPeer: peerID must not be empty".to_string());
    }
    let (lo, hi) = client::kind_prefix_bounds(&command_exec_index_kind(peer_id));

    let mut window: Vec<CommandExecution> = Vec::new();
    let mut err: Option<String> = None;
    client::local_scan_all(learner, &lo, &hi, |_key, value| {
        let rec = match logrecord::Record::decode(value) {
            Ok(r) => r,
            Err(e) => {
                err = Some(format!("list executions by peer: decode: {e}"));
                return ControlFlow::Break(());
            }
        };
        let command_id = rec.fields.get("command_id").cloned().unwrap_or_default();

        let mut exec = CommandExecution {
            instance_id: rec.unit_id.clone(),
            command_id: command_id.clone(),
            requested_by: rec.author_peer_id.clone(),
            target_peer_id: String::new(),
            role: String::new(),
            requested_at: rec.timestamp,
        };
        if rec.fields.get("role").map(String::as_str) == Some(EXEC_INDEX_ROLE_TARGET) {
            exec.role = "target".to_string();
            exec.target_peer_id = peer_id.to_string();
        } else {
            exec.role = "requester".to_string();
            exec.target_peer_id = target_peer_id_for_command(learner, &command_id);
        }

        window.push(exec);
        if window.len() > MAX_EXECUTIONS_BY_PEER {
            window.remove(0);
        }
        ControlFlow::Continue(())
    })
    .map_err(|e| e.to_string())?;
    if let Some(e) = err {
        return Err(e);
    }

    window.reverse();
    Ok(window)
}

/// Writes one execution-log entry for `instance_id` -- the target device
/// calls this as it works through a command, and
/// `query_command_log`/`latest_command_log` is how the requester reads it
/// back. Also sends `requester_peer_id` a low-latency `Execute` poke,
/// best-effort. Pass `""` for `requester_peer_id` to skip the poke.
/// Matches `mobile/kvmobile.AppendCommandLog`.
pub async fn append_command_log(
    state: &Rc<RefCell<WorkerState>>,
    requester_peer_id: &str,
    instance_id: &str,
    fields_json: &str,
    narrative: &str,
) -> Result<(), String> {
    if instance_id.is_empty() {
        return Err("instance id must not be empty".to_string());
    }
    client::log_append_json(state, LOG_COMMAND_EXEC_KIND, instance_id, fields_json, narrative)
        .await
        .map_err(|e| e.to_string())?;

    if !requester_peer_id.is_empty() {
        let poke = ExecutePoke {
            kind: "cmd_log".to_string(),
            command_id: String::new(),
            instance_id: instance_id.to_string(),
        };
        if let Ok(poke_json) = serde_json::to_vec(&poke) {
            if let Ok(mut sess) = RemoteSession::from_worker_state(state) {
                let _ = sess.execute(requester_peer_id, &poke_json).await; // best-effort
            }
        }
    }
    Ok(())
}

/// Lists every `append_command_log` entry for `instance_id` with a
/// timestamp in `[since, until]`, oldest first, up to `limit` records --
/// a thin wrapper over [`client::log_query_json`] scoped to
/// [`LOG_COMMAND_EXEC_KIND`]. Matches `mobile/kvmobile.QueryCommandLog`.
pub fn query_command_log(
    learner: &Learner,
    instance_id: &str,
    since: &str,
    until: &str,
    limit: &str,
) -> Result<Vec<logrecord::Record>, String> {
    client::log_query_json(learner, LOG_COMMAND_EXEC_KIND, instance_id, since, until, limit)
        .map_err(|e| e.to_string())
}

/// Returns `instance_id`'s single most recent `append_command_log` entry.
/// Returns an error if `instance_id` has no log entries yet. Matches
/// `mobile/kvmobile.LatestCommandLog`.
pub fn latest_command_log(learner: &Learner, instance_id: &str) -> Result<logrecord::Record, String> {
    if instance_id.is_empty() {
        return Err("LatestCommandLog: instanceID must not be empty".to_string());
    }
    let (lo, hi) = logrecord::scan_bounds(
        LOG_COMMAND_EXEC_KIND,
        instance_id,
        OffsetDateTime::UNIX_EPOCH,
        client::now(),
    );

    let mut latest: Option<logrecord::Record> = None;
    let mut err: Option<String> = None;
    client::local_scan_all(learner, &lo, &hi, |_key, value| match logrecord::Record::decode(value) {
        Ok(rec) => {
            latest = Some(rec);
            ControlFlow::Continue(())
        }
        Err(e) => {
            err = Some(format!("latest command log: decode: {e}"));
            ControlFlow::Break(())
        }
    })
    .map_err(|e| e.to_string())?;
    if let Some(e) = err {
        return Err(e);
    }
    latest.ok_or_else(|| format!("no command log entries for instance {instance_id}"))
}
