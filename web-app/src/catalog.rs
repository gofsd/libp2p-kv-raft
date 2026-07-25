//! Worker-side, mirrors `mobile/kvmobile/catalog.go`'s **read-only**
//! subset: `Group`/`Command` lookups, the group/peer association reverse
//! lookups, and `is_permitted_for_command` (the ACL check
//! `dispatch::submit_command` needs). No `put_group`/`delete_group`/
//! `add_command_to_group`/etc. -- those are voter-gated server-side and
//! permanently unreachable from a non-voting learner (see `crate::client`'s
//! doc comment); `CreateGroup`/`AddPeerToGroup`/etc. still exist as `mage`/
//! `kvctl-cli` targets and `mobile/kvmobile` FFI calls, they're just not
//! ported here since they could never succeed.
//!
//! Every lookup here is a local read against [`crate::learner::Learner`]
//! (via [`crate::client::local_get`]/[`crate::client::local_scan_all`]),
//! never a network round trip -- see this port's architecture note on why
//! (`mobile/kvmobile` targets its own peer id, so its reads are answered
//! by its own already-raft-replicated store; this tab's `Learner` is that
//! same thing).
#![cfg(target_arch = "wasm32")]

use std::ops::ControlFlow;

use crate::client;
use crate::learner::Learner;
use crate::shmevent::catalog_keys;

/// A named container Commands can be linked to via `AddCommandToGroup`
/// (server-side only, see this module's doc comment) -- peers become
/// permitted to submit/execute a command by being added to a group linked
/// to it. Matches `mobile/kvmobile.Group`'s JSON shape.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Group {
    pub id: String,
    pub name: String,
}

/// A single submittable/executable operation: `target_peer_id` is where
/// it runs, gated by whichever groups it's linked to. Matches
/// `mobile/kvmobile.Command`'s JSON shape.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Command {
    pub id: String,
    pub name: String,
    pub target_peer_id: String,
}

/// Returns `id`'s current definition, or an error if it doesn't exist.
/// Matches `mobile/kvmobile.GetGroup`.
pub fn get_group(learner: &Learner, id: &str) -> Result<Group, String> {
    let key = catalog_keys::group_key(id.as_bytes());
    let value = client::local_get(learner, &key)
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("group {id} not found"))?;
    Ok(Group {
        id: id.to_string(),
        name: catalog_keys::decode_group_payload(&value),
    })
}

/// Returns every Group (empty if none exist). Matches
/// `mobile/kvmobile.ListGroups`.
pub fn list_groups(learner: &Learner) -> Result<Vec<Group>, String> {
    let (lo, hi) = catalog_keys::group_key_bounds();
    let mut groups = Vec::new();
    let mut err: Option<String> = None;
    client::local_scan_all(learner, &lo, &hi, |key, value| {
        if key.len() < catalog_keys::SYSTEM_KEY_ID_OFFSET {
            err = Some(format!("malformed group key {}", hex(key)));
            return ControlFlow::Break(());
        }
        let id =
            String::from_utf8_lossy(&key[catalog_keys::SYSTEM_KEY_ID_OFFSET..]).to_string();
        groups.push(Group {
            id,
            name: catalog_keys::decode_group_payload(value),
        });
        ControlFlow::Continue(())
    })
    .map_err(|e| e.to_string())?;
    match err {
        Some(e) => Err(e),
        None => Ok(groups),
    }
}

/// Returns `id`'s current definition, or an error if it doesn't exist.
/// Matches `mobile/kvmobile.GetCommand`.
pub fn get_command(learner: &Learner, id: &str) -> Result<Command, String> {
    let key = catalog_keys::command_key(id.as_bytes());
    let value = client::local_get(learner, &key)
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("command {id} not found"))?;
    let (name, target_peer_id) =
        catalog_keys::decode_command_payload(&value).map_err(|e| e.to_string())?;
    Ok(Command {
        id: id.to_string(),
        name,
        target_peer_id: String::from_utf8_lossy(target_peer_id).to_string(),
    })
}

/// Returns every Command -- the full catalog, not scoped to any one group
/// (see [`list_groups_for_command`] for that relation). Matches
/// `mobile/kvmobile.ListCommands`.
pub fn list_commands(learner: &Learner) -> Result<Vec<Command>, String> {
    let (lo, hi) = catalog_keys::command_key_bounds();
    let mut commands = Vec::new();
    let mut err: Option<String> = None;
    client::local_scan_all(learner, &lo, &hi, |key, value| {
        if key.len() < catalog_keys::SYSTEM_KEY_ID_OFFSET {
            err = Some(format!("malformed command key {}", hex(key)));
            return ControlFlow::Break(());
        }
        let id =
            String::from_utf8_lossy(&key[catalog_keys::SYSTEM_KEY_ID_OFFSET..]).to_string();
        match catalog_keys::decode_command_payload(value) {
            Ok((name, target_peer_id)) => {
                commands.push(Command {
                    id,
                    name,
                    target_peer_id: String::from_utf8_lossy(target_peer_id).to_string(),
                });
                ControlFlow::Continue(())
            }
            Err(e) => {
                err = Some(format!("list commands: decode {id}: {e}"));
                ControlFlow::Break(())
            }
        }
    })
    .map_err(|e| e.to_string())?;
    match err {
        Some(e) => Err(e),
        None => Ok(commands),
    }
}

/// Returns every group id `command_id` is linked to (empty if none).
/// Matches `mobile/kvmobile.ListGroupsForCommand`.
pub fn list_groups_for_command(learner: &Learner, command_id: &str) -> Result<Vec<String>, String> {
    let (lo, hi) =
        catalog_keys::group_command_bounds(command_id.as_bytes()).map_err(|e| e.to_string())?;
    let mut group_ids = Vec::new();
    let mut err: Option<String> = None;
    client::local_scan_all(learner, &lo, &hi, |key, _value| {
        match catalog_keys::parse_group_command_key(key) {
            Ok((_command_id, group_id)) => {
                group_ids.push(String::from_utf8_lossy(group_id).to_string());
                ControlFlow::Continue(())
            }
            Err(e) => {
                err = Some(e.to_string());
                ControlFlow::Break(())
            }
        }
    })
    .map_err(|e| e.to_string())?;
    match err {
        Some(e) => Err(e),
        None => Ok(group_ids),
    }
}

/// Returns every group id `peer_id` belongs to (empty if none). Matches
/// `mobile/kvmobile.ListGroupsForPeer`.
pub fn list_groups_for_peer(learner: &Learner, peer_id: &str) -> Result<Vec<String>, String> {
    let (lo, hi) = catalog_keys::peer_group_bounds(peer_id.as_bytes()).map_err(|e| e.to_string())?;
    let mut group_ids = Vec::new();
    let mut err: Option<String> = None;
    client::local_scan_all(learner, &lo, &hi, |key, _value| {
        match catalog_keys::parse_peer_group_key(key) {
            Ok((_peer_id, group_id)) => {
                group_ids.push(String::from_utf8_lossy(group_id).to_string());
                ControlFlow::Continue(())
            }
            Err(e) => {
                err = Some(e.to_string());
                ControlFlow::Break(())
            }
        }
    })
    .map_err(|e| e.to_string())?;
    match err {
        Some(e) => Err(e),
        None => Ok(group_ids),
    }
}

/// Reports whether `peer_id` may submit/execute `command_id`: `true` if
/// some group `G` satisfies both `PeerGroup(peer_id, G)` and
/// `GroupCommand(command_id, G)`. Scans `GroupCommandBounds(command_id)`
/// first (a command is expected to be linked to few groups, unlike a
/// peer, which may belong to many) and point-checks
/// `PeerGroupKey(peer_id, group)` for each hit -- first match
/// short-circuits. Not exposed outside this crate -- matches
/// `mobile/kvmobile`'s own `isPermittedForCommand`, unexported there too,
/// used only by `dispatch::submit_command`.
pub(crate) fn is_permitted_for_command(
    learner: &Learner,
    peer_id: &str,
    command_id: &str,
) -> Result<bool, String> {
    let (lo, hi) =
        catalog_keys::group_command_bounds(command_id.as_bytes()).map_err(|e| e.to_string())?;
    let mut permitted = false;
    let mut err: Option<String> = None;
    client::local_scan_all(learner, &lo, &hi, |key, _value| {
        let (_command_id, group_id) = match catalog_keys::parse_group_command_key(key) {
            Ok(v) => v,
            Err(e) => {
                err = Some(e.to_string());
                return ControlFlow::Break(());
            }
        };
        let peer_group_key = match catalog_keys::peer_group_key(peer_id.as_bytes(), group_id) {
            Ok(v) => v,
            Err(e) => {
                err = Some(e.to_string());
                return ControlFlow::Break(());
            }
        };
        match client::local_get(learner, &peer_group_key) {
            Ok(Some(_)) => {
                permitted = true;
                ControlFlow::Break(())
            }
            Ok(None) => ControlFlow::Continue(()),
            Err(e) => {
                err = Some(e.to_string());
                ControlFlow::Break(())
            }
        }
    })
    .map_err(|e| e.to_string())?;
    match err {
        Some(e) => Err(e),
        None => Ok(permitted),
    }
}

fn hex(raw: &[u8]) -> String {
    crate::shmevent::hex_encode(raw)
}
