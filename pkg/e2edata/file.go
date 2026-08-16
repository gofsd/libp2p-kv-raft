// Package e2edata implements the single JSON file that records everything
// the end-to-end test/deploy pipeline needs: a version history stamped
// with this repo's own semver (shared across every platform's
// implementation -- one monorepo, one release version), the deterministic
// node identities used across platforms (desktop, android, web, and the
// one SSH-deployed remote leader), and a durable, human-readable log of
// test rows (one shmevent, one node, one recorded pass/fail) run against
// those identities.
//
// The file is meant to be committed to the repo (test/e2e/testdata.json)
// and read/edited by a human, not just tooling: identities are
// deterministic (see identity.go) so every checkout/deploy reproduces the
// exact same peer ids and keys ("predictable deploy" from the design
// brief), events are recorded by name with plain-text values rather than
// raw wire bytes (see Event's doc comment), and PublishedVersion tracks
// how far the recorded test history has been confirmed passing, so mage
// e2e:current only re-runs what's new since the last time this file's
// version count moved ("version increment based on new tests").
package e2edata

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// Platform identifies which client implementation a Node's identity is
// deployed under -- each has its own way of being handed a deterministic
// seed (see cmd/kvctl-cli's -key-path file for desktop, mobile/kvmobile's
// identitySeedHex ldflag for android, web-app's worker_main_with_seed for
// web), except PlatformRemote, which identifies the one SSH-deployed
// bootstrap/leader node (see pkg/e2erun.EnsureBootstrap and its doc
// comment on BootstrapHost) that every other platform's test node joins.
type Platform string

const (
	PlatformDesktop Platform = "desktop"
	PlatformAndroid Platform = "android"
	PlatformWeb     Platform = "web"
	PlatformRemote  Platform = "remote"
)

// Node is one deterministic identity recorded in the file, keyed by a small
// integer id in File.Nodes. PublicKey/PrivateKey are hex-encoded stdlib
// crypto/ed25519 keys (32 and 64 raw bytes respectively) -- the same format
// pkg/shmevent.PublicKey/PrivateKey and EventGetPublicKey/EventGetPrivateKey
// use, so a row's expectations can be checked against a live node's actual
// keys with no conversion.
type Node struct {
	Platform   Platform `json:"platform"`
	PeerID     string   `json:"peer_id"`
	PublicKey  string   `json:"public_key"`
	PrivateKey string   `json:"private_key"`
}

// Event is the JSON form of a shmevent.Msg used inside a test Row's
// "event" field and as kvctl-cli sendevent's argument/output shape --
// human-readable and self-describing on purpose: Op names the operation
// (shmevent.EventName's snake_case form -- "dial_submit_command",
// "group_put", ...), and Fields holds only that operation's own named
// fields (see api/shmevent.capnp's union), keyed by their own snake_case
// name -- e.g. dial_submit_command's Fields has "target_peer"/
// "command_id"/"inputs_json"/"note", nothing else. A field absent from
// Fields is simply left unset on the built Msg (zero-valued, or -- for a
// response-only field like a Get's answer -- not something a request
// needs to set in the first place). Text fields are plain strings; binary
// (Data) fields are plain text if the underlying bytes are valid UTF-8,
// or a "0x"-prefixed hex string otherwise (see valueToJSON/valueFromJSON)
// -- unambiguous to parse back either way, just not pretending binary
// data is text. txn is the one operation with no JSON representation at
// all (its ops field is a list, not a scalar) -- see ToMsg's own txn
// case.
//
// See ToMsg/EventFromMsg for the actual translation to/from
// pkg/shmevent's generated per-variant accessors, and MarshalJSON/
// UnmarshalJSON for (de)serialization; the exported fields below are for
// in-Go construction/inspection only.
type Event struct {
	Op     string
	ID     uint16
	Fields map[string]string
}

// eventJSON is Event's on-disk shape -- see Event's doc comment.
type eventJSON struct {
	Event  string            `json:"event"`
	ID     uint16            `json:"id,omitempty"`
	Fields map[string]string `json:"fields,omitempty"`
}

func (e Event) MarshalJSON() ([]byte, error) {
	return json.Marshal(eventJSON{Event: e.Op, ID: e.ID, Fields: e.Fields})
}

func (e *Event) UnmarshalJSON(data []byte) error {
	var j eventJSON
	if err := json.Unmarshal(data, &j); err != nil {
		return err
	}
	*e = Event{Op: j.Event, ID: j.ID, Fields: j.Fields}
	return nil
}

// ToMsg converts e to the wire struct pkg/ipc.Call/pkg/shmevent.Encode
// need, dispatching on Op and setting whichever fields e.Fields names --
// see Event's own doc comment and api/shmevent.capnp's per-variant field
// lists.
func (e Event) ToMsg() (shmevent.Msg, error) {
	m, err := shmevent.NewMsg()
	if err != nil {
		return shmevent.Msg{}, fmt.Errorf("e2edata: %w", err)
	}
	m.SetId(e.ID)
	switch e.Op {
	case "set_key":
		m.SetSetKey()
		grp := m.SetKey()
		if v, ok := e.Fields["value"]; ok {
			raw, err := valueFromJSON(v)
			if err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: set_key.value: %w", err)
			}
			if err := grp.SetValue(raw); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: set_key.value: %w", err)
			}
		}
		return m, nil
	case "set_field":
		m.SetSetField()
		grp := m.SetField()
		if v, ok := e.Fields["source_id"]; ok {
			n, err := strconv.ParseUint(v, 10, 16)
			if err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: set_field.source_id: %w", err)
			}
			grp.SetSourceId(uint16(n))
		}
		if v, ok := e.Fields["value"]; ok {
			raw, err := valueFromJSON(v)
			if err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: set_field.value: %w", err)
			}
			if err := grp.SetValue(raw); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: set_field.value: %w", err)
			}
		}
		return m, nil
	case "get_key":
		m.SetGetKey()
		grp := m.GetKey()
		if v, ok := e.Fields["source_id"]; ok {
			n, err := strconv.ParseUint(v, 10, 16)
			if err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: get_key.source_id: %w", err)
			}
			grp.SetSourceId(uint16(n))
		}
		if v, ok := e.Fields["key"]; ok {
			raw, err := valueFromJSON(v)
			if err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: get_key.key: %w", err)
			}
			if err := grp.SetKey(raw); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: get_key.key: %w", err)
			}
		}
		return m, nil
	case "get_field_by_registry":
		m.SetGetFieldByRegistry()
		grp := m.GetFieldByRegistry()
		if v, ok := e.Fields["source_id"]; ok {
			n, err := strconv.ParseUint(v, 10, 16)
			if err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: get_field_by_registry.source_id: %w", err)
			}
			grp.SetSourceId(uint16(n))
		}
		if v, ok := e.Fields["value"]; ok {
			raw, err := valueFromJSON(v)
			if err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: get_field_by_registry.value: %w", err)
			}
			if err := grp.SetValue(raw); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: get_field_by_registry.value: %w", err)
			}
		}
		return m, nil
	case "get_field_by_key":
		m.SetGetFieldByKey()
		grp := m.GetFieldByKey()
		if v, ok := e.Fields["key"]; ok {
			raw, err := valueFromJSON(v)
			if err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: get_field_by_key.key: %w", err)
			}
			if err := grp.SetKey(raw); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: get_field_by_key.key: %w", err)
			}
		}
		if v, ok := e.Fields["value"]; ok {
			raw, err := valueFromJSON(v)
			if err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: get_field_by_key.value: %w", err)
			}
			if err := grp.SetValue(raw); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: get_field_by_key.value: %w", err)
			}
		}
		return m, nil
	case "get_public_key":
		m.SetGetPublicKey()
		grp := m.GetPublicKey()
		if v, ok := e.Fields["pub_key"]; ok {
			raw, err := valueFromJSON(v)
			if err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: get_public_key.pub_key: %w", err)
			}
			if err := grp.SetPubKey(raw); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: get_public_key.pub_key: %w", err)
			}
		}
		return m, nil
	case "get_private_key":
		m.SetGetPrivateKey()
		grp := m.GetPrivateKey()
		if v, ok := e.Fields["priv_key"]; ok {
			raw, err := valueFromJSON(v)
			if err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: get_private_key.priv_key: %w", err)
			}
			if err := grp.SetPrivKey(raw); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: get_private_key.priv_key: %w", err)
			}
		}
		return m, nil
	case "bootstrap_or_join_cluster":
		m.SetBootstrapOrJoinCluster()
		grp := m.BootstrapOrJoinCluster()
		if v, ok := e.Fields["leader_addr"]; ok {
			if err := grp.SetLeaderAddr(v); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: bootstrap_or_join_cluster.leader_addr: %w", err)
			}
		}
		return m, nil
	case "add_learner":
		m.SetAddLearner()
		grp := m.AddLearner()
		if v, ok := e.Fields["claimed_peer_id"]; ok {
			n, err := strconv.ParseUint(v, 10, 16)
			if err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: add_learner.claimed_peer_id: %w", err)
			}
			grp.SetClaimedPeerId(uint16(n))
		}
		if v, ok := e.Fields["addr"]; ok {
			if err := grp.SetAddr(v); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: add_learner.addr: %w", err)
			}
		}
		return m, nil
	case "set":
		m.SetSet()
		grp := m.Set()
		if v, ok := e.Fields["key"]; ok {
			raw, err := valueFromJSON(v)
			if err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: set.key: %w", err)
			}
			if err := grp.SetKey(raw); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: set.key: %w", err)
			}
		}
		if v, ok := e.Fields["value"]; ok {
			raw, err := valueFromJSON(v)
			if err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: set.value: %w", err)
			}
			if err := grp.SetValue(raw); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: set.value: %w", err)
			}
		}
		return m, nil
	case "execute":
		m.SetExecute()
		grp := m.Execute()
		if v, ok := e.Fields["source_id"]; ok {
			n, err := strconv.ParseUint(v, 10, 16)
			if err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: execute.source_id: %w", err)
			}
			grp.SetSourceId(uint16(n))
		}
		if v, ok := e.Fields["destination_id"]; ok {
			n, err := strconv.ParseUint(v, 10, 16)
			if err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: execute.destination_id: %w", err)
			}
			grp.SetDestinationId(uint16(n))
		}
		if v, ok := e.Fields["sender_peer_id"]; ok {
			if err := grp.SetSenderPeerId(v); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: execute.sender_peer_id: %w", err)
			}
		}
		if v, ok := e.Fields["value"]; ok {
			raw, err := valueFromJSON(v)
			if err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: execute.value: %w", err)
			}
			if err := grp.SetValue(raw); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: execute.value: %w", err)
			}
		}
		return m, nil
	case "poll_execute":
		m.SetPollExecute()
		grp := m.PollExecute()
		if v, ok := e.Fields["sender_peer_id"]; ok {
			if err := grp.SetSenderPeerId(v); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: poll_execute.sender_peer_id: %w", err)
			}
		}
		if v, ok := e.Fields["value"]; ok {
			raw, err := valueFromJSON(v)
			if err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: poll_execute.value: %w", err)
			}
			if err := grp.SetValue(raw); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: poll_execute.value: %w", err)
			}
		}
		return m, nil
	case "list_range":
		m.SetListRange()
		grp := m.ListRange()
		if v, ok := e.Fields["start"]; ok {
			raw, err := valueFromJSON(v)
			if err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: list_range.start: %w", err)
			}
			if err := grp.SetStart(raw); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: list_range.start: %w", err)
			}
		}
		if v, ok := e.Fields["end"]; ok {
			raw, err := valueFromJSON(v)
			if err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: list_range.end: %w", err)
			}
			if err := grp.SetEnd(raw); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: list_range.end: %w", err)
			}
		}
		if v, ok := e.Fields["key"]; ok {
			raw, err := valueFromJSON(v)
			if err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: list_range.key: %w", err)
			}
			if err := grp.SetKey(raw); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: list_range.key: %w", err)
			}
		}
		if v, ok := e.Fields["value"]; ok {
			raw, err := valueFromJSON(v)
			if err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: list_range.value: %w", err)
			}
			if err := grp.SetValue(raw); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: list_range.value: %w", err)
			}
		}
		return m, nil
	case "log_append":
		m.SetLogAppend()
		grp := m.LogAppend()
		if v, ok := e.Fields["key"]; ok {
			raw, err := valueFromJSON(v)
			if err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: log_append.key: %w", err)
			}
			if err := grp.SetKey(raw); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: log_append.key: %w", err)
			}
		}
		if v, ok := e.Fields["value"]; ok {
			raw, err := valueFromJSON(v)
			if err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: log_append.value: %w", err)
			}
			if err := grp.SetValue(raw); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: log_append.value: %w", err)
			}
		}
		return m, nil
	case "leave":
		m.SetLeave()
		return m, nil
	case "exec_invite_redeem":
		m.SetExecInviteRedeem()
		grp := m.ExecInviteRedeem()
		if v, ok := e.Fields["source_addr"]; ok {
			if err := grp.SetSourceAddr(v); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: exec_invite_redeem.source_addr: %w", err)
			}
		}
		if v, ok := e.Fields["token"]; ok {
			raw, err := valueFromJSON(v)
			if err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: exec_invite_redeem.token: %w", err)
			}
			if err := grp.SetToken(raw); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: exec_invite_redeem.token: %w", err)
			}
		}
		if v, ok := e.Fields["instance_id"]; ok {
			if err := grp.SetInstanceId(v); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: exec_invite_redeem.instance_id: %w", err)
			}
		}
		if v, ok := e.Fields["redeemer_peer_id"]; ok {
			if err := grp.SetRedeemerPeerId(v); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: exec_invite_redeem.redeemer_peer_id: %w", err)
			}
		}
		return m, nil
	case "join_request_create":
		m.SetJoinRequestCreate()
		grp := m.JoinRequestCreate()
		if v, ok := e.Fields["token"]; ok {
			raw, err := valueFromJSON(v)
			if err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: join_request_create.token: %w", err)
			}
			if err := grp.SetToken(raw); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: join_request_create.token: %w", err)
			}
		}
		return m, nil
	case "join_request_cancel":
		m.SetJoinRequestCancel()
		grp := m.JoinRequestCancel()
		if v, ok := e.Fields["token"]; ok {
			raw, err := valueFromJSON(v)
			if err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: join_request_cancel.token: %w", err)
			}
			if err := grp.SetToken(raw); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: join_request_cancel.token: %w", err)
			}
		}
		return m, nil
	case "recruit":
		m.SetRecruit()
		grp := m.Recruit()
		if v, ok := e.Fields["ticket"]; ok {
			if err := grp.SetTicket(v); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: recruit.ticket: %w", err)
			}
		}
		if v, ok := e.Fields["suffrage"]; ok {
			n, err := strconv.ParseUint(v, 10, 8)
			if err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: recruit.suffrage: %w", err)
			}
			grp.SetSuffrage(uint8(n))
		}
		return m, nil
	case "get_own_addr":
		m.SetGetOwnAddr()
		grp := m.GetOwnAddr()
		if v, ok := e.Fields["addr"]; ok {
			if err := grp.SetAddr(v); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: get_own_addr.addr: %w", err)
			}
		}
		return m, nil
	case "channel_open":
		m.SetChannelOpen()
		grp := m.ChannelOpen()
		if v, ok := e.Fields["peer_id"]; ok {
			if err := grp.SetPeerId(v); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: channel_open.peer_id: %w", err)
			}
		}
		if v, ok := e.Fields["sender_peer_id"]; ok {
			if err := grp.SetSenderPeerId(v); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: channel_open.sender_peer_id: %w", err)
			}
		}
		return m, nil
	case "channel_send":
		m.SetChannelSend()
		grp := m.ChannelSend()
		if v, ok := e.Fields["channel_id"]; ok {
			if err := grp.SetChannelId(v); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: channel_send.channel_id: %w", err)
			}
		}
		if v, ok := e.Fields["purpose"]; ok {
			n, err := strconv.ParseUint(v, 10, 8)
			if err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: channel_send.purpose: %w", err)
			}
			grp.SetPurpose(uint8(n))
		}
		if v, ok := e.Fields["chunk"]; ok {
			raw, err := valueFromJSON(v)
			if err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: channel_send.chunk: %w", err)
			}
			if err := grp.SetChunk(raw); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: channel_send.chunk: %w", err)
			}
		}
		return m, nil
	case "channel_poll":
		m.SetChannelPoll()
		grp := m.ChannelPoll()
		if v, ok := e.Fields["channel_id"]; ok {
			if err := grp.SetChannelId(v); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: channel_poll.channel_id: %w", err)
			}
		}
		if v, ok := e.Fields["status"]; ok {
			n, err := strconv.ParseUint(v, 10, 8)
			if err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: channel_poll.status: %w", err)
			}
			grp.SetStatus(uint8(n))
		}
		if v, ok := e.Fields["purpose"]; ok {
			n, err := strconv.ParseUint(v, 10, 8)
			if err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: channel_poll.purpose: %w", err)
			}
			grp.SetPurpose(uint8(n))
		}
		if v, ok := e.Fields["chunk"]; ok {
			raw, err := valueFromJSON(v)
			if err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: channel_poll.chunk: %w", err)
			}
			if err := grp.SetChunk(raw); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: channel_poll.chunk: %w", err)
			}
		}
		return m, nil
	case "channel_listen":
		m.SetChannelListen()
		grp := m.ChannelListen()
		if v, ok := e.Fields["channel_id"]; ok {
			if err := grp.SetChannelId(v); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: channel_listen.channel_id: %w", err)
			}
		}
		if v, ok := e.Fields["remote_peer_id"]; ok {
			if err := grp.SetRemotePeerId(v); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: channel_listen.remote_peer_id: %w", err)
			}
		}
		return m, nil
	case "channel_close":
		m.SetChannelClose()
		grp := m.ChannelClose()
		if v, ok := e.Fields["channel_id"]; ok {
			if err := grp.SetChannelId(v); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: channel_close.channel_id: %w", err)
			}
		}
		return m, nil
	case "channel_close_write":
		m.SetChannelCloseWrite()
		grp := m.ChannelCloseWrite()
		if v, ok := e.Fields["channel_id"]; ok {
			if err := grp.SetChannelId(v); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: channel_close_write.channel_id: %w", err)
			}
		}
		return m, nil
	case "channel_data_ready":
		m.SetChannelDataReady()
		grp := m.ChannelDataReady()
		if v, ok := e.Fields["channel_id"]; ok {
			if err := grp.SetChannelId(v); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: channel_data_ready.channel_id: %w", err)
			}
		}
		return m, nil
	case "kick":
		m.SetKick()
		grp := m.Kick()
		if v, ok := e.Fields["peer_id"]; ok {
			if err := grp.SetPeerId(v); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: kick.peer_id: %w", err)
			}
		}
		return m, nil
	case "txn":
		return shmevent.Msg{}, fmt.Errorf("e2edata: txn: not representable as a generic event (ops is a list) -- use ParseTxnOpsString/kvctl.Txn instead")
	case "get_version":
		m.SetGetVersion()
		grp := m.GetVersion()
		if v, ok := e.Fields["commit"]; ok {
			if err := grp.SetCommit(v); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: get_version.commit: %w", err)
			}
		}
		if v, ok := e.Fields["dirty"]; ok {
			b, err := strconv.ParseBool(v)
			if err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: get_version.dirty: %w", err)
			}
			grp.SetDirty(b)
		}
		if v, ok := e.Fields["build_time"]; ok {
			if err := grp.SetBuildTime(v); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: get_version.build_time: %w", err)
			}
		}
		if v, ok := e.Fields["go_version"]; ok {
			if err := grp.SetGoVersion(v); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: get_version.go_version: %w", err)
			}
		}
		if v, ok := e.Fields["libp2p_version"]; ok {
			if err := grp.SetLibp2pVersion(v); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: get_version.libp2p_version: %w", err)
			}
		}
		return m, nil
	case "public_access":
		m.SetPublicAccess()
		grp := m.PublicAccess()
		if v, ok := e.Fields["target_peer"]; ok {
			if err := grp.SetTargetPeer(v); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: public_access.target_peer: %w", err)
			}
		}
		if v, ok := e.Fields["note"]; ok {
			if err := grp.SetNote(v); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: public_access.note: %w", err)
			}
		}
		if v, ok := e.Fields["instance_id"]; ok {
			if err := grp.SetInstanceId(v); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: public_access.instance_id: %w", err)
			}
		}
		return m, nil
	case "exec_ticket":
		m.SetExecTicket()
		grp := m.ExecTicket()
		if v, ok := e.Fields["source_addr"]; ok {
			if err := grp.SetSourceAddr(v); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: exec_ticket.source_addr: %w", err)
			}
		}
		if v, ok := e.Fields["token"]; ok {
			raw, err := valueFromJSON(v)
			if err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: exec_ticket.token: %w", err)
			}
			if err := grp.SetToken(raw); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: exec_ticket.token: %w", err)
			}
		}
		return m, nil
	case "join_ticket":
		m.SetJoinTicket()
		grp := m.JoinTicket()
		if v, ok := e.Fields["source_addr"]; ok {
			if err := grp.SetSourceAddr(v); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: join_ticket.source_addr: %w", err)
			}
		}
		if v, ok := e.Fields["token"]; ok {
			raw, err := valueFromJSON(v)
			if err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: join_ticket.token: %w", err)
			}
			if err := grp.SetToken(raw); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: join_ticket.token: %w", err)
			}
		}
		return m, nil
	case "join_request_ticket":
		m.SetJoinRequestTicket()
		grp := m.JoinRequestTicket()
		if v, ok := e.Fields["source_addr"]; ok {
			if err := grp.SetSourceAddr(v); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: join_request_ticket.source_addr: %w", err)
			}
		}
		if v, ok := e.Fields["token"]; ok {
			raw, err := valueFromJSON(v)
			if err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: join_request_ticket.token: %w", err)
			}
			if err := grp.SetToken(raw); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: join_request_ticket.token: %w", err)
			}
		}
		return m, nil
	case "dial_submit_command":
		m.SetDialSubmitCommand()
		grp := m.DialSubmitCommand()
		if v, ok := e.Fields["target_peer"]; ok {
			if err := grp.SetTargetPeer(v); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: dial_submit_command.target_peer: %w", err)
			}
		}
		if v, ok := e.Fields["command_id"]; ok {
			if err := grp.SetCommandId(v); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: dial_submit_command.command_id: %w", err)
			}
		}
		if v, ok := e.Fields["inputs_json"]; ok {
			if err := grp.SetInputsJson(v); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: dial_submit_command.inputs_json: %w", err)
			}
		}
		if v, ok := e.Fields["note"]; ok {
			if err := grp.SetNote(v); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: dial_submit_command.note: %w", err)
			}
		}
		if v, ok := e.Fields["instance_id"]; ok {
			if err := grp.SetInstanceId(v); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: dial_submit_command.instance_id: %w", err)
			}
		}
		return m, nil
	case "dial_query_command_log":
		m.SetDialQueryCommandLog()
		grp := m.DialQueryCommandLog()
		if v, ok := e.Fields["target_peer"]; ok {
			if err := grp.SetTargetPeer(v); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: dial_query_command_log.target_peer: %w", err)
			}
		}
		if v, ok := e.Fields["instance_id"]; ok {
			if err := grp.SetInstanceId(v); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: dial_query_command_log.instance_id: %w", err)
			}
		}
		if v, ok := e.Fields["since"]; ok {
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: dial_query_command_log.since: %w", err)
			}
			grp.SetSince(n)
		}
		if v, ok := e.Fields["until"]; ok {
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: dial_query_command_log.until: %w", err)
			}
			grp.SetUntil(n)
		}
		if v, ok := e.Fields["limit"]; ok {
			n, err := strconv.ParseInt(v, 10, 32)
			if err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: dial_query_command_log.limit: %w", err)
			}
			grp.SetLimit(int32(n))
		}
		return m, nil
	case "error":
		m.SetError()
		grp := m.Error()
		if v, ok := e.Fields["message"]; ok {
			if err := grp.SetMessage_(v); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: error.message: %w", err)
			}
		}
		return m, nil
	case "group_put":
		m.SetGroupPut()
		grp := m.GroupPut()
		if v, ok := e.Fields["id"]; ok {
			if err := grp.SetId(v); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: group_put.id: %w", err)
			}
		}
		if v, ok := e.Fields["name"]; ok {
			if err := grp.SetName(v); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: group_put.name: %w", err)
			}
		}
		if v, ok := e.Fields["public"]; ok {
			b, err := strconv.ParseBool(v)
			if err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: group_put.public: %w", err)
			}
			grp.SetPublic(b)
		}
		return m, nil
	case "group_delete":
		m.SetGroupDelete()
		grp := m.GroupDelete()
		if v, ok := e.Fields["id"]; ok {
			if err := grp.SetId(v); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: group_delete.id: %w", err)
			}
		}
		return m, nil
	case "command_put":
		m.SetCommandPut()
		grp := m.CommandPut()
		if v, ok := e.Fields["id"]; ok {
			if err := grp.SetId(v); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: command_put.id: %w", err)
			}
		}
		if v, ok := e.Fields["name"]; ok {
			if err := grp.SetName(v); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: command_put.name: %w", err)
			}
		}
		if v, ok := e.Fields["peer_id"]; ok {
			if err := grp.SetPeerId(v); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: command_put.peer_id: %w", err)
			}
		}
		if v, ok := e.Fields["spec"]; ok {
			if err := grp.SetSpec(v); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: command_put.spec: %w", err)
			}
		}
		return m, nil
	case "command_delete":
		m.SetCommandDelete()
		grp := m.CommandDelete()
		if v, ok := e.Fields["id"]; ok {
			if err := grp.SetId(v); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: command_delete.id: %w", err)
			}
		}
		return m, nil
	case "station_put":
		m.SetStationPut()
		grp := m.StationPut()
		if v, ok := e.Fields["peer_id"]; ok {
			if err := grp.SetPeerId(v); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: station_put.peer_id: %w", err)
			}
		}
		if v, ok := e.Fields["name"]; ok {
			if err := grp.SetName(v); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: station_put.name: %w", err)
			}
		}
		if v, ok := e.Fields["attrs"]; ok {
			if err := grp.SetAttrs(v); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: station_put.attrs: %w", err)
			}
		}
		return m, nil
	case "station_delete":
		m.SetStationDelete()
		grp := m.StationDelete()
		if v, ok := e.Fields["peer_id"]; ok {
			if err := grp.SetPeerId(v); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: station_delete.peer_id: %w", err)
			}
		}
		return m, nil
	case "group_command_put":
		m.SetGroupCommandPut()
		grp := m.GroupCommandPut()
		if v, ok := e.Fields["command_id"]; ok {
			if err := grp.SetCommandId(v); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: group_command_put.command_id: %w", err)
			}
		}
		if v, ok := e.Fields["group_id"]; ok {
			if err := grp.SetGroupId(v); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: group_command_put.group_id: %w", err)
			}
		}
		return m, nil
	case "group_command_delete":
		m.SetGroupCommandDelete()
		grp := m.GroupCommandDelete()
		if v, ok := e.Fields["command_id"]; ok {
			if err := grp.SetCommandId(v); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: group_command_delete.command_id: %w", err)
			}
		}
		if v, ok := e.Fields["group_id"]; ok {
			if err := grp.SetGroupId(v); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: group_command_delete.group_id: %w", err)
			}
		}
		return m, nil
	case "peer_group_put":
		m.SetPeerGroupPut()
		grp := m.PeerGroupPut()
		if v, ok := e.Fields["peer_id"]; ok {
			if err := grp.SetPeerId(v); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: peer_group_put.peer_id: %w", err)
			}
		}
		if v, ok := e.Fields["group_id"]; ok {
			if err := grp.SetGroupId(v); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: peer_group_put.group_id: %w", err)
			}
		}
		return m, nil
	case "peer_group_delete":
		m.SetPeerGroupDelete()
		grp := m.PeerGroupDelete()
		if v, ok := e.Fields["peer_id"]; ok {
			if err := grp.SetPeerId(v); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: peer_group_delete.peer_id: %w", err)
			}
		}
		if v, ok := e.Fields["group_id"]; ok {
			if err := grp.SetGroupId(v); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: peer_group_delete.group_id: %w", err)
			}
		}
		return m, nil
	case "permit_request":
		m.SetPermitRequest()
		grp := m.PermitRequest()
		if v, ok := e.Fields["kind"]; ok {
			n, err := strconv.ParseUint(v, 10, 8)
			if err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: permit_request.kind: %w", err)
			}
			grp.SetKind(uint8(n))
		}
		if v, ok := e.Fields["peer_id"]; ok {
			if err := grp.SetPeerId(v); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: permit_request.peer_id: %w", err)
			}
		}
		if v, ok := e.Fields["metadata"]; ok {
			if err := grp.SetMetadata(v); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: permit_request.metadata: %w", err)
			}
		}
		return m, nil
	case "permit_confirm":
		m.SetPermitConfirm()
		grp := m.PermitConfirm()
		if v, ok := e.Fields["kind"]; ok {
			n, err := strconv.ParseUint(v, 10, 8)
			if err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: permit_confirm.kind: %w", err)
			}
			grp.SetKind(uint8(n))
		}
		if v, ok := e.Fields["peer_id"]; ok {
			if err := grp.SetPeerId(v); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: permit_confirm.peer_id: %w", err)
			}
		}
		return m, nil
	case "permit_revoke":
		m.SetPermitRevoke()
		grp := m.PermitRevoke()
		if v, ok := e.Fields["kind"]; ok {
			n, err := strconv.ParseUint(v, 10, 8)
			if err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: permit_revoke.kind: %w", err)
			}
			grp.SetKind(uint8(n))
		}
		if v, ok := e.Fields["peer_id"]; ok {
			if err := grp.SetPeerId(v); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: permit_revoke.peer_id: %w", err)
			}
		}
		return m, nil
	case "join_invite_create":
		m.SetJoinInviteCreate()
		grp := m.JoinInviteCreate()
		if v, ok := e.Fields["token"]; ok {
			raw, err := valueFromJSON(v)
			if err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: join_invite_create.token: %w", err)
			}
			if err := grp.SetToken(raw); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: join_invite_create.token: %w", err)
			}
		}
		if v, ok := e.Fields["suffrage"]; ok {
			n, err := strconv.ParseUint(v, 10, 8)
			if err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: join_invite_create.suffrage: %w", err)
			}
			grp.SetSuffrage(uint8(n))
		}
		return m, nil
	case "join_invite_revoke":
		m.SetJoinInviteRevoke()
		grp := m.JoinInviteRevoke()
		if v, ok := e.Fields["token"]; ok {
			raw, err := valueFromJSON(v)
			if err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: join_invite_revoke.token: %w", err)
			}
			if err := grp.SetToken(raw); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: join_invite_revoke.token: %w", err)
			}
		}
		return m, nil
	case "exec_invite_create":
		m.SetExecInviteCreate()
		grp := m.ExecInviteCreate()
		if v, ok := e.Fields["token"]; ok {
			raw, err := valueFromJSON(v)
			if err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: exec_invite_create.token: %w", err)
			}
			if err := grp.SetToken(raw); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: exec_invite_create.token: %w", err)
			}
		}
		if v, ok := e.Fields["command_id"]; ok {
			if err := grp.SetCommandId(v); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: exec_invite_create.command_id: %w", err)
			}
		}
		if v, ok := e.Fields["inputs_json"]; ok {
			if err := grp.SetInputsJson(v); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: exec_invite_create.inputs_json: %w", err)
			}
		}
		return m, nil
	case "exec_invite_revoke":
		m.SetExecInviteRevoke()
		grp := m.ExecInviteRevoke()
		if v, ok := e.Fields["token"]; ok {
			raw, err := valueFromJSON(v)
			if err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: exec_invite_revoke.token: %w", err)
			}
			if err := grp.SetToken(raw); err != nil {
				return shmevent.Msg{}, fmt.Errorf("e2edata: exec_invite_revoke.token: %w", err)
			}
		}
		return m, nil
	default:
		return shmevent.Msg{}, fmt.Errorf("e2edata: unknown or unsupported event %q", e.Op)
	}
}

// EventFromMsg converts a decoded Msg back to the JSON-friendly Event
// shape, for recording/printing -- ToMsg's inverse, reading every field
// the matched variant carries (request or response alike) into Fields.
func EventFromMsg(m shmevent.Msg) (Event, error) {
	id := m.Id()
	fields := map[string]string{}
	switch m.Which() {
	case shmevent.Event_Which_setKey:
		grp := m.SetKey()
		{
			v, err := grp.Value()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: set_key.value: %w", err)
			}
			if len(v) > 0 {
				fields["value"] = valueToJSON(v)
			}
		}
		return Event{Op: "set_key", ID: id, Fields: fields}, nil
	case shmevent.Event_Which_setField:
		grp := m.SetField()
		if v := grp.SourceId(); v != 0 {
			fields["source_id"] = strconv.FormatUint(uint64(v), 10)
		}
		{
			v, err := grp.Value()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: set_field.value: %w", err)
			}
			if len(v) > 0 {
				fields["value"] = valueToJSON(v)
			}
		}
		return Event{Op: "set_field", ID: id, Fields: fields}, nil
	case shmevent.Event_Which_getKey:
		grp := m.GetKey()
		if v := grp.SourceId(); v != 0 {
			fields["source_id"] = strconv.FormatUint(uint64(v), 10)
		}
		{
			v, err := grp.Key()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: get_key.key: %w", err)
			}
			if len(v) > 0 {
				fields["key"] = valueToJSON(v)
			}
		}
		return Event{Op: "get_key", ID: id, Fields: fields}, nil
	case shmevent.Event_Which_getFieldByRegistry:
		grp := m.GetFieldByRegistry()
		if v := grp.SourceId(); v != 0 {
			fields["source_id"] = strconv.FormatUint(uint64(v), 10)
		}
		{
			v, err := grp.Value()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: get_field_by_registry.value: %w", err)
			}
			if len(v) > 0 {
				fields["value"] = valueToJSON(v)
			}
		}
		return Event{Op: "get_field_by_registry", ID: id, Fields: fields}, nil
	case shmevent.Event_Which_getFieldByKey:
		grp := m.GetFieldByKey()
		{
			v, err := grp.Key()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: get_field_by_key.key: %w", err)
			}
			if len(v) > 0 {
				fields["key"] = valueToJSON(v)
			}
		}
		{
			v, err := grp.Value()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: get_field_by_key.value: %w", err)
			}
			if len(v) > 0 {
				fields["value"] = valueToJSON(v)
			}
		}
		return Event{Op: "get_field_by_key", ID: id, Fields: fields}, nil
	case shmevent.Event_Which_getPublicKey:
		grp := m.GetPublicKey()
		{
			v, err := grp.PubKey()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: get_public_key.pub_key: %w", err)
			}
			if len(v) > 0 {
				fields["pub_key"] = valueToJSON(v)
			}
		}
		return Event{Op: "get_public_key", ID: id, Fields: fields}, nil
	case shmevent.Event_Which_getPrivateKey:
		grp := m.GetPrivateKey()
		{
			v, err := grp.PrivKey()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: get_private_key.priv_key: %w", err)
			}
			if len(v) > 0 {
				fields["priv_key"] = valueToJSON(v)
			}
		}
		return Event{Op: "get_private_key", ID: id, Fields: fields}, nil
	case shmevent.Event_Which_bootstrapOrJoinCluster:
		grp := m.BootstrapOrJoinCluster()
		{
			v, err := grp.LeaderAddr()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: bootstrap_or_join_cluster.leader_addr: %w", err)
			}
			if v != "" {
				fields["leader_addr"] = v
			}
		}
		return Event{Op: "bootstrap_or_join_cluster", ID: id, Fields: fields}, nil
	case shmevent.Event_Which_addLearner:
		grp := m.AddLearner()
		if v := grp.ClaimedPeerId(); v != 0 {
			fields["claimed_peer_id"] = strconv.FormatUint(uint64(v), 10)
		}
		{
			v, err := grp.Addr()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: add_learner.addr: %w", err)
			}
			if v != "" {
				fields["addr"] = v
			}
		}
		return Event{Op: "add_learner", ID: id, Fields: fields}, nil
	case shmevent.Event_Which_set:
		grp := m.Set()
		{
			v, err := grp.Key()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: set.key: %w", err)
			}
			if len(v) > 0 {
				fields["key"] = valueToJSON(v)
			}
		}
		{
			v, err := grp.Value()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: set.value: %w", err)
			}
			if len(v) > 0 {
				fields["value"] = valueToJSON(v)
			}
		}
		return Event{Op: "set", ID: id, Fields: fields}, nil
	case shmevent.Event_Which_execute:
		grp := m.Execute()
		if v := grp.SourceId(); v != 0 {
			fields["source_id"] = strconv.FormatUint(uint64(v), 10)
		}
		if v := grp.DestinationId(); v != 0 {
			fields["destination_id"] = strconv.FormatUint(uint64(v), 10)
		}
		{
			v, err := grp.SenderPeerId()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: execute.sender_peer_id: %w", err)
			}
			if v != "" {
				fields["sender_peer_id"] = v
			}
		}
		{
			v, err := grp.Value()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: execute.value: %w", err)
			}
			if len(v) > 0 {
				fields["value"] = valueToJSON(v)
			}
		}
		return Event{Op: "execute", ID: id, Fields: fields}, nil
	case shmevent.Event_Which_pollExecute:
		grp := m.PollExecute()
		{
			v, err := grp.SenderPeerId()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: poll_execute.sender_peer_id: %w", err)
			}
			if v != "" {
				fields["sender_peer_id"] = v
			}
		}
		{
			v, err := grp.Value()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: poll_execute.value: %w", err)
			}
			if len(v) > 0 {
				fields["value"] = valueToJSON(v)
			}
		}
		return Event{Op: "poll_execute", ID: id, Fields: fields}, nil
	case shmevent.Event_Which_listRange:
		grp := m.ListRange()
		{
			v, err := grp.Start()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: list_range.start: %w", err)
			}
			if len(v) > 0 {
				fields["start"] = valueToJSON(v)
			}
		}
		{
			v, err := grp.End()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: list_range.end: %w", err)
			}
			if len(v) > 0 {
				fields["end"] = valueToJSON(v)
			}
		}
		{
			v, err := grp.Key()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: list_range.key: %w", err)
			}
			if len(v) > 0 {
				fields["key"] = valueToJSON(v)
			}
		}
		{
			v, err := grp.Value()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: list_range.value: %w", err)
			}
			if len(v) > 0 {
				fields["value"] = valueToJSON(v)
			}
		}
		return Event{Op: "list_range", ID: id, Fields: fields}, nil
	case shmevent.Event_Which_logAppend:
		grp := m.LogAppend()
		{
			v, err := grp.Key()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: log_append.key: %w", err)
			}
			if len(v) > 0 {
				fields["key"] = valueToJSON(v)
			}
		}
		{
			v, err := grp.Value()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: log_append.value: %w", err)
			}
			if len(v) > 0 {
				fields["value"] = valueToJSON(v)
			}
		}
		return Event{Op: "log_append", ID: id, Fields: fields}, nil
	case shmevent.Event_Which_leave:
		return Event{Op: "leave", ID: id, Fields: fields}, nil
	case shmevent.Event_Which_execInviteRedeem:
		grp := m.ExecInviteRedeem()
		{
			v, err := grp.SourceAddr()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: exec_invite_redeem.source_addr: %w", err)
			}
			if v != "" {
				fields["source_addr"] = v
			}
		}
		{
			v, err := grp.Token()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: exec_invite_redeem.token: %w", err)
			}
			if len(v) > 0 {
				fields["token"] = valueToJSON(v)
			}
		}
		{
			v, err := grp.InstanceId()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: exec_invite_redeem.instance_id: %w", err)
			}
			if v != "" {
				fields["instance_id"] = v
			}
		}
		{
			v, err := grp.RedeemerPeerId()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: exec_invite_redeem.redeemer_peer_id: %w", err)
			}
			if v != "" {
				fields["redeemer_peer_id"] = v
			}
		}
		return Event{Op: "exec_invite_redeem", ID: id, Fields: fields}, nil
	case shmevent.Event_Which_joinRequestCreate:
		grp := m.JoinRequestCreate()
		{
			v, err := grp.Token()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: join_request_create.token: %w", err)
			}
			if len(v) > 0 {
				fields["token"] = valueToJSON(v)
			}
		}
		return Event{Op: "join_request_create", ID: id, Fields: fields}, nil
	case shmevent.Event_Which_joinRequestCancel:
		grp := m.JoinRequestCancel()
		{
			v, err := grp.Token()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: join_request_cancel.token: %w", err)
			}
			if len(v) > 0 {
				fields["token"] = valueToJSON(v)
			}
		}
		return Event{Op: "join_request_cancel", ID: id, Fields: fields}, nil
	case shmevent.Event_Which_recruit:
		grp := m.Recruit()
		{
			v, err := grp.Ticket()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: recruit.ticket: %w", err)
			}
			if v != "" {
				fields["ticket"] = v
			}
		}
		if v := grp.Suffrage(); v != 0 {
			fields["suffrage"] = strconv.FormatUint(uint64(v), 10)
		}
		return Event{Op: "recruit", ID: id, Fields: fields}, nil
	case shmevent.Event_Which_getOwnAddr:
		grp := m.GetOwnAddr()
		{
			v, err := grp.Addr()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: get_own_addr.addr: %w", err)
			}
			if v != "" {
				fields["addr"] = v
			}
		}
		return Event{Op: "get_own_addr", ID: id, Fields: fields}, nil
	case shmevent.Event_Which_channelOpen:
		grp := m.ChannelOpen()
		{
			v, err := grp.PeerId()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: channel_open.peer_id: %w", err)
			}
			if v != "" {
				fields["peer_id"] = v
			}
		}
		{
			v, err := grp.SenderPeerId()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: channel_open.sender_peer_id: %w", err)
			}
			if v != "" {
				fields["sender_peer_id"] = v
			}
		}
		return Event{Op: "channel_open", ID: id, Fields: fields}, nil
	case shmevent.Event_Which_channelSend:
		grp := m.ChannelSend()
		{
			v, err := grp.ChannelId()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: channel_send.channel_id: %w", err)
			}
			if v != "" {
				fields["channel_id"] = v
			}
		}
		if v := grp.Purpose(); v != 0 {
			fields["purpose"] = strconv.FormatUint(uint64(v), 10)
		}
		{
			v, err := grp.Chunk()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: channel_send.chunk: %w", err)
			}
			if len(v) > 0 {
				fields["chunk"] = valueToJSON(v)
			}
		}
		return Event{Op: "channel_send", ID: id, Fields: fields}, nil
	case shmevent.Event_Which_channelPoll:
		grp := m.ChannelPoll()
		{
			v, err := grp.ChannelId()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: channel_poll.channel_id: %w", err)
			}
			if v != "" {
				fields["channel_id"] = v
			}
		}
		if v := grp.Status(); v != 0 {
			fields["status"] = strconv.FormatUint(uint64(v), 10)
		}
		if v := grp.Purpose(); v != 0 {
			fields["purpose"] = strconv.FormatUint(uint64(v), 10)
		}
		{
			v, err := grp.Chunk()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: channel_poll.chunk: %w", err)
			}
			if len(v) > 0 {
				fields["chunk"] = valueToJSON(v)
			}
		}
		return Event{Op: "channel_poll", ID: id, Fields: fields}, nil
	case shmevent.Event_Which_channelListen:
		grp := m.ChannelListen()
		{
			v, err := grp.ChannelId()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: channel_listen.channel_id: %w", err)
			}
			if v != "" {
				fields["channel_id"] = v
			}
		}
		{
			v, err := grp.RemotePeerId()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: channel_listen.remote_peer_id: %w", err)
			}
			if v != "" {
				fields["remote_peer_id"] = v
			}
		}
		return Event{Op: "channel_listen", ID: id, Fields: fields}, nil
	case shmevent.Event_Which_channelClose:
		grp := m.ChannelClose()
		{
			v, err := grp.ChannelId()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: channel_close.channel_id: %w", err)
			}
			if v != "" {
				fields["channel_id"] = v
			}
		}
		return Event{Op: "channel_close", ID: id, Fields: fields}, nil
	case shmevent.Event_Which_channelCloseWrite:
		grp := m.ChannelCloseWrite()
		{
			v, err := grp.ChannelId()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: channel_close_write.channel_id: %w", err)
			}
			if v != "" {
				fields["channel_id"] = v
			}
		}
		return Event{Op: "channel_close_write", ID: id, Fields: fields}, nil
	case shmevent.Event_Which_channelDataReady:
		grp := m.ChannelDataReady()
		{
			v, err := grp.ChannelId()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: channel_data_ready.channel_id: %w", err)
			}
			if v != "" {
				fields["channel_id"] = v
			}
		}
		return Event{Op: "channel_data_ready", ID: id, Fields: fields}, nil
	case shmevent.Event_Which_kick:
		grp := m.Kick()
		{
			v, err := grp.PeerId()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: kick.peer_id: %w", err)
			}
			if v != "" {
				fields["peer_id"] = v
			}
		}
		return Event{Op: "kick", ID: id, Fields: fields}, nil
	case shmevent.Event_Which_txn:
		return Event{Op: "txn", ID: id, Fields: fields}, nil
	case shmevent.Event_Which_getVersion:
		grp := m.GetVersion()
		{
			v, err := grp.Commit()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: get_version.commit: %w", err)
			}
			if v != "" {
				fields["commit"] = v
			}
		}
		fields["dirty"] = strconv.FormatBool(grp.Dirty())
		{
			v, err := grp.BuildTime()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: get_version.build_time: %w", err)
			}
			if v != "" {
				fields["build_time"] = v
			}
		}
		{
			v, err := grp.GoVersion()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: get_version.go_version: %w", err)
			}
			if v != "" {
				fields["go_version"] = v
			}
		}
		{
			v, err := grp.Libp2pVersion()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: get_version.libp2p_version: %w", err)
			}
			if v != "" {
				fields["libp2p_version"] = v
			}
		}
		return Event{Op: "get_version", ID: id, Fields: fields}, nil
	case shmevent.Event_Which_publicAccess:
		grp := m.PublicAccess()
		{
			v, err := grp.TargetPeer()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: public_access.target_peer: %w", err)
			}
			if v != "" {
				fields["target_peer"] = v
			}
		}
		{
			v, err := grp.Note()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: public_access.note: %w", err)
			}
			if v != "" {
				fields["note"] = v
			}
		}
		{
			v, err := grp.InstanceId()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: public_access.instance_id: %w", err)
			}
			if v != "" {
				fields["instance_id"] = v
			}
		}
		return Event{Op: "public_access", ID: id, Fields: fields}, nil
	case shmevent.Event_Which_execTicket:
		grp := m.ExecTicket()
		{
			v, err := grp.SourceAddr()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: exec_ticket.source_addr: %w", err)
			}
			if v != "" {
				fields["source_addr"] = v
			}
		}
		{
			v, err := grp.Token()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: exec_ticket.token: %w", err)
			}
			if len(v) > 0 {
				fields["token"] = valueToJSON(v)
			}
		}
		return Event{Op: "exec_ticket", ID: id, Fields: fields}, nil
	case shmevent.Event_Which_joinTicket:
		grp := m.JoinTicket()
		{
			v, err := grp.SourceAddr()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: join_ticket.source_addr: %w", err)
			}
			if v != "" {
				fields["source_addr"] = v
			}
		}
		{
			v, err := grp.Token()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: join_ticket.token: %w", err)
			}
			if len(v) > 0 {
				fields["token"] = valueToJSON(v)
			}
		}
		return Event{Op: "join_ticket", ID: id, Fields: fields}, nil
	case shmevent.Event_Which_joinRequestTicket:
		grp := m.JoinRequestTicket()
		{
			v, err := grp.SourceAddr()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: join_request_ticket.source_addr: %w", err)
			}
			if v != "" {
				fields["source_addr"] = v
			}
		}
		{
			v, err := grp.Token()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: join_request_ticket.token: %w", err)
			}
			if len(v) > 0 {
				fields["token"] = valueToJSON(v)
			}
		}
		return Event{Op: "join_request_ticket", ID: id, Fields: fields}, nil
	case shmevent.Event_Which_dialSubmitCommand:
		grp := m.DialSubmitCommand()
		{
			v, err := grp.TargetPeer()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: dial_submit_command.target_peer: %w", err)
			}
			if v != "" {
				fields["target_peer"] = v
			}
		}
		{
			v, err := grp.CommandId()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: dial_submit_command.command_id: %w", err)
			}
			if v != "" {
				fields["command_id"] = v
			}
		}
		{
			v, err := grp.InputsJson()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: dial_submit_command.inputs_json: %w", err)
			}
			if v != "" {
				fields["inputs_json"] = v
			}
		}
		{
			v, err := grp.Note()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: dial_submit_command.note: %w", err)
			}
			if v != "" {
				fields["note"] = v
			}
		}
		{
			v, err := grp.InstanceId()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: dial_submit_command.instance_id: %w", err)
			}
			if v != "" {
				fields["instance_id"] = v
			}
		}
		return Event{Op: "dial_submit_command", ID: id, Fields: fields}, nil
	case shmevent.Event_Which_dialQueryCommandLog:
		grp := m.DialQueryCommandLog()
		{
			v, err := grp.TargetPeer()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: dial_query_command_log.target_peer: %w", err)
			}
			if v != "" {
				fields["target_peer"] = v
			}
		}
		{
			v, err := grp.InstanceId()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: dial_query_command_log.instance_id: %w", err)
			}
			if v != "" {
				fields["instance_id"] = v
			}
		}
		if v := grp.Since(); v != 0 {
			fields["since"] = strconv.FormatInt(v, 10)
		}
		if v := grp.Until(); v != 0 {
			fields["until"] = strconv.FormatInt(v, 10)
		}
		if v := grp.Limit(); v != 0 {
			fields["limit"] = strconv.FormatInt(int64(v), 10)
		}
		return Event{Op: "dial_query_command_log", ID: id, Fields: fields}, nil
	case shmevent.Event_Which_error:
		grp := m.Error()
		{
			v, err := grp.Message_()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: error.message: %w", err)
			}
			if v != "" {
				fields["message"] = v
			}
		}
		return Event{Op: "error", ID: id, Fields: fields}, nil
	case shmevent.Event_Which_groupPut:
		grp := m.GroupPut()
		{
			v, err := grp.Id()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: group_put.id: %w", err)
			}
			if v != "" {
				fields["id"] = v
			}
		}
		{
			v, err := grp.Name()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: group_put.name: %w", err)
			}
			if v != "" {
				fields["name"] = v
			}
		}
		fields["public"] = strconv.FormatBool(grp.Public())
		return Event{Op: "group_put", ID: id, Fields: fields}, nil
	case shmevent.Event_Which_groupDelete:
		grp := m.GroupDelete()
		{
			v, err := grp.Id()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: group_delete.id: %w", err)
			}
			if v != "" {
				fields["id"] = v
			}
		}
		return Event{Op: "group_delete", ID: id, Fields: fields}, nil
	case shmevent.Event_Which_commandPut:
		grp := m.CommandPut()
		{
			v, err := grp.Id()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: command_put.id: %w", err)
			}
			if v != "" {
				fields["id"] = v
			}
		}
		{
			v, err := grp.Name()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: command_put.name: %w", err)
			}
			if v != "" {
				fields["name"] = v
			}
		}
		{
			v, err := grp.PeerId()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: command_put.peer_id: %w", err)
			}
			if v != "" {
				fields["peer_id"] = v
			}
		}
		{
			v, err := grp.Spec()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: command_put.spec: %w", err)
			}
			if v != "" {
				fields["spec"] = v
			}
		}
		return Event{Op: "command_put", ID: id, Fields: fields}, nil
	case shmevent.Event_Which_commandDelete:
		grp := m.CommandDelete()
		{
			v, err := grp.Id()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: command_delete.id: %w", err)
			}
			if v != "" {
				fields["id"] = v
			}
		}
		return Event{Op: "command_delete", ID: id, Fields: fields}, nil
	case shmevent.Event_Which_stationPut:
		grp := m.StationPut()
		{
			v, err := grp.PeerId()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: station_put.peer_id: %w", err)
			}
			if v != "" {
				fields["peer_id"] = v
			}
		}
		{
			v, err := grp.Name()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: station_put.name: %w", err)
			}
			if v != "" {
				fields["name"] = v
			}
		}
		{
			v, err := grp.Attrs()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: station_put.attrs: %w", err)
			}
			if v != "" {
				fields["attrs"] = v
			}
		}
		return Event{Op: "station_put", ID: id, Fields: fields}, nil
	case shmevent.Event_Which_stationDelete:
		grp := m.StationDelete()
		{
			v, err := grp.PeerId()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: station_delete.peer_id: %w", err)
			}
			if v != "" {
				fields["peer_id"] = v
			}
		}
		return Event{Op: "station_delete", ID: id, Fields: fields}, nil
	case shmevent.Event_Which_groupCommandPut:
		grp := m.GroupCommandPut()
		{
			v, err := grp.CommandId()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: group_command_put.command_id: %w", err)
			}
			if v != "" {
				fields["command_id"] = v
			}
		}
		{
			v, err := grp.GroupId()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: group_command_put.group_id: %w", err)
			}
			if v != "" {
				fields["group_id"] = v
			}
		}
		return Event{Op: "group_command_put", ID: id, Fields: fields}, nil
	case shmevent.Event_Which_groupCommandDelete:
		grp := m.GroupCommandDelete()
		{
			v, err := grp.CommandId()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: group_command_delete.command_id: %w", err)
			}
			if v != "" {
				fields["command_id"] = v
			}
		}
		{
			v, err := grp.GroupId()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: group_command_delete.group_id: %w", err)
			}
			if v != "" {
				fields["group_id"] = v
			}
		}
		return Event{Op: "group_command_delete", ID: id, Fields: fields}, nil
	case shmevent.Event_Which_peerGroupPut:
		grp := m.PeerGroupPut()
		{
			v, err := grp.PeerId()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: peer_group_put.peer_id: %w", err)
			}
			if v != "" {
				fields["peer_id"] = v
			}
		}
		{
			v, err := grp.GroupId()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: peer_group_put.group_id: %w", err)
			}
			if v != "" {
				fields["group_id"] = v
			}
		}
		return Event{Op: "peer_group_put", ID: id, Fields: fields}, nil
	case shmevent.Event_Which_peerGroupDelete:
		grp := m.PeerGroupDelete()
		{
			v, err := grp.PeerId()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: peer_group_delete.peer_id: %w", err)
			}
			if v != "" {
				fields["peer_id"] = v
			}
		}
		{
			v, err := grp.GroupId()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: peer_group_delete.group_id: %w", err)
			}
			if v != "" {
				fields["group_id"] = v
			}
		}
		return Event{Op: "peer_group_delete", ID: id, Fields: fields}, nil
	case shmevent.Event_Which_permitRequest:
		grp := m.PermitRequest()
		if v := grp.Kind(); v != 0 {
			fields["kind"] = strconv.FormatUint(uint64(v), 10)
		}
		{
			v, err := grp.PeerId()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: permit_request.peer_id: %w", err)
			}
			if v != "" {
				fields["peer_id"] = v
			}
		}
		{
			v, err := grp.Metadata()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: permit_request.metadata: %w", err)
			}
			if v != "" {
				fields["metadata"] = v
			}
		}
		return Event{Op: "permit_request", ID: id, Fields: fields}, nil
	case shmevent.Event_Which_permitConfirm:
		grp := m.PermitConfirm()
		if v := grp.Kind(); v != 0 {
			fields["kind"] = strconv.FormatUint(uint64(v), 10)
		}
		{
			v, err := grp.PeerId()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: permit_confirm.peer_id: %w", err)
			}
			if v != "" {
				fields["peer_id"] = v
			}
		}
		return Event{Op: "permit_confirm", ID: id, Fields: fields}, nil
	case shmevent.Event_Which_permitRevoke:
		grp := m.PermitRevoke()
		if v := grp.Kind(); v != 0 {
			fields["kind"] = strconv.FormatUint(uint64(v), 10)
		}
		{
			v, err := grp.PeerId()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: permit_revoke.peer_id: %w", err)
			}
			if v != "" {
				fields["peer_id"] = v
			}
		}
		return Event{Op: "permit_revoke", ID: id, Fields: fields}, nil
	case shmevent.Event_Which_joinInviteCreate:
		grp := m.JoinInviteCreate()
		{
			v, err := grp.Token()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: join_invite_create.token: %w", err)
			}
			if len(v) > 0 {
				fields["token"] = valueToJSON(v)
			}
		}
		if v := grp.Suffrage(); v != 0 {
			fields["suffrage"] = strconv.FormatUint(uint64(v), 10)
		}
		return Event{Op: "join_invite_create", ID: id, Fields: fields}, nil
	case shmevent.Event_Which_joinInviteRevoke:
		grp := m.JoinInviteRevoke()
		{
			v, err := grp.Token()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: join_invite_revoke.token: %w", err)
			}
			if len(v) > 0 {
				fields["token"] = valueToJSON(v)
			}
		}
		return Event{Op: "join_invite_revoke", ID: id, Fields: fields}, nil
	case shmevent.Event_Which_execInviteCreate:
		grp := m.ExecInviteCreate()
		{
			v, err := grp.Token()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: exec_invite_create.token: %w", err)
			}
			if len(v) > 0 {
				fields["token"] = valueToJSON(v)
			}
		}
		{
			v, err := grp.CommandId()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: exec_invite_create.command_id: %w", err)
			}
			if v != "" {
				fields["command_id"] = v
			}
		}
		{
			v, err := grp.InputsJson()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: exec_invite_create.inputs_json: %w", err)
			}
			if v != "" {
				fields["inputs_json"] = v
			}
		}
		return Event{Op: "exec_invite_create", ID: id, Fields: fields}, nil
	case shmevent.Event_Which_execInviteRevoke:
		grp := m.ExecInviteRevoke()
		{
			v, err := grp.Token()
			if err != nil {
				return Event{}, fmt.Errorf("e2edata: exec_invite_revoke.token: %w", err)
			}
			if len(v) > 0 {
				fields["token"] = valueToJSON(v)
			}
		}
		return Event{Op: "exec_invite_revoke", ID: id, Fields: fields}, nil
	default:
		return Event{}, fmt.Errorf("e2edata: unknown event %s", shmevent.EventName(m.Which()))
	}
}

// valueToJSON renders raw as plain text if it's valid UTF-8 (every KV test
// value in practice), or as a "0x"-prefixed hex string otherwise (a raw
// Ed25519 key, or deliberately-corrupt test bytes) -- see Event's doc
// comment.
func valueToJSON(raw []byte) string {
	if len(raw) == 0 {
		return ""
	}
	if utf8.Valid(raw) {
		return string(raw)
	}
	return "0x" + hex.EncodeToString(raw)
}

// valueFromJSON is valueToJSON's inverse.
func valueFromJSON(s string) ([]byte, error) {
	if s == "" {
		return nil, nil
	}
	if rest, ok := hexPrefix(s); ok {
		raw, err := hex.DecodeString(rest)
		if err != nil {
			return nil, fmt.Errorf("decode 0x-prefixed value: %w", err)
		}
		return raw, nil
	}
	return []byte(s), nil
}

func hexPrefix(s string) (rest string, ok bool) {
	if len(s) >= 2 && s[0] == '0' && s[1] == 'x' {
		return s[2:], true
	}
	return "", false
}

// ExpectSucceeded/ExpectRejected/ExpectNoCrash are OpticalExpectSpec.Result's canned values --
// the on-disk names for UiCommandE2ETest.kt's own assertSucceeded/assertRejected/assertNoCrash
// assertion functions (see that file's doc comment for what each means: a rejection is expected
// when this device isn't currently a raft voter, no_crash tolerates either outcome for a case
// whose result depends on state this pipeline doesn't control). Empty/unrecognized defaults to
// ExpectSucceeded on the Kotlin side.
//
// Result also accepts a fourth, parameterized form these three constants don't cover:
// "contains:<substring>" (tokens substituted the same as Generate's Params, e.g.
// "contains:{{testValue}}"), asserting the result line actually contains <substring> rather than
// merely not reporting "FAILED" -- see UiCommandE2ETest.kt's assertContains. This is what closes
// the gap between "the command didn't error" and "the command had the effect it should have"
// (e.g. a KV: Get case checking the exact value an earlier KV: Submit case wrote, not just that
// Get itself succeeded) -- the three canned checks above can't express that on their own. A case
// that depends on an earlier case's own effect this way relies on OpticalScanCase.Order.
const (
	ExpectSucceeded = "succeeded"
	ExpectRejected  = "rejected"
	ExpectNoCrash   = "no_crash"
)

// StatusPass/StatusFail/StatusSkipped are the Row.Status conventions this
// package's runner uses. Any other non-zero value is still "failed" as far
// as File methods are concerned; StatusSkipped exists only so a platform
// this pipeline can't yet drive for real (see e2erun's android gap) doesn't
// get reported as a false failure or a false pass.
const (
	StatusPass    = 0
	StatusFail    = 1
	StatusSkipped = 2
)

// Row is one recorded test: send Event to the node identified by Node
// (against File.Nodes), as it stood the last time this version's tests
// ran, with the outcome in Status/Error.
type Row struct {
	Version int    `json:"version"`
	Node    int    `json:"node"`
	Event   Event  `json:"event"`
	Status  int    `json:"status"`
	Error   string `json:"error,omitempty"`

	// Platform is Node's platform at the time this row was recorded (see
	// AddTest), kept independently of whatever File.Nodes[Node] currently
	// holds -- unlike Node, this survives that node later being deleted
	// (mage e2e:deletenode/destroyall). It's what lets Load's backfill (for
	// rows predating this field) and Run's EnsureNode recover from exactly
	// that: a row whose node id no longer resolves still knows what
	// platform to re-provision under that same id, instead of being
	// permanently orphaned the way "unknown node id" used to mean.
	Platform Platform `json:"platform,omitempty"`
}

// UICaseResult is the generic shape UiCommandE2ETest.kt's own writeResults writes one
// instrumentation method's result line as, to the device's ui_e2e_results.json --
// pkg/e2erun/android_optical.go's generateAndHold/awaitAndVerifyScan/verifyLogContains each pull
// and parse this same shape (see runOpticalMethod) to learn what a single method invocation
// actually did, regardless of which of those three it was.
type UICaseResult struct {
	Command string `json:"command"`
	Pass    bool   `json:"pass"`
	Error   string `json:"error,omitempty"`
	Output  string `json:"output,omitempty"`
}

// OpticalGenerateSpec is device A's half of one OpticalScanCase: which of android-app's two
// remaining DataMatrix-generating screens to reach and what to fill in, mirroring
// UiCommandE2ETest.kt's generateAndHold switch exactly (same field names, snake_case on the wire,
// read directly by that Kotlin code -- no translation layer). Only two targets exist now that
// every CommandCatalog.kt spec generates uniformly through CommandDetailScreen (see that screen's
// own doc comment): EventBuilderScreen/ConnectDeviceScreen and their own bespoke generate targets
// are gone, fully subsumed by "run".
type OpticalGenerateSpec struct {
	// Target selects the generating screen: "run" (CommandDetailScreen's own "Generate
	// DataMatrix" button -- the uniform path for every CommandCatalog.kt spec, including the one
	// exception, CreateJoinRequestTicket, whose generateFromResultBase64 flag makes that same
	// button render a real signed ticket instead of a RunCode -- CommandDetailScreen handles that
	// distinction internally, so this spec doesn't need to know which case it is), or "nav_group"
	// (GroupPickerScreen, a pure navigation shortcut unrelated to command execution).
	Target string `json:"target"`
	// Category names a CommandCatalog.kt category -- required for both targets: "run" uses it
	// (with Name) to navigate to the right CommandDetailScreen, "nav_group" uses it alone to
	// build the NavCode.Group code directly.
	Category string `json:"category,omitempty"`
	// Name is the CommandSpec name within Category -- required for "run", unused by "nav_group".
	Name string `json:"name,omitempty"`
	// Params are typed into CommandDetailScreen's own ordered param_N fields before Generate --
	// required (may be an empty list) for "run", unused by "nav_group". May contain the same
	// substitution tokens Result/VerifyOnDeviceA accept -- see OpticalExpectSpec's doc comment.
	Params []string `json:"params,omitempty"`
}

// OpticalExpectSpec is device B's half of one OpticalScanCase: what a real camera scan of the
// generated code should produce there, mirroring UiCommandE2ETest.kt's awaitAndVerifyScan switch.
type OpticalExpectSpec struct {
	// Kind selects the expected outcome: "run" (RunConfirmDialog, Execute tapped -- covers every
	// command now, including what used to be a separate "event" outcome before RunCode unified
	// them), "nav_group" (silent NavCode.Group navigation, no dialog), or "ticket"
	// (RecruitConfirmDialog, Approve tapped).
	Kind string `json:"kind"`
	// CategoryTitle is CommandListScreen's own categoryTitle text to expect after a "nav_group"
	// scan.
	CategoryTitle string `json:"category_title,omitempty"`
	// Result is required for "run" -- the same succeeded/rejected/no_crash/"contains:<substring>"
	// convention ExpectSucceeded/ExpectRejected/ExpectNoCrash describe, checked against the
	// post-Execute OutputLog entry CommandExecutor.execute records on device B. Tokens
	// ("{{selfPeerID}}", "{{testKey}}", "{{testValue}}", ...) substituted the same as Generate's
	// own Params.
	Result string `json:"result,omitempty"`
	// VerifyOnDeviceA, when non-empty, additionally polls device A's own Activity Log
	// (OutputLog, not device B's) for an entry containing this string, with the literal
	// substring "{{instance_id}}" first replaced by whatever instance_id device B's own
	// CommandExecutor result reported -- needed for "Dispatch: DialSubmitCommand" cases
	// specifically, since B executing that command makes B dial back and submit against A's own
	// RunCommandDispatcher, so the actual dispatch is only ever observable on A. Empty (every
	// other kind) means no cross-device check.
	VerifyOnDeviceA string `json:"verify_on_device_a,omitempty"`
}

// OpticalScanCase is one round trip through android-app's real DataMatrix/camera pipeline:
// device A generates a code (Generate), a real camera on device B reads it, and device B is
// expected to react as described (Expect). This is the only android command-execution test plan
// now -- every command in the app executes exactly this way, so there's no separate single-device
// in-process equivalent left to distinguish it from.
//
// A list, not a map keyed by label: there is no natural unique key here (the same category+name
// could legitimately appear more than once with different Params to test different states), and
// there is no "every catalog command gets a default entry" fallback -- every case that should run
// has to be listed explicitly.
type OpticalScanCase struct {
	// CaseID names this entry for logging/results -- must be unique across the list, since it's
	// also the correlation key UiCommandE2ETest.kt's generateAndHold/awaitAndVerifyScan use to
	// label their own ui_e2e_results.json entries ("OpticalGenerate: <CaseID>"/
	// "OpticalScan: <CaseID>").
	CaseID   string              `json:"case_id"`
	Generate OpticalGenerateSpec `json:"generate"`
	Expect   OpticalExpectSpec   `json:"expect"`
	// HoldMillis bounds how long device A keeps the generated code on screen for device B's
	// camera to read it; TimeoutMs bounds how long device B waits for the expected outcome
	// before giving up. Real camera decode latency, unlike an in-process Compose assertion, is
	// neither instant nor perfectly reliable, and it scales with how dense the generated symbol
	// is: a 336-byte signed ticket is a 144x144-module DataMatrix that needs materially longer
	// than the 53-93 byte RunCode most cases generate (measured live on the two-device rig).
	//
	// Both are optional and must be set together (pkg/e2erun/android_optical.go rejects a case
	// setting only one). A case that sets neither -- almost all of them -- falls back to
	// UiCommandE2ETest.kt's BATCH_HOLD_MILLIS/BATCH_TIMEOUT_MS, which is the right default for
	// any code a warmed-up rig reads in a second or two; setting them is for the exceptions,
	// and buys that one case more room without slowing the other 89 down.
	//
	// HoldMillis >= TimeoutMs is a correctness invariant, checked before either device is
	// launched: A advancing to the next case while B is still waiting for this one
	// desynchronizes the whole batch, which then reports as a cascade of unrelated per-case
	// timeouts. Both are floors rather than caps for the first two cases in a batch, which
	// device B's own one-time join/relay setup already extends (see FIRST_CASE_HOLD_MILLIS).
	HoldMillis int64 `json:"hold_millis,omitempty"`
	TimeoutMs  int64 `json:"timeout_ms,omitempty"`
	// Order, when non-zero, is this case's 1-based position in a sequence the caller needs run in
	// exactly that order -- this is a list, not a map, so file order already reads top to bottom,
	// but Order documents *intent* (a case that depends on a specific earlier case's own effect,
	// e.g. a "run" case Get-ing a value an earlier "run" case Set) versus cases that merely happen
	// to be adjacent. Set on every case in a sequence or on none.
	Order int `json:"order,omitempty"`
}

// OpticalScanCaseResult is one OpticalScanCase's outcome -- written by the Go orchestrator
// (pkg/e2erun/android_optical.go) after combining both devices' own results, not by either device
// directly (unlike UICaseResult, this describes a whole two-device round trip, not one on-device
// command).
type OpticalScanCaseResult struct {
	CaseID string `json:"case_id"`
	Pass   bool   `json:"pass"`
	Error  string `json:"error,omitempty"`
}

// OpticalScanResult is the most recent optical-scan suite's outcome -- same overwritten-not-
// versioned convention as Row's own AddTest history is not: nil means never run since this field
// existed, not "it failed". Persisted by TestManualOpticalScan itself (the only thing that ever
// runs this suite -- see that test's own doc comment on why it's manual/hardware-gated, never part
// of mage e2e:current/all) after every invocation, so "did the optical suite pass last time" has
// an answer on disk instead of only ever being visible in that one run's console output.
type OpticalScanResult struct {
	RanAt  time.Time               `json:"ran_at"`
	Status int                     `json:"status"`
	Error  string                  `json:"error,omitempty"`
	Cases  []OpticalScanCaseResult `json:"cases,omitempty"`
}

// File is the on-disk shape of test/e2e/testdata.json.
type File struct {
	// Versions maps a version id to this repo's semver at the time that
	// version was created (see mage e2e:newversion, which stamps it from
	// the same git-tag-derived semver `mage patch`/`minor`/`major` manage)
	// -- one shared version across every platform's implementation, not a
	// separate number per platform. CurrentVersion is always the highest
	// key.
	Versions map[int]string `json:"versions"`
	// PublishedVersion is the highest version id confirmed passing and
	// pushed. e2e:current runs every row with Version > PublishedVersion;
	// once they all pass, the pipeline advances PublishedVersion to
	// CurrentVersion.
	PublishedVersion int          `json:"published_version"`
	Nodes            map[int]Node `json:"nodes"`
	Rows             []Row        `json:"rows"`
	// OpticalScanCases is the real-camera two-device optical scan suite's test plan -- see
	// OpticalScanCase's own doc comment. Deliberately not versioned/rowed the way Rows is: it
	// always describes how to exercise whatever the *current* app can generate/scan, not a
	// historical regression log of past runs.
	OpticalScanCases []OpticalScanCase `json:"android_optical_cases,omitempty"`
	// OpticalScanResult is the most recent optical scan suite's outcome -- see
	// OpticalScanResult's own doc comment.
	OpticalScanResult *OpticalScanResult `json:"android_optical_scan_result,omitempty"`
}

// DefaultPath is where the testdata file lives relative to the repo root.
const DefaultPath = "test/e2e/testdata.json"

// Load reads and parses the file at path. A missing file is not an error --
// it returns a freshly initialized empty File, since the very first
// `mage e2e:newversion` run has nothing to load yet.
func Load(path string) (*File, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return &File{Versions: map[int]string{}, Nodes: map[int]Node{}}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("e2edata: read %s: %w", path, err)
	}
	var f File
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("e2edata: parse %s: %w", path, err)
	}
	if f.Versions == nil {
		f.Versions = map[int]string{}
	}
	if f.Nodes == nil {
		f.Nodes = map[int]Node{}
	}
	// Backfill Row.Platform for files predating that field: only possible
	// while the referenced node still exists, which is exactly the state
	// every pre-existing testdata.json is in the first time it's loaded
	// after this field was introduced. A row whose node was already gone
	// by then has no platform to recover -- see EnsureNode's doc comment
	// for what that means for it going forward.
	for i, r := range f.Rows {
		if r.Platform == "" {
			if n, ok := f.Nodes[r.Node]; ok {
				f.Rows[i].Platform = n.Platform
			}
		}
	}
	return &f, nil
}

// Save writes f to path, creating parent directories as needed, formatted
// for a readable diff in version control.
func (f *File) Save(path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("e2edata: create %s: %w", filepath.Dir(path), err)
	}
	data, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return fmt.Errorf("e2edata: encode: %w", err)
	}
	data = append(data, '\n')
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// CurrentVersion returns the highest version id in Versions -- the
// not-yet-published version any new AddTest row targets -- or 0 if none
// exists yet.
func (f *File) CurrentVersion() int {
	max := 0
	for v := range f.Versions {
		if v > max {
			max = v
		}
	}
	return max
}

// NewVersion records a new version stamped with semver (see File.Versions'
// doc comment on where that string comes from) and returns its id
// (CurrentVersion()+1). Called by `mage e2e:newversion`, or lazily by
// AddTest if no version exists yet.
func (f *File) NewVersion(semver string) int {
	id := f.CurrentVersion() + 1
	if f.Versions == nil {
		f.Versions = map[int]string{}
	}
	f.Versions[id] = semver
	return id
}

// nextNodeID returns the smallest node id greater than every id currently
// in Nodes *and* every id any Row still references -- the latter matters
// once a node has been deleted (mage e2e:deletenode/destroyall) while its
// rows remain: without also considering Rows here, a freshly (re-)added
// node (e.g. EnsureBootstrap's remote node, provisioned before
// pkg/e2erun.Run gets a chance to call EnsureNode for orphaned rows) could
// claim an id an orphaned row is about to need back, silently stealing it
// instead of leaving it free to be recovered under its original platform.
func (f *File) nextNodeID() int {
	max := 0
	for id := range f.Nodes {
		if id > max {
			max = id
		}
	}
	for _, r := range f.Rows {
		if r.Node > max {
			max = r.Node
		}
	}
	return max + 1
}

// AddNode generates a fresh deterministic identity for platform, records it
// under a new node id, and returns that id.
func (f *File) AddNode(platform Platform) (int, Node, error) {
	pub, priv, err := GenerateIdentity()
	if err != nil {
		return 0, Node{}, err
	}
	peerID, err := PeerIDFromPrivateKey(priv)
	if err != nil {
		return 0, Node{}, err
	}
	if f.Nodes == nil {
		f.Nodes = map[int]Node{}
	}
	id := f.nextNodeID()
	n := Node{
		Platform:   platform,
		PeerID:     peerID,
		PublicKey:  encodeHex(pub),
		PrivateKey: encodeHex(priv),
	}
	f.Nodes[id] = n
	return id, n, nil
}

// RemoteNode returns the id and Node of the (at most one expected)
// PlatformRemote entry in Nodes -- the SSH-deployed bootstrap/leader every
// other test node joins (see pkg/e2erun.EnsureBootstrap). ok is false if
// none has been provisioned yet.
func (f *File) RemoteNode() (id int, node Node, ok bool) {
	for id, n := range f.Nodes {
		if n.Platform == PlatformRemote {
			return id, n, true
		}
	}
	return 0, Node{}, false
}

// DeleteNode removes nodeID from Nodes and returns it, along with how many
// rows still reference it (left in place -- see this package's doc comment
// on the file being a durable, human-reviewed log; a caller wanting those
// gone too should say so explicitly rather than have deletion silently
// rewrite history). pkg/e2erun.DeleteNode wraps this with the real
// process/data teardown for the node's platform before calling it.
func (f *File) DeleteNode(nodeID int) (Node, int, error) {
	node, ok := f.Nodes[nodeID]
	if !ok {
		return Node{}, 0, fmt.Errorf("e2edata: unknown node id %d", nodeID)
	}
	delete(f.Nodes, nodeID)
	affected := 0
	for _, r := range f.Rows {
		if r.Node == nodeID {
			affected++
		}
	}
	return node, affected, nil
}

// AddTest appends a row against CurrentVersion (creating version 1 first if
// none exists yet), targeting nodeID with event ev.
func (f *File) AddTest(nodeID int, ev Event) (Row, error) {
	node, ok := f.Nodes[nodeID]
	if !ok {
		return Row{}, fmt.Errorf("e2edata: unknown node id %d", nodeID)
	}
	v := f.CurrentVersion()
	if v == 0 {
		v = f.NewVersion("0.0.0")
	}
	row := Row{Version: v, Node: nodeID, Event: ev, Platform: node.Platform}
	f.Rows = append(f.Rows, row)
	return row, nil
}

// EnsureNode returns the existing node at id if present, or provisions a
// fresh identity for platform and records it under exactly id -- not the
// next available id the way AddNode picks -- if missing. This is how
// pkg/e2erun.Run recovers a row whose node was deleted (mage
// e2e:deletenode/destroyall) while the row itself remained: the row
// carries its own Platform (see Row's doc comment) precisely so this
// recovery doesn't need to remember what id N "used to be" from anywhere
// else. The freshly generated identity is unrelated to whatever the
// original one was -- there's no way to recover that once deleted, and
// none of this package's callers need it to be the same, only valid.
func (f *File) EnsureNode(id int, platform Platform) (Node, error) {
	if n, ok := f.Nodes[id]; ok {
		return n, nil
	}
	pub, priv, err := GenerateIdentity()
	if err != nil {
		return Node{}, err
	}
	peerID, err := PeerIDFromPrivateKey(priv)
	if err != nil {
		return Node{}, err
	}
	if f.Nodes == nil {
		f.Nodes = map[int]Node{}
	}
	n := Node{
		Platform:   platform,
		PeerID:     peerID,
		PublicKey:  encodeHex(pub),
		PrivateKey: encodeHex(priv),
	}
	f.Nodes[id] = n
	return n, nil
}

// PendingRows returns every row whose Version is newer than
// PublishedVersion -- what `mage e2e:current` runs.
func (f *File) PendingRows() []int {
	var idx []int
	for i, r := range f.Rows {
		if r.Version > f.PublishedVersion {
			idx = append(idx, i)
		}
	}
	return idx
}

// AllRowIndices returns every row index -- what `mage e2e:all` runs.
func (f *File) AllRowIndices() []int {
	idx := make([]int, len(f.Rows))
	for i := range f.Rows {
		idx[i] = i
	}
	return idx
}

// MarkPublished advances PublishedVersion to CurrentVersion. The e2e
// runner calls this once every pending row has Status == StatusPass.
func (f *File) MarkPublished() {
	f.PublishedVersion = f.CurrentVersion()
}
