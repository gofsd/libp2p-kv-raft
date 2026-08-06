package daemon

import (
	"fmt"

	"github.com/gofsd/libp2p-kv-raft/pkg/kvfsm"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// This file backs EventLifecycleWrite: one generic envelope covering
// EventPermitRequest/Confirm/Revoke, EventJoinInviteCreate/Revoke, and
// EventExecInviteCreate/Revoke (see EventLifecycleWrite's own doc comment
// in pkg/shmevent/event.go), which stay exactly as they are -- this is a
// dispatch-table addition alongside them, not a reimplementation, the
// identical shape catalog_envelope.go already uses for
// EventCatalogPut/Delete.
//
// Two tables, not one, because the family splits into two genuinely
// different shapes: permitLifecycleSpecs is generic over kind (any byte
// other than KindJoinInvite/KindExecInvite selects it, keyed only by
// action -- mirroring EventPermitRequest's own kind-agnostic design, see
// its doc comment), while inviteLifecycleSpecs is keyed by the exact
// (kind, action) pair since KindJoinInvite/KindExecInvite each need
// genuinely different key/value construction.

// lifecycleWriteSpec builds one or two store keys (and, for a Set, a
// value) out of an EventLifecycleWrite (kind, action) pair's payload
// tail, and names how to apply it.
type lifecycleWriteSpec struct {
	// voterGated mirrors the inline "only a raft voter may act" guard
	// every one of the seven collapsed events except EventPermitRequest
	// already had -- see EventPermitRequest's own doc comment on why
	// that one alone has none.
	voterGated bool
	// viaSetForward selects handleSetForward (EventPermitRequest's own
	// "any raft node may relay, no voter check anywhere" forwarding)
	// instead of handleConfirmForward. Only ever true for Permit-style
	// Request: handleConfirmForward's forwarded path unconditionally
	// voter-checks the forwarding identity regardless of op, which would
	// silently break EventPermitRequest's "not-yet-permitted peer has no
	// elevated standing to earn here" design if reused for it.
	viaSetForward bool
	// op is the kvfsm.OpType handleConfirmForward applies key1/key2
	// with -- unused (handleSetForward always implies kvfsm.OpSet) when
	// viaSetForward is true.
	op kvfsm.OpType
	// build decodes inner (already stripped of the envelope's own kind
	// and action bytes) into key1 (the key, for OpSet/OpDel/
	// OpCascadeDelete; the pending record's key, for OpConfirm) and key2
	// (the value, for OpSet via handleSetForward; the confirmed record's
	// key, for OpConfirm; unused otherwise).
	build func(inner []byte) (key1, key2 []byte, err error)
}

// permitLifecycleSpecs mirrors EventPermitRequest/Confirm/Revoke's case
// bodies exactly (pkg/daemon/daemon.go's handleShmEvent, kept unchanged
// alongside this table) -- generic over kind, so keyed by action alone.
var permitLifecycleSpecs = map[byte]lifecycleWriteSpec{
	shmevent.LifecycleActionRequest: {
		viaSetForward: true,
		build: func(inner []byte) (key1, key2 []byte, err error) {
			kind, peerID, metadata, err := shmevent.DecodePermitRequestPayload(inner)
			if err != nil {
				return nil, nil, err
			}
			return shmevent.SystemKey(kind, shmevent.StatusPending, peerID), metadata, nil
		},
	},
	shmevent.LifecycleActionConfirm: {
		voterGated: true,
		op:         kvfsm.OpConfirm,
		build: func(inner []byte) (key1, key2 []byte, err error) {
			kind, peerID, err := shmevent.DecodePermitConfirmPayload(inner)
			if err != nil {
				return nil, nil, err
			}
			pendingKey := shmevent.SystemKey(kind, shmevent.StatusPending, peerID)
			confirmedKey := shmevent.SystemKey(kind, shmevent.StatusConfirmed, peerID)
			return pendingKey, confirmedKey, nil
		},
	},
	shmevent.LifecycleActionRevoke: {
		voterGated: true,
		op:         kvfsm.OpDel,
		build: func(inner []byte) (key1, key2 []byte, err error) {
			kind, peerID, err := shmevent.DecodePermitConfirmPayload(inner)
			if err != nil {
				return nil, nil, err
			}
			return shmevent.SystemKey(kind, shmevent.StatusConfirmed, peerID), nil, nil
		},
	},
}

// lifecycleSpecKey selects one inviteLifecycleSpecs entry.
type lifecycleSpecKey struct {
	kind   byte
	action byte
}

// inviteLifecycleSpecs mirrors EventJoinInviteCreate/Revoke and
// EventExecInviteCreate/Revoke's case bodies exactly.
var inviteLifecycleSpecs = map[lifecycleSpecKey]lifecycleWriteSpec{
	{shmevent.KindJoinInvite, shmevent.LifecycleActionRequest}: {
		voterGated: true,
		op:         kvfsm.OpSet,
		build: func(inner []byte) (key1, key2 []byte, err error) {
			token, suffrage, err := shmevent.DecodeJoinInviteCreatePayload(inner)
			if err != nil {
				return nil, nil, err
			}
			return shmevent.JoinInviteKey(token), shmevent.EncodeJoinInviteRecord(suffrage), nil
		},
	},
	{shmevent.KindJoinInvite, shmevent.LifecycleActionRevoke}: {
		voterGated: true,
		op:         kvfsm.OpDel,
		build: func(inner []byte) (key1, key2 []byte, err error) {
			token, err := shmevent.DecodeJoinInviteRevokePayload(inner)
			if err != nil {
				return nil, nil, err
			}
			return shmevent.JoinInviteKey(token), nil, nil
		},
	},
	{shmevent.KindExecInvite, shmevent.LifecycleActionRequest}: {
		voterGated: true,
		op:         kvfsm.OpSet,
		build: func(inner []byte) (key1, key2 []byte, err error) {
			token, commandID, inputsJSON, err := shmevent.DecodeExecInviteCreatePayload(inner)
			if err != nil {
				return nil, nil, err
			}
			return shmevent.ExecInviteKey(token), shmevent.EncodeExecInviteRecord(commandID, inputsJSON), nil
		},
	},
	{shmevent.KindExecInvite, shmevent.LifecycleActionRevoke}: {
		voterGated: true,
		op:         kvfsm.OpDel,
		build: func(inner []byte) (key1, key2 []byte, err error) {
			token, err := shmevent.DecodeExecInviteRevokePayload(inner)
			if err != nil {
				return nil, nil, err
			}
			return shmevent.ExecInviteKey(token), nil, nil
		},
	},
}

// lookupLifecycleWriteSpec resolves an EventLifecycleWrite (kind, action)
// pair to its spec: an exact (kind, action) match in inviteLifecycleSpecs
// for KindJoinInvite/KindExecInvite, or an action-only match in
// permitLifecycleSpecs for any other kind (Permit-style, generic over
// kind -- see permitLifecycleSpecs' own doc comment).
func lookupLifecycleWriteSpec(kind, action byte) (lifecycleWriteSpec, bool) {
	if kind == shmevent.KindJoinInvite || kind == shmevent.KindExecInvite {
		spec, ok := inviteLifecycleSpecs[lifecycleSpecKey{kind, action}]
		return spec, ok
	}
	spec, ok := permitLifecycleSpecs[action]
	return spec, ok
}

// unrecognizedLifecycleWriteError formats lookupLifecycleWriteSpec's
// failure case consistently.
func unrecognizedLifecycleWriteError(kind, action byte) error {
	return fmt.Errorf("lifecycle_write: action %d not valid for entity kind %d", action, kind)
}

// isExemptPermitLifecycleWrite reports whether value (an EventLifecycleWrite
// Msg.Value) decodes to Permit-style Request or Confirm -- the two actions
// EventPermitRequest/EventPermitConfirm's own doc comments explain must
// stay reachable by a remote caller with no standing at all (a
// not-yet-permitted peer has no elevated standing to earn otherwise;
// EventPermitConfirm's own inline voter check is what actually gates it).
// EventLifecycleWrite folds seven Event bytes into one, but the top-level
// remote-caller gate in handleShmEvent that used to switch on
// EventPermitRequest/EventPermitConfirm directly runs before this event's
// own case body decodes anything -- so it needs to peek at just enough of
// the payload to tell those two actions apart from Permit-style Revoke and
// every JoinInvite/ExecInvite action, none of which were ever exempt. A
// malformed payload is treated as not exempt (falls through to the normal
// authorization check, which will reject it -- the case body's own decode
// step is what produces a real decode error for an already-authorized
// caller).
func isExemptPermitLifecycleWrite(value []byte) bool {
	kind, action, _, err := shmevent.DecodeLifecycleWritePayload(value)
	if err != nil {
		return false
	}
	if kind == shmevent.KindJoinInvite || kind == shmevent.KindExecInvite {
		return false
	}
	return action == shmevent.LifecycleActionRequest || action == shmevent.LifecycleActionConfirm
}
