package daemon

import (
	"fmt"

	"github.com/gofsd/libp2p-kv-raft/pkg/kvfsm"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// This file backs EventCatalogPut/EventCatalogDelete: one generic envelope
// pair, additive alongside EventGroupPut/Delete, EventCommandPut/Delete,
// EventGroupCommandPut/Delete, EventPeerGroupPut/Delete, and
// EventStationPut/Delete (see those events' own doc comments in
// pkg/shmevent/event.go), which stay exactly as they are -- see
// pkg/shmevent/event.go's package doc comment on why a new catalog kind
// belongs here rather than as an eleventh/twelfth Event constant.
//
// Every one of the ten kind-specific events above does the identical
// "decode payload, build a store key(+value), call handleConfirmForward
// with one kvfsm.OpType" reduction, differing only in which decode
// function, which key-building function, and which (often kind-specific)
// validation applies. catalogPutSpecs/catalogDeleteSpecs capture exactly
// that per-kind difference as a small closure, keyed by the same
// shmevent.Kind* byte the record's own SystemKey already carries -- reusing
// every existing Decode*/Encode*/Key function unchanged, so this is a
// dispatch-table addition, not a reimplementation. Adding an eleventh
// catalog kind in the future means one more map entry here, not a new
// Event constant, capnp change, or per-language client port.

// catalogPutSpec builds a store key+value out of one EventCatalogPut
// entityKind's payload tail (see shmevent.DecodeCatalogPayload), and names
// the kvfsm.OpType handleConfirmForward should apply it with.
type catalogPutSpec struct {
	build func(inner []byte) (key, value []byte, err error)
	op    kvfsm.OpType
}

// catalogDeleteSpec is catalogPutSpec's EventCatalogDelete counterpart --
// a delete only ever needs a key, never a value.
type catalogDeleteSpec struct {
	build func(inner []byte) (key []byte, err error)
	op    kvfsm.OpType
}

// catalogPutSpecs mirrors EventGroupPut/EventCommandPut/EventStationPut/
// EventGroupCommandPut/EventPeerGroupPut's case bodies exactly -- see
// pkg/daemon/daemon.go's handleShmEvent for those, kept unchanged
// alongside this table.
var catalogPutSpecs = map[byte]catalogPutSpec{
	shmevent.KindGroup: {op: kvfsm.OpSet, build: func(inner []byte) (key, value []byte, err error) {
		id, name, public, err := shmevent.DecodeGroupPutPayload(inner)
		if err != nil {
			return nil, nil, err
		}
		if shmevent.IsReservedGroupID(id) || isPeerIdentityGroupID(id) {
			return nil, nil, fmt.Errorf("group id %q is reserved and managed automatically", id)
		}
		return shmevent.GroupKey([]byte(id)), shmevent.EncodeGroupPayload(name, public), nil
	}},
	shmevent.KindCommand: {op: kvfsm.OpSet, build: func(inner []byte) (key, value []byte, err error) {
		id, name, peerID, spec, err := shmevent.DecodeCommandPutPayloadFull(inner)
		if err != nil {
			return nil, nil, err
		}
		// "Carried an empty spec" and "didn't mention the spec" must stay
		// distinguishable all the way to the FSM -- see the identical
		// comment on EventCommandPut's own case in daemon.go.
		if len(spec) == 0 && shmevent.CommandPutPayloadHasSpec(inner) {
			value, err = shmevent.EncodeCommandPayloadClearingSpec(name, peerID)
		} else {
			value, err = shmevent.EncodeCommandPayloadWithSpec(name, peerID, spec)
		}
		if err != nil {
			return nil, nil, err
		}
		return shmevent.CommandKey([]byte(id)), value, nil
	}},
	shmevent.KindStation: {op: kvfsm.OpSet, build: func(inner []byte) (key, value []byte, err error) {
		peerID, name, attrs, err := shmevent.DecodeStationPutPayload(inner)
		if err != nil {
			return nil, nil, err
		}
		if len(peerID) == 0 {
			return nil, nil, fmt.Errorf("station peer id must not be empty")
		}
		value, err = shmevent.EncodeStationPayload(name, attrs)
		if err != nil {
			return nil, nil, err
		}
		return shmevent.StationKey(peerID), value, nil
	}},
	shmevent.KindGroupCommand: {op: kvfsm.OpSet, build: func(inner []byte) (key, value []byte, err error) {
		commandID, groupID, err := shmevent.DecodeGroupCommandPayload(inner)
		if err != nil {
			return nil, nil, err
		}
		key, err = shmevent.GroupCommandKey(commandID, groupID)
		return key, nil, err
	}},
	shmevent.KindPeerGroup: {op: kvfsm.OpSet, build: func(inner []byte) (key, value []byte, err error) {
		peerID, groupID, err := shmevent.DecodePeerGroupPayload(inner)
		if err != nil {
			return nil, nil, err
		}
		if shmevent.IsAutoManagedGroupID(string(groupID)) {
			return nil, nil, fmt.Errorf("group %q membership is managed automatically", groupID)
		}
		key, err = shmevent.PeerGroupKey(peerID, groupID)
		return key, nil, err
	}},
}

// catalogDeleteSpecs mirrors EventGroupDelete/EventCommandDelete/
// EventStationDelete/EventGroupCommandDelete/EventPeerGroupDelete's case
// bodies exactly.
var catalogDeleteSpecs = map[byte]catalogDeleteSpec{
	shmevent.KindGroup: {op: kvfsm.OpCascadeDelete, build: func(inner []byte) (key []byte, err error) {
		if shmevent.IsReservedGroupID(string(inner)) || isPeerIdentityGroupID(string(inner)) {
			return nil, fmt.Errorf("group id %q is reserved and cannot be deleted", inner)
		}
		return shmevent.GroupKey(inner), nil
	}},
	shmevent.KindCommand: {op: kvfsm.OpCascadeDelete, build: func(inner []byte) (key []byte, err error) {
		return shmevent.CommandKey(inner), nil
	}},
	shmevent.KindStation: {op: kvfsm.OpDel, build: func(inner []byte) (key []byte, err error) {
		// Plain OpDel, not OpCascadeDelete: nothing references a station
		// record -- see EventStationDelete's own case comment in daemon.go.
		return shmevent.StationKey(inner), nil
	}},
	shmevent.KindGroupCommand: {op: kvfsm.OpDel, build: func(inner []byte) (key []byte, err error) {
		commandID, groupID, err := shmevent.DecodeGroupCommandPayload(inner)
		if err != nil {
			return nil, err
		}
		return shmevent.GroupCommandKey(commandID, groupID)
	}},
	shmevent.KindPeerGroup: {op: kvfsm.OpDel, build: func(inner []byte) (key []byte, err error) {
		peerID, groupID, err := shmevent.DecodePeerGroupPayload(inner)
		if err != nil {
			return nil, err
		}
		if shmevent.IsAutoManagedGroupID(string(groupID)) {
			return nil, fmt.Errorf("group %q membership is managed automatically", groupID)
		}
		return shmevent.PeerGroupKey(peerID, groupID)
	}},
}
