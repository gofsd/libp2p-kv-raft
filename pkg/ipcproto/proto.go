// Package ipcproto defines the fixed-size wire messages exchanged between
// the mage CLI (client) and a running kvnode daemon (server) over the
// shmring-backed local IPC channel implemented in pkg/ipc.
//
// Messages are fixed size so a single shmring Write/Read maps to exactly one
// message, with no separate length framing needed.
package ipcproto

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// Action identifies what the daemon should do with a Request.
type Action uint8

const (
	// ActionAdd bootstraps a freshly spawned daemon process: Key carries the
	// leader's peer id to join ("" if this node is itself to become the
	// cluster's initial leader), Value carries this node's own peer id, used
	// by the daemon only to double check it was started with the identity
	// the caller expected.
	ActionAdd Action = 1
	// ActionSet stores Key=Value through raft.
	ActionSet Action = 2
	// ActionGet reads the current value of Key.
	ActionGet Action = 3
)

func (a Action) String() string {
	switch a {
	case ActionAdd:
		return "add"
	case ActionSet:
		return "set"
	case ActionGet:
		return "get"
	default:
		return fmt.Sprintf("unknown(%d)", uint8(a))
	}
}

const (
	// KeySize is the fixed width of the Request.Key field, in bytes.
	KeySize = 256
	// ValueSize is the fixed width of the Request.Value and Response.Value
	// fields, in bytes.
	ValueSize = 256
	// idSize is the fixed width of the Request.ID/Response.ID fields, in bytes.
	idSize = 8

	// RequestSize is the total encoded size of a Request.
	RequestSize = idSize + 1 + KeySize + ValueSize
	// ResponseSize is the total encoded size of a Response.
	ResponseSize = idSize + 1 + ValueSize
)

// Request is the fixed-size message the CLI sends to the daemon.
type Request struct {
	// ID is a per-call nonce that pkg/ipc uses to name a response channel
	// unique to this round trip; see that package's doc comment for why a
	// shared, reused name is unsafe. Callers building a Request via
	// NewRequest leave this zero -- pkg/ipc.Call fills it in.
	ID     uint64
	Action Action
	Key    [KeySize]byte
	Value  [ValueSize]byte
}

// NewRequest builds a Request from Go strings, truncating key/value to their
// field widths if necessary.
func NewRequest(action Action, key, value string) Request {
	var req Request
	req.Action = action
	putString(req.Key[:], key)
	putString(req.Value[:], value)
	return req
}

// Encode serializes req to its wire form.
func (req Request) Encode() [RequestSize]byte {
	var buf [RequestSize]byte
	binary.BigEndian.PutUint64(buf[0:idSize], req.ID)
	buf[idSize] = byte(req.Action)
	copy(buf[idSize+1:idSize+1+KeySize], req.Key[:])
	copy(buf[idSize+1+KeySize:], req.Value[:])
	return buf
}

// DecodeRequest parses a wire-form Request. buf must be at least RequestSize
// bytes.
func DecodeRequest(buf []byte) (Request, error) {
	if len(buf) < RequestSize {
		return Request{}, fmt.Errorf("ipcproto: short request: %d bytes", len(buf))
	}
	var req Request
	req.ID = binary.BigEndian.Uint64(buf[0:idSize])
	req.Action = Action(buf[idSize])
	copy(req.Key[:], buf[idSize+1:idSize+1+KeySize])
	copy(req.Value[:], buf[idSize+1+KeySize:idSize+1+KeySize+ValueSize])
	return req, nil
}

// KeyString returns Key with trailing zero padding trimmed.
func (req Request) KeyString() string { return getString(req.Key[:]) }

// ValueString returns Value with trailing zero padding trimmed.
func (req Request) ValueString() string { return getString(req.Value[:]) }

// Status is the outcome of a Request as reported in a Response.
type Status uint8

const (
	StatusOK    Status = 0
	StatusError Status = 1
)

// Response is the fixed-size message the daemon sends back to the CLI.
type Response struct {
	// ID echoes the Request.ID it answers; see Request.ID.
	ID     uint64
	Status Status
	// Value carries the requested value (ActionGet), the confirmed/assigned
	// peer id (ActionAdd), or a truncated error message (Status == StatusError).
	Value [ValueSize]byte
}

// NewResponse builds a Response from a Go string, truncating to ValueSize if
// necessary.
func NewResponse(status Status, value string) Response {
	var resp Response
	resp.Status = status
	putString(resp.Value[:], value)
	return resp
}

// Encode serializes resp to its wire form.
func (resp Response) Encode() [ResponseSize]byte {
	var buf [ResponseSize]byte
	binary.BigEndian.PutUint64(buf[0:idSize], resp.ID)
	buf[idSize] = byte(resp.Status)
	copy(buf[idSize+1:], resp.Value[:])
	return buf
}

// DecodeResponse parses a wire-form Response. buf must be at least
// ResponseSize bytes.
func DecodeResponse(buf []byte) (Response, error) {
	if len(buf) < ResponseSize {
		return Response{}, fmt.Errorf("ipcproto: short response: %d bytes", len(buf))
	}
	var resp Response
	resp.ID = binary.BigEndian.Uint64(buf[0:idSize])
	resp.Status = Status(buf[idSize])
	copy(resp.Value[:], buf[idSize+1:idSize+1+ValueSize])
	return resp, nil
}

// ValueString returns Value with trailing zero padding trimmed.
func (resp Response) ValueString() string { return getString(resp.Value[:]) }

func putString(dst []byte, s string) {
	n := copy(dst, s)
	for i := n; i < len(dst); i++ {
		dst[i] = 0
	}
}

func getString(src []byte) string {
	i := bytes.IndexByte(src, 0)
	if i < 0 {
		return string(src)
	}
	return string(src[:i])
}
