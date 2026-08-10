// Package shmclient implements the caller-side orchestration for
// pkg/shmevent's relational protocol over pkg/ipc: the SetKey+SetField
// message pair a Set needs, the single inline-key GetField a one-shot Get
// needs, and the GetPrivateKey bootstrap every signed call needs first
// (see pkg/shmevent's doc comment on why a local caller signs with the
// same Ed25519 key the node's own identity uses). Used by pkg/kvctl (the
// desktop CLI) and mobile/kvmobile (the in-process Android UI) -- anything
// that drives a node over pkg/ipc rather than pkg/shmevent's wire struct
// directly (as web-app's Rust build does, over ClientProtocolID).
//
// This file holds the shared Session core (Open/call/respErr/newID) plus
// the two unsigned bootstrap calls (GetPublicKey/GetPrivateKey) Open
// itself depends on. Everything else is split by domain into its own
// file: kv.go (Set/Get/Txn/CompareAndSwap/LogAppend/ListRange), cluster.go
// (Add/Leave/Kick/GetOwnAddr/GetVersion), permit.go, catalog.go (Group/
// Command/Station CRUD), invite.go (join/exec invite + ticket lifecycle),
// dialcommand.go (PublicAccess/DialSubmitCommand/DialQueryCommandLog),
// execute.go (Execute/PollExecute), and channel.go (the channel
// data-plane -- channelPipe and everything built on it).
package shmclient

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/gofsd/libp2p-kv-raft/pkg/ipc"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// Session caches the signing key fetched from peerID for the lifetime of
// the caller's own process/session, so repeated Set/Get/Add calls only
// pay the GetPrivateKey round trip once -- important for a long-lived
// caller like mobile/kvmobile, less so for pkg/kvctl's short-lived CLI
// invocations, which still go through it via the package-level
// convenience functions below.
type Session struct {
	peerID string
	priv   shmevent.PrivateKey

	// channels holds every channel this session has OpenChannel/
	// ListenChannel'd its pkg/chandata data-plane ring pair for -- see
	// setupChannelData/channelPipe.
	channelsMu sync.Mutex
	channels   map[string]*channelPipe
}

// Open fetches peerID's signing key (see pkg/shmevent's doc comment on why
// this is safe/expected for a local, same-machine caller) and returns a
// Session ready for Set/Get/Add.
func Open(ctx context.Context, peerID string) (*Session, error) {
	priv, err := GetPrivateKey(ctx, peerID)
	if err != nil {
		return nil, fmt.Errorf("shmclient: fetch signing key: %w", err)
	}
	return &Session{peerID: peerID, priv: priv, channels: make(map[string]*channelPipe)}, nil
}

// newID returns a random non-zero id for a new message -- 0 is reserved
// meaning "not used" (see api/shmevent.capnp), so a real message's own id
// avoids it too, even though nothing currently cites these particular ids
// by sourceId.
func newID() uint16 {
	for {
		var b [2]byte
		if _, err := rand.Read(b[:]); err != nil {
			return 1
		}
		if id := binary.BigEndian.Uint16(b[:]); id != 0 {
			return id
		}
	}
}

// call builds m (via one of shmevent's NewXxx constructors), sets a fresh
// id, sends it through ipc.Call, and returns the response -- shared by
// every Session method below.
func (s *Session) call(ctx context.Context, m shmevent.Msg) (shmevent.Msg, error) {
	m.SetId(newID())
	return ipc.Call(ctx, s.peerID, m, s.priv)
}

// respErr returns an error if resp is an error response, formatted as
// "shmclient: <name>: <message>" -- shared by every Session method below.
func respErr(name string, resp shmevent.Msg) error {
	if resp.Which() != shmevent.Event_Which_error {
		return nil
	}
	msg, _ := resp.Error().Message_()
	return fmt.Errorf("shmclient: %s: %s", name, msg)
}

// GetPublicKey fetches peerID's Ed25519 public key -- unsigned, since it's
// one of the two bootstrap events a node accepts without a key to check a
// signature against yet (see pkg/shmevent.RequiresSignature).
func GetPublicKey(ctx context.Context, peerID string) (shmevent.PublicKey, error) {
	m, err := shmevent.NewGetPublicKey()
	if err != nil {
		return nil, fmt.Errorf("shmclient: get_public_key: %w", err)
	}
	m.SetId(newID())
	resp, err := ipc.Call(ctx, peerID, m, nil)
	if err != nil {
		return nil, fmt.Errorf("shmclient: get_public_key: %w", err)
	}
	if err := respErr("get_public_key", resp); err != nil {
		return nil, err
	}
	pub, err := resp.GetPublicKey().PubKey()
	if err != nil {
		return nil, fmt.Errorf("shmclient: get_public_key: %w", err)
	}
	return shmevent.PublicKey(pub), nil
}

// GetPrivateKey fetches peerID's Ed25519 private key -- unsigned, same
// bootstrap exception as GetPublicKey.
func GetPrivateKey(ctx context.Context, peerID string) (shmevent.PrivateKey, error) {
	m, err := shmevent.NewGetPrivateKey()
	if err != nil {
		return nil, fmt.Errorf("shmclient: get_private_key: %w", err)
	}
	m.SetId(newID())
	resp, err := ipc.Call(ctx, peerID, m, nil)
	if err != nil {
		return nil, fmt.Errorf("shmclient: get_private_key: %w", err)
	}
	if err := respErr("get_private_key", resp); err != nil {
		return nil, err
	}
	priv, err := resp.GetPrivateKey().PrivKey()
	if err != nil {
		return nil, fmt.Errorf("shmclient: get_private_key: %w", err)
	}
	return shmevent.PrivateKey(priv), nil
}
