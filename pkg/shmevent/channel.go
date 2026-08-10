package shmevent

import (
	"fmt"
	"strconv"
)

// NewChannelOpen builds a channelOpen Msg: opens a new data-plane Channel
// to peerId. Local-only. Response fills channelId with a freshly minted
// opaque local handle.
func NewChannelOpen(peerID string) (Msg, error) {
	m, err := newMsg()
	if err != nil {
		return Msg{}, err
	}
	m.SetChannelOpen()
	if err := m.ChannelOpen().SetPeerId(peerID); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_channel_open: %w", err)
	}
	return m, nil
}

// NewChannelSend builds a channelSend Msg: sends one framed chunk on an
// already-open channel, tagged with purpose (see ChannelPurpose* below).
func NewChannelSend(channelID string, purpose byte, chunk []byte) (Msg, error) {
	if err := checkValueSize(chunk); err != nil {
		return Msg{}, err
	}
	m, err := newMsg()
	if err != nil {
		return Msg{}, err
	}
	m.SetChannelSend()
	grp := m.ChannelSend()
	if err := grp.SetChannelId(channelID); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_channel_send: %w", err)
	}
	grp.SetPurpose(purpose)
	if err := grp.SetChunk(chunk); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_channel_send: %w", err)
	}
	return m, nil
}

// NewChannelPoll builds a channelPoll request Msg: drains one buffered
// chunk received on channelID since the last poll, oldest first. The
// response reuses this same variant with status/purpose/chunk filled in.
func NewChannelPoll(channelID string) (Msg, error) {
	m, err := newMsg()
	if err != nil {
		return Msg{}, err
	}
	m.SetChannelPoll()
	if err := m.ChannelPoll().SetChannelId(channelID); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_channel_poll: %w", err)
	}
	return m, nil
}

// NewChannelListen builds a channelListen request Msg: drains one pending
// incoming channel this node has accepted but that no local caller has
// claimed yet. The response reuses this same variant with
// channelId/remotePeerId filled in if one was pending.
func NewChannelListen() (Msg, error) {
	m, err := newMsg()
	if err != nil {
		return Msg{}, err
	}
	m.SetChannelListen()
	return m, nil
}

// NewChannelClose builds a channelClose Msg: ends channelID outright.
func NewChannelClose(channelID string) (Msg, error) {
	m, err := newMsg()
	if err != nil {
		return Msg{}, err
	}
	m.SetChannelClose()
	if err := m.ChannelClose().SetChannelId(channelID); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_channel_close: %w", err)
	}
	return m, nil
}

// NewChannelCloseWrite builds a channelCloseWrite Msg: half-closes
// channelID's outgoing direction only.
func NewChannelCloseWrite(channelID string) (Msg, error) {
	m, err := newMsg()
	if err != nil {
		return Msg{}, err
	}
	m.SetChannelCloseWrite()
	if err := m.ChannelCloseWrite().SetChannelId(channelID); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_channel_close_write: %w", err)
	}
	return m, nil
}

// NewChannelDataReady builds a channelDataReady Msg: notifies that new
// data is available to poll on channelID -- the handshake that hands an
// already-open channel off to pkg/chandata's high-throughput data plane.
func NewChannelDataReady(channelID string) (Msg, error) {
	m, err := newMsg()
	if err != nil {
		return Msg{}, err
	}
	m.SetChannelDataReady()
	if err := m.ChannelDataReady().SetChannelId(channelID); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_channel_data_ready: %w", err)
	}
	return m, nil
}

// ChannelPoll* are channelPoll's response status byte -- see that
// variant's doc comment in api/shmevent.capnp.
const (
	// ChannelPollNoData means nothing new has arrived since the last
	// poll, and the channel is still open.
	ChannelPollNoData byte = iota
	// ChannelPollChunk means a chunk of received bytes follows.
	ChannelPollChunk
	// ChannelPollClosed means the channel has ended and every
	// already-buffered chunk has already been drained by a prior poll --
	// returned idempotently on every subsequent poll rather than an
	// error, so a caller that stops polling right after seeing it once
	// doesn't miss anything.
	ChannelPollClosed
)

// ChannelPurpose* tag what a channel chunk actually is -- the same
// channelOpen session is a single raw stream, but a caller may want to
// interleave more than one kind of traffic over it (e.g. a control
// message alongside a bulk data transfer, or a video frame). This set is
// deliberately open-ended: any byte value not named here is carried
// through untouched by every layer (IPC payload, network frame, IPC
// response) -- these three are just the ones this package gives a name
// to.
const (
	ChannelPurposeData    byte = 0
	ChannelPurposeControl byte = 1
	ChannelPurposeVideo   byte = 2
)

// ChannelPurposeName returns a human-readable name for p ("data",
// "control", "video"), or its decimal string form for any other value --
// see ChannelPurposeFromName for the inverse.
func ChannelPurposeName(p byte) string {
	switch p {
	case ChannelPurposeData:
		return "data"
	case ChannelPurposeControl:
		return "control"
	case ChannelPurposeVideo:
		return "video"
	default:
		return strconv.FormatUint(uint64(p), 10)
	}
}

// ChannelPurposeFromName is the inverse of ChannelPurposeName: it accepts
// "data"/"control"/"video", or a plain decimal number (e.g. "7") for any
// other purpose byte a caller wants to use -- ok is false only if name is
// neither a known name nor a valid byte-range decimal number. An empty
// name resolves to ChannelPurposeData, the default.
func ChannelPurposeFromName(name string) (byte, bool) {
	switch name {
	case "", "data":
		return ChannelPurposeData, true
	case "control":
		return ChannelPurposeControl, true
	case "video":
		return ChannelPurposeVideo, true
	default:
		n, err := strconv.ParseUint(name, 10, 8)
		if err != nil {
			return 0, false
		}
		return byte(n), true
	}
}
