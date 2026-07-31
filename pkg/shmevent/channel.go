package shmevent

import (
	"fmt"
	"strconv"
)

// EncodeChannelSendPayload packs channelID, purpose and chunk into
// EventChannelSend's Value: EncodeSetPayload's usual 2-byte length-prefixed
// first field (channelID), then a second field that is itself purpose
// (one byte) followed by chunk verbatim -- purpose has no length prefix of
// its own since it's always exactly one byte, and chunk needs none since
// Value's own end already marks its end (see EncodeSetPayload's doc
// comment). purpose is carried all the way through to the corresponding
// network frame (see pkg/daemon.ChannelProtocolID's doc comment) and back
// out through EventChannelPoll's response (EncodeChannelPollResponse) --
// see ChannelPurpose* below for the byte's meaning.
func EncodeChannelSendPayload(channelID string, purpose byte, chunk []byte) ([]byte, error) {
	packed := make([]byte, 1+len(chunk))
	packed[0] = purpose
	copy(packed[1:], chunk)
	return EncodeSetPayload([]byte(channelID), packed)
}

// DecodeChannelSendPayload is the inverse of EncodeChannelSendPayload.
func DecodeChannelSendPayload(payload []byte) (channelID []byte, purpose byte, chunk []byte, err error) {
	channelID, rest, err := DecodeSetPayload(payload)
	if err != nil {
		return nil, 0, nil, err
	}
	if len(rest) < 1 {
		return nil, 0, nil, fmt.Errorf("shmevent: channel send payload missing purpose byte")
	}
	return channelID, rest[0], rest[1:], nil
}

// EncodeChannelWireChunk packs purpose and chunk into the Value of the
// signed shmevent.Event frame a channel's underlying network stream
// actually carries once its handshake completes -- see
// pkg/daemon.ChannelProtocolID's doc comment for the full wire design.
// Unlike EncodeChannelSendPayload, there is no channelID field here: the
// stream itself already is the channel, so there is nothing to look up.
func EncodeChannelWireChunk(purpose byte, chunk []byte) []byte {
	buf := make([]byte, 1+len(chunk))
	buf[0] = purpose
	copy(buf[1:], chunk)
	return buf
}

// DecodeChannelWireChunk is the inverse of EncodeChannelWireChunk.
func DecodeChannelWireChunk(payload []byte) (purpose byte, chunk []byte, err error) {
	if len(payload) < 1 {
		return 0, nil, fmt.Errorf("shmevent: channel wire chunk missing purpose byte")
	}
	return payload[0], payload[1:], nil
}

// EncodeChannelAccept packs channelID and remotePeerID into
// EventChannelListen's response Value -- identical in shape to
// EncodeExecuteNotification, so it's an alias rather than duplicated
// logic.
func EncodeChannelAccept(channelID, remotePeerID string) ([]byte, error) {
	return EncodeExecuteNotification([]byte(channelID), []byte(remotePeerID))
}

// DecodeChannelAccept is the inverse of EncodeChannelAccept.
func DecodeChannelAccept(payload []byte) (channelID, remotePeerID []byte, err error) {
	return DecodeExecuteNotification(payload)
}

// ChannelPoll* are EventChannelPoll's response status byte -- see that
// event's doc comment.
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
// EventChannelOpen session is a single raw stream, but a caller may want
// to interleave more than one kind of traffic over it (e.g. a control
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

// EncodeChannelPollResponse packs status, purpose and chunk into
// EventChannelPoll's response Value: a status byte, then a purpose byte,
// then chunk verbatim (purpose/chunk are only meaningful when status is
// ChannelPollChunk, and are otherwise ChannelPurposeData/empty).
func EncodeChannelPollResponse(status, purpose byte, chunk []byte) []byte {
	buf := make([]byte, 2+len(chunk))
	buf[0] = status
	buf[1] = purpose
	copy(buf[2:], chunk)
	return buf
}

// DecodeChannelPollResponse is the inverse of EncodeChannelPollResponse.
func DecodeChannelPollResponse(payload []byte) (status, purpose byte, chunk []byte, err error) {
	if len(payload) < 2 {
		return 0, 0, nil, fmt.Errorf("shmevent: channel poll response too short: %d bytes", len(payload))
	}
	return payload[0], payload[1], payload[2:], nil
}
