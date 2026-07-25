package shmevent

import "fmt"

// EncodeChannelSendPayload packs channelID and chunk into
// EventChannelSend's Value -- identical in shape to EncodeSetPayload (a
// 2-byte length-prefixed first field, then the second field verbatim), so
// it's just an alias rather than duplicated logic.
func EncodeChannelSendPayload(channelID string, chunk []byte) ([]byte, error) {
	return EncodeSetPayload([]byte(channelID), chunk)
}

// DecodeChannelSendPayload is the inverse of EncodeChannelSendPayload.
func DecodeChannelSendPayload(payload []byte) (channelID, chunk []byte, err error) {
	return DecodeSetPayload(payload)
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

// EncodeChannelPollResponse packs status and chunk into EventChannelPoll's
// response Value: a single status byte, then chunk verbatim (chunk is
// only meaningful when status is ChannelPollChunk, and is otherwise
// empty).
func EncodeChannelPollResponse(status byte, chunk []byte) []byte {
	buf := make([]byte, 1+len(chunk))
	buf[0] = status
	copy(buf[1:], chunk)
	return buf
}

// DecodeChannelPollResponse is the inverse of EncodeChannelPollResponse.
func DecodeChannelPollResponse(payload []byte) (status byte, chunk []byte, err error) {
	if len(payload) < 1 {
		return 0, nil, fmt.Errorf("shmevent: channel poll response empty")
	}
	return payload[0], payload[1:], nil
}
