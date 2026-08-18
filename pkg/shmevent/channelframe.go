package shmevent

import (
	"crypto/ed25519"
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

// SignChannelChunk/VerifyChannelChunk/EncodeChannelFrame/DecodeChannelFrame
// are pkg/daemon.ChannelProtocolID's own signed-frame scheme for its
// high-throughput data plane (pkg/chandata), deliberately separate from
// Sign/Verify/Encode/Decode's Msg-based one every other event type
// (including the legacy, small-chunk EventChannelSend/EventChannelPoll
// IPC path) shares.
//
// It signs purpose+chunk at their *real*, variable length. That is what
// the Msg path could not do when this was written: back then Msg's signed
// form had no length field of its own for Value, so canonicalPayload
// zero-padded Value up to a fixed width per event type (the retired
// ValueSize/ChannelValueSize tiers -- see README's "Value size ceilings")
// to give every signer and verifier one width to agree on in advance.
// Padding was cheap at ValueSize's 512 bytes and tolerable at
// ChannelValueSize's 16KB, but it would have been actively harmful here:
// a frame carrying a 200-byte control ping would still pay CRC32+Ed25519
// cost over however many hundred KB the *ceiling* was, every frame,
// regardless of the frame's real size -- raising a tier to fit chandata's
// much larger MaxChunkSize would have made padding cost dominate real
// transfer cost, defeating the point of the rewrite. The union rewrite
// has since retired the tiers and the padding both (every variant now
// carries self-describing, variable-length fields), so this is no longer
// the only variable-length signing path in the package -- but it remains
// a separate scheme, and safe as one, because a chunk's length is never
// ambiguous the way a padded Msg.Value's was: pkg/daemon.writeFramed/readFramed
// already puts a 4-byte length prefix around the whole frame this
// produces, so nothing downstream needs to guess where chunk ends.
func channelFrameSignedPayload(purpose byte, chunk []byte, crc uint32) []byte {
	buf := make([]byte, 1+len(chunk)+4)
	buf[0] = purpose
	copy(buf[1:], chunk)
	binary.BigEndian.PutUint32(buf[1+len(chunk):], crc)
	return buf
}

func channelFrameCRC(purpose byte, chunk []byte) uint32 {
	buf := make([]byte, 1+len(chunk))
	buf[0] = purpose
	copy(buf[1:], chunk)
	return crc32.ChecksumIEEE(buf)
}

// SignChannelChunk signs one purpose-tagged chunk, returning its CRC32
// and 64-byte Ed25519 signature -- EncodeChannelFrame's own inputs.
func SignChannelChunk(priv PrivateKey, purpose byte, chunk []byte) (crc uint32, sig []byte, err error) {
	if len(priv) != ed25519.PrivateKeySize {
		return 0, nil, fmt.Errorf("shmevent: private key must be %d bytes, got %d", ed25519.PrivateKeySize, len(priv))
	}
	crc = channelFrameCRC(purpose, chunk)
	sig = ed25519.Sign(priv, channelFrameSignedPayload(purpose, chunk, crc))
	return crc, sig, nil
}

// VerifyChannelChunk is SignChannelChunk's inverse check.
func VerifyChannelChunk(pub PublicKey, purpose byte, chunk []byte, crc uint32, sig []byte) error {
	if len(pub) != ed25519.PublicKeySize {
		return fmt.Errorf("shmevent: public key must be %d bytes, got %d", ed25519.PublicKeySize, len(pub))
	}
	if len(sig) != SignatureSize {
		return fmt.Errorf("shmevent: signature must be %d bytes, got %d", SignatureSize, len(sig))
	}
	if channelFrameCRC(purpose, chunk) != crc {
		return fmt.Errorf("shmevent: channel frame crc mismatch")
	}
	if !ed25519.Verify(pub, channelFrameSignedPayload(purpose, chunk, crc), sig) {
		return fmt.Errorf("shmevent: channel frame signature verification failed")
	}
	return nil
}

// channelFrameHeaderSize is purpose(1) + crc32(4) + signature(64).
const channelFrameHeaderSize = 1 + 4 + SignatureSize

// EncodeChannelFrame packs purpose, crc and sig (SignChannelChunk's
// output) and chunk into the bytes pkg/daemon.writeFramed puts on the
// wire for ChannelProtocolID's post-handshake data path: purpose(1) ||
// crc32_BE(4) || signature(64) || chunk.
func EncodeChannelFrame(purpose byte, crc uint32, sig, chunk []byte) []byte {
	buf := make([]byte, channelFrameHeaderSize+len(chunk))
	buf[0] = purpose
	binary.BigEndian.PutUint32(buf[1:5], crc)
	copy(buf[5:channelFrameHeaderSize], sig)
	copy(buf[channelFrameHeaderSize:], chunk)
	return buf
}

// DecodeChannelFrame is EncodeChannelFrame's inverse -- chunk aliases
// buf, matching Decode's own no-copy convention for Msg.Value.
func DecodeChannelFrame(buf []byte) (purpose byte, crc uint32, sig, chunk []byte, err error) {
	if len(buf) < channelFrameHeaderSize {
		return 0, 0, nil, nil, fmt.Errorf("shmevent: channel frame too short: %d bytes (need at least %d)", len(buf), channelFrameHeaderSize)
	}
	purpose = buf[0]
	crc = binary.BigEndian.Uint32(buf[1:5])
	sig = buf[5:channelFrameHeaderSize]
	chunk = buf[channelFrameHeaderSize:]
	return purpose, crc, sig, chunk, nil
}
