# shmevent.capnp defines the single wire structure used for every message
# exchanged between a raft node instance and a local "user" (the desktop
# CLI, the in-process Android UI, or a browser tab's main thread) over
# shmring shared memory -- and, since the same relationship holds for a
# remote browser learner talking to a node over libp2p, also over
# pkg/daemon.ClientProtocolID's network stream. One struct, one encoding,
# every hop.
#
# # Design: events as rows, source_id/destination_id as foreign keys
#
# Unlike the fixed Key+Value request this replaces (pkg/ipcproto.Request),
# every message here carries exactly one raw `value`. A logical operation
# that needs more than one piece of data -- a Set needs both a key and a
# value -- is expressed as a short sequence of linked messages instead:
#
#   1. SetKey{value: "hello", id: X}      -- registers "hello" under id X
#      in the node's key registry (an id<->key-string interning table;
#      see pkg/shmevent's doc comment for its lifetime/eviction policy).
#      The response echoes id X.
#   2. SetField{value: "world", sourceId: X, id: Y} -- looks up the key
#      registered under sourceId (X), then performs the real Set("hello",
#      "world") against the replicated store. Response echoes id Y.
#
# Get mirrors this, and additionally allows skipping the registry
# round-trip entirely when the caller already knows the raw key:
#
#   GetKey{sourceId: X}         -- returns the key string registered under X.
#   GetField{sourceId: X}       -- looks up X's key, returns its value.
#   GetField{value: "hello"}    -- one-shot: reads "hello" directly, no
#                                  prior SetKey/registry entry needed.
#
# `destinationId` is reserved the same way `sourceId` is -- a second
# relational reference -- for a future event that needs to relate two
# registered rows to each other (e.g. a compare-and-swap or a rename); no
# event defined today sets it.
#
# `id` is dual-purpose: it's the request/response correlation nonce (a
# response always echoes the request's `id`, exactly like
# pkg/ipcproto.Request.ID/Response.ID did), and, because the *client*
# chooses it, it doubles as a stable handle the client can cite later via
# `sourceId`/`destinationId`.
#
# # Integrity and authenticity
#
# `crc32` covers every other field except itself and `signature` (see
# pkg/shmevent.signedPayload / web-app/src/shmevent.rs's equivalent for the
# exact byte layout) -- a cheap corruption check, not a security boundary.
# `signature` is a real Ed25519 signature over the same payload, checked
# against the sender's public key. GetPublicKey/GetPrivateKey (see Event
# below) are how a caller with no key of its own yet bootstraps into having
# one: both node and every local caller share the *same* Ed25519 keypair --
# the node's own libp2p identity key (already used for its peer ID) --
# since shmring IPC is inherently same-machine, same-trust-boundary, no
# different from a local process already being able to read that key's
# file on disk. Those two event types are the only ones a node accepts
# without a valid signature (there is no key to check one against yet).
@0x907f33b2bf56870e;

using Go = import "go.capnp";
$Go.package("shmevent");
$Go.import("github.com/gofsd/libp2p-kv-raft/pkg/shmevent");

struct Event {
  # What operation this message performs (a request) or answers (a
  # response) -- see pkg/shmevent's Event* constants / web-app's
  # shmevent::EventType for the full list and their exact semantics.
  event @0 :UInt8;

  # References a previous message's `id` this message relates to -- e.g.
  # SetField/GetField/GetKey's key-registry lookup. 0 means "not used".
  sourceId @1 :UInt16;

  # Reserved for a future event relating two registered rows to each
  # other; no event defined today reads or sets it. 0 means "not used".
  destinationId @2 :UInt16;

  # The operation's single raw payload -- a key, a value, a public key, a
  # private key, or an error message, depending on `event`. Capped at 512
  # bytes by convention (enforced in application code, not by this
  # schema); a value your application needs to store larger than that
  # must be chunked at a higher layer.
  value @3 :Data;

  # CRC-32 (IEEE polynomial) over event/sourceId/destinationId/value/id,
  # in that field order, each integer big-endian -- see this file's doc
  # comment. Corruption check only.
  crc32 @4 :UInt32;

  # Ed25519 signature (64 bytes) over the same payload crc32 covers plus
  # the crc32 value itself -- see this file's doc comment.
  signature @5 :Data;

  # Request/response correlation nonce, chosen by whichever side
  # originates the message that starts an exchange (always the caller,
  # for every event defined today) -- see this file's doc comment for its
  # dual use as a key-registry handle.
  id @6 :UInt16;
}
