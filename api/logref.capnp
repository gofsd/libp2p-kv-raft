# logref.capnp defines the payload of a *log-reference* Data Matrix code:
# the smallest thing this project puts in front of a camera. It names one
# pkg/logrecord.Record -- the object, order, or line a person is standing
# in front of -- and nothing else.
#
# # Why this is not an Event variant
#
# api/shmevent.capnp's Event is the one struct every user-to-node hop
# speaks, and its union is a permanent, hand-duplicated commitment across
# pkg/shmevent, pkg/daemon's dispatch, web-app's Rust mirror and every
# wrapper layer (see that file's own note before adding a variant). A log
# reference asks a node for nothing: it is read by the *scanning device's
# own UI*, which turns it into a local lookup over primitives that
# already exist (listRange, via kvmobile.LogQuery). Nothing about it
# belongs in the wire union, so it gets its own tiny schema instead --
# the same reasoning that keeps examples/relations out of the core.
#
# # Why three fields for one number
#
# A Data Matrix symbol is read off a screen or a label by a phone camera
# at whatever angle and focus the person holding it manages, and ZXing
# reports a successful decode or nothing at all -- it has no notion of
# "probably right". The symbol's own error correction is what makes that
# safe for a payload whose every byte is checked downstream (a signed
# Event's Ed25519 signature, a RunCode's base64+JSON framing, both of
# which fail loudly on a flipped bit). A bare uint64 has no such
# downstream: every one of its 2^64 values is a syntactically valid log
# id, so a misdecode would not fail, it would silently name a *different*
# record -- and the whole point of this code is that the person scanning
# it never types the number and so cannot notice it changed.
#
# fieldCrc32 closes that: CRC-32 (IEEE) over the record field's own
# canonical bytes (pkg/logref.UnitID -- the id's decimal ASCII form, the
# exact bytes pkg/logrecord.BuildKey packs into the record's key), so a
# decode that survives the symbol's own correction but disagrees with
# this is rejected as corruption rather than resolved as a different
# object. It is an integrity check on one field, not a security boundary
# -- the same role, and the same non-role, as Event's own crc32.
#
# tag closes a second gap, and pays for itself twice. capnp has no magic
# number and no self-description: a struct is whatever layout the reader
# asks for, so a foreign barcode's bytes parse as a LogRef with junk
# field values roughly as often as they fail, and the scanning device's
# dispatch (android-app's AppRoot) needs to tell "this code is not mine"
# from "this code names log 4417". A required constant discriminant is
# the same defence NavCode/RunCode get from their package-qualified
# string prefixes, applied to a binary payload.
#
# The second payment is optical. A tagless LogRef message is ~32 bytes,
# which is below the ~40-byte floor android-app's own DataMatrixCodecTest
# measured for reliably decoding a generated symbol (ZXing's
# HybridBinarizer works in fixed 8px blocks and does poorly on a symbol
# too sparse relative to what it is drawn on). Carrying the tag lands the
# message at ~72 bytes -- comfortably inside the range every other code
# this app generates already lives in, with correspondingly more error
# correction to spend on a label read at arm's length.
@0xd50e5af971a4abee;

using Go = import "go.capnp";
$Go.package("logref");
$Go.import("github.com/gofsd/libp2p-kv-raft/pkg/logref");

struct LogRef {
  # Constant discriminant, pkg/logref.Tag -- see this file's own doc
  # comment. A message whose tag is anything else is not a log reference
  # and is refused by Decode, whatever its other fields say.
  tag @0 :Text;

  # The uint64 representation of the log record field this code names --
  # the record's unitID, which pkg/logref.UnitID renders back into the
  # decimal ASCII form the record's key carries. A scanning device
  # resolves it to the record (and so to the group the record declares)
  # with one ordinary range scan; see pkg/logref's own doc comment.
  logId @1 :UInt64;

  # CRC-32 (IEEE) over UnitID(logId)'s bytes -- see this file's own doc
  # comment on why a bare uint64 needs one.
  fieldCrc32 @2 :UInt32;
}
