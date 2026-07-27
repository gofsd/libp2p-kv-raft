#![no_main]

use kv_raft_web::msgpack::Reader;
use kv_raft_web::raft_wire::decode_append_entries_request;
use libfuzzer_sys::fuzz_target;

// AppendEntries is the highest-frequency, highest-complexity RPC a learner
// decodes from the real raft leader (see raft_wire.rs's own doc comment) --
// it recurses into decode_log for every entry in the batch, so this target
// also exercises that helper. The only property under test is "never
// panics on malformed input" (matches pkg/shmevent's FuzzDecode goal) --
// wrong-but-non-panicking output is expected and fine, since decode errors
// already propagate as a normal Result to the caller.
fuzz_target!(|data: &[u8]| {
    let mut r = Reader::new(data);
    let _ = decode_append_entries_request(&mut r);
});
