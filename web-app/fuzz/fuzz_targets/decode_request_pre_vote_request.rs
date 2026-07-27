#![no_main]

use kv_raft_web::msgpack::Reader;
use kv_raft_web::raft_wire::decode_request_pre_vote_request;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let mut r = Reader::new(data);
    let _ = decode_request_pre_vote_request(&mut r);
});
