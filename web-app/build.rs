// Compiles ../api/shmevent.capnp into Rust bindings at build time (needs
// the `capnp` schema compiler on PATH -- see api/shmevent.capnp's doc
// comment for why no Rust/Go library can do this on its own). Mirrors
// pkg/shmevent's `capnp compile -ogo` invocation for the Go side.
fn main() {
    capnpc::CompilerCommand::new()
        .src_prefix("../api")
        .file("../api/shmevent.capnp")
        .run()
        .expect("compile api/shmevent.capnp -- is `capnp` installed and on PATH?");
}
