// Compiles ../api/shmevent.capnp into Rust bindings at build time (needs
// the `capnp` schema compiler on PATH -- see api/shmevent.capnp's doc
// comment for why no Rust/Go library can do this on its own). Mirrors
// pkg/shmevent's `capnp compile -ogo` invocation for the Go side.
fn main() {
    capnpc::CompilerCommand::new()
        .src_prefix("../api")
        // The generated file is included as `pub mod shmevent_capnp` inside
        // src/shmevent/mod.rs, i.e. at crate::shmevent::shmevent_capnp, not
        // at the crate root capnpc assumes by default. capnpc composes every
        // absolute reference it emits as `crate` + this + `<file>_capnp`, so
        // without the "shmevent" hop those references point at a module that
        // doesn't exist. It only started mattering once the schema became a
        // real union (see api/shmevent.capnp): union *groups* are the first
        // types whose accessors are emitted as fully qualified paths
        // (`init_set_key() -> crate::…::event::set_key::Builder`), so before
        // that every reference happened to be relative and the mismatch was
        // invisible.
        .default_parent_module(vec!["shmevent".into()])
        .file("../api/shmevent.capnp")
        .run()
        .expect("compile api/shmevent.capnp -- is `capnp` installed and on PATH?");
}
