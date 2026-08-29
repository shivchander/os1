#![deny(unsafe_code)]
//! `buzz-node` binary entry point. Real argument parsing, key loading, and the
//! live substrate/relay wiring land in Phase 3; this stub keeps the bin target
//! compiling in Phase 2.

fn main() {
    eprintln!("buzz-node: not yet runnable (Phase 3 wires the live substrate + relay)");
    std::process::exit(1);
}
