#!/bin/sh
# Stub agent process for crates/buzz-node/tests/e2e_nodes.rs.
#
# Stands in for a real `buzz-acp`/LLM-backed agent harness so the two-node
# e2e test can prove the execution-node engine's spawn/observe/stop/move
# wiring without needing a live LLM provider key. `AcpRuntime::spawn`
# (crates/buzz-node/src/runtime.rs) always execs whatever
# `harness_command`/`harness_args` it is configured with -- it never reads
# the assignment secret's own `launch.command` -- and
# `crates/buzz-node/tests/harness/mod.rs`'s `start_node` points
# `harness_command` at this script. So this script does not need to parse
# its environment, speak ACP, or talk to the relay at all: it only needs to
# be a real, benign, long-lived process that `LocalProcessSubstrate` can
# observe as `Running` (a live child process) and terminate on command
# (`Substrate::stop`'s SIGTERM-then-SIGKILL process-group kill).
#
# Exits promptly on SIGTERM/SIGINT so the substrate's graceful path is what
# normally ends this process, rather than its 500ms-later SIGKILL fallback.
trap 'exit 0' TERM INT

while true; do
    sleep 1
done
