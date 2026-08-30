# Deep improvement implementation plans

Generated from the 2026-08-30 deep audit and implemented on the
`codex/deep-improvements-20260830` delivery branch.

## Execution order and status

| Plan | Title | Status |
|---|---|---|
| 001 | Concurrent bounded JSON-RPC dispatch | DONE |
| 002 | Retain Bun child output ownership | DONE |
| 003 | Enforce supply-chain and performance gates | DONE |

## Review state

Every plan records its implementation-specific tests and measurements. The Rust
workspace, Rust 1.94 minimum-version check, real Bun conformance suite, package
pipeline, and release wire benchmark pass. No finding in this repository remains
deferred or blocked.
