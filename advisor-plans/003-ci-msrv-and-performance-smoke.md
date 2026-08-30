# Enforce Bun Adapter supply-chain and performance gates

Status: IMPLEMENTED AND VALIDATED

Finding: CI uses mutable action tags and the moving stable Rust toolchain despite declaring Rust 1.94; the wire benchmark is built but never executed.

Scope:
- pin third-party actions and Rust 1.94 across CI/release workflows;
- execute a small deterministic wire benchmark smoke in CI;
- keep the full evidence run reproducible locally.

Implementation:
- CI and release workflows pin immutable action commits with exact version
  comments; CI installs Rust 1.94.0 with rustfmt/clippy and runs clippy with
  warnings denied;
- CI now executes and validates a five-request wire benchmark smoke instead of
  only compiling the fixture; the unused non-relocatable benchmark bundle step
  was removed;
- the full 50-request evidence file is reproducible from the documented fixture.

Validation:
- `RUSTUP_TOOLCHAIN=1.94.0 ... lenso-cargo check --locked --workspace
  --all-targets` passed;
- workspace fmt/check/clippy/test passed and the real Bun suite passed 23/23;
- official `git ls-remote` tag proof matched checkout v6.1.0, setup-bun v2.2.0,
  setup-node v6.4.0, and rust-cache v2.9.2's peeled commit; the dtolnay pin was
  verified as the action repository HEAD on 2026-08-30 while `toolchain: 1.94.0`
  remains explicit;
- `LENSO_BUN_BENCHMARK_REQUESTS=50 bun fixtures/bun/wire-benchmark.ts` passed
  every corpus, failure, boundary, crash/recovery, and overload case.
