# Concurrent bounded JSON-RPC dispatch

Status: IMPLEMENTED AND VALIDATED

Finding: ordinary JSON-RPC requests execute serially behind one blocking worker, while stream capacity creates one OS thread/runtime/client per slot.

Scope:
- use bounded async dispatch on one request worker and one stream worker;
- preserve separate Event ordering, cancellation, admission, and process-failure semantics;
- add fast-behind-slow concurrency and fixed worker-budget regressions.

Implementation:
- bounded Tokio channels feed one request runtime thread and one stream runtime
  thread, each multiplexing a bounded `FuturesUnordered` set;
- Event dispatch stays separately ordered at one in-flight request;
- request/stream cancellation uses an independent two-slot HTTP control client,
  so saturated request plus Event traffic cannot occupy its permits;
- request and stream cancellation flags are checked before any HTTP RPC and
  after completion, and only five persistent transport worker threads remain
  regardless of the default queue capacity of 32.

Validation:
- Rust unit suite: 27 passed, including fixed five-thread and independent
  control-capacity regressions;
- real Bun ignored conformance: 23 passed, including saturated cancellation,
  `json_rpc_fast_request_is_not_blocked_behind_a_slow_request` and cancellation;
- Bun TypeScript/package pipeline: 11 tests plus 3 package-smoke tests passed;
- 50-request release wire evidence refreshed in
  `docs/evidence/bun-wire-benchmark.json` (JSON-RPC p50 0.127 ms, p95 0.592 ms,
  4,258.565 requests/s; bounded overload rejected exactly 8 of 40 above 32).
