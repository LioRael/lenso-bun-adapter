# Retain Bun child output ownership

Status: IMPLEMENTED AND VALIDATED

Finding: JSON-RPC readiness consumes and then drops child stdout, causing later writes to see a broken pipe, while stderr is drained and discarded so exit diagnostics lose their only context.

Scope:
- keep stdout drained after readiness without losing buffered bytes;
- retain a small bounded tail of stdout/stderr and attach it only to process-failure diagnostics;
- test readiness ownership and bounded redaction-safe retention.

Implementation:
- readiness returns its buffered stdout reader for continued ownership and both
  stdout/stderr are continuously drained;
- diagnostics retain at most 32 lines of 512 characters, dynamically decorate
  the latest process failure, redact common credential markers and URL
  user-info, and fully suppress every truncated line;
- fixed-buffer line draining bounds memory even when a child emits MiB without
  a newline.

Validation: 27 Rust unit tests passed, including pre-ready stdout, post-ready
stdout, late stderr, credential redaction, multi-MiB no-newline drainage, and a
URL credential whose `@` occurs after the truncation boundary; the redaction
regression also covers a safe URL followed by a credential-bearing URL on the
same line. All 23 real Bun process/conformance tests passed.
