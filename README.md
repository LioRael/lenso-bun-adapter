# Lenso Bun SDK and Runtime

The Bun authoring and execution surface for Lenso Plugins:

- `@lenso/bun` is the supported authoring SDK. It combines the runtime with
  directly consumable projections of official portable Capabilities.
- `@lenso/bun-plugin` lets authors register generated, typed Capability
  Providers without implementing the wire protocol. It remains the low-level
  runtime package behind the SDK.
- `lenso-bun-adapter` owns the child-process mechanics used by a Rust Host.
- `lenso-host-runtime` is the Rust executable inside a prepared TypeScript Host
  distribution. It installs Bun and native Process Adapters, activates the
  resolved Generation, and recovers exact durable state.
- the cross-runtime fixtures prove the generated TypeScript contract against
  the Rust Kernel and preserve low-level wire conformance coverage.

Managed Bun Plugins receive their resolved Capability clients during activation.
The Adapter exposes a bounded, token-protected loopback import endpoint for that
single process and dispatches every call through the Kernel dependency handle.
Imports are keyed by stable requirement identity rather than Capability alone,
so two uses of the same contract route independently.
The same TypeScript consumer is covered against both a Bun provider and a
prebuilt Rust Process provider.

This repository consumes released Lenso core packages and does not own Kernel
semantics or product Plugins.

## Author a Bun Plugin

Install one SDK, implement an official generated Provider interface, and export
one Plugin definition. The generated entrypoint owns runtime startup:

```sh
bun add @lenso/bun
```

```ts
import { definePlugin } from "@lenso/bun";
import { bindJobsProvider } from "@lenso/bun/capabilities/jobs";
import { jobs } from "./jobs.ts"; // Implements JobsProvider.

export default definePlugin({ providers: [bindJobsProvider(jobs)] });
```

Plugin projects created by Lenso contain a generated entrypoint that imports
this default export and starts the runtime. Authors do not call `serve`, handle
the process handshake, or implement the transport.

Custom Capability contracts can still be generated during authoring. Official
Capability projections belong in `@lenso/bun`, not beside Rust crate source.

The current SDK surface supports request Capabilities, per-Instance construction,
resolved dependency clients, and bounded stop hooks over the production
JSON-RPC loopback wire. Stream and Event descriptors fail closed until their
typed sessions are available. Framed stdio remains a conformance and benchmark
wire, not a user-facing authoring API.

The source was extracted from `LioRael/lenso` at monorepo commit
`67d21499548d07e92c2f6529d7c8345e58c067d9` under ADR 0064. Imported subtrees
retain their relevant Git history.

## Child process diagnostics

The Rust Host continuously drains Bun stdout and stderr into a bounded
32-line diagnostic tail. Common credential markers and URL user information
are redacted before failures become user visible. Lines longer than the
diagnostic bound are fully suppressed instead of retaining a potentially
sensitive prefix.

JSON-RPC data requests and ordered Events share a bounded concurrent client.
Request and stream cancellation use a separate two-slot control client, so a
full set of slow data requests cannot delay cancellation delivery.

## Validation

```sh
cargo fmt --all -- --check
cargo check --locked --workspace --all-targets
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked --workspace
bun install --frozen-lockfile
bun run --filter '@lenso/bun' capabilities:check
bun run build
bun run typecheck
bun run test:typescript
bun run package-smoke
npm pack --dry-run ./packages/lenso-bun
npm pack --dry-run ./packages/lenso-bun-plugin
bun build --target bun fixtures/bun/sdk-request-provider.ts --outdir /tmp/lenso-bun-fixtures
cargo test --locked -p lenso-bun-adapter --test bun_cross_runtime -- --ignored --test-threads=1
```
