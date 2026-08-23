# Lenso Bun Runtime

The Bun authoring and execution surface for Lenso vNext Modules:

- `@lenso/bun-module` lets authors register generated, typed Capability
  Providers without implementing the wire protocol.
- `lenso-bun-adapter` owns the child-process mechanics used by a Rust Host.
- the cross-runtime fixtures prove the generated TypeScript contract against
  the Rust Kernel and preserve low-level wire conformance coverage.

This repository consumes released Lenso core packages and does not own Kernel
semantics or product Modules.

## Author a Bun Module

Generate TypeScript bindings from a Capability descriptor, implement the
generated Provider interface, and hand its binding to the runtime:

```ts
import { defineModule, serve } from "@lenso/bun-module";
import {
  bindGreetingProvider,
  type GreetingProvider,
} from "./generated/greeting.ts";

const greeting: GreetingProvider = {
  async greet(_context, request) {
    return {
      ok: true,
      value: { message: `Hello, ${request.name}!` },
    };
  },
};

serve(defineModule({ providers: [bindGreetingProvider(greeting)] }));
```

The first public SDK release supports request Capabilities over the production
JSON-RPC loopback wire. Stream and Event descriptors fail closed until their
typed sessions are available. Framed stdio remains a conformance and benchmark
wire, not a user-facing authoring API.

The source was extracted from `LioRael/lenso` at monorepo commit
`67d21499548d07e92c2f6529d7c8345e58c067d9` under ADR 0064. Imported subtrees
retain their relevant Git history.

## Validation

```sh
cargo fmt --all -- --check
cargo check --locked --workspace --all-targets
cargo test --locked --workspace
bun install --frozen-lockfile
bun run build
bun run typecheck
bun run test:typescript
bun run package-smoke
npm pack --dry-run ./packages/lenso-bun-module
bun build --target bun fixtures/bun/sdk-request-provider.ts --outdir /tmp/lenso-bun-fixtures
cargo test --locked -p lenso-bun-adapter --test bun_cross_runtime -- --ignored --test-threads=1
```
