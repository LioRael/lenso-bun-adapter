# @lenso/bun

The supported Bun authoring SDK for Lenso Plugins. It combines the Bun Plugin
runtime with generated projections of official portable Capability contracts.
Capability semantics remain in their owning repositories; this package owns
their directly consumable Bun projection. `capabilities.lock.json` pins every
source repository revision and the checked Descriptor/Schema snapshot used to
reproduce it.

```ts
import { definePlugin, serve } from "@lenso/bun";
import { bindJobsProvider } from "@lenso/bun/capabilities/jobs";
import { jobs } from "./jobs.ts"; // Implements JobsProvider.

serve(definePlugin({ providers: [bindJobsProvider(jobs)] }));
```

Run `bun run capabilities:check` with `lenso-contract-codegen` on `PATH` to
verify all locked projections. Maintainers use `bun run capabilities:sync`
after advancing an immutable source revision. Custom Capability projections
remain an authoring-time code-generation path in the Bun package that owns
them.
