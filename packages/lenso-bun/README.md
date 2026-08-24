# @lenso/bun

The supported Bun authoring SDK for Lenso Modules. It combines the Bun Module
runtime with generated projections of official portable Capability contracts.
Capability semantics remain in their owning repositories; this package owns
their directly consumable Bun projection.

```ts
import { defineModule, serve } from "@lenso/bun";
import { bindJobsProvider } from "@lenso/bun/capabilities/jobs";
import { jobs } from "./jobs.ts"; // Implements JobsProvider.

serve(defineModule({ providers: [bindJobsProvider(jobs)] }));
```

The initial SDK release supports request Capabilities over the production
JSON-RPC loopback wire. Custom Capability projections remain an advanced
authoring-time code-generation path.
