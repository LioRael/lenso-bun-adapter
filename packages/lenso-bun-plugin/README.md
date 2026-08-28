# @lenso/bun-plugin

Author a Lenso Plugin in Bun without implementing the Execution Adapter
wire protocol. Generated Capability bindings provide typed Provider interfaces
and `bind*Provider` functions; this package owns the Bun process handshake,
bounded JSON-RPC server, cancellation, shutdown, and Runtime Failure mapping.

```ts
import { definePlugin, serve } from "@lenso/bun-plugin";
import {
  bindGreetingProvider,
  type GreetingProvider,
} from "./generated/greeting.ts";

const provider: GreetingProvider = {
  async greet(_context, request) {
    return {
      ok: true,
      value: { message: `Hello, ${request.name}!` },
    };
  },
};

serve(definePlugin({ providers: [bindGreetingProvider(provider)] }));
```

The initial public surface supports request Capabilities over the production
JSON-RPC loopback wire. Framed stdio remains a conformance and benchmark wire,
not an authoring surface. Stream and Event descriptors are rejected until their
typed SDK sessions are available rather than silently exposing partial support.
