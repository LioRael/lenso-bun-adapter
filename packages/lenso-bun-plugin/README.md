# @lenso/bun-plugin

`definePlugin(...)` is runtime-independent authoring. Generated wrappers may
compile the same definition to a bounded QuickJS ES module or a Bun executable;
`serve(...)` is only the Bun transport lowering. Portable definitions must not
use `Bun.*`, filesystem, socket, or other platform globals in Provider logic.

Author a Lenso Plugin in Bun without implementing the Execution Adapter
wire protocol. Generated Capability bindings provide typed Provider interfaces
and `bind*Provider` functions; this package owns the Bun process handshake,
bounded JSON-RPC server, cancellation, shutdown, and Runtime Failure mapping.

```ts
import { definePlugin } from "@lenso/bun-plugin";
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

export default definePlugin({ providers: [bindGreetingProvider(provider)] });
```

Lenso generates the executable entrypoint that calls the low-level `serve`
function. Plugin authors export the definition and do not implement startup or
wire handling themselves.

The initial public surface supports request Capabilities over the production
JSON-RPC loopback wire. Framed stdio remains a conformance and benchmark wire,
not an authoring surface. Stream and Event descriptors are rejected until their
typed SDK sessions are available rather than silently exposing partial support.
