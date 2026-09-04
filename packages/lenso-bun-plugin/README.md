# @lenso/bun-plugin

`definePlugin(...)` is runtime-independent authoring. Generated wrappers may
compile the same definition to a bounded QuickJS ES module or a Bun executable;
`serve(...)` is only the Bun transport lowering. Portable definitions must not
use `Bun.*`, filesystem, socket, or other platform globals in Provider logic.

The authored file exports only the definition:

```ts
// src/plugin.ts
import { definePlugin } from "@lenso/bun-plugin";

export default definePlugin({ providers: [greeting] });
```

Generated entrypoints lower that definition without duplicating business code:

```ts
// QuickJS entrypoint
import plugin from "./plugin.ts";
import {
  describePortablePlugin,
  invokePortablePlugin,
} from "@lenso/bun-plugin";

export function describe() {
  return JSON.stringify(describePortablePlugin(plugin));
}

export function invoke(capability: string, operation: string, request: string) {
  return invokePortablePlugin(plugin, capability, operation, request);
}
```

```ts
// Bun entrypoint
import plugin from "./plugin.ts";
import { serve } from "@lenso/bun-plugin";

serve(plugin);
```

The build targets are then ordinary Bun outputs:

```sh
bun build src/lenso.quickjs.generated.ts --target=browser --format=esm --outfile=dist/plugin.js
bun build src/lenso.bun.generated.ts --compile --outfile=dist/plugin
```

Author a Lenso Plugin in Bun without implementing the Execution Adapter
wire protocol. Generated Capability bindings provide typed Provider interfaces
and `bind*Provider` functions; this package owns the Bun process handshake,
bounded JSON-RPC server, construction, dependency imports, cancellation,
shutdown, and Runtime Failure mapping.

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

Plugins that consume Capabilities declare a closed dependency table. The Host
injects only the exact providers selected by the immutable Plan. `create`
receives decoded configuration and generated clients once per Instance; it may
return any complete object used by Provider handlers. `stop` runs at most once
during managed shutdown.

```ts
export default definePlugin({
  dependencies: { store: storeDependency },
  decodeConfig: NotesConfig.parse,
  async create({ config, dependencies }) {
    return { config, store: dependencies.store, cache: new Map() };
  },
  providers: [notesProvider],
  async stop(instance) {
    instance.cache.clear();
  },
});
```

Generated dependency projections construct the typed clients represented by
`storeDependency`. Each table key is the stable consumer-local requirement
identity, so `source` and `destination` may use the same Capability while binding
different provider Instances. The portable descriptor carries those keys into
Bundle admission and the Adapter matches imports by requirement identity. They
carry the current invocation context through the Host
import, so TS-to-TS and TS-to-Process calls use the same Kernel admission,
deadline, cancellation, generation, and supervision path. A Plugin with no
Providers is valid when it only consumes Capabilities or owns lifecycle work.

Lenso generates the executable entrypoint that calls the low-level `serve`
function. Plugin authors export the definition and do not implement startup or
wire handling themselves.

The current public surface supports request Capabilities over the production
JSON-RPC loopback wire. Framed stdio remains a conformance and benchmark wire,
not an authoring surface. Stream and Event descriptors are rejected until their
typed SDK sessions are available rather than silently exposing partial support.
