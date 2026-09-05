import {
  definePlugin,
  dependency,
  servePluginV2,
  type BunInvocationContext,
  type CapabilityProviderDescriptor,
  type ProviderDispatchOutcome,
} from "../../src/index.ts";

const descriptor: CapabilityProviderDescriptor = {
  capability_id: "example.document-store@1",
  descriptor_version: "1.0.0",
  descriptor_digest:
    "sha256:1100000000000000000000000000000000000000000000000000000000000011",
  operations: ["read"],
  stream_operations: [],
  event_operations: [],
};

interface StoreClient {
  read(context: BunInvocationContext, payload: unknown): Promise<ProviderDispatchOutcome>;
}

const source = dependency({
  id: "source",
  contract: {
    descriptor,
    createClient: (invoke): StoreClient => ({
      read: (context, payload) => invoke("read", context, payload),
    }),
  },
});

const definition = definePlugin({
  dependencies: { source },
  async create({ dependencies }, lifecycle) {
    await dependencies.source.read(lifecycle, { document: "create" });
    return { source: dependencies.source };
  },
  async stop(instance, lifecycle) {
    await instance.source.read(lifecycle, { document: "stop" });
  },
  providers: [],
});

await servePluginV2(definition);
