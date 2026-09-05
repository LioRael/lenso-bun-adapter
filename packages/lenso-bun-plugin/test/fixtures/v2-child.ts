import {
  definePlugin,
  dependency,
  provider,
  servePluginV2,
  type BunInvocationContext,
  type CapabilityProviderDescriptor,
  type ProviderDispatchOutcome,
} from "../../src/index.ts";

const STORE_ID = "example.document-store@1";
const SYNC_ID = "example.sync@1";
const VERSION = "1.0.0";
const STORE_DIGEST =
  "sha256:1100000000000000000000000000000000000000000000000000000000000011";
const SYNC_DIGEST =
  "sha256:2200000000000000000000000000000000000000000000000000000000000022";

const descriptor = (
  capability_id: string,
  descriptor_digest: string,
  operations: ReadonlyArray<string>,
): CapabilityProviderDescriptor => ({
  capability_id,
  descriptor_version: VERSION,
  descriptor_digest,
  operations,
  stream_operations: [],
  event_operations: [],
});

interface StoreClient {
  read(context: BunInvocationContext, payload: unknown): Promise<ProviderDispatchOutcome>;
}

const source = dependency({
  id: "source",
  contract: {
    descriptor: descriptor(STORE_ID, STORE_DIGEST, ["read"]),
    createClient: (invoke): StoreClient => ({
      read: (context, payload) => invoke("read", context, payload),
    }),
  },
});

const definition = definePlugin({
  dependencies: { source },
  create: ({ dependencies }) => ({ source: dependencies.source, running: false }),
  providers: [
    provider(
      descriptor(SYNC_ID, SYNC_DIGEST, ["sync"]),
      (instance: { readonly source: StoreClient; running: boolean }) => ({
        descriptor: descriptor(SYNC_ID, SYNC_DIGEST, ["sync"]),
        invokeRequest(operation, context, payload) {
          if (operation !== "sync") {
            return Promise.resolve({
              kind: "runtime",
              failure: { kind: "unknown_operation" },
            });
          }
          if (instance.running) {
            return Promise.resolve({ kind: "domain", value: "already_running" });
          }
          instance.running = true;
          return instance.source
            .read(context as BunInvocationContext, payload)
            .finally(() => { instance.running = false; });
        },
      }),
    ),
  ],
});

await servePluginV2(definition);
