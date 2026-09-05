import {
  definePlugin,
  provider,
  servePluginV2,
  type CapabilityProviderDescriptor,
} from "../../src/index.ts";

const descriptor: CapabilityProviderDescriptor = {
  capability_id: "example.echo@1",
  descriptor_version: "1.0.0",
  descriptor_digest:
    "sha256:4400000000000000000000000000000000000000000000000000000000000044",
  operations: ["echo"],
  stream_operations: [],
  event_operations: [],
};

const definition = definePlugin({
  providers: [
    provider(descriptor, () => ({
      descriptor,
      invokeRequest(operation, _context, payload) {
        if (operation !== "echo") {
          return Promise.resolve({
            kind: "runtime" as const,
            failure: { kind: "unknown_operation" as const },
          });
        }
        return Promise.resolve({ kind: "success" as const, value: payload });
      },
    })),
  ],
});

await servePluginV2(definition);
