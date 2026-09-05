import {
  definePlugin,
  provider,
  servePluginV2,
  type CapabilityProviderDescriptor,
} from "../../src/index.ts";

const descriptor: CapabilityProviderDescriptor = {
  capability_id: "example.blocking@1",
  descriptor_version: "1.0.0",
  descriptor_digest:
    "sha256:3300000000000000000000000000000000000000000000000000000000000033",
  operations: ["block"],
  stream_operations: [],
  event_operations: [],
};

await servePluginV2(
  definePlugin({
    providers: [
      provider(descriptor, () => ({
        descriptor,
        invokeRequest: () => new Promise(() => {}),
      })),
    ],
  }),
);
