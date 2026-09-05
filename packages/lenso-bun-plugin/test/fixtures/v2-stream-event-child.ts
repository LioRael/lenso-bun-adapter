import {
  definePlugin,
  provider,
  servePluginV2,
  type CapabilityProviderDescriptor,
} from "../../src/index.ts";

const descriptor: CapabilityProviderDescriptor = {
  capability_id: "example.channel@1",
  descriptor_version: "1.0.0",
  descriptor_digest:
    "sha256:5500000000000000000000000000000000000000000000000000000000000055",
  operations: ["chat", "notify"],
  stream_operations: ["chat"],
  event_operations: ["notify"],
};

const definition = definePlugin({
  providers: [
    provider(descriptor, () => ({
      descriptor,
      async invokeRequest(operation) {
        return { kind: "runtime" as const, failure: { kind: "unknown_operation" as const, operation } };
      },
      async publishEvent(operation, _context, payload) {
        return operation === "notify" && (payload as { message?: string }).message === "ready"
          ? { kind: "accepted" as const }
          : { kind: "runtime" as const, failure: { kind: "plugin_failure" as const, detail: "invalid notification" } };
      },
      async openStream(operation, _context, payload) {
        if (operation !== "chat") {
          return { kind: "runtime" as const, failure: { kind: "unknown_operation" as const, operation } };
        }
        if ((payload as { room?: string }).room !== "general") {
          return { kind: "domain" as const, value: "unknown_room" };
        }
        const messages: unknown[] = [];
        let sendClosed = false;
        let cancelled = false;
        return {
          kind: "opened" as const,
          stream: {
            async send(message: unknown) {
              if (sendClosed || cancelled) return { kind: "runtime" as const, failure: { kind: "admission_closed" as const } };
              messages.push(message);
              return { kind: "accepted" as const };
            },
            async receive() {
              const value = messages.shift();
              if (value !== undefined) return { kind: "message" as const, value };
              if (sendClosed) return { kind: "terminal_success" as const };
              return { kind: "peer_half_closed" as const };
            },
            async closeSend() {
              sendClosed = true;
              return { kind: "accepted" as const };
            },
            cancel() { cancelled = true; },
          },
        };
      },
    })),
  ],
});

await servePluginV2(definition);
