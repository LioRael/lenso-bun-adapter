import {
  definePlugin,
  dependency,
  provider,
  servePluginV2,
  type CapabilityDependencyBinding,
  type CapabilityProviderDescriptor,
  type InteractionDependencyInvoker,
  type ProviderEventPublishOutcome,
  type ProviderStreamOpenOutcome,
} from "../../src/index.ts";
import type { InvocationContext } from "@lenso/contract-runtime";

const CHANNEL_ID = "example.channel@1";
const ECHO_ID = "example.echo@1";
const VERSION = "1.0.0";
const CHANNEL_DIGEST =
  "sha256:5500000000000000000000000000000000000000000000000000000000000055";
const ECHO_DIGEST =
  "sha256:4400000000000000000000000000000000000000000000000000000000000044";

const channelDescriptor: CapabilityProviderDescriptor = {
  capability_id: CHANNEL_ID,
  descriptor_version: VERSION,
  descriptor_digest: CHANNEL_DIGEST,
  operations: ["chat", "notify"],
  stream_operations: ["chat"],
  event_operations: ["notify"],
};
const echoDescriptor: CapabilityProviderDescriptor = {
  capability_id: ECHO_ID,
  descriptor_version: VERSION,
  descriptor_digest: ECHO_DIGEST,
  operations: ["echo"],
  stream_operations: [],
  event_operations: [],
};

interface ChannelClient {
  open(context: InvocationContext, room?: string): Promise<ProviderStreamOpenOutcome>;
  publish(context: InvocationContext): Promise<ProviderEventPublishOutcome>;
}

const channelContract: CapabilityDependencyBinding<ChannelClient, InteractionDependencyInvoker> = {
  descriptor: channelDescriptor,
  createClient: (invoke) => ({
    open: (context, room = "general") => invoke.openStream("chat", context, { room }),
    publish: (context) => invoke.publishEvent("notify", context, { message: "ready" }),
  }),
};
const channel = dependency({ id: "channel", contract: channelContract });

const definition = definePlugin({
  dependencies: { channel },
  create: ({ dependencies }) => dependencies,
  providers: [provider(echoDescriptor, (instance: { readonly channel: ChannelClient }) => ({
    descriptor: echoDescriptor,
    async invokeRequest(operation, context) {
      if (operation !== "echo") {
        return { kind: "runtime", failure: { kind: "unknown_operation", operation } };
      }
      const publication = await instance.channel.publish(context);
      const opened = await instance.channel.open(context);
      if (opened.kind !== "opened") return opened;
      await opened.stream.send({ text: "from Bun" });
      const message = await opened.stream.receive();
      await opened.stream.closeSend();
      const halfClosed = await opened.stream.receive();
      const terminal = await opened.stream.receive();
      const blocked = await instance.channel.open(context, "blocked");
      if (blocked.kind !== "opened") return blocked;
      const pendingReceive = blocked.stream.receive();
      await new Promise((resolve) => setTimeout(resolve, 10));
      const concurrentPublication = await instance.channel.publish(context);
      blocked.stream.cancel();
      await pendingReceive.catch(() => undefined);
      const cancellable = await instance.channel.open(context);
      if (cancellable.kind === "opened") cancellable.stream.cancel();
      return {
        kind: "success",
        value: {
          publication: publication.kind,
          concurrentPublication: concurrentPublication.kind,
          message,
          halfClosed,
          terminal,
        },
      };
    },
  }))],
});

export default definition;
await servePluginV2(definition);
