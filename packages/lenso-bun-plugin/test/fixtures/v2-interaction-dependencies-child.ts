import {
  definePlugin,
  provider,
  servePluginV2,
  type CapabilityProviderDescriptor,
  type BoundCapabilityClient,
  type DependencyDeclarations,
} from "../../src/index.ts";
import { Conversation, type ConversationClient } from "./generated/conversation.ts";
import { Notifications, type NotificationsClient } from "./generated/notifications.ts";

const RESULT_ID = "example.interaction-result@1";
const VERSION = "1.0.0";
const RESULT_DIGEST =
  "sha256:6600000000000000000000000000000000000000000000000000000000000066";
const resultDescriptor: CapabilityProviderDescriptor = {
  capability_id: RESULT_ID,
  descriptor_version: VERSION,
  descriptor_digest: RESULT_DIGEST,
  operations: ["exercise"],
  stream_operations: [],
  event_operations: [],
};
type Instance = {
  readonly conversation: ConversationClient;
  readonly notifications: ReadonlyArray<BoundCapabilityClient<NotificationsClient>>;
  readonly optionalNotifications: NotificationsClient | undefined;
};
const dependencies = {
  conversation: Conversation.required("conversation"),
  notifications: Notifications.many("notifications"),
  optionalNotifications: Notifications.optional("optional_notifications"),
} as const satisfies DependencyDeclarations;

const definition = definePlugin({
  dependencies,
  create: ({ dependencies }) => dependencies,
  providers: [
    provider<Instance>(resultDescriptor, (dependencies) => ({
      descriptor: resultDescriptor,
      async invokeRequest(operation, context) {
        if (operation !== "exercise") {
          return { kind: "runtime" as const, failure: { kind: "unknown_operation" as const, operation } };
        }
        const opened = await dependencies.conversation.chat({ room: "general" }, context);
        if (!opened.ok) return opened.error.kind === "domain"
          ? { kind: "domain" as const, value: opened.error.error }
          : { kind: "runtime" as const, failure: opened.error.error };
        await opened.value.send({ text: "hello" });
        const message = await opened.value.receive();
        await opened.value.closeSend();
        const halfClosed = await opened.value.receive();
        const terminal = await opened.value.receive();
        const cancellable = await dependencies.conversation.chat({ room: "cancel" }, context);
        if (!cancellable.ok) return cancellable.error.kind === "domain"
          ? { kind: "domain" as const, value: cancellable.error.error }
          : { kind: "runtime" as const, failure: cancellable.error.error };
        cancellable.value.cancel();
        const admissions = (await Promise.all(dependencies.notifications.map(
          ({ client }, sequence) => client.notify({ message: "ready", sequence }, context),
        ))).flat();
        return {
          kind: "success" as const,
          value: {
            message,
            halfClosed,
            terminal,
            admissions,
            optionalMissing: dependencies.optionalNotifications === undefined,
          },
        };
      },
    })),
  ],
});

await servePluginV2(definition);
