import type { CapabilityProviderDescriptor } from "../src/index.ts";
import {
  definePlugin,
  dependency,
  provider,
  type CapabilityDependencyBinding,
  type ConfigDeclaration,
  type DependencyDeclarations,
} from "../src/index.ts";
import {
  Conversation,
  type ConversationProvider,
} from "./fixtures/generated/conversation.ts";
import {
  Notifications,
  type NotificationsProvider,
} from "./fixtures/generated/notifications.ts";
import { Profile, type ProfileClient } from "./fixtures/generated/profile.ts";

interface StoreClient {
  get(key: string): Promise<string | undefined>;
}

const descriptor: CapabilityProviderDescriptor = {
  capability_id: "example.output@1",
  descriptor_version: "1.0.0",
  operations: ["emit"],
  stream_operations: [],
  event_operations: [],
};

const storeContract = null as unknown as CapabilityDependencyBinding<StoreClient>;
const config = null as unknown as ConfigDeclaration<{ readonly prefix: string }>;

const dependencies = {
  primary: dependency({ id: "primary", contract: storeContract }),
  cache: dependency({
    id: "cache",
    contract: storeContract,
    cardinality: "optional",
  }),
  replicas: dependency({
    id: "replicas",
    contract: storeContract,
    cardinality: "many",
  }),
} as const;

config satisfies ConfigDeclaration<unknown>;
dependencies satisfies DependencyDeclarations;

const authoredProvider = provider<{
  readonly prefix: string;
  readonly primary: StoreClient;
}>(descriptor, (instance) => ({
  descriptor,
  async invokeRequest() {
    await instance.primary.get(instance.prefix);
    return { kind: "success", value: instance.prefix };
  },
}));

definePlugin({
  create() {
    return { value: 1 };
  },
  providers: [],
});

definePlugin({
  config,
  dependencies,
  async create(inputs) {
    inputs.config.prefix satisfies string;
    inputs.dependencies.primary satisfies StoreClient;
    inputs.dependencies.cache satisfies StoreClient | undefined;
    inputs.dependencies.replicas satisfies ReadonlyArray<{
      readonly providerInstance: string;
      readonly client: StoreClient;
    }>;
    return {
      prefix: inputs.config.prefix,
      primary: inputs.dependencies.primary,
    };
  },
  providers: [authoredProvider],
  async stop(instance, lifecycle) {
    instance.prefix satisfies string;
    lifecycle.signal satisfies AbortSignal;
    lifecycle.remainingTimeoutMs() satisfies number;
  },
});

const wrongProvider = provider<{ readonly wrong: number }>(descriptor, () => ({
  descriptor,
  async invokeRequest() {
    return { kind: "success", value: null };
  },
}));

const mismatchedPlugin = {
  async create() {
    return { readonlyValue: 1 } as const;
  },
  providers: [wrongProvider],
} as const;

// @ts-expect-error Providers must bind the object returned by create.
definePlugin(mismatchedPlugin);

definePlugin({
  dependencies,
  providers: [
    provider(descriptor, (instance: { readonly dependencies: {
      readonly primary: StoreClient;
      readonly cache: StoreClient | undefined;
      readonly replicas: ReadonlyArray<unknown>;
    } }) => ({
      descriptor,
      async invokeRequest() {
        await instance.dependencies.primary.get("key");
        return { kind: "success", value: null };
      },
    })),
  ],
});

definePlugin({
  providers: [],
  // @ts-expect-error Agent-owned syntax is not a generic Plugin option.
  tools: [],
});

definePlugin({
  provides: [Conversation, Notifications],
  dependencies: { profile: Profile.required() },
  create({ dependencies }) {
    dependencies.profile satisfies ProfileClient;
    return {
      async *chat(_context, request) {
        yield { text: request.room };
      },
      async notify(_context, _event) {},
    } satisfies ConversationProvider & NotificationsProvider;
  },
});

definePlugin({
  provides: [Conversation],
  // @ts-expect-error create must return the methods required by every provided Capability.
  create() {
    return { unrelated: true };
  },
});
