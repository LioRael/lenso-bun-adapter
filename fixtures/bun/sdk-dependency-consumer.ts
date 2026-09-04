import {
  definePlugin,
  type CapabilityProviderBinding,
} from "@lenso/bun-plugin";
import {
  bindGreetingDependency,
  type GreetingClient,
} from "./generated/greeting.ts";

const proxy = {
  capability_id: "example.greeting-proxy@1",
  descriptor_version: "1.0.0",
  operations: ["greet"],
  stream_operations: [],
  event_operations: [],
} as const;

const source = bindGreetingDependency();

type Instance = { source: GreetingClient; prefix: string };

const provider: CapabilityProviderBinding<Instance> = {
  descriptor: proxy,
  async invokeRequest(operation, context, payload, instance) {
    if (operation !== "greet") {
      return { kind: "runtime", failure: { kind: "unknown_operation", operation } };
    }
    const outcome = await instance.source.greet(payload as { name: string }, context);
    if (!outcome.ok) {
      return outcome.error.kind === "domain"
        ? { kind: "domain", value: outcome.error.error }
        : { kind: "runtime", failure: outcome.error.error };
    }
    return {
      kind: "success",
      value: { message: `${instance.prefix}${outcome.value.message}` },
    };
  },
};

export default definePlugin({
  dependencies: { source },
  decodeConfig(value) {
    const prefix = (value as { prefix?: unknown }).prefix;
    if (typeof prefix !== "string") throw new Error("prefix is required");
    return { prefix };
  },
  create({ config, dependencies }) {
    return { source: dependencies.source, prefix: config.prefix };
  },
  providers: [provider],
});
