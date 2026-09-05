import {
  definePlugin,
  dependency,
  type CapabilityDependencyBinding,
  type ProviderDispatchOutcome,
} from "@lenso/bun-plugin";
import type { InvocationContext } from "@lenso/contract-runtime";

const proxy = {
  capability_id: "example.greeting-proxy@1",
  descriptor_version: "1.0.0",
  operations: ["greet"],
  stream_operations: [],
  event_operations: [],
} as const;

type ProxyClient = {
  greet(payload: unknown, context: InvocationContext): Promise<ProviderDispatchOutcome>;
};

const targetContract: CapabilityDependencyBinding<ProxyClient> = {
  descriptor: proxy,
  createClient: (invoke) => ({
    greet: (payload, context) => invoke("greet", context, payload),
  }),
};

const target = dependency({ id: "target", contract: targetContract });

export default definePlugin({ dependencies: { target }, providers: [] });
