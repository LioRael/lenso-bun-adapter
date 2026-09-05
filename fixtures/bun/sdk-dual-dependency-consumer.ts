import { dependency, definePlugin, provider, type CapabilityProviderBinding } from "@lenso/bun-plugin";
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

const source = dependency({ id: "source", contract: bindGreetingDependency() });
const destination = dependency({ id: "destination", contract: bindGreetingDependency() });
type Instance = { source: GreetingClient; destination: GreetingClient };

const proxyProvider = provider<Instance>(proxy, (instance) => ({
  descriptor: proxy,
  async invokeRequest(operation, context, payload) {
    if (operation !== "greet") {
      return { kind: "runtime", failure: { kind: "unknown_operation", operation } };
    }
    const request = payload as { name: string };
    const [left, right] = await Promise.all([
      instance.source.greet({ name: `${request.name} source` }, context),
      instance.destination.greet({ name: `${request.name} destination` }, context),
    ]);
    if (!left.ok) {
      return left.error.kind === "domain"
        ? { kind: "domain", value: left.error.error }
        : { kind: "runtime", failure: left.error.error };
    }
    if (!right.ok) {
      return right.error.kind === "domain"
        ? { kind: "domain", value: right.error.error }
        : { kind: "runtime", failure: right.error.error };
    }
    return {
      kind: "success",
      value: { message: `${left.value.message} / ${right.value.message}` },
    };
  },
}));

export default definePlugin({
  dependencies: { source, destination },
  create: ({ dependencies }) => dependencies,
  providers: [proxyProvider],
});
