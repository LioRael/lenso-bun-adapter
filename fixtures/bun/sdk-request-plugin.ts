import { definePlugin, provider } from "@lenso/bun-plugin";
import {
  bindProvider,
  type Provider,
} from "./generated/greeting.ts";

export const greeting: Provider = {
  async greet(_context, request) {
    if (request.name.length === 0) {
      return {
        ok: false,
        error: { kind: "domain", error: "empty_name" },
      };
    }
    return {
      ok: true,
      value: { message: `Hello from Bun, ${request.name}!` },
    };
  },
};

type Instance = { greeting: Provider };

export default definePlugin({
  create() {
    return { greeting };
  },
  providers: [
    provider<Instance>(bindProvider(greeting).descriptor, (instance) =>
      bindProvider(instance.greeting)),
  ],
});
