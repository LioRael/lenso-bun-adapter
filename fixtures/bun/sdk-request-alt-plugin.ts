import { definePlugin, provider } from "@lenso/bun-plugin";
import { bindProvider, type Provider } from "./generated/greeting.ts";

export const greeting: Provider = {
  async greet(_context, request) {
    return {
      ok: true,
      value: { message: `Hello from alternate Bun, ${request.name}!` },
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
