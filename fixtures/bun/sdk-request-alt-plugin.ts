import { definePlugin } from "@lenso/bun-plugin";
import { bindProvider, type Provider } from "./generated/greeting.ts";

const greeting: Provider = {
  async greet(_context, request) {
    return {
      ok: true,
      value: { message: `Hello from alternate Bun, ${request.name}!` },
    };
  },
};

export default definePlugin({ providers: [bindProvider(greeting)] });
