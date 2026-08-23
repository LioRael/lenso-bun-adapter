import { defineModule, serve } from "@lenso/bun-module";
import {
  bindProvider,
  type Provider,
} from "./generated/greeting.ts";

const greeting: Provider = {
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

serve(
  defineModule({
    providers: [bindProvider(greeting)],
  }),
);
