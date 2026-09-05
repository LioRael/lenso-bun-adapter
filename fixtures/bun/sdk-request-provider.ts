// Legacy execution entrypoint retained to exercise the Bun runtime profile v1.
import { definePlugin, serve } from "@lenso/bun-plugin";
import { bindProvider } from "./generated/greeting.ts";
import { greeting } from "./sdk-request-plugin.ts";

serve(definePlugin({ providers: [bindProvider(greeting)] }));
