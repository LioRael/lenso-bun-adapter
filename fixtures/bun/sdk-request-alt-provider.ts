import { definePlugin, serve } from "@lenso/bun-plugin";
import { bindProvider } from "./generated/greeting.ts";
import { greeting } from "./sdk-request-alt-plugin.ts";

serve(definePlugin({ providers: [bindProvider(greeting)] }));
