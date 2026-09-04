import { definePlugin, tools } from "./sdk.ts";

function discoverTools() {
  return [];
}

export default definePlugin({ providers: [tools(discoverTools())] });
