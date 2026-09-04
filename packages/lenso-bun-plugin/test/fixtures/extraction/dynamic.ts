import { definePlugin, tools } from "./sdk.ts";

export default definePlugin({
  providers: [tools(process.env.LENSO_TOOLS)],
});
