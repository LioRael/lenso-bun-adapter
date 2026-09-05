import { definePlugin, tools } from "./sdk.ts";
import { common } from "./reexports.ts";

export default definePlugin({
  providers: [tools([{ ...common, name: "override" }])],
});
