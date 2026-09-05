import { definePlugin } from "@lenso/bun-plugin";

export default definePlugin({
  providers: [],
  create() {
    throw new Error("fixture construction failed");
  },
});
