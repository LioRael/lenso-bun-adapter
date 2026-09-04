import { definePlugin, serve } from "@lenso/bun-plugin";

serve(
  definePlugin({
    providers: [],
    create() {
      throw new Error("fixture construction failed");
    },
  }),
);
