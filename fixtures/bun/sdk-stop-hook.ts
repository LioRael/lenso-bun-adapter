import { definePlugin, serve } from "@lenso/bun-plugin";

serve(
  definePlugin({
    decodeConfig(value) {
      const marker = (value as { marker?: unknown }).marker;
      if (typeof marker !== "string") throw new Error("marker is required");
      return { marker };
    },
    create({ config }) {
      return config;
    },
    providers: [],
    async stop(instance) {
      await Bun.write(instance.marker, "stopped\n");
    },
  }),
);
