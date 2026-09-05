import { configuration, definePlugin } from "@lenso/bun-plugin";

const config = configuration(true, (value) => {
  const marker = (value as { marker?: unknown }).marker;
  if (typeof marker !== "string") throw new Error("marker is required");
  return { marker };
});

export default definePlugin({
  config,
  create({ config }) {
    return config;
  },
  providers: [],
  async stop(instance) {
    await Bun.write(instance.marker, "stopped\n");
  },
});
