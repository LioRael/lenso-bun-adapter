import { expect, test } from "bun:test";

test("published package exports the Bun Plugin authoring surface", async () => {
  const module = await import("@lenso/bun-plugin");
  expect(module.definePlugin).toBeFunction();
  expect(module.serve).toBeFunction();
  expect(module.startPlugin).toBeFunction();
});

test("published package exports build and extraction entrypoints", async () => {
  const build = await import("@lenso/bun-plugin/build");
  const extract = await import("@lenso/bun-plugin/extract");
  expect(build.runLowering).toBeFunction();
  expect(build.fingerprintBuildInputs).toBeFunction();
  expect(extract.extractPluginDefinition).toBeFunction();
});
