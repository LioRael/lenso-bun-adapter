import { expect, test } from "bun:test";

test("published package exports the Bun Module authoring surface", async () => {
  const module = await import("@lenso/bun-module");
  expect(module.defineModule).toBeFunction();
  expect(module.serve).toBeFunction();
  expect(module.startModule).toBeFunction();
});
