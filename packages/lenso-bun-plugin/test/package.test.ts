import { expect, test } from "bun:test";

test("published package exports the Bun Plugin authoring surface", async () => {
  const module = await import("@lenso/bun-plugin");
  expect(module.definePlugin).toBeFunction();
  expect(module.serve).toBeFunction();
  expect(module.startPlugin).toBeFunction();
});
