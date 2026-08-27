import { expect, test } from "bun:test";

test("published SDK exports the Bun runtime and official Jobs projection", async () => {
  const sdk = await import("@lenso/bun");
  const jobs = await import("@lenso/bun/capabilities/jobs");
  expect(sdk.definePlugin).toBeFunction();
  expect(sdk.serve).toBeFunction();
  expect(jobs.bindJobsProvider).toBeFunction();
  expect(jobs.CAPABILITY_ID).toBe("lenso.jobs@1");
});

test("publishes every locked Capability projection", async () => {
  const lock = await Bun.file(
    new URL("../capabilities.lock.json", import.meta.url),
  ).json();
  for (const source of Object.values(lock.capabilities) as Array<{ export: string }>) {
    const projection = await import(`@lenso/bun/${source.export.slice(2)}`);
    expect(projection.CAPABILITY_ID).toBeString();
  }
});
