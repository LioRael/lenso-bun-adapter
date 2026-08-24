import { expect, test } from "bun:test";

test("published SDK exports the Bun runtime and official Jobs projection", async () => {
  const sdk = await import("@lenso/bun");
  const jobs = await import("@lenso/bun/capabilities/jobs");
  expect(sdk.defineModule).toBeFunction();
  expect(sdk.serve).toBeFunction();
  expect(jobs.bindJobsProvider).toBeFunction();
  expect(jobs.CAPABILITY_ID).toBe("lenso.jobs@1");
});
