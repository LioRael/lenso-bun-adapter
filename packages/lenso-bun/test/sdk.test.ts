import { expect, test } from "bun:test";
import type { InvocationContext, Uint64 } from "@lenso/contract-runtime";
import { definePlugin, startPlugin } from "../src/index.ts";
import {
  bindJobsProvider,
  CAPABILITY_ID,
  DESCRIPTOR_VERSION,
  type JobsProvider,
  type Timestamp,
} from "../src/capabilities/jobs.ts";

const provider: JobsProvider = {
  async claim() {
    throw new Error("not exercised");
  },
  async complete() {
    throw new Error("not exercised");
  },
  async enqueue(_context, request) {
    return {
      ok: true,
      value: { created: true, job_id: `job:${request.idempotency_key}` },
    };
  },
  async fail() {
    throw new Error("not exercised");
  },
  async inspect() {
    throw new Error("not exercised");
  },
  async renew() {
    throw new Error("not exercised");
  },
};

async function rpc(
  port: number,
  id: number,
  method: string,
  params: unknown,
): Promise<Record<string, unknown>> {
  const response = await fetch(`http://127.0.0.1:${port}`, {
    method: "POST",
    body: JSON.stringify({ jsonrpc: "2.0", id, method, params: [params] }),
  });
  return (await response.json()) as Record<string, unknown>;
}

test("binds the official Jobs Capability without a repository-local import", async () => {
  const binding = bindJobsProvider(provider);
  const definition = definePlugin({ providers: [binding] });
  expect(definition.providers).toHaveLength(1);
  expect(binding.descriptor).toEqual({
    capability_id: CAPABILITY_ID,
    descriptor_version: DESCRIPTOR_VERSION,
    operations: ["claim", "complete", "enqueue", "fail", "inspect", "renew"],
    stream_operations: [],
    event_operations: [],
  });

  const context: InvocationContext = {
    requestId: "1" as Uint64,
    cancelled: false,
  };
  const availableAt = "2026-08-24T00:00:00Z" as Timestamp;
  await expect(
    binding.invokeRequest("enqueue", context, {
      available_at: availableAt,
      idempotency_key: "welcome:42",
      kind: "send-welcome-email",
      max_attempts: 3,
      payload: { account_id: "42" },
      queue: "email",
    }),
  ).resolves.toEqual({
    kind: "success",
    value: { created: true, job_id: "job:welcome:42" },
  });
});

test("serves the centralized Jobs projection through the Bun runtime", async () => {
  const binding = bindJobsProvider(provider);
  const server = startPlugin(definePlugin({ providers: [binding] }));
  try {
    const handshake = await rpc(server.port, 1, "lenso.handshake", {
      protocol_version: 1,
      value_profile: "lenso-json-value-v1",
      max_frame_bytes: 65536,
      endpoints: [binding.descriptor],
    });
    const session = (handshake.result as { session: string }).session;
    expect(handshake.result).toMatchObject({ accepted: true, session });

    expect(
      (
        await rpc(server.port, 2, "lenso.request", {
          request_id: 7,
          capability_id: CAPABILITY_ID,
          operation: "enqueue",
          session,
          payload: {
            available_at: "2026-08-24T00:00:00Z",
            idempotency_key: "welcome:42",
            kind: "send-welcome-email",
            max_attempts: 3,
            payload: { account_id: "42" },
            queue: "email",
          },
        })
      ).result,
    ).toEqual({
      kind: "success",
      value: { created: true, job_id: "job:welcome:42" },
    });
  } finally {
    server.stop();
  }
});

test("records the authoritative source of each generated Capability projection", async () => {
  const lock = await Bun.file(
    new URL("../capabilities.lock.json", import.meta.url),
  ).json();
  expect(lock.schema_version).toBe(2);
  expect(Object.keys(lock.capabilities)).toHaveLength(24);
  expect(lock.capabilities[CAPABILITY_ID]).toEqual({
    descriptor_version: DESCRIPTOR_VERSION,
    export: "./capabilities/jobs",
    snapshot_descriptor: "contracts/jobs/capability.json",
    source_descriptor: "crates/lenso-capability-jobs/capability.json",
    source_package: "lenso-capability-jobs",
    source_package_version: "0.1.0",
    source_repository: "https://github.com/LioRael/lenso-jobs-plugin",
    source_revision: "e4f0e097bfbc46284fc4aa678029a79f2f46ada4",
    typescript_projection: "src/capabilities/jobs.ts",
  });
});
