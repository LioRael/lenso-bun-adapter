import { expect, test } from "bun:test";
import type { InvocationContext } from "@lenso/contract-runtime";
import {
  definePlugin,
  startPlugin,
  type CapabilityProviderBinding,
  type ProviderDispatchOutcome,
} from "../src/index.ts";

const descriptor = {
  capability_id: "example.greeting@1",
  descriptor_version: "1.0.0",
  operations: ["greet"],
  stream_operations: [],
  event_operations: [],
} as const;

function binding(): CapabilityProviderBinding {
  return {
    descriptor,
    async invokeRequest(
      operation: string,
      context: InvocationContext,
      payload: unknown,
    ): Promise<ProviderDispatchOutcome> {
      if (operation !== "greet") {
        return {
          kind: "runtime",
          failure: { kind: "unknown_operation", operation },
        };
      }
      const request = payload as { name?: unknown };
      if (typeof request.name !== "string") {
        return {
          kind: "runtime",
          failure: { kind: "protocol_violation" },
        };
      }
      if (request.name.length === 0) return { kind: "domain", value: "empty_name" };
      if (request.name === "huge") {
        return { kind: "success", value: { message: "x".repeat(2048) } };
      }
      if (request.name === "wait") {
        while (!context.cancelled) await Bun.sleep(2);
      }
      return {
        kind: "success",
        value: { message: `Hello, ${request.name}!` },
      };
    },
  };
}

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

test("serves a generated request Provider with exact handshake and shutdown", async () => {
  const server = startPlugin(definePlugin({ providers: [binding()] }));
  try {
    const handshake = await rpc(server.port, 1, "lenso.handshake", {
      protocol_version: 1,
      value_profile: "lenso-json-value-v1",
      max_frame_bytes: 65536,
      endpoints: [descriptor],
    });
    const session = (handshake.result as { session: string }).session;
    expect(handshake.result).toMatchObject({ accepted: true, session });

    expect(
      (
        await rpc(server.port, 2, "lenso.request", {
          request_id: 7,
          capability_id: descriptor.capability_id,
          operation: "greet",
          session,
          payload: { name: "Ada" },
        })
      ).result,
    ).toEqual({ kind: "success", value: { message: "Hello, Ada!" } });
    expect(
      (
        await rpc(server.port, 3, "lenso.request", {
          request_id: 8,
          capability_id: descriptor.capability_id,
          operation: "greet",
          session,
          payload: { name: "" },
        })
      ).result,
    ).toEqual({ kind: "domain", value: "empty_name" });
    expect(
      (
        await rpc(server.port, 4, "lenso.handshake", {
          protocol_version: 1,
          value_profile: "lenso-json-value-v1",
          max_frame_bytes: 65536,
          endpoints: [],
        })
      ).result,
    ).toMatchObject({ accepted: false });
    expect(
      (
        await rpc(server.port, 5, "lenso.request", {
          request_id: 9,
          capability_id: descriptor.capability_id,
          operation: "greet",
          session,
          payload: { name: "Grace" },
        })
      ).result,
    ).toEqual({ kind: "success", value: { message: "Hello, Grace!" } });
    expect(
      (await rpc(server.port, 6, "lenso.cancel", null)).result,
    ).toMatchObject({
      kind: "runtime",
      failure: { kind: "protocol_violation" },
    });
    expect((await rpc(server.port, 7, "lenso.shutdown", { session })).result).toBe(
      true,
    );
  } finally {
    server.stop();
  }
});

test("delivers cancellation to the typed Invocation Context", async () => {
  const server = startPlugin(definePlugin({ providers: [binding()] }));
  try {
    const handshake = await rpc(server.port, 1, "lenso.handshake", {
      protocol_version: 1,
      value_profile: "lenso-json-value-v1",
      max_frame_bytes: 65536,
      endpoints: [descriptor],
    });
    const session = (handshake.result as { session: string }).session;
    const pending = rpc(server.port, 2, "lenso.request", {
      request_id: 9,
      capability_id: descriptor.capability_id,
      operation: "greet",
      session,
      payload: { name: "wait" },
    });
    await Bun.sleep(5);
    expect((await rpc(server.port, 3, "lenso.cancel", { request_id: 9, session })).result).toBe(
      true,
    );
    expect((await pending).result).toEqual({
      kind: "runtime",
      failure: { kind: "cancelled", request_id: 9 },
    });
  } finally {
    server.stop();
  }
});

test("rejects Provider responses that exceed the negotiated frame bound", async () => {
  const server = startPlugin(definePlugin({ providers: [binding()] }), {
    maxFrameBytes: 512,
  });
  try {
    const handshake = await rpc(server.port, 1, "lenso.handshake", {
      protocol_version: 1,
      value_profile: "lenso-json-value-v1",
      max_frame_bytes: 512,
      endpoints: [descriptor],
    });
    const session = (handshake.result as { session: string }).session;
    const response = await fetch(`http://127.0.0.1:${server.port}`, {
      method: "POST",
      body: JSON.stringify({
        jsonrpc: "2.0",
        id: 2,
        method: "lenso.request",
        params: [
          {
            request_id: 10,
            capability_id: descriptor.capability_id,
            operation: "greet",
            session,
            payload: { name: "huge" },
          },
        ],
      }),
    });
    expect(response.status).toBe(413);
  } finally {
    server.stop();
  }
});

test("rejects duplicate providers and unsupported partial interaction support", () => {
  expect(() => definePlugin({ providers: [binding(), binding()] })).toThrow(
    "duplicate Capability Provider",
  );
  expect(() =>
    definePlugin({
      providers: [
        {
          ...binding(),
          descriptor: {
            ...descriptor,
            stream_operations: ["greet"],
          },
        },
      ],
    }),
  ).toThrow("supports request Capabilities only");
});
