import { expect, test } from "bun:test";
import type { InvocationContext } from "@lenso/contract-runtime";
import {
  describePortablePlugin,
  definePlugin,
  dependency,
  configuration,
  invokePortablePlugin,
  provider,
  startPlugin,
  type CapabilityProviderBinding,
  type CapabilityDependencyBinding,
  type ProviderDispatchOutcome,
} from "../src/index.ts";

const descriptor = {
  capability_id: "example.greeting@1",
  descriptor_version: "1.0.0",
  operations: ["greet"],
  stream_operations: [],
  event_operations: [],
} as const;

test("configuration keeps portable validation beside its typed decoder", () => {
  const config = configuration(
    {
      type: "object",
      additionalProperties: false,
      properties: { prefix: { type: "string" } },
      required: ["prefix"],
    },
    (input) => ({ prefix: (input as { prefix: string }).prefix }),
  );
  const plugin = definePlugin({ config, providers: [] });
  expect(plugin.config?.schema).toEqual(config.schema);
  expect(plugin.config?.parse({ prefix: "Hello " })).toEqual({ prefix: "Hello " });
});

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

test("one definition exposes the portable QuickJS ABI without Bun transport", async () => {
  const plugin = definePlugin({ providers: [binding()] });
  expect(describePortablePlugin(plugin)).toEqual({
    abi: "lenso.json-request@1",
    capabilities: [
      {
        capability_id: "example.greeting@1",
        descriptor_version: "1.0.0",
        request_operations: ["greet"],
      },
    ],
    required_capabilities: [],
  });
  expect(
    JSON.parse(
      await invokePortablePlugin(
        plugin,
        "example.greeting@1",
        "greet",
        '{"name":"Ada"}',
      ),
    ),
  ).toEqual({ ok: { message: "Hello, Ada!" } });
});

test("records named dependencies and instance-bound providers without running user code", () => {
  let bound = 0;
  const contract = {
    descriptor,
    createClient() {
      return { greet: async () => "hello" };
    },
  };
  const definition = definePlugin({
    dependencies: {
      source: dependency({ id: "source", contract }),
      fallbacks: dependency({
        id: "fallbacks",
        contract,
        cardinality: "many",
      }),
    },
    providers: [
      provider(descriptor, () => {
        bound += 1;
        return binding();
      }),
    ],
  });

  expect(bound).toBe(0);
  expect(definition.dependencies?.source.id).toBe("source");
  expect(Object.isFrozen(definition.dependencies)).toBe(true);
  expect(() => startPlugin(definition)).toThrow(
    "instance-bound providers require the Bun runtime profile v2",
  );
});

test("rejects duplicate public dependency ids", () => {
  const contract = {
    descriptor,
    createClient() {
      return {};
    },
  };
  expect(() =>
    definePlugin({
      dependencies: {
        first: dependency({ id: "store", contract }),
        second: dependency({ id: "store", contract, cardinality: "optional" }),
      },
      providers: [binding()],
    }),
  ).toThrow("duplicate dependency id store");
});

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

test("rejects duplicate providers and admits classified Stream providers", () => {
  expect(() => definePlugin({ providers: [binding(), binding()] })).toThrow(
    "duplicate Capability Provider",
  );
  expect(
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
    }).providers[0]?.descriptor.stream_operations,
  ).toEqual(["greet"]);
});

test("constructs one instance from admitted config and dependency clients, then stops it", async () => {
  const storeDescriptor = {
    capability_id: "example.store@1",
    descriptor_version: "1.0.0",
    operations: ["get"],
    stream_operations: [],
    event_operations: [],
  } as const;
  type StoreClient = {
    get(key: string, context: InvocationContext): Promise<ProviderDispatchOutcome>;
  };
  const storeDependency: CapabilityDependencyBinding<StoreClient> = {
    descriptor: storeDescriptor,
    createClient: (invoke) => ({
      get: (key, context) => invoke("get", context, { key }),
    }),
  };
  type Instance = { prefix: string; store: StoreClient };
  let stopped = false;
  const provider: CapabilityProviderBinding<Instance> = {
    descriptor,
    async invokeRequest(operation, context, payload, instance) {
      if (operation !== "greet") {
        return { kind: "runtime", failure: { kind: "unknown_operation", operation } };
      }
      const stored = await instance.store.get((payload as { name: string }).name, context);
      if (stored.kind !== "success") return stored;
      return { kind: "success", value: `${instance.prefix}${String(stored.value)}` };
    },
  };
  const plugin = definePlugin({
    dependencies: { store: storeDependency },
    configurationSchema: {
      type: "object",
      additionalProperties: false,
      properties: { prefix: { type: "string", minLength: 1 } },
      required: ["prefix"],
    },
    decodeConfig(value) {
      const prefix = (value as { prefix?: unknown }).prefix;
      if (typeof prefix !== "string") throw new Error("prefix is required");
      return { prefix };
    },
    async create({ config, dependencies }) {
      const seeded = await dependencies.store.get("seed", {
        requestId: "startup" as InvocationContext["requestId"],
        cancelled: false,
      });
      if (seeded.kind !== "success") throw new Error("startup dependency failed");
      return { prefix: config.prefix, store: dependencies.store };
    },
    providers: [provider],
    stop() {
      stopped = true;
    },
  });
  expect(describePortablePlugin(plugin).configuration_schema).toEqual({
    type: "object",
    additionalProperties: false,
    properties: { prefix: { type: "string", minLength: 1 } },
    required: ["prefix"],
  });
  expect(describePortablePlugin(plugin).required_capabilities).toEqual([
    {
      requirement_id: "store",
      capability_id: "example.store@1",
      descriptor_version: "1.0.0",
      cardinality: "one",
    },
  ]);
  const importToken = "test-import-token-123456";
  const importRequestIds: number[] = [];
  const imports = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(request) {
      expect(request.headers.get("authorization")).toBe(`Bearer ${importToken}`);
      const envelope = (await request.json()) as {
        id: number;
        params: [{ request_id: number; payload: { key: string } }];
      };
      expect(envelope.params[0].request_id).toBe(envelope.id);
      importRequestIds.push(envelope.id);
      return Response.json({
        jsonrpc: "2.0",
        id: envelope.id,
        result: { kind: "success", value: envelope.params[0].payload.key.toUpperCase() },
      });
    },
  });
  const server = startPlugin(plugin, { managedLifecycle: true });
  try {
    const handshake = await rpc(server.port, 1, "lenso.handshake", {
      protocol_version: 1,
      value_profile: "lenso-json-value-v1",
      max_frame_bytes: 65536,
      endpoints: [descriptor],
    });
    const session = (handshake.result as { session: string }).session;
    expect(
      (await rpc(server.port, 2, "lenso.request", {
        request_id: 20,
        capability_id: descriptor.capability_id,
        operation: "greet",
        session,
        payload: { name: "ada" },
      })).result,
    ).toMatchObject({ kind: "runtime", failure: { kind: "protocol_violation" } });
    expect(
      (await rpc(server.port, 3, "lenso.activate", {
        session,
        configuration: { prefix: "Hello " },
        imports_url: `http://127.0.0.1:${imports.port}`,
        imports_token: importToken,
        imports: [{ requirement_id: "store", ...storeDescriptor }],
      })).result,
    ).toBe(true);
    expect(importRequestIds).toEqual([1]);
    expect(
      (await rpc(server.port, 4, "lenso.request", {
        request_id: 21,
        capability_id: descriptor.capability_id,
        operation: "greet",
        session,
        payload: { name: "ada" },
      })).result,
    ).toEqual({ kind: "success", value: "Hello ADA" });
    expect(importRequestIds).toEqual([1, 2]);
    expect((await rpc(server.port, 5, "lenso.shutdown", { session })).result).toBe(true);
    expect(stopped).toBe(true);
  } finally {
    server.stop();
    imports.stop(true);
  }
});
