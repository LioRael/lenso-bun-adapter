import { timingSafeEqual } from "node:crypto";
import { expect, test } from "bun:test";
import {
  authoringCallbackProofMessage,
  authoringChildProofMessage,
  authoringHandshakeProofPayload,
  authoringHostProofMessage,
  decodeBase64Url32,
  encodeBase64Url,
  type InitializeParams,
} from "@lenso/process-protocol";

import { BUN_AUTHORING_CALLBACK_PROOF_HEADER } from "../src/v2.ts";

const fixture = new URL("./fixtures/v2-child.ts", import.meta.url).pathname;
const blockingFixture = new URL("./fixtures/v2-blocking-child.ts", import.meta.url).pathname;
const STORE_ID = "example.document-store@1";
const SYNC_ID = "example.sync@1";
const VERSION = "1.0.0";
const STORE_DIGEST =
  "sha256:1100000000000000000000000000000000000000000000000000000000000011";
const SYNC_DIGEST =
  "sha256:2200000000000000000000000000000000000000000000000000000000000022";

test("constructs once, calls a named Host route, and stops over V2 HTTP", async () => {
  const session = randomValue();
  const secret = randomValue();
  const settlements: unknown[] = [];
  const callback = callbackServer(secret, session, settlements);
  const child = await startChild(fixture, secret, callback.origin);
  const initialize = initialization(session);
  await initializeChild(child.origin, secret, callback.origin, initialize);

  expect(await rpc(child.origin, "lenso.construct", {
    session,
    lifecycle_scope_id: "construct-1",
    remaining_budget_nanos: "10000000000",
  })).toMatchObject({ outcome: { kind: "constructed" } });

  const result = await rpc(child.origin, "lenso.invoke", invocation(session, "40"));
  expect(result).toEqual({
    session,
    correlation_id: "40",
    outcome: { kind: "success", value: { text: "complete object" } },
  });
  expect(settlements).toEqual([
    expect.objectContaining({ correlation_id: "40", state: "completed" }),
  ]);

  expect(await rpc(child.origin, "lenso.stop", {
    session,
    cleanup_scope_id: "cleanup-1",
    remaining_budget_nanos: "1000000000",
  })).toMatchObject({ cleanup_scope_id: "cleanup-1", hook: "not_declared" });
  expect(await child.process.exited).toBe(0);
  callback.server.stop();
});

test("cancelled noncooperative work retains capacity until physical termination", async () => {
  const session = randomValue();
  const secret = randomValue();
  const settlements: unknown[] = [];
  const callback = callbackServer(secret, session, settlements);
  const child = await startChild(blockingFixture, secret, callback.origin);
  const endpoint = {
    endpoint_id: "endpoint-0",
    capability_id: "example.blocking@1",
    descriptor_version: VERSION,
    descriptor_digest:
      "sha256:3300000000000000000000000000000000000000000000000000000000000033",
  };
  const initialize: InitializeParams = {
    api_version: 2,
    identity: identity(session, "blocked"),
    config: {},
    required_declarations: [],
    routes: [],
    provided_endpoints: [endpoint],
    limits: limits(1),
  };
  await initializeChild(child.origin, secret, callback.origin, initialize);
  await rpc(child.origin, "lenso.construct", {
    session,
    lifecycle_scope_id: "construct-1",
    remaining_budget_nanos: "10000000000",
  });

  const first = rpc(child.origin, "lenso.invoke", {
    ...invocation(session, "50"),
    ...endpoint,
    operation: "block",
    payload: {},
  });
  await Bun.sleep(20);
  expect(await rpc(child.origin, "lenso.cancel", {
    session,
    scope_id: "invoke-50",
    correlation_id: "50",
    reason: "test cancellation",
  })).toMatchObject({ correlation_id: "50", accepted: true });

  expect(await rpc(child.origin, "lenso.invoke", {
    ...invocation(session, "51"),
    ...endpoint,
    operation: "block",
    payload: {},
  })).toMatchObject({
    correlation_id: "51",
    outcome: { kind: "runtime", failure: { kind: "resource_exhausted" } },
  });
  expect(settlements).toContainEqual(
    expect.objectContaining({ correlation_id: "51", state: "completed" }),
  );

  child.process.kill();
  await child.process.exited;
  callback.server.stop();
  void first.catch(() => {});
});

function initialization(session: string): InitializeParams {
  return {
    api_version: 2,
    identity: identity(session, "sync"),
    config: {},
    required_declarations: [{
      requirement_id: "source",
      capability_id: STORE_ID,
      descriptor_version: VERSION,
      descriptor_digest: STORE_DIGEST,
      cardinality: "one",
    }],
    routes: [{
      route_id: "route-0",
      requirement_id: "source",
      capability_id: STORE_ID,
      descriptor_version: VERSION,
      descriptor_digest: STORE_DIGEST,
      provider_instance: "store",
      provider_order: 0,
    }],
    provided_endpoints: [{
      endpoint_id: "endpoint-0",
      capability_id: SYNC_ID,
      descriptor_version: VERSION,
      descriptor_digest: SYNC_DIGEST,
    }],
    limits: limits(4),
  };
}

function identity(session: string, plugin: string) {
  return {
    session,
    plugin_instance: plugin,
    plugin_generation: "1",
    artifact_digest: `sha256:${"a".repeat(64)}`,
    contract_digest: `sha256:${"b".repeat(64)}`,
    runtime_profile: "lenso.bun-authoring@2",
    value_profile: "lenso-json-value-v1" as const,
  };
}

function limits(active: number) {
  return {
    max_frame_bytes: 1_048_576,
    max_active_invocations: active,
    max_active_outbound_calls: active,
    max_queued_calls: active,
    max_unfinished_executions: active,
    max_retired_ids: 16,
  };
}

function invocation(session: string, correlation_id: string) {
  return {
    session,
    correlation_id,
    endpoint_id: "endpoint-0",
    capability_id: SYNC_ID,
    descriptor_version: VERSION,
    descriptor_digest: SYNC_DIGEST,
    operation: "sync",
    scope: {
      scope_id: `invoke-${correlation_id}`,
      parent_scope_id: null,
      remaining_budget_nanos: "5000000000",
      permissions: [],
      extensions: [],
    },
    payload: { document: "guide" },
  };
}

async function initializeChild(
  origin: string,
  secret: string,
  callbackOrigin: string,
  initialize: InitializeParams,
): Promise<void> {
  const hostNonce = randomValue();
  const payload = authoringHandshakeProofPayload({
    initialize,
    callback_origin: callbackOrigin,
    host_nonce: hostNonce,
  });
  const digest = new Uint8Array(new Bun.CryptoHasher("sha256").update(payload).digest());
  const result = await rpc(origin, "lenso.initialize", {
    initialize,
    callback_origin: callbackOrigin,
    host_nonce: hostNonce,
    host_proof: await proof(secret, authoringHostProofMessage(digest)),
  }) as { child_nonce: string; child_proof: string; initialized: InitializeParams };
  expect(result.initialized).toEqual(initialize);
  expect(decodeBase64Url32(result.child_proof)).toEqual(
    decodeBase64Url32(await proof(
      secret,
      authoringChildProofMessage(digest, result.child_nonce),
    )),
  );
}

function callbackServer(secret: string, session: string, settlements: unknown[]) {
  const server = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(request) {
      const rpcRequest = await request.json() as {
        id: string;
        method: "lenso.call" | "lenso.settled";
        params: any;
      };
      const received = request.headers.get(BUN_AUTHORING_CALLBACK_PROOF_HEADER) ?? "";
      const expected = await proof(
        secret,
        authoringCallbackProofMessage(session, rpcRequest.method, rpcRequest.params),
      );
      if (!timingSafeEqual(decodeBase64Url32(received), decodeBase64Url32(expected))) {
        return new Response(null, { status: 401 });
      }
      if (rpcRequest.method === "lenso.settled") {
        settlements.push(rpcRequest.params);
        return Response.json({ jsonrpc: "2.0", id: rpcRequest.id, result: {} });
      }
      expect(rpcRequest.params).toMatchObject({
        requirement_id: "source",
        route_id: "route-0",
        operation: "read",
      });
      return Response.json({
        jsonrpc: "2.0",
        id: rpcRequest.id,
        result: {
          session,
          correlation_id: rpcRequest.params.correlation_id,
          outcome: { kind: "success", value: { text: "complete object" } },
        },
      });
    },
  });
  return { server, origin: `http://127.0.0.1:${server.port}/` };
}

async function startChild(script: string, secret: string, callbackOrigin: string) {
  const childProcess = Bun.spawn({
    cmd: [process.execPath, script],
    stdin: "pipe",
    stdout: "pipe",
    stderr: "pipe",
    env: {},
  });
  childProcess.stdin.write(`${JSON.stringify({
    callback_origin: callbackOrigin,
    bootstrap_secret: secret,
  })}\n`);
  await childProcess.stdin.flush();
  childProcess.stdin.end();
  const ready = await new LineReader(childProcess.stdout).next();
  expect(ready.protocol).toBe("lenso.bun-authoring@2");
  return { process: childProcess, origin: `http://127.0.0.1:${ready.port}/` };
}

async function rpc(origin: string, method: string, params: unknown): Promise<any> {
  const response = await fetch(origin, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ jsonrpc: "2.0", id: crypto.randomUUID(), method, params }),
  });
  const value = await response.json() as { result?: unknown; error?: { message: string } };
  if (value.error !== undefined) throw new Error(value.error.message);
  return value.result;
}

async function proof(secret: string, message: Uint8Array): Promise<string> {
  const key = await crypto.subtle.importKey(
    "raw",
    Uint8Array.from(decodeBase64Url32(secret)),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const signature = await crypto.subtle.sign("HMAC", key, Uint8Array.from(message));
  return encodeBase64Url(new Uint8Array(signature));
}

function randomValue(): string {
  return encodeBase64Url(crypto.getRandomValues(new Uint8Array(32)));
}

class LineReader {
  readonly #reader: ReadableStreamDefaultReader<Uint8Array>;
  readonly #decoder = new TextDecoder();
  #buffer = "";

  constructor(stream: ReadableStream<Uint8Array>) {
    this.#reader = stream.getReader();
  }

  async next(): Promise<any> {
    while (true) {
      const newline = this.#buffer.indexOf("\n");
      if (newline >= 0) return JSON.parse(this.#buffer.slice(0, newline));
      const chunk = await this.#reader.read();
      if (chunk.done) throw new Error("child closed stdout before readiness");
      this.#buffer += this.#decoder.decode(chunk.value, { stream: true });
    }
  }
}
