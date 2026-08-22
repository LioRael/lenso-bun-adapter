import {
  decodeGreetRequest,
  encodeGreetError,
  encodeGreetResponse,
} from "./vendor/greeting.ts";
import {
  decodeGreetRequest as decodeSecureGreetRequest,
  encodeGreetError as encodeSecureGreetError,
  encodeGreetResponse as encodeSecureGreetResponse,
} from "./vendor/secure-greeting.ts";
import {
  bindActor,
  type WireExtension,
} from "./vendor/actor.ts";
import { extractTraceContext } from "./vendor/trace-context.ts";

type EndpointDescriptor = {
  capability_id: string;
  descriptor_version: string;
  operations: string[];
};

type Handshake = {
  protocol_version: number;
  value_profile: string;
  max_frame_bytes: number;
  endpoints: EndpointDescriptor[];
};

type HandshakeAck = Handshake & { accepted: boolean; session?: string };

type WireRequest = {
  request_id: number;
  capability_id: string;
  operation: string;
  deadline_nanos?: number;
  caller_instance?: string;
  session?: string;
  extensions?: WireExtension[];
  payload: unknown;
};

type WireOutcome =
  | { kind: "success"; value: unknown }
  | { kind: "domain"; value: unknown }
  | { kind: "runtime"; failure: Record<string, unknown> };

const args = Bun.argv.slice(2);
const argument = (name: string, fallback: string): string => {
  const index = args.indexOf(name);
  return index >= 0 ? (args[index + 1] ?? fallback) : fallback;
};

const transport = argument("--lenso-transport", "framed-stdio");
const maxFrameBytes = Number(argument("--lenso-max-frame-bytes", "65536"));
const protocolVersion = 1;
const valueProfile = "lenso-json-value-v1";
const greetingEndpoint: EndpointDescriptor = {
  capability_id: "example.greeting@1",
  descriptor_version: "1.0.0",
  operations: ["greet"],
};
const secureGreetingEndpoint: EndpointDescriptor = {
  capability_id: "example.secure-greeting@1",
  descriptor_version: "1.0.0",
  operations: ["greet"],
};
const traceEndpoint: EndpointDescriptor = {
  capability_id: "example.trace@1",
  descriptor_version: "1.0.0",
  operations: ["invoke"],
};
const expectedEndpoints = JSON.parse(
  argument("--lenso-endpoints-json", JSON.stringify([greetingEndpoint])),
) as EndpointDescriptor[];
const cancelled = new Map<number, number>();
const maxCancelledIds = 1024;
const activeRequestIds = new Set<number>();
const retiredRequestIds = new Set<number>();
const maxRetiredRequestIds = 1024;
const maxActiveRequests = 32;
let activeHandshake: Handshake | undefined;
let sessionToken: string | undefined;

function markCancelled(requestId: number): void {
  if (!cancelled.has(requestId) && cancelled.size >= maxCancelledIds) {
    const oldest = cancelled.keys().next().value;
    if (oldest !== undefined) cancelled.delete(oldest);
  }
  cancelled.set(requestId, Date.now());
}

function markRetired(requestId: number): void {
  if (!retiredRequestIds.has(requestId) && retiredRequestIds.size >= maxRetiredRequestIds) {
    const oldest = retiredRequestIds.values().next().value;
    if (oldest !== undefined) retiredRequestIds.delete(oldest);
  }
  retiredRequestIds.add(requestId);
}

function runtime(kind: string, detail?: string, requestId?: number): WireOutcome {
  const failure = detail === undefined ? { kind } : { kind, detail };
  if (requestId !== undefined && (kind === "cancelled" || kind === "deadline_exceeded")) {
    return { kind: "runtime", failure: { ...failure, request_id: requestId } };
  }
  return {
    kind: "runtime",
    failure,
  };
}

function expectedHandshake(handshake: Handshake): boolean {
  if (
    !handshake ||
    !Array.isArray(handshake.endpoints) ||
    typeof handshake.protocol_version !== "number" ||
    typeof handshake.value_profile !== "string" ||
    typeof handshake.max_frame_bytes !== "number"
  ) {
    return false;
  }
  if (
    handshake.protocol_version !== protocolVersion ||
    handshake.value_profile !== valueProfile ||
    handshake.max_frame_bytes !== maxFrameBytes
  ) {
    return false;
  }
  return JSON.stringify(handshake.endpoints) === JSON.stringify(expectedEndpoints);
}

function handshakeAck(handshake: Handshake): HandshakeAck {
  const accepted = expectedHandshake(handshake);
  const session = accepted
    ? `lenso-bun-session-${Date.now()}-${Math.random().toString(16).slice(2)}`
    : undefined;
  sessionToken = session;
  return {
    accepted,
    protocol_version: protocolVersion,
    value_profile: valueProfile,
    max_frame_bytes: maxFrameBytes,
    endpoints: Array.isArray(handshake.endpoints) ? handshake.endpoints : [],
    session,
  };
}

async function handleRequest(request: WireRequest): Promise<WireOutcome> {
  if (!activeHandshake) return runtime("protocol_violation", "request before handshake");
  if (transport === "json-rpc-http" && request.session !== sessionToken) {
    return runtime("protocol_violation", "request session mismatch");
  }
  if (request.deadline_nanos !== undefined && request.deadline_nanos === 0) {
    return runtime("deadline_exceeded", undefined, request.request_id);
  }
  if (cancelled.has(request.request_id)) {
    return runtime("cancelled", undefined, request.request_id);
  }
  if (request.capability_id === traceEndpoint.capability_id && request.operation === "invoke") {
    try {
      const trace = await extractTraceContext(
        request.extensions,
        traceEndpoint.capability_id,
        traceEndpoint.operations[0],
        "lenso.otel",
        "trace-key",
      );
      if (!trace) return runtime("protocol_violation", "trace context is missing");
      return {
        kind: "success",
        value: trace,
      };
    } catch (error) {
      return runtime("protocol_violation", String(error));
    }
  }
  if (request.operation !== "greet") {
    return {
      kind: "runtime",
      failure: { kind: "unknown_operation", operation: request.operation },
    };
  }

  if (request.capability_id === secureGreetingEndpoint.capability_id) {
    let typedRequest: { name: string };
    try {
      typedRequest = decodeSecureGreetRequest(JSON.stringify(request.payload));
    } catch (error) {
      return runtime("protocol_violation", String(error));
    }
    let actor: { subject: string };
    try {
      actor = await bindActor(
        request.extensions,
        secureGreetingEndpoint.capability_id,
        "greet",
        "auth.users",
        "shared-auth-key",
        "user",
      );
    } catch {
      return {
        kind: "domain",
        value: JSON.parse(encodeSecureGreetError("actor_required")),
      };
    }
    if (actor.subject === "forbidden") {
      return {
        kind: "domain",
        value: JSON.parse(encodeSecureGreetError("not_allowed")),
      };
    }
    if (typedRequest.name.length === 0) {
      return {
        kind: "domain",
        value: JSON.parse(encodeSecureGreetError("empty_name")),
      };
    }
    return {
      kind: "success",
      value: JSON.parse(
        encodeSecureGreetResponse({ message: `Hello from Bun, ${actor.subject}!` }),
      ),
    };
  }
  if (request.capability_id !== greetingEndpoint.capability_id) {
    return {
      kind: "runtime",
      failure: { kind: "unknown_operation", operation: request.operation },
    };
  }

  let typedRequest: { name: string };
  try {
    typedRequest = decodeGreetRequest(JSON.stringify(request.payload));
  } catch (error) {
    return runtime("protocol_violation", String(error));
  }
  const requestedFailure: Record<string, Record<string, unknown>> = {
    __runtime_unavailable__: { kind: "unavailable" },
    __runtime_ambiguous_binding__: { kind: "ambiguous_binding", providers: 2 },
    __runtime_missing_module_factory__: {
      kind: "missing_module_factory",
      instance: "provider",
      package_id: "fixture.provider",
    },
    __runtime_unavailable_execution_class__: {
      kind: "unavailable_execution_class",
      instance_key: "provider",
      execution_class: "fixture.missing@1",
    },
    __runtime_invalid_resolved_plan__: { kind: "invalid_resolved_plan", detail: "invalid" },
    __runtime_admission_closed__: { kind: "admission_closed" },
    __runtime_internal__: { kind: "internal", detail: "internal" },
    __runtime_module_restart_exhausted__: {
      kind: "module_restart_exhausted",
      instance: "provider",
      attempts: 3,
    },
  };
  const requestedFailureValue = requestedFailure[typedRequest.name];
  if (requestedFailureValue) {
    return { kind: "runtime", failure: requestedFailureValue };
  }
  if (typedRequest.name === "__crash__") {
    process.exit(17);
  }
  if (typedRequest.name === "__delay__") {
    for (let index = 0; index < 50; index += 1) {
      await Bun.sleep(5);
      if (cancelled.has(request.request_id)) {
        return runtime("cancelled", undefined, request.request_id);
      }
    }
  }
  if (cancelled.has(request.request_id)) {
    return runtime("cancelled", undefined, request.request_id);
  }
  if (typedRequest.name.length === 0) {
    return { kind: "domain", value: JSON.parse(encodeGreetError("empty_name")) };
  }
  if (typedRequest.name === "__future_domain__") {
    return {
      kind: "domain",
      value: { code: "future_variant", payload: { retry_after_ms: 2500 } },
    };
  }
  return {
    kind: "success",
    value: JSON.parse(
      encodeGreetResponse({ message: `Hello from Bun, ${typedRequest.name}!` }),
    ),
  };
}

function appendBytes(left: Uint8Array, right: Uint8Array): Uint8Array {
  if (left.length + right.length > maxFrameBytes + 4) {
    throw new Error("frame exceeds configured maximum");
  }
  const result = new Uint8Array(left.length + right.length);
  result.set(left);
  result.set(right, left.length);
  return result;
}

async function framedProvider(): Promise<void> {
  const reader = Bun.stdin.stream().getReader();
  const writer = Bun.stdout.writer();
  let buffered = new Uint8Array();
  let writeQueue = Promise.resolve();

  const readFrame = async (): Promise<unknown | undefined> => {
    while (buffered.length < 4) {
      const next = await reader.read();
      if (next.done) return undefined;
      buffered = appendBytes(buffered, next.value);
    }
    const length = new DataView(buffered.buffer, buffered.byteOffset, 4).getUint32(0);
    if (length > maxFrameBytes) throw new Error("frame exceeds configured maximum");
    while (buffered.length < length + 4) {
      const next = await reader.read();
      if (next.done) throw new Error("truncated frame");
      buffered = appendBytes(buffered, next.value);
    }
    const payload = buffered.slice(4, length + 4);
    buffered = buffered.slice(length + 4);
    return JSON.parse(new TextDecoder().decode(payload));
  };

  const send = (message: unknown) => {
    const payload = new TextEncoder().encode(JSON.stringify(message));
    if (payload.length > maxFrameBytes) throw new Error("frame exceeds configured maximum");
    const frame = new Uint8Array(payload.length + 4);
    new DataView(frame.buffer).setUint32(0, payload.length);
    frame.set(payload, 4);
    writeQueue = writeQueue.then(async () => {
      await writer.write(frame);
      await writer.flush();
    });
  };

  const first = (await readFrame()) as { kind?: string; protocol_version?: number } | undefined;
  if (!first || first.kind !== "handshake") throw new Error("handshake required");
  const handshake = first as unknown as Handshake;
  activeHandshake = expectedHandshake(handshake) ? handshake : undefined;
  send({ kind: "handshake_ack", ...handshakeAck(handshake) });
  if (!activeHandshake) return;

  while (true) {
    const message = (await readFrame()) as
      | { kind: string; request?: WireRequest; request_id?: number }
      | undefined;
    if (!message) return;
    if (message.kind === "cancel" && message.request_id !== undefined) {
      markCancelled(message.request_id);
      continue;
    }
    if (message.kind === "shutdown") return;
    if (message.kind !== "request" || message.request_id === undefined) {
      throw new Error("protocol violation");
    }
    // The Rust FramedMessage uses the serde externally-tagged shape with the
    // request fields flattened beside `kind`.
    const request = message as unknown as WireRequest;
    if (activeRequestIds.has(request.request_id) || retiredRequestIds.has(request.request_id)) {
      send({
        kind: "response",
        request_id: request.request_id,
        outcome: runtime("protocol_violation"),
      });
      continue;
    }
    if (activeRequestIds.size >= maxActiveRequests) {
      send({
        kind: "response",
        request_id: request.request_id,
        outcome: {
          kind: "runtime",
          failure: { kind: "resource_exhausted", operation: request.operation },
        },
      });
      continue;
    }
    activeRequestIds.add(request.request_id);
    void handleRequest(request).then((outcome) => {
      send({ kind: "response", request_id: request.request_id, outcome });
      activeRequestIds.delete(request.request_id);
      cancelled.delete(request.request_id);
      markRetired(request.request_id);
    });
  }
}

async function jsonRpcProvider(): Promise<void> {
  const readBoundedBody = async (request: Request): Promise<string> => {
    if (!request.body) return "";
    const reader = request.body.getReader();
    const decoder = new TextDecoder();
    let total = 0;
    let body = "";
    while (true) {
      const next = await reader.read();
      if (next.done) break;
      total += next.value.byteLength;
      if (total > maxFrameBytes) throw new Error("request too large");
      body += decoder.decode(next.value, { stream: true });
    }
    return body + decoder.decode();
  };
  const server = Bun.serve({
    hostname: "127.0.0.1",
    port: Number(argument("--lenso-port", "0")),
    async fetch(request) {
      if (request.method !== "POST") return new Response("method not allowed", { status: 405 });
      let body: string;
      try {
        body = await readBoundedBody(request);
      } catch {
        return new Response("request too large", { status: 413 });
      }
      let message: { jsonrpc?: string; id?: number; method?: string; params?: unknown };
      try {
        message = JSON.parse(body);
      } catch {
        return Response.json({ jsonrpc: "2.0", id: null, error: { code: -32700, message: "Parse error" } });
      }
      const id = message.id ?? null;
      if (message.jsonrpc !== "2.0" || typeof message.method !== "string") {
        return Response.json({ jsonrpc: "2.0", id, error: { code: -32600, message: "Invalid Request" } });
      }
      const params = Array.isArray(message.params) && message.params.length === 1
        ? message.params[0]
        : message.params;
      if (message.method === "lenso.handshake") {
        const handshake = params as Handshake;
        const accepted = expectedHandshake(handshake);
        activeHandshake = accepted ? handshake : undefined;
        return Response.json({ jsonrpc: "2.0", id, result: handshakeAck(handshake) });
      }
      if (message.method === "lenso.cancel") {
        if (!activeHandshake) {
          return Response.json({ jsonrpc: "2.0", id, result: runtime("protocol_violation") });
        }
        const cancel = params as { request_id?: number; session?: string };
        if (cancel.session !== sessionToken) {
          return Response.json({
            jsonrpc: "2.0",
            id,
            result: runtime("protocol_violation"),
          });
        }
        if (cancel.request_id !== undefined) markCancelled(cancel.request_id);
        return Response.json({ jsonrpc: "2.0", id, result: true });
      }
      if (message.method === "lenso.shutdown") {
        if (!activeHandshake) {
          return Response.json({ jsonrpc: "2.0", id, result: runtime("protocol_violation") });
        }
        if ((params as { session?: string }).session !== sessionToken) {
          return Response.json({
            jsonrpc: "2.0",
            id,
            result: runtime("protocol_violation"),
          });
        }
        const response = Response.json({ jsonrpc: "2.0", id, result: true });
        server.stop();
        return response;
      }
      if (message.method !== "lenso.request" || !activeHandshake) {
        return Response.json({ jsonrpc: "2.0", id, result: runtime("protocol_violation") });
      }
      const wireRequest = params as WireRequest;
      if (
        activeRequestIds.has(wireRequest.request_id) ||
        retiredRequestIds.has(wireRequest.request_id)
      ) {
        return Response.json({
          jsonrpc: "2.0",
          id,
          result: runtime("protocol_violation"),
        });
      }
      if (activeRequestIds.size >= maxActiveRequests) {
        return Response.json({
          jsonrpc: "2.0",
          id,
          result: {
            kind: "runtime",
            failure: { kind: "resource_exhausted", operation: wireRequest.operation },
          },
        });
      }
      activeRequestIds.add(wireRequest.request_id);
      const outcome = await handleRequest(wireRequest);
      activeRequestIds.delete(wireRequest.request_id);
      cancelled.delete(wireRequest.request_id);
      markRetired(wireRequest.request_id);
      return Response.json({ jsonrpc: "2.0", id, result: outcome });
    },
  });
  console.log(`LENSO_READY ${server.port}`);
}

if (transport === "json-rpc-http") {
  await jsonRpcProvider();
} else {
  await framedProvider();
}
