type EndpointDescriptor = {
  capability_id: string;
  descriptor_version: string;
  operations: string[];
  stream_operations: string[];
  event_operations: string[];
};

type Handshake = {
  protocol_version: number;
  value_profile: string;
  max_frame_bytes: number;
  endpoints: EndpointDescriptor[];
};

type WireEventPublish = {
  request_id: number;
  capability_id: string;
  operation: string;
  deadline_nanos?: number;
  caller_instance?: string;
  session?: string;
  payload: unknown;
};

type EventBindingDescriptor = {
  capability_id: string;
  caller_instance: string;
  capacity: number;
};

type WireOutcome =
  | { kind: "success"; value: unknown }
  | { kind: "runtime"; failure: Record<string, unknown> };

type EventMode = "accept" | "reject";

const args = Bun.argv.slice(2);
const argument = (name: string, fallback: string): string => {
  const index = args.indexOf(name);
  return index >= 0 ? (args[index + 1] ?? fallback) : fallback;
};

const transport = argument("--lenso-transport", "framed-stdio");
const maxFrameBytes = Number(argument("--lenso-max-frame-bytes", "65536"));
const protocolVersion = 1;
const valueProfile = "lenso-json-value-v1";
const eventEndpoint: EndpointDescriptor = {
  capability_id: "example.notifications@1",
  descriptor_version: "1.0.0",
  operations: ["notify"],
  stream_operations: [],
  event_operations: ["notify"],
};
const expectedEndpoints = JSON.parse(
  argument("--lenso-endpoints-json", JSON.stringify([eventEndpoint])),
) as EndpointDescriptor[];
const expectedEventBindings = JSON.parse(
  argument("--lenso-event-bindings-json", "[]"),
) as EventBindingDescriptor[];
type EventQueueState = {
  queue: WireEventPublish[];
  active: boolean;
  capacity: number;
};

const eventQueues = new Map(
  expectedEventBindings.map((binding) => [
    `${binding.caller_instance}:${binding.capability_id}`,
    { queue: [], active: false, capacity: binding.capacity } satisfies EventQueueState,
  ]),
);
const cancelled = new Set<number>();
let activeHandshake: Handshake | undefined;
let sessionToken: string | undefined;

function eventBindingKey(event: WireEventPublish): string | undefined {
  if (!event.caller_instance) return undefined;
  return `${event.caller_instance}:${event.capability_id}`;
}

function eventQueueFor(event: WireEventPublish): EventQueueState | undefined {
  const key = eventBindingKey(event);
  if (!key) return undefined;
  return eventQueues.get(key);
}

function runtime(kind: string, detail?: string, requestId?: number): WireOutcome {
  const failure = kind === "resource_exhausted"
    ? { kind, operation: detail ?? "notify" }
    : requestId !== undefined && (kind === "cancelled" || kind === "deadline_exceeded")
      ? { kind, request_id: requestId }
    : detail === undefined
      ? { kind }
      : { kind, detail };
  return {
    kind: "runtime",
    failure,
  };
}

function expectedHandshake(handshake: Handshake): boolean {
  return Boolean(
    handshake &&
      Array.isArray(handshake.endpoints) &&
      handshake.protocol_version === protocolVersion &&
      handshake.value_profile === valueProfile &&
      handshake.max_frame_bytes === maxFrameBytes &&
      JSON.stringify(handshake.endpoints) === JSON.stringify(expectedEndpoints),
  );
}

function handshakeAck(handshake: Handshake): Record<string, unknown> {
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

async function drainEvents(state: EventQueueState): Promise<void> {
  if (state.active) return;
  state.active = true;
  try {
    while (state.queue.length > 0) {
      const event = state.queue.shift();
      if (!event) continue;
      const payload = event.payload as { message?: unknown };
      await Bun.sleep(payload.message === "slow" ? 100 : 10);
      if (payload.message === "__crash__") process.exit(17);
    }
  } finally {
    state.active = false;
  }
}

function publishEvent(event: WireEventPublish, mode: EventMode): WireOutcome {
  if (!activeHandshake || event.session !== sessionToken) {
    return runtime("protocol_violation", "event session mismatch");
  }
  const endpoint = expectedEndpoints.find(
    (candidate) => candidate.capability_id === event.capability_id,
  );
  if (!endpoint || !endpoint.event_operations.includes(event.operation)) {
    return runtime("unknown_operation", event.operation);
  }
  if (!event.caller_instance) {
    return runtime("protocol_violation", "event caller instance is required");
  }
  if (event.deadline_nanos === 0) {
    return runtime("deadline_exceeded", undefined, event.request_id);
  }
  if (mode === "reject") {
    return runtime("resource_exhausted", event.operation);
  }
  const state = eventQueueFor(event);
  if (!state) {
    return runtime("protocol_violation", "event caller instance is required");
  }
  const retained = state.queue.length + (state.active ? 1 : 0);
  if (retained >= state.capacity) {
    return runtime("resource_exhausted", event.operation);
  }
  state.queue.push(event);
  void drainEvents(state);
  return { kind: "success", value: null };
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

async function framedProvider(mode: EventMode): Promise<void> {
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

  const send = (message: unknown): void => {
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

  const first = (await readFrame()) as { kind?: string } | undefined;
  if (!first || first.kind !== "handshake") throw new Error("handshake required");
  const handshake = first as unknown as Handshake;
  activeHandshake = expectedHandshake(handshake) ? handshake : undefined;
  send({ kind: "handshake_ack", ...handshakeAck(handshake) });
  if (!activeHandshake) return;

  while (true) {
    const message = (await readFrame()) as
      | (WireEventPublish & { kind: "event_publish" })
      | { kind: "cancel"; request_id: number }
      | { kind: "shutdown" }
      | undefined;
    if (!message || message.kind === "shutdown") return;
    if (message.kind === "cancel") {
      cancelled.add(message.request_id);
      continue;
    }
    if (message.kind !== "event_publish") throw new Error("protocol violation");
    if (cancelled.has(message.request_id)) {
      cancelled.delete(message.request_id);
      continue;
    }
    send({
      kind: "response",
      request_id: message.request_id,
      outcome: publishEvent(message, mode),
    });
  }
}

async function jsonRpcProvider(mode: EventMode): Promise<void> {
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
        activeHandshake = expectedHandshake(handshake) ? handshake : undefined;
        return Response.json({ jsonrpc: "2.0", id, result: handshakeAck(handshake) });
      }
      if (message.method === "lenso.cancel") {
        const cancel = params as { request_id?: number; session?: string };
        if (cancel.session !== sessionToken) {
          return Response.json({ jsonrpc: "2.0", id, result: runtime("protocol_violation") });
        }
        if (cancel.request_id !== undefined) cancelled.add(cancel.request_id);
        return Response.json({ jsonrpc: "2.0", id, result: true });
      }
      if (message.method === "lenso.shutdown") {
        const response = Response.json({ jsonrpc: "2.0", id, result: true });
        server.stop();
        return response;
      }
      if (message.method !== "lenso.event.publish") {
        return Response.json({ jsonrpc: "2.0", id, result: runtime("protocol_violation") });
      }
      return Response.json({
        jsonrpc: "2.0",
        id,
        result: publishEvent(params as WireEventPublish, mode),
      });
    },
  });
  console.log(`LENSO_READY ${server.port}`);
}

export async function runEventProvider(mode: EventMode): Promise<void> {
  if (transport === "json-rpc-http") {
    await jsonRpcProvider(mode);
  } else {
    await framedProvider(mode);
  }
}
