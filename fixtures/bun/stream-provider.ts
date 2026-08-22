type EndpointDescriptor = {
  capability_id: string;
  descriptor_version: string;
  operations: string[];
  stream_operations: string[];
};

type Handshake = {
  protocol_version: number;
  value_profile: string;
  max_frame_bytes: number;
  endpoints: EndpointDescriptor[];
};

type HandshakeAck = Handshake & { accepted: boolean; session?: string };

type WireStreamOpen = {
  request_id: number;
  stream_id: number;
  capability_id: string;
  operation: string;
  deadline_nanos?: number;
  caller_instance?: string;
  session?: string;
  credit: number;
  payload: unknown;
};

type WireStreamCall =
  | {
      kind: "stream_call";
      action: "send";
      request_id: number;
      stream_id: number;
      session: string;
      sequence: number;
      payload: unknown;
    }
  | {
      kind: "stream_call";
      action: "receive" | "close_send";
      request_id: number;
      stream_id: number;
      session: string;
    };

type WireStreamEvent =
  | { kind: "message"; sequence: number; payload: unknown }
  | { kind: "peer_half_closed" }
  | {
      kind: "terminal";
      outcome: { kind: "success" } | { kind: "domain"; value: unknown };
    };

type WireStreamOutcome =
  | { kind: "opened"; stream_id: number; credit: number }
  | { kind: "accepted"; credit: number }
  | { kind: "event"; event: WireStreamEvent }
  | { kind: "domain"; value: unknown }
  | { kind: "runtime"; failure: Record<string, unknown> };

type StreamEvent =
  | { kind: "message"; payload: unknown }
  | { kind: "peer_half_closed" }
  | { kind: "terminal"; outcome: { kind: "success" } | { kind: "domain"; value: unknown } };

type StreamState = {
  nextInboundSequence: number;
  nextOutboundSequence: number;
  credit: number;
  events: StreamEvent[];
  peerHalfClosed: boolean;
  localHalfClosed: boolean;
  terminalQueued: boolean;
  terminalSeen: boolean;
  receiveInFlight: boolean;
  providerClosesFirst: boolean;
};

const args = Bun.argv.slice(2);
const argument = (name: string, fallback: string): string => {
  const index = args.indexOf(name);
  return index >= 0 ? (args[index + 1] ?? fallback) : fallback;
};

const transport = argument("--lenso-transport", "framed-stdio");
const maxFrameBytes = Number(argument("--lenso-max-frame-bytes", "65536"));
const protocolVersion = 1;
const valueProfile = "lenso-json-value-v1";
const fallbackEndpoint: EndpointDescriptor = {
  capability_id: "example.chat@1",
  descriptor_version: "1.0.0",
  operations: ["chat"],
  stream_operations: ["chat"],
};
const expectedEndpoints = JSON.parse(
  argument("--lenso-endpoints-json", JSON.stringify([fallbackEndpoint])),
) as EndpointDescriptor[];
const streams = new Map<number, StreamState>();
const MAX_BUFFERED_STREAM_EVENTS = 16;
const retiredStreamIds = new Set<number>();
const maxRetiredStreamIds = 1024;
let activeHandshake: Handshake | undefined;
let sessionToken: string | undefined;

function runtime(kind: string, detail?: string, requestId?: number): WireStreamOutcome {
  if (kind === "resource_exhausted") {
    return { kind: "runtime", failure: { kind, operation: detail ?? "chat" } };
  }
  const failure = detail === undefined ? { kind } : { kind, detail };
  if (requestId !== undefined && (kind === "cancelled" || kind === "deadline_exceeded")) {
    return { kind: "runtime", failure: { ...failure, request_id: requestId } };
  }
  return { kind: "runtime", failure };
}

function markRetiredStream(streamId: number): void {
  if (!retiredStreamIds.has(streamId) && retiredStreamIds.size >= maxRetiredStreamIds) {
    const oldest = retiredStreamIds.values().next().value;
    if (oldest !== undefined) retiredStreamIds.delete(oldest);
  }
  retiredStreamIds.add(streamId);
}

function protocolViolation(): WireStreamOutcome {
  return runtime("protocol_violation");
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

function resetSession(): void {
  streams.clear();
  retiredStreamIds.clear();
}

function handshakeAck(handshake: Handshake): HandshakeAck {
  const accepted = expectedHandshake(handshake);
  resetSession();
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

function hasSession(session: string | undefined): boolean {
  return session !== undefined && session === sessionToken && activeHandshake !== undefined;
}

function streamEndpoint(): EndpointDescriptor | undefined {
  return expectedEndpoints.find((endpoint) => endpoint.capability_id === "example.chat@1");
}

function openStream(open: WireStreamOpen): WireStreamOutcome {
  if (!hasSession(open.session)) return protocolViolation();
  const endpoint = streamEndpoint();
  if (
    !endpoint ||
    open.capability_id !== endpoint.capability_id ||
    !endpoint.stream_operations.includes(open.operation) ||
    open.credit <= 0
  ) {
    return runtime("unknown_operation", open.operation);
  }
  if (open.deadline_nanos === 0) {
    return runtime("deadline_exceeded", undefined, open.request_id);
  }
  if (streams.has(open.stream_id) || retiredStreamIds.has(open.stream_id)) {
    return protocolViolation();
  }
  const payload = open.payload as { room?: unknown };
  if (typeof payload?.room !== "string") return protocolViolation();
  if (payload.room === "__crash__") process.exit(17);
  if (payload.room === "closed") return { kind: "domain", value: "room_closed" };
  const credit = Math.min(open.credit, 16);
  streams.set(open.stream_id, {
    nextInboundSequence: 0,
    nextOutboundSequence: 0,
    credit,
    events: payload.room === "provider-closes-first" ? [{ kind: "peer_half_closed" }] : [],
    peerHalfClosed: false,
    localHalfClosed: false,
    terminalQueued: false,
    terminalSeen: false,
    receiveInFlight: false,
    providerClosesFirst: payload.room === "provider-closes-first",
  });
  return { kind: "opened", stream_id: open.stream_id, credit };
}

function streamCall(call: WireStreamCall): WireStreamOutcome {
  if (!hasSession(call.session)) return protocolViolation();
  const stream = streams.get(call.stream_id);
  if (!stream) return protocolViolation();
  if (call.action === "send") {
    if (
      stream.peerHalfClosed ||
      stream.terminalQueued ||
      stream.terminalSeen ||
      call.sequence !== stream.nextInboundSequence
    ) {
      return protocolViolation();
    }
    if (stream.events.length >= MAX_BUFFERED_STREAM_EVENTS) {
      return runtime("resource_exhausted", call.action);
    }
    if (stream.credit <= 0) return runtime("resource_exhausted", call.action);
    stream.credit -= 1;
    stream.nextInboundSequence += 1;
    const payload = call.payload as { text?: unknown };
    if (typeof payload?.text !== "string") return protocolViolation();
    if (payload.text === "__crash__") process.exit(17);
    if (payload.text === "__domain__") {
      stream.events.push({ kind: "terminal", outcome: { kind: "domain", value: "room_closed" } });
      stream.terminalQueued = true;
    } else if (!stream.providerClosesFirst) {
      stream.events.push({
        kind: "message",
        payload: { text: `Bun echo: ${payload.text}` },
      });
    }
    stream.credit += 1;
    return { kind: "accepted", credit: stream.credit };
  }
  if (call.action === "close_send") {
    if (stream.peerHalfClosed || stream.terminalQueued || stream.terminalSeen) {
      return protocolViolation();
    }
    stream.peerHalfClosed = true;
    if (!stream.providerClosesFirst) stream.events.push({ kind: "peer_half_closed" });
    stream.events.push({ kind: "terminal", outcome: { kind: "success" } });
    stream.terminalQueued = true;
    return { kind: "accepted", credit: stream.credit };
  }
  if (stream.receiveInFlight) return runtime("resource_exhausted", "chat.receive");
  stream.receiveInFlight = true;
  try {
    const next = stream.events.shift();
    if (!next) return runtime("resource_exhausted", "chat.receive");
    if (next.kind === "message") {
      if (stream.localHalfClosed || stream.terminalQueued) return protocolViolation();
      const event: WireStreamEvent = {
        kind: "message",
        sequence: stream.nextOutboundSequence,
        payload: next.payload,
      };
      stream.nextOutboundSequence += 1;
      return { kind: "event", event };
    }
    if (next.kind === "peer_half_closed") {
      if (stream.localHalfClosed) return protocolViolation();
      stream.localHalfClosed = true;
      return { kind: "event", event: { kind: "peer_half_closed" } };
    }
    stream.terminalSeen = true;
    streams.delete(call.stream_id);
    markRetiredStream(call.stream_id);
    return { kind: "event", event: { kind: "terminal", outcome: next.outcome } };
  } finally {
    stream.receiveInFlight = false;
  }
}

function cancelStream(streamId: number, session: string): void {
  if (!hasSession(session)) return;
  streams.delete(streamId);
  markRetiredStream(streamId);
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
      | (WireStreamOpen & { kind: "stream_open" })
      | (WireStreamCall & { kind: "stream_call" })
      | { kind: "stream_cancel"; stream_id: number; session: string }
      | { kind: "shutdown" }
      | undefined;
    if (!message) return;
    if (message.kind === "shutdown") return;
    if (message.kind === "stream_cancel") {
      cancelStream(message.stream_id, message.session);
      continue;
    }
    if (message.kind === "stream_open") {
      send({
        kind: "stream_response",
        request_id: message.request_id,
        response: openStream(message),
      });
      continue;
    }
    if (message.kind === "stream_call") {
      send({
        kind: "stream_response",
        request_id: message.request_id,
        response: streamCall(message),
      });
      continue;
    }
    throw new Error("protocol violation");
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
      if (message.method === "lenso.stream.cancel") {
        const cancel = params as { stream_id?: number; session?: string };
        if (typeof cancel.stream_id !== "number" || typeof cancel.session !== "string") {
          return Response.json({ jsonrpc: "2.0", id, result: protocolViolation() });
        }
        cancelStream(cancel.stream_id, cancel.session);
        return Response.json({ jsonrpc: "2.0", id, result: true });
      }
      if (message.method === "lenso.shutdown") {
        const response = Response.json({ jsonrpc: "2.0", id, result: true });
        server.stop();
        return response;
      }
      if (!activeHandshake) {
        return Response.json({ jsonrpc: "2.0", id, result: protocolViolation() });
      }
      if (message.method === "lenso.stream.open") {
        return Response.json({ jsonrpc: "2.0", id, result: openStream(params as WireStreamOpen) });
      }
      if (
        message.method === "lenso.stream.send" ||
        message.method === "lenso.stream.receive" ||
        message.method === "lenso.stream.close_send"
      ) {
        return Response.json({ jsonrpc: "2.0", id, result: streamCall(params as WireStreamCall) });
      }
      return Response.json({ jsonrpc: "2.0", id, result: protocolViolation() });
    },
  });
  console.log(`LENSO_READY ${server.port}`);
}

if (transport === "json-rpc-http") {
  await jsonRpcProvider();
} else {
  await framedProvider();
}
