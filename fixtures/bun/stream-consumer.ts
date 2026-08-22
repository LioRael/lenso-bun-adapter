type EndpointDescriptor = {
  capability_id: string;
  descriptor_version: string;
  operations: string[];
  stream_operations: string[];
};

type WireStreamOutcome =
  | { kind: "opened"; stream_id: number; credit: number }
  | { kind: "accepted"; credit: number }
  | {
      kind: "event";
      event:
        | { kind: "message"; sequence: number; payload: unknown }
        | { kind: "peer_half_closed" }
        | {
            kind: "terminal";
            outcome: { kind: "success" } | { kind: "domain"; value: unknown };
          };
    }
  | { kind: "domain"; value: unknown }
  | { kind: "runtime"; failure: Record<string, unknown> };

const args = Bun.argv.slice(2);
const argument = (name: string, fallback: string): string => {
  const index = args.indexOf(name);
  return index >= 0 ? (args[index + 1] ?? fallback) : fallback;
};

const url = argument("--lenso-url", "");
const room = argument("--room", "room-1");
const text = argument("--text", "hello from Bun");
if (!url) throw new Error("--lenso-url is required");

let nextId = 1;
async function rpc(method: string, params: unknown): Promise<any> {
  const id = nextId++;
  const response = await fetch(url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ jsonrpc: "2.0", id, method, params }),
  });
  if (!response.ok) {
    throw new Error(`provider returned HTTP ${response.status}: ${await response.text()}`);
  }
  const value = await response.json();
  if (value.id !== id) throw new Error("provider returned the wrong JSON-RPC id");
  if (value.error) throw new Error(JSON.stringify(value.error));
  return value.result;
}

const endpoint: EndpointDescriptor = {
  capability_id: "example.chat@1",
  descriptor_version: "1.0.0",
  operations: ["chat"],
  stream_operations: ["chat"],
};
const handshake = await rpc("lenso.handshake", {
  protocol_version: 1,
  value_profile: "lenso-json-value-v1",
  max_frame_bytes: 65536,
  endpoints: [endpoint],
});
if (!handshake.accepted || typeof handshake.session !== "string") {
  throw new Error("provider rejected the exact handshake");
}
const session = handshake.session;
const streamId = nextId++;
const call = (requestId: number, action: string, payload: Record<string, unknown> = {}) => ({
  request_id: requestId,
  stream_id: streamId,
  session,
  action,
  ...payload,
});

const opened = (await rpc("lenso.stream.open", {
  request_id: streamId,
  stream_id: streamId,
  capability_id: endpoint.capability_id,
  operation: "chat",
  session,
  credit: 16,
  payload: { room },
})) as WireStreamOutcome;
if (opened.kind === "domain") {
  process.stdout.write(JSON.stringify({ kind: "domain", value: opened.value }));
  process.exit(0);
}
if (opened.kind !== "opened") throw new Error(`stream open failed: ${JSON.stringify(opened)}`);

if (room === "provider-closes-first") {
  const halfClosed = (await rpc("lenso.stream.receive", call(nextId++, "receive"))) as WireStreamOutcome;
  if (halfClosed.kind !== "event" || halfClosed.event.kind !== "peer_half_closed") {
    throw new Error(`provider-first half-close failed: ${JSON.stringify(halfClosed)}`);
  }
  const accepted = (await rpc(
    "lenso.stream.send",
    call(nextId++, "send", { sequence: 0, payload: { text } }),
  )) as WireStreamOutcome;
  if (accepted.kind !== "accepted") {
    throw new Error(`send after provider half-close failed: ${JSON.stringify(accepted)}`);
  }
  const closed = (await rpc(
    "lenso.stream.close_send",
    call(nextId++, "close_send"),
  )) as WireStreamOutcome;
  if (closed.kind !== "accepted") throw new Error(`stream close failed: ${JSON.stringify(closed)}`);
  const terminal = (await rpc("lenso.stream.receive", call(nextId++, "receive"))) as WireStreamOutcome;
  if (
    terminal.kind !== "event" ||
    terminal.event.kind !== "terminal" ||
    terminal.event.outcome.kind !== "success"
  ) {
    throw new Error(`stream terminal failed: ${JSON.stringify(terminal)}`);
  }
  process.stdout.write(
    JSON.stringify({
      kind: "success",
      events: [halfClosed.event.kind, terminal.event.outcome.kind],
    }),
  );
  process.exit(0);
}

const accepted = (await rpc(
  "lenso.stream.send",
  call(nextId++, "send", { sequence: 0, payload: { text } }),
)) as WireStreamOutcome;
if (accepted.kind !== "accepted") throw new Error(`stream send failed: ${JSON.stringify(accepted)}`);

const message = (await rpc("lenso.stream.receive", call(nextId++, "receive"))) as WireStreamOutcome;
if (message.kind !== "event" || message.event.kind !== "message") {
  throw new Error(`stream message failed: ${JSON.stringify(message)}`);
}

const closed = (await rpc("lenso.stream.close_send", call(nextId++, "close_send"))) as WireStreamOutcome;
if (closed.kind !== "accepted") throw new Error(`stream close failed: ${JSON.stringify(closed)}`);

const halfClosed = (await rpc("lenso.stream.receive", call(nextId++, "receive"))) as WireStreamOutcome;
if (halfClosed.kind !== "event" || halfClosed.event.kind !== "peer_half_closed") {
  throw new Error(`stream half-close failed: ${JSON.stringify(halfClosed)}`);
}
const terminal = (await rpc("lenso.stream.receive", call(nextId++, "receive"))) as WireStreamOutcome;
if (
  terminal.kind !== "event" ||
  terminal.event.kind !== "terminal" ||
  terminal.event.outcome.kind !== "success"
) {
  throw new Error(`stream terminal failed: ${JSON.stringify(terminal)}`);
}

process.stdout.write(
  JSON.stringify({
    kind: "success",
    value: message.event.payload,
    events: [halfClosed.event.kind, terminal.event.outcome.kind],
  }),
);
