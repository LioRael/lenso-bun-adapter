const args = Bun.argv.slice(2);
const argument = (name: string, fallback: string): string => {
  const index = args.indexOf(name);
  return index >= 0 ? (args[index + 1] ?? fallback) : fallback;
};

const url = argument("--lenso-url", "");
const message = argument("--message", "hello");
const sequence = Number(argument("--sequence", "1"));
if (!url) throw new Error("--lenso-url is required");

let nextId = 1;
async function rpc(method: string, params: unknown): Promise<any> {
  const id = nextId++;
  const response = await fetch(url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ jsonrpc: "2.0", id, method, params }),
  });
  if (!response.ok) throw new Error(`provider returned HTTP ${response.status}: ${await response.text()}`);
  const value = await response.json();
  if (value.id !== id) throw new Error("provider returned the wrong JSON-RPC id");
  if (value.error) throw new Error(JSON.stringify(value.error));
  return value.result;
}

const handshake = await rpc("lenso.handshake", {
  protocol_version: 1,
  value_profile: "lenso-json-value-v1",
  max_frame_bytes: 65536,
  endpoints: [
    {
      capability_id: "example.notifications@1",
      descriptor_version: "1.0.0",
      operations: ["notify"],
      stream_operations: [],
      event_operations: ["notify"],
    },
  ],
});
if (!handshake.accepted || typeof handshake.session !== "string") {
  throw new Error("provider rejected the exact handshake");
}

const outcome = await rpc("lenso.event.publish", {
  request_id: nextId++,
  capability_id: "example.notifications@1",
  operation: "notify",
  session: handshake.session,
  caller_instance: "bun-consumer",
  payload: { message, sequence },
});
process.stdout.write(JSON.stringify(outcome));
