import {
  injectTraceContext,
  type WireExtension,
} from "../../crates/lenso-otel-module/typescript/trace-context.ts";

type EndpointDescriptor = {
  capability_id: string;
  descriptor_version: string;
  operations: string[];
};

type WireOutcome =
  | { kind: "success"; value: unknown }
  | { kind: "domain"; value: unknown }
  | { kind: "runtime"; failure: Record<string, unknown> };

const CAPABILITY_ID = "example.trace@1";
const OPERATION = "invoke";
const args = Bun.argv.slice(2);
const argument = (name: string, fallback: string): string => {
  const index = args.indexOf(name);
  return index >= 0 ? (args[index + 1] ?? fallback) : fallback;
};

const url = argument("--lenso-url", "");
if (!url) throw new Error("--lenso-url is required");

let nextId = 1;
async function rpc(method: string, params: unknown): Promise<any> {
  const id = nextId++;
  const response = await fetch(url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ jsonrpc: "2.0", id, method, params }),
  });
  if (!response.ok) throw new Error(`provider returned HTTP ${response.status}`);
  const value = await response.json();
  if (value.id !== id) throw new Error("provider returned the wrong JSON-RPC id");
  if (value.error) throw new Error(JSON.stringify(value.error));
  return value.result;
}

const endpoint: EndpointDescriptor = {
  capability_id: CAPABILITY_ID,
  descriptor_version: "1.0.0",
  operations: [OPERATION],
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

const extensions: WireExtension[] = await injectTraceContext(
  [],
  {
    traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
    tracestate: "vendor=value",
  },
  "lenso.otel",
  "trace-key",
  [`${CAPABILITY_ID}:${OPERATION}`],
);
const outcome: WireOutcome = await rpc("lenso.request", {
  request_id: nextId++,
  capability_id: CAPABILITY_ID,
  operation: OPERATION,
  session: handshake.session,
  extensions,
  payload: { message: "from-bun" },
});
process.stdout.write(JSON.stringify(outcome));
