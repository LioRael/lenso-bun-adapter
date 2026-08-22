import {
  decodeGreetResponse,
  encodeGreetRequest,
} from "./vendor/greeting.ts";

const args = Bun.argv.slice(2);
const argument = (name: string, fallback: string): string => {
  const index = args.indexOf(name);
  return index >= 0 ? (args[index + 1] ?? fallback) : fallback;
};

const url = argument("--lenso-url", "");
const name = argument("--name", "Ada");
const operation = argument("--operation", "greet");
const cancelAfterMs = Number(argument("--cancel-after-ms", "0"));
const parallel = Number(argument("--parallel", "1"));
const deadlineNanosArgument = argument("--deadline-nanos", "");
const deadlineNanos = deadlineNanosArgument === "" ? undefined : Number(deadlineNanosArgument);
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
    if (response.status === 413 && method === "lenso.request") {
      return { kind: "runtime", failure: { kind: "protocol_violation" } };
    }
    throw new Error(`provider returned HTTP ${response.status}: ${await response.text()}`);
  }
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
      capability_id: "example.greeting@1",
      descriptor_version: "1.0.0",
      operations: ["greet"],
    },
  ],
});
if (!handshake.accepted || typeof handshake.session !== "string") {
  throw new Error("provider rejected the exact handshake");
}
const session = handshake.session;

const request = (requestId: number) => ({
  request_id: requestId,
  capability_id: "example.greeting@1",
  operation,
  deadline_nanos: deadlineNanos,
  session,
  payload: JSON.parse(
    encodeGreetRequest({ name: name === "__oversized__" ? "x".repeat(65537) : name }),
  ),
});
if (parallel > 1) {
  const outcomes = await Promise.all(
    Array.from({ length: parallel }, () => {
      const requestId = nextId++;
      return rpc("lenso.request", request(requestId));
    }),
  );
  process.stdout.write(JSON.stringify({ kind: "batch", outcomes }));
  process.exit(0);
}

const requestId = nextId++;
const outcomePromise = rpc("lenso.request", request(requestId));
if (cancelAfterMs > 0) {
  await Bun.sleep(cancelAfterMs);
  await rpc("lenso.cancel", { request_id: requestId, session });
}
const outcome = await outcomePromise;
if (outcome.kind === "success") {
  process.stdout.write(JSON.stringify({
    kind: outcome.kind,
    value: decodeGreetResponse(JSON.stringify(outcome.value)),
  }));
} else {
  process.stdout.write(JSON.stringify(outcome));
}
