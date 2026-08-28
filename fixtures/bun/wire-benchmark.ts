import { encodeGreetRequest } from "../../crates/lenso-capability-greeting/generated/bindings.ts";

type Wire = "framed-stdio" | "json-rpc-http";
type RequestCase = {
  name: string;
  scenario?: string;
  request: {
    capability_id: string;
    operation: string;
    payload: unknown;
    deadline_nanos?: number;
  };
  outcome: "success" | "domain" | "runtime";
  failure_kind?: string;
  boundary_bytes?: number;
  queue_capacity?: number;
};

const cases = (await Bun.file(
  new URL("./request-conformance.json", import.meta.url),
).json()) as RequestCase[];
const requestCases = cases.filter((value) => (value.scenario ?? "request") === "request");
const provider = new URL("./request-provider.ts", import.meta.url).pathname;
const bunBinary = process.env.BUN_BIN ?? process.execPath;
const maxFrameBytes = 64 * 1024;
const requestCount = Number(process.env.LENSO_BUN_BENCHMARK_REQUESTS ?? "50");
const uname = new TextDecoder()
  .decode(Bun.spawnSync(["uname", "-a"], { stdout: "pipe", stderr: "pipe" }).stdout)
  .trim();

class ByteReader {
  private readonly reader: ReadableStreamDefaultReader<Uint8Array>;
  private buffered = new Uint8Array();

  constructor(stream: ReadableStream<Uint8Array>) {
    this.reader = stream.getReader();
  }

  async readBytes(length: number): Promise<Uint8Array> {
    while (this.buffered.length < length) {
      const next = await this.reader.read();
      if (next.done) throw new Error("child process stdout ended");
      const merged = new Uint8Array(this.buffered.length + next.value.length);
      merged.set(this.buffered);
      merged.set(next.value, this.buffered.length);
      this.buffered = merged;
    }
    const result = this.buffered.slice(0, length);
    this.buffered = this.buffered.slice(length);
    return result;
  }

  async readLine(): Promise<string> {
    while (true) {
      const newline = this.buffered.indexOf(10);
      if (newline >= 0) {
        const line = this.buffered.slice(0, newline);
        this.buffered = this.buffered.slice(newline + 1);
        return new TextDecoder().decode(line).replace(/\r$/, "");
      }
      const next = await this.reader.read();
      if (next.done) throw new Error("child process stdout ended before readiness");
      const merged = new Uint8Array(this.buffered.length + next.value.length);
      merged.set(this.buffered);
      merged.set(next.value, this.buffered.length);
      this.buffered = merged;
    }
  }
}

const handshake = () => ({
  protocol_version: 1,
  value_profile: "lenso-json-value-v1",
  max_frame_bytes: maxFrameBytes,
  endpoints: [
    {
      capability_id: "example.greeting@1",
      descriptor_version: "1.0.0",
      operations: ["greet"],
    },
  ],
});

async function writeFrame(stdin: Bun.FileSink, message: unknown): Promise<void> {
  const payload = new TextEncoder().encode(JSON.stringify(message));
  if (payload.length > maxFrameBytes) throw new Error("benchmark frame exceeds limit");
  const frame = new Uint8Array(payload.length + 4);
  new DataView(frame.buffer).setUint32(0, payload.length);
  frame.set(payload, 4);
  await stdin.write(frame);
  await stdin.flush();
}

async function readFrame(reader: ByteReader): Promise<any> {
  const header = await reader.readBytes(4);
  const length = new DataView(header.buffer, header.byteOffset, 4).getUint32(0);
  if (length > maxFrameBytes) throw new Error("benchmark frame exceeds limit");
  return JSON.parse(new TextDecoder().decode(await reader.readBytes(length)));
}

async function spawn(wire: Wire) {
  const child = Bun.spawn(
    [
      bunBinary,
      "run",
      provider,
      "--",
      "--lenso-transport",
      wire,
      "--lenso-max-frame-bytes",
      String(maxFrameBytes),
      "--lenso-endpoints-json",
      JSON.stringify(handshake().endpoints),
      "--lenso-port",
      "0",
    ],
    { stdin: "pipe", stdout: "pipe", stderr: "pipe" },
  );
  const started = performance.now();
  if (wire === "framed-stdio") {
    const reader = new ByteReader(child.stdout as ReadableStream<Uint8Array>);
    await writeFrame(child.stdin as Bun.FileSink, {
      kind: "handshake",
      ...handshake(),
    });
    const ack = await readFrame(reader);
    if (!ack.accepted) throw new Error("framed benchmark handshake rejected");
    return { child, reader, startupMs: performance.now() - started };
  }
  const reader = new ByteReader(child.stdout as ReadableStream<Uint8Array>);
  const ready = await reader.readLine();
  const port = ready.startsWith("LENSO_READY ") ? Number(ready.slice(12)) : 0;
  if (!port) throw new Error(`invalid JSON-RPC readiness: ${ready}`);
  const url = `http://127.0.0.1:${port}`;
  const response = await rpc(url, 1, "lenso.handshake", handshake());
  if (!response.accepted || typeof response.session !== "string") {
    throw new Error("JSON-RPC benchmark handshake rejected");
  }
  return { child, reader, url, session: response.session, startupMs: performance.now() - started };
}

async function rpc(url: string, id: number, method: string, params: unknown): Promise<any> {
  const response = await fetch(url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ jsonrpc: "2.0", id, method, params }),
  });
  if (!response.ok) throw new Error(`JSON-RPC returned HTTP ${response.status}`);
  const value = await response.json();
  if (value.id !== id) throw new Error("JSON-RPC response id mismatch");
  if (value.error) throw new Error(JSON.stringify(value.error));
  return value.result;
}

function request(
  id: number,
  value: RequestCase["request"],
  name = "Ada",
  session?: string,
) {
  const payload = value.payload as { name?: string };
  return {
    request_id: id,
    capability_id: value.capability_id,
    operation: value.operation,
    deadline_nanos: value.deadline_nanos,
    session,
    payload:
      value.operation === "greet"
        ? {
            name:
              payload.name === "__oversized__"
                ? "x".repeat(maxFrameBytes)
                : (payload.name ?? name),
          }
        : value.payload,
  };
}

async function call(childState: Awaited<ReturnType<typeof spawn>>, id: number, value: RequestCase["request"]) {
  if ("url" in childState) {
    return rpc(childState.url, id, "lenso.request", request(id, value, "Ada", childState.session));
  }
  await writeFrame(childState.child.stdin as Bun.FileSink, {
    kind: "request",
    ...request(id, value),
  });
  while (true) {
    const response = await readFrame(childState.reader);
    if (response.kind === "response" && response.request_id === id) return response.outcome;
    throw new Error("framed benchmark received an unexpected response");
  }
}

async function cancel(childState: Awaited<ReturnType<typeof spawn>>, id: number): Promise<number> {
  const started = performance.now();
  const value = { capability_id: "example.greeting@1", operation: "greet", payload: { name: "__delay__" } };
  if ("url" in childState) {
    const pending = rpc(
      childState.url,
      id,
      "lenso.request",
      request(id, value, "Ada", childState.session),
    );
    await Bun.sleep(25);
    await rpc(childState.url, id + 1000000, "lenso.cancel", {
      request_id: id,
      session: childState.session,
    });
    const outcome = await pending;
    if (outcome.kind !== "runtime" || outcome.failure.kind !== "cancelled") {
      throw new Error(`JSON-RPC cancellation outcome was ${JSON.stringify(outcome)}`);
    }
    return performance.now() - started;
  }
  await writeFrame(childState.child.stdin as Bun.FileSink, { kind: "request", ...request(id, value) });
  await writeFrame(childState.child.stdin as Bun.FileSink, { kind: "cancel", request_id: id });
  while (true) {
    const response = await readFrame(childState.reader);
    if (response.kind === "response" && response.request_id === id) {
      if (response.outcome.kind !== "runtime" || response.outcome.failure.kind !== "cancelled") {
        throw new Error(`framed cancellation outcome was ${JSON.stringify(response.outcome)}`);
      }
      return performance.now() - started;
    }
  }
}

async function close(childState: Awaited<ReturnType<typeof spawn>>) {
  if ("url" in childState) {
    try {
      await rpc(childState.url, 900000, "lenso.shutdown", { session: childState.session });
    } catch {
      childState.child.kill();
    }
  } else {
    await writeFrame(childState.child.stdin as Bun.FileSink, { kind: "shutdown" });
  }
  if (!childState.child.killed) childState.child.kill();
  await childState.child.exited;
}

async function crash(wire: Wire) {
  const state = await spawn(wire);
  const started = performance.now();
  try {
    await call(state, 99, {
      capability_id: "example.greeting@1",
      operation: "greet",
      payload: { name: "__crash__" },
    });
  } catch {
    // The provider deliberately exits without a response.
  }
  const exitCode = await state.child.exited;
  const detectionMs = performance.now() - started;
  const recoveryStarted = performance.now();
  const recovered = await spawn(wire);
  const recoveryMs = performance.now() - recoveryStarted;
  await close(recovered);
  return {
    detection_ms: Number(detectionMs.toFixed(3)),
    recovery_ms: Number(recoveryMs.toFixed(3)),
    exit_code: exitCode,
    observed_failure: exitCode === 17 ? "plugin_failure" : "unexpected_exit",
  };
}

async function overload(wire: Wire) {
  const state = await spawn(wire);
  const requestTotal = 40;
  const value = {
    capability_id: "example.greeting@1",
    operation: "greet",
    payload: { name: "__delay__" },
  };
  let outcomes: any[];
  if ("url" in state) {
    outcomes = await Promise.all(
      Array.from({ length: requestTotal }, (_, index) =>
        call(state, 800000 + index, value),
      ),
    );
  } else {
    for (let index = 0; index < requestTotal; index += 1) {
      await writeFrame(state.child.stdin as Bun.FileSink, {
        kind: "request",
        ...request(800000 + index, value),
      });
    }
    outcomes = [];
    while (outcomes.length < requestTotal) {
      const response = await readFrame(state.reader);
      if (response.kind !== "response") {
        throw new Error("framed overload received an unexpected message");
      }
      outcomes.push(response.outcome);
    }
  }
  const rejected = outcomes.filter(
    (outcome) =>
      outcome.kind === "runtime" && outcome.failure?.kind === "resource_exhausted",
  ).length;
  await close(state);
  if (rejected === 0) throw new Error(`${wire} did not reject bounded overload`);
  return {
    request_count: requestTotal,
    rejected,
    observed_failure: "resource_exhausted",
  };
}

async function measureContractCases(wire: Wire): Promise<Record<string, string>> {
  const state = await spawn(wire);
  const observed: Record<string, string> = {};
  const deadline = cases.find((value) => value.scenario === "deadline");
  const oversized = cases.find((value) => value.scenario === "size-boundary");
  try {
    if (deadline) {
      const outcome = await call(state, 700001, deadline.request);
      observed.deadline =
        outcome.kind === "runtime" &&
        (outcome.failure as { kind?: string }).kind === "deadline_exceeded"
          ? "deadline_exceeded"
          : "unexpected";
    }
    await cancel(state, 700002);
    observed.cancellation = "cancelled";
    if (oversized) {
      try {
        await call(state, 700003, oversized.request);
        observed["size-boundary"] = "unexpected_success";
      } catch {
        observed["size-boundary"] = "protocol_violation";
      }
    }
    await close(state);
    return observed;
  } catch (error) {
    state.child.kill();
    await state.child.exited;
    throw error;
  }
}

function percentile(values: number[], fraction: number): number {
  const sorted = [...values].sort((left, right) => left - right);
  return sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * fraction))] ?? 0;
}

async function measure(wire: Wire) {
  const state = await spawn(wire);
  const samples: number[] = [];
  const corpusResults: Record<string, string> = {};
  try {
    for (let index = 0; index < requestCount; index += 1) {
      const fixture = requestCases[index % requestCases.length];
      const started = performance.now();
      const outcome = await call(state, index + 2, fixture.request);
      samples.push(performance.now() - started);
      if (outcome.kind !== fixture.outcome) {
        throw new Error(
          `${wire} corpus case ${fixture.name} expected ${fixture.outcome}, got ${outcome.kind}`,
        );
      }
      corpusResults[fixture.name] = outcome.kind;
    }
    const cancellationMs = await cancel(state, requestCount + 2);
    const totalMs = samples.reduce((total, value) => total + value, 0);
    await close(state);
    const usage = state.child.resourceUsage();
    return {
      instance_count: 1,
      startup_ms: Number(state.startupMs.toFixed(3)),
      request_count: requestCount,
      latency_ms: {
        p50: Number(percentile(samples, 0.5).toFixed(3)),
        p95: Number(percentile(samples, 0.95).toFixed(3)),
        p99: Number(percentile(samples, 0.99).toFixed(3)),
      },
      throughput_requests_per_second: Number((requestCount / (totalMs / 1000)).toFixed(3)),
      memory_max_rss_bytes: usage.maxRSS,
      cancellation_ms: Number(cancellationMs.toFixed(3)),
      corpus_results: corpusResults,
    };
  } catch (error) {
    state.child.kill();
    await state.child.exited;
    throw error;
  }
}

const measurements = Object.fromEntries(
  await Promise.all(
    (["framed-stdio", "json-rpc-http"] as Wire[]).map(async (wire) => [
      wire,
      await measure(wire),
    ]),
  ),
);
const crashMeasurements = Object.fromEntries(
  await Promise.all(
    (["framed-stdio", "json-rpc-http"] as Wire[]).map(async (wire) => [
      wire,
      await crash(wire),
    ]),
  ),
);
const overloadMeasurements = Object.fromEntries(
  await Promise.all(
    (["framed-stdio", "json-rpc-http"] as Wire[]).map(async (wire) => [
      wire,
      await overload(wire),
    ]),
  ),
);
const contractMeasurements = Object.fromEntries(
  await Promise.all(
    (['framed-stdio', 'json-rpc-http'] as Wire[]).map(async (wire) => [
      wire,
      await measureContractCases(wire),
    ]),
  ),
);
const evidence = {
  schema: "lenso.bun-wire-benchmark.v1",
  generated_at: new Date().toISOString(),
  runtime: { bun: Bun.version, node: process.version, platform: process.platform, arch: process.arch },
  hardware: {
    uname,
    logical_cpu_count: navigator.hardwareConcurrency ?? null,
  },
  corpus: "fixtures/bun/request-conformance.json",
  request_count: requestCount,
  candidates: measurements,
  crash: crashMeasurements,
  contract_cases: cases
    .filter((value) => (value.scenario ?? "request") !== "request")
    .map(({ name, scenario, outcome, failure_kind, boundary_bytes, queue_capacity }) => ({
      name,
      scenario,
      expected_outcome: outcome,
      expected_failure: failure_kind ?? null,
      boundary_bytes: boundary_bytes ?? null,
      queue_capacity: queue_capacity ?? null,
      observed_by_candidate: Object.fromEntries(
        (['framed-stdio', 'json-rpc-http'] as Wire[]).map((wire) => [
          wire,
          scenario === "process-failure"
            ? crashMeasurements[wire].observed_failure
            : scenario === "overload"
              ? overloadMeasurements[wire].observed_failure
              : contractMeasurements[wire][scenario ?? ""] ?? "not-measured",
        ]),
      ),
    })),
  bounded_overload: {
    provider_capacity: 32,
    candidates: overloadMeasurements,
  },
};
const output = JSON.stringify(evidence, null, 2);
const outputPath = process.argv.includes("--output")
  ? process.argv[process.argv.indexOf("--output") + 1]
  : process.env.LENSO_BUN_BENCHMARK_OUTPUT;
if (outputPath) await Bun.write(outputPath, `${output}\n`);
else console.log(output);
