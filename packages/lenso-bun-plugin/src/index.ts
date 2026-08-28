import type {
  InvocationContext,
  RuntimeFailure,
} from "@lenso/contract-runtime";

const PROTOCOL_VERSION = 1;
const VALUE_PROFILE = "lenso-json-value-v1";
const DEFAULT_MAX_FRAME_BYTES = 64 * 1024;
const DEFAULT_MAX_CONCURRENT_REQUESTS = 32;
const DEFAULT_MAX_RETIRED_REQUEST_IDS = 1024;

export interface CapabilityProviderDescriptor {
  readonly capability_id: string;
  readonly descriptor_version: string;
  readonly operations: ReadonlyArray<string>;
  readonly stream_operations: ReadonlyArray<string>;
  readonly event_operations: ReadonlyArray<string>;
}

export type ProviderDispatchOutcome =
  | { readonly kind: "success"; readonly value: unknown }
  | { readonly kind: "domain"; readonly value: unknown }
  | { readonly kind: "runtime"; readonly failure: RuntimeFailure };

export interface CapabilityProviderBinding {
  readonly descriptor: CapabilityProviderDescriptor;
  invokeRequest(
    operation: string,
    context: InvocationContext,
    payload: unknown,
  ): Promise<ProviderDispatchOutcome>;
}

export interface BunPluginOptions {
  readonly providers: ReadonlyArray<CapabilityProviderBinding>;
  readonly maxConcurrentRequests?: number;
}

export interface BunPluginDefinition {
  readonly providers: ReadonlyArray<CapabilityProviderBinding>;
  readonly maxConcurrentRequests: number;
}

/** Runtime-independent descriptor consumed by generated QuickJS and Bun wrappers. */
export interface PortablePluginDescriptor {
  readonly abi: "lenso.json-request@1";
  readonly capabilities: ReadonlyArray<{
    readonly capability_id: string;
    readonly descriptor_version: string;
    readonly request_operations: ReadonlyArray<string>;
  }>;
}

export interface StartPluginOptions {
  readonly hostname?: string;
  readonly port?: number;
  readonly maxFrameBytes?: number;
  readonly expectedEndpoints?: ReadonlyArray<CapabilityProviderDescriptor>;
  readonly maxRetiredRequestIds?: number;
  readonly announceReady?: boolean;
}

export interface BunPluginServer {
  readonly port: number;
  stop(closeActiveConnections?: boolean): void;
}

interface Handshake {
  readonly protocol_version: number;
  readonly value_profile: string;
  readonly max_frame_bytes: number;
  readonly endpoints: ReadonlyArray<CapabilityProviderDescriptor>;
}

interface WireInvocationExtension {
  readonly key: string;
  readonly value: ReadonlyArray<number>;
  readonly issuer?: string;
  readonly audience?: ReadonlyArray<string>;
  readonly proof?: string;
  readonly sealed?: boolean;
}

interface WireRequest {
  readonly request_id: number;
  readonly capability_id: string;
  readonly operation: string;
  readonly deadline_nanos?: number | null;
  readonly caller_instance?: string | null;
  readonly session?: string | null;
  readonly extensions?: ReadonlyArray<WireInvocationExtension>;
  readonly payload: unknown;
}

interface RequestState {
  cancelled: boolean;
  deadlineExceeded: boolean;
}

interface JsonRpcEnvelope {
  readonly jsonrpc?: string;
  readonly id?: string | number | null;
  readonly method?: string;
  readonly params?: unknown;
}

export function definePlugin(options: BunPluginOptions): BunPluginDefinition {
  if (options.providers.length === 0) {
    throw new Error("a Bun Plugin must register at least one Capability Provider");
  }
  const seen = new Set<string>();
  for (const provider of options.providers) {
    validateDescriptor(provider.descriptor);
    if (seen.has(provider.descriptor.capability_id)) {
      throw new Error(
        `duplicate Capability Provider ${provider.descriptor.capability_id}`,
      );
    }
    seen.add(provider.descriptor.capability_id);
    if (
      provider.descriptor.stream_operations.length > 0 ||
      provider.descriptor.event_operations.length > 0
    ) {
      throw new Error(
        `@lenso/bun-plugin 0.1 supports request Capabilities only; ${provider.descriptor.capability_id} declares Stream or Event Operations`,
      );
    }
  }
  const maxConcurrentRequests =
    options.maxConcurrentRequests ?? DEFAULT_MAX_CONCURRENT_REQUESTS;
  if (!Number.isSafeInteger(maxConcurrentRequests) || maxConcurrentRequests <= 0) {
    throw new Error("maxConcurrentRequests must be a positive safe integer");
  }
  return Object.freeze({
    providers: Object.freeze([...options.providers]),
    maxConcurrentRequests,
  });
}

/** Describes the same Plugin definition without touching any Bun global API. */
export function describePortablePlugin(
  definition: BunPluginDefinition,
): PortablePluginDescriptor {
  return {
    abi: "lenso.json-request@1",
    capabilities: definition.providers.map(({ descriptor }) => ({
      capability_id: descriptor.capability_id,
      descriptor_version: descriptor.descriptor_version,
      request_operations: [...descriptor.operations],
    })),
  };
}

/** Dispatches the portable QuickJS ABI through the same authored Provider binding. */
export async function invokePortablePlugin(
  definition: BunPluginDefinition,
  capability: string,
  operation: string,
  requestJson: string,
): Promise<string> {
  const provider = definition.providers.find(
    ({ descriptor }) => descriptor.capability_id === capability,
  );
  if (provider === undefined) throw new Error(`unknown capability ${capability}`);
  const context = Object.freeze({
    requestId: 0,
    deadlineNanos: undefined,
    callerInstance: undefined,
    cancelled: false,
    extensions: Object.freeze([]),
  }) as unknown as InvocationContext;
  const outcome = await provider.invokeRequest(
    operation,
    context,
    JSON.parse(requestJson) as unknown,
  );
  switch (outcome.kind) {
    case "success":
      return JSON.stringify({ ok: outcome.value });
    case "domain":
      return JSON.stringify({ error: outcome.value });
    case "runtime":
      throw new Error(`Plugin runtime failure: ${outcome.failure.kind}`);
  }
}

export function serve(definition: BunPluginDefinition): BunPluginServer {
  const arguments_ = Bun.argv.slice(2);
  const transport = argument(arguments_, "--lenso-transport", "json-rpc-http");
  if (transport !== "json-rpc-http") {
    throw new Error(
      `@lenso/bun-plugin supports the production json-rpc-http wire, received ${transport}`,
    );
  }
  const maxFrameBytes = numericArgument(
    arguments_,
    "--lenso-max-frame-bytes",
    DEFAULT_MAX_FRAME_BYTES,
  );
  const port = numericArgument(arguments_, "--lenso-port", 0);
  const expectedEndpoints = jsonArgument<CapabilityProviderDescriptor[]>(
    arguments_,
    "--lenso-endpoints-json",
    definition.providers.map(({ descriptor }) => descriptor),
  );
  return startPlugin(definition, {
    announceReady: true,
    expectedEndpoints,
    maxFrameBytes,
    port,
  });
}

export function startPlugin(
  definition: BunPluginDefinition,
  options: StartPluginOptions = {},
): BunPluginServer {
  const maxFrameBytes = options.maxFrameBytes ?? DEFAULT_MAX_FRAME_BYTES;
  const maxRetiredRequestIds =
    options.maxRetiredRequestIds ?? DEFAULT_MAX_RETIRED_REQUEST_IDS;
  if (!Number.isSafeInteger(maxFrameBytes) || maxFrameBytes <= 0) {
    throw new Error("maxFrameBytes must be a positive safe integer");
  }
  if (!Number.isSafeInteger(maxRetiredRequestIds) || maxRetiredRequestIds <= 0) {
    throw new Error("maxRetiredRequestIds must be a positive safe integer");
  }

  const providerByCapability = new Map(
    definition.providers.map((provider) => [
      provider.descriptor.capability_id,
      provider,
    ]),
  );
  const pluginEndpoints = definition.providers.map(({ descriptor }) => descriptor);
  const expectedEndpoints = options.expectedEndpoints ?? pluginEndpoints;
  const activeRequests = new Map<number, RequestState>();
  const retiredRequestIds = new Set<number>();
  let session: string | undefined;
  let admittedHandshake: Handshake | undefined;
  let accepting = true;

  const retire = (requestId: number): void => {
    while (retiredRequestIds.size >= maxRetiredRequestIds) {
      const oldest = retiredRequestIds.values().next().value;
      if (oldest === undefined) break;
      retiredRequestIds.delete(oldest);
    }
    retiredRequestIds.add(requestId);
  };

  const server = Bun.serve({
    hostname: options.hostname ?? "127.0.0.1",
    port: options.port ?? 0,
    async fetch(request) {
      if (request.method !== "POST") {
        return new Response("method not allowed", { status: 405 });
      }
      let envelope: JsonRpcEnvelope;
      try {
        envelope = JSON.parse(
          await readBoundedBody(request, maxFrameBytes),
        ) as JsonRpcEnvelope;
      } catch (error) {
        if (error instanceof BodyTooLargeError) {
          return new Response("request too large", { status: 413 });
        }
        return jsonRpcError(null, -32700, "Parse error");
      }
      const id = envelope.id ?? null;
      if (envelope.jsonrpc !== "2.0" || typeof envelope.method !== "string") {
        return jsonRpcError(id, -32600, "Invalid Request");
      }
      const params =
        Array.isArray(envelope.params) && envelope.params.length === 1
          ? envelope.params[0]
          : envelope.params;

      if (envelope.method === "lenso.handshake") {
        const handshake = objectParam(params) as Handshake | undefined;
        const accepted =
          accepting &&
          activeRequests.size === 0 &&
          validHandshake(handshake, maxFrameBytes) &&
          sameEndpoints(handshake.endpoints, expectedEndpoints) &&
          sameEndpoints(handshake.endpoints, pluginEndpoints);
        if (accepted) {
          admittedHandshake = handshake;
          session = crypto.randomUUID();
        }
        return jsonRpcResult(id, {
          accepted,
          protocol_version: PROTOCOL_VERSION,
          value_profile: VALUE_PROFILE,
          max_frame_bytes: maxFrameBytes,
          endpoints: Array.isArray(handshake?.endpoints) ? handshake.endpoints : [],
          ...(session === undefined ? {} : { session }),
        });
      }

      if (envelope.method === "lenso.cancel") {
        const cancellation = objectParam(params);
        const cancellationSession = cancellation?.session;
        if (
          !hasSession(
            typeof cancellationSession === "string"
              ? cancellationSession
              : undefined,
            session,
            admittedHandshake,
          )
        ) {
          return jsonRpcResult(id, protocolViolation("request session mismatch"));
        }
        const requestId = cancellation?.request_id;
        if (typeof requestId === "number" && isRequestId(requestId)) {
          const state = activeRequests.get(requestId);
          if (state) state.cancelled = true;
        }
        return jsonRpcResult(id, true);
      }

      if (envelope.method === "lenso.shutdown") {
        const shutdownSession = objectParam(params)?.session;
        if (
          !hasSession(
            typeof shutdownSession === "string" ? shutdownSession : undefined,
            session,
            admittedHandshake,
          )
        ) {
          return jsonRpcResult(id, protocolViolation("shutdown session mismatch"));
        }
        accepting = false;
        for (const state of activeRequests.values()) state.cancelled = true;
        setTimeout(() => server.stop(false), 0);
        return jsonRpcResult(id, true);
      }

      if (envelope.method !== "lenso.request" || !accepting || !admittedHandshake) {
        return jsonRpcResult(id, protocolViolation("request before handshake"));
      }

      const wireRequest = objectParam(params) as WireRequest | undefined;
      if (!validWireRequest(wireRequest, session)) {
        return jsonRpcResult(id, protocolViolation("invalid request envelope"));
      }
      if (
        activeRequests.has(wireRequest.request_id) ||
        retiredRequestIds.has(wireRequest.request_id)
      ) {
        return jsonRpcResult(id, protocolViolation("duplicate or retired request id"));
      }
      if (activeRequests.size >= definition.maxConcurrentRequests) {
        return jsonRpcResult(id, {
          kind: "runtime",
          failure: {
            kind: "resource_exhausted",
            operation: wireRequest.operation,
          },
        });
      }
      const provider = providerByCapability.get(wireRequest.capability_id);
      if (!provider) {
        return jsonRpcResult(id, {
          kind: "runtime",
          failure: { kind: "unknown_operation", operation: wireRequest.operation },
        });
      }
      const state: RequestState = {
        cancelled: false,
        deadlineExceeded: wireRequest.deadline_nanos === 0,
      };
      activeRequests.set(wireRequest.request_id, state);
      const timeout = startDeadline(wireRequest.deadline_nanos, state);
      let outcome: ProviderDispatchOutcome;
      try {
        const context = invocationContext(wireRequest, state);
        if (state.deadlineExceeded) {
          outcome = deadlineExceeded(wireRequest.request_id);
        } else {
          outcome = await provider.invokeRequest(
            wireRequest.operation,
            context,
            wireRequest.payload,
          );
          if (state.deadlineExceeded) {
            outcome = deadlineExceeded(wireRequest.request_id);
          } else if (state.cancelled) {
            outcome = cancelled(wireRequest.request_id);
          }
        }
      } catch (error) {
        outcome = {
          kind: "runtime",
          failure: { kind: "plugin_failure", detail: errorMessage(error) },
        };
      } finally {
        if (timeout !== undefined) clearTimeout(timeout);
        activeRequests.delete(wireRequest.request_id);
        retire(wireRequest.request_id);
      }
      return jsonRpcResult(id, outcome, maxFrameBytes);
    },
  });

  if (server.port === undefined) {
    server.stop(true);
    throw new Error("Bun Plugin server did not bind a TCP port");
  }
  if (options.announceReady ?? false) {
    console.log(`LENSO_READY ${server.port}`);
  }
  return {
    port: server.port,
    stop(closeActiveConnections = true) {
      accepting = false;
      for (const state of activeRequests.values()) state.cancelled = true;
      server.stop(closeActiveConnections);
    },
  };
}

function validateDescriptor(descriptor: CapabilityProviderDescriptor): void {
  if (
    descriptor.capability_id.length === 0 ||
    descriptor.descriptor_version.length === 0 ||
    descriptor.operations.length === 0
  ) {
    throw new Error("Capability Provider descriptor is incomplete");
  }
  if (new Set(descriptor.operations).size !== descriptor.operations.length) {
    throw new Error(
      `Capability Provider ${descriptor.capability_id} declares duplicate Operations`,
    );
  }
  for (const operation of [
    ...descriptor.stream_operations,
    ...descriptor.event_operations,
  ]) {
    if (!descriptor.operations.includes(operation)) {
      throw new Error(
        `Capability Provider ${descriptor.capability_id} classifies unknown Operation ${operation}`,
      );
    }
  }
}

function argument(arguments_: string[], name: string, fallback: string): string {
  const index = arguments_.indexOf(name);
  return index < 0 ? fallback : (arguments_[index + 1] ?? fallback);
}

function objectParam(value: unknown): Record<string, unknown> | undefined {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
}

function numericArgument(
  arguments_: string[],
  name: string,
  fallback: number,
): number {
  const value = Number(argument(arguments_, name, String(fallback)));
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new Error(`${name} must be a non-negative safe integer`);
  }
  return value;
}

function jsonArgument<T>(arguments_: string[], name: string, fallback: T): T {
  const value = argument(arguments_, name, JSON.stringify(fallback));
  try {
    return JSON.parse(value) as T;
  } catch (error) {
    throw new Error(`${name} must contain valid JSON: ${errorMessage(error)}`);
  }
}

function validHandshake(
  handshake: Handshake | undefined,
  maxFrameBytes: number,
): handshake is Handshake {
  return Boolean(
    handshake &&
      handshake.protocol_version === PROTOCOL_VERSION &&
      handshake.value_profile === VALUE_PROFILE &&
      handshake.max_frame_bytes === maxFrameBytes &&
      Array.isArray(handshake.endpoints),
  );
}

function sameEndpoints(
  left: ReadonlyArray<CapabilityProviderDescriptor>,
  right: ReadonlyArray<CapabilityProviderDescriptor>,
): boolean {
  return JSON.stringify(left) === JSON.stringify(right);
}

function hasSession(
  candidate: string | null | undefined,
  session: string | undefined,
  handshake: Handshake | undefined,
): boolean {
  return handshake !== undefined && session !== undefined && candidate === session;
}

function validWireRequest(
  request: WireRequest | undefined,
  session: string | undefined,
): request is WireRequest {
  return Boolean(
    request &&
      isRequestId(request.request_id) &&
      typeof request.capability_id === "string" &&
      typeof request.operation === "string" &&
      request.session === session &&
      (request.deadline_nanos == null ||
        (Number.isSafeInteger(request.deadline_nanos) && request.deadline_nanos >= 0)) &&
      (request.caller_instance == null ||
        typeof request.caller_instance === "string") &&
      (request.extensions === undefined || Array.isArray(request.extensions)),
  );
}

function isRequestId(value: number | undefined): value is number {
  return value !== undefined && Number.isSafeInteger(value) && value >= 0;
}

function invocationContext(
  request: WireRequest,
  state: RequestState,
): InvocationContext {
  const extensions = extensionRecord(request.extensions ?? []);
  return {
    requestId: String(request.request_id) as InvocationContext["requestId"],
    get cancelled() {
      return state.cancelled || state.deadlineExceeded;
    },
    ...(request.caller_instance == null
      ? {}
      : { callerInstance: request.caller_instance }),
    ...(extensions === undefined ? {} : { extensions }),
  };
}

function extensionRecord(
  extensions: ReadonlyArray<WireInvocationExtension>,
): Record<string, unknown> | undefined {
  if (extensions.length === 0) return undefined;
  const record: Record<string, unknown> = {};
  for (const extension of extensions) {
    if (
      typeof extension.key !== "string" ||
      extension.key.length === 0 ||
      Object.hasOwn(record, extension.key)
    ) {
      throw new Error("Invocation Context contains an invalid extension key");
    }
    record[extension.key] = Object.freeze({ ...extension });
  }
  return Object.freeze(record);
}

function startDeadline(
  deadlineNanos: number | null | undefined,
  state: RequestState,
): ReturnType<typeof setTimeout> | undefined {
  if (deadlineNanos == null || deadlineNanos === 0) return undefined;
  const milliseconds = Math.max(
    1,
    Math.min(Math.ceil(deadlineNanos / 1_000_000), 2_147_483_647),
  );
  return setTimeout(() => {
    state.deadlineExceeded = true;
  }, milliseconds);
}

function protocolViolation(detail: string): ProviderDispatchOutcome {
  return { kind: "runtime", failure: { kind: "protocol_violation", detail } };
}

function deadlineExceeded(requestId: number): ProviderDispatchOutcome {
  return {
    kind: "runtime",
    failure: { kind: "deadline_exceeded", request_id: requestId },
  };
}

function cancelled(requestId: number): ProviderDispatchOutcome {
  return {
    kind: "runtime",
    failure: { kind: "cancelled", request_id: requestId },
  };
}

class BodyTooLargeError extends Error {}

async function readBoundedBody(
  request: Request,
  maxFrameBytes: number,
): Promise<string> {
  const declaredLength = Number(request.headers.get("content-length"));
  if (Number.isFinite(declaredLength) && declaredLength > maxFrameBytes) {
    throw new BodyTooLargeError();
  }
  if (!request.body) return "";
  const reader = request.body.getReader();
  const decoder = new TextDecoder();
  let total = 0;
  let body = "";
  while (true) {
    const next = await reader.read();
    if (next.done) break;
    total += next.value.byteLength;
    if (total > maxFrameBytes) throw new BodyTooLargeError();
    body += decoder.decode(next.value, { stream: true });
  }
  return body + decoder.decode();
}

function jsonRpcResult(
  id: string | number | null,
  result: unknown,
  maxFrameBytes?: number,
): Response {
  const body = JSON.stringify({ jsonrpc: "2.0", id, result });
  if (
    maxFrameBytes !== undefined &&
    new TextEncoder().encode(body).byteLength > maxFrameBytes
  ) {
    return new Response("response too large", { status: 413 });
  }
  return new Response(body, {
    headers: { "content-type": "application/json" },
  });
}

function jsonRpcError(
  id: string | number | null,
  code: number,
  message: string,
): Response {
  return Response.json({ jsonrpc: "2.0", id, error: { code, message } });
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
