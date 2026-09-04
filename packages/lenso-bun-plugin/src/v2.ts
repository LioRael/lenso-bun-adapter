import { timingSafeEqual } from "node:crypto";
import { createInterface } from "node:readline";

import type { InvocationContext } from "@lenso/contract-runtime";
import {
  AUTHORING_API_VERSION,
  authoringCallbackProofMessage,
  authoringChildProofMessage,
  authoringHandshakeProofPayload,
  authoringHostProofMessage,
  decodeBase64Url32,
  encodeBase64Url,
  validateAuthoringMessage,
  validateInitializeForRuntimeProfile,
  validateInvoke,
  validateResultFor,
  type AuthoringCancelParams,
  type CancelAck,
  type ConstructParams,
  type ConstructedResult,
  type InitializeParams,
  type InvocationOutcome,
  type InvocationResult,
  type InvocationScope,
  type InvokeParams,
  type OutboundCallParams,
  type OutboundCallResult,
  type RouteDescriptor,
  type Settlement,
  type StopParams,
  type StoppedResult,
} from "@lenso/process-protocol";

import type {
  BoundCapabilityClient,
  DependencyCardinality,
  DependencyDeclaration,
  DependencyDeclarations,
  DependencyInvoker,
  LifecycleContext,
  PluginDefinition,
  PluginInputs,
} from "./authoring.js";
import type { CapabilityProviderBinding, ProviderDispatchOutcome } from "./index.js";

export const BUN_AUTHORING_RUNTIME_PROFILE = "lenso.bun-authoring@2";
export const BUN_AUTHORING_CALLBACK_PROOF_HEADER = "x-lenso-authoring-proof";

interface Bootstrap {
  readonly callback_origin: string;
  readonly bootstrap_secret: string;
}

interface InitializeRequest {
  readonly initialize: InitializeParams;
  readonly callback_origin: string;
  readonly host_nonce: string;
  readonly host_proof: string;
}

interface InitializedResponse {
  readonly initialized: InitializeParams;
  readonly child_nonce: string;
  readonly child_proof: string;
}

interface JsonRpcRequest {
  readonly jsonrpc: "2.0";
  readonly id: string | number;
  readonly method: string;
  readonly params: unknown;
}

export interface BunInvocationContext extends InvocationContext {
  readonly signal: AbortSignal;
  remainingTimeoutMs(): number;
}

const remainingNanos = Symbol("lenso.remaining-nanos");
type InternalInvocationContext = BunInvocationContext & {
  readonly [remainingNanos]: () => string;
};

interface ActiveInvocation {
  readonly params: InvokeParams;
  readonly controller: AbortController;
  readonly context: BunInvocationContext;
}

/** Runs one statically built complete-object Plugin over authenticated JSON-RPC/HTTP. */
export async function servePluginV2<
  Instance extends object,
  Config extends import("./authoring.js").ConfigDeclaration<unknown> | undefined,
  Dependencies extends DependencyDeclarations | undefined,
>(
  definition: PluginDefinition<Instance, Config, Dependencies>,
  runtimeProfile = BUN_AUTHORING_RUNTIME_PROFILE,
): Promise<void> {
  const bootstrap = await readBootstrap();
  await new BunAuthoringServer(definition, runtimeProfile, bootstrap).serve();
}

class BunAuthoringServer<
  Instance extends object,
  Config extends import("./authoring.js").ConfigDeclaration<unknown> | undefined,
  Dependencies extends DependencyDeclarations | undefined,
> {
  readonly #active = new Map<string, ActiveInvocation>();
  readonly #contexts = new WeakMap<InvocationContext, ActiveInvocation>();
  readonly #retired = new Set<string>();
  readonly #bootstrapSecret: Uint8Array;
  #initialize: InitializeParams | undefined;
  #instance: Instance | undefined;
  #providers = new Map<string, CapabilityProviderBinding>();
  #constructionAttempted = false;
  #stopAttempted = false;
  #nextOutboundId = 1n;
  #nextCallbackId = 1n;
  #activeOutboundCalls = 0;
  #server: { stop(closeActiveConnections?: boolean): void } | undefined;
  #finish: (() => void) | undefined;

  constructor(
    readonly definition: PluginDefinition<Instance, Config, Dependencies>,
    readonly runtimeProfile: string,
    readonly bootstrap: Bootstrap,
  ) {
    this.#bootstrapSecret = decodeBase64Url32(
      bootstrap.bootstrap_secret,
      "bootstrap_secret",
    );
  }

  async serve(): Promise<void> {
    const server = Bun.serve({
      hostname: "127.0.0.1",
      port: 0,
      fetch: (request) => this.#fetch(request),
    });
    this.#server = server;
    await writePrivateLine({ protocol: BUN_AUTHORING_RUNTIME_PROFILE, port: server.port });
    await new Promise<void>((resolve) => {
      this.#finish = resolve;
    });
  }

  async #fetch(request: Request): Promise<Response> {
    if (request.method !== "POST") return new Response(null, { status: 405 });
    const limit = this.#initialize?.limits.max_frame_bytes ?? 1_048_576;
    const bytes = new Uint8Array(await request.arrayBuffer());
    if (bytes.byteLength > limit) return new Response(null, { status: 413 });
    let id: string | number | null = null;
    try {
      const rpc = JSON.parse(new TextDecoder().decode(bytes)) as JsonRpcRequest;
      id = rpc.id;
      if (rpc.jsonrpc !== "2.0" || id === undefined || typeof rpc.method !== "string") {
        throw new Error("invalid JSON-RPC request");
      }
      const result = await this.#dispatch(rpc.method, rpc.params);
      return boundedJsonResponse({ jsonrpc: "2.0", id, result }, limit);
    } catch (error) {
      return boundedJsonResponse({
        jsonrpc: "2.0",
        id,
        error: { code: -32602, message: boundedError(error) },
      }, limit);
    }
  }

  async #dispatch(method: string, params: unknown): Promise<unknown> {
    switch (method) {
      case "lenso.initialize": return this.#initializeSession(params as InitializeRequest);
      case "lenso.construct": return this.#construct(params as ConstructParams);
      case "lenso.invoke": return this.#invoke(params as InvokeParams);
      case "lenso.cancel": return this.#cancel(params as AuthoringCancelParams);
      case "lenso.stop": return this.#stop(params as StopParams);
      default: throw new Error(`unknown Bun Authoring method ${method}`);
    }
  }

  async #initializeSession(request: InitializeRequest): Promise<InitializedResponse> {
    if (this.#initialize !== undefined) throw new Error("Bun Authoring V2 initialized more than once");
    if (request.callback_origin !== this.bootstrap.callback_origin) {
      throw new Error("callback origin does not match the private bootstrap");
    }
    const payload = authoringHandshakeProofPayload({
      initialize: request.initialize,
      callback_origin: request.callback_origin,
      host_nonce: request.host_nonce,
    });
    const digest = sha256(payload);
    await verifyProof(
      this.#bootstrapSecret,
      authoringHostProofMessage(digest),
      request.host_proof,
      "host_proof",
    );
    this.#validateDefinition(request.initialize);
    this.#initialize = request.initialize;
    const childNonce = randomBase64Url32();
    return {
      initialized: request.initialize,
      child_nonce: childNonce,
      child_proof: await createProof(
        this.#bootstrapSecret,
        authoringChildProofMessage(digest, childNonce),
      ),
    };
  }

  #validateDefinition(initialize: InitializeParams): void {
    validateInitializeForRuntimeProfile(initialize, this.runtimeProfile);
    if (initialize.api_version !== AUTHORING_API_VERSION) throw new Error("unsupported Authoring API");
    const declaredDependencies = this.definition.dependencies ?? {};
    const names = Object.values(declaredDependencies)
      .map((raw) => (raw as DependencyDeclaration<unknown, DependencyCardinality>).id)
      .sort();
    const admittedNames = initialize.required_declarations.map((value) => value.requirement_id);
    if (JSON.stringify(names) !== JSON.stringify(admittedNames)) {
      throw new Error("admitted named requirements do not match the Plugin declaration");
    }
    for (const [name, raw] of Object.entries(declaredDependencies)) {
      const declaration = raw as DependencyDeclaration<unknown, DependencyCardinality>;
      const admitted = initialize.required_declarations.find(
        (candidate) => candidate.requirement_id === declaration.id,
      );
      if (
        admitted === undefined || declaration.cardinality !== admitted.cardinality ||
        declaration.contract.descriptor.capability_id !== admitted.capability_id ||
        declaration.contract.descriptor.descriptor_version !== admitted.descriptor_version ||
        (declaration.contract.descriptor.descriptor_digest !== undefined &&
          declaration.contract.descriptor.descriptor_digest !== admitted.descriptor_digest)
      ) {
        throw new Error(`admitted requirement ${name} does not match its declaration`);
      }
    }
    const declaredProviders = this.definition.providers.map((provider) => provider.descriptor);
    if (declaredProviders.length !== initialize.provided_endpoints.length) {
      throw new Error("admitted endpoints do not match the Plugin declaration");
    }
    for (const endpoint of initialize.provided_endpoints) {
      const descriptor = declaredProviders.find(
        (candidate) => candidate.capability_id === endpoint.capability_id,
      );
      if (
        descriptor?.descriptor_version !== endpoint.descriptor_version ||
        (descriptor.descriptor_digest !== undefined &&
          descriptor.descriptor_digest !== endpoint.descriptor_digest)
      ) {
        throw new Error(`admitted endpoint ${endpoint.endpoint_id} does not match its declaration`);
      }
    }
  }

  async #construct(params: ConstructParams): Promise<ConstructedResult> {
    const initialize = this.#requireInitialize();
    validateAuthoringMessage(params, "construct", initialize.identity);
    if (this.#constructionAttempted) throw new Error("Bun Plugin constructed more than once");
    this.#constructionAttempted = true;
    try {
      const controller = new AbortController();
      const inputs = this.#inputs(initialize);
      const instance = this.definition.create === undefined
        ? (inputs as unknown as Instance)
        : await this.definition.create(
          inputs,
          lifecycleContext(params.remaining_budget_nanos, controller.signal),
        );
      if (typeof instance !== "object" || instance === null) {
        throw new Error("Plugin factory must return one complete object");
      }
      this.#providers = bindProviders(this.definition, instance, initialize);
      this.#instance = instance;
      return {
        session: params.session,
        lifecycle_scope_id: params.lifecycle_scope_id,
        outcome: { kind: "constructed" },
      };
    } catch (error) {
      return {
        session: params.session,
        lifecycle_scope_id: params.lifecycle_scope_id,
        outcome: { kind: "failed", detail: boundedError(error) },
      };
    }
  }

  #inputs(initialize: InitializeParams): PluginInputs<Config, Dependencies> {
    const dependencies: Record<string, unknown> = {};
    for (const [name, raw] of Object.entries(this.definition.dependencies ?? {})) {
      const declaration = raw as DependencyDeclaration<unknown, DependencyCardinality>;
      const routes = initialize.routes.filter((route) => route.requirement_id === declaration.id);
      const clients = routes.map((route) => this.#boundClient(declaration, route));
      dependencies[name] = declaration.cardinality === "many"
        ? Object.freeze(clients)
        : declaration.cardinality === "optional"
          ? clients[0]?.client
          : clients[0]!.client;
    }
    const config = this.definition.config?.parse(initialize.config);
    return {
      ...(this.definition.config === undefined ? {} : { config }),
      ...(this.definition.dependencies === undefined ? {} : { dependencies }),
    } as PluginInputs<Config, Dependencies>;
  }

  #boundClient(
    declaration: DependencyDeclaration<unknown, DependencyCardinality>,
    route: RouteDescriptor,
  ): BoundCapabilityClient<unknown> {
    const invoke: DependencyInvoker = async (operation, context, payload) => {
      const parent = this.#contexts.get(context);
      if (parent === undefined) throw new Error("Host calls require the active invocation context");
      return this.#outboundCall(parent, route, operation, payload);
    };
    return {
      providerInstance: route.provider_instance,
      client: declaration.contract.createClient(invoke),
    };
  }

  async #invoke(params: InvokeParams): Promise<InvocationResult> {
    const initialize = this.#requireInitialize();
    validateInvoke(params, initialize);
    if (this.#instance === undefined) throw new Error("Bun Plugin invoked before construction");
    if (this.#active.has(params.correlation_id) || this.#retired.has(params.correlation_id)) {
      throw new Error("Bun Plugin reused a correlation id");
    }
    if (this.#retired.size >= initialize.limits.max_retired_ids) {
      throw new Error("Bun Plugin retired identity limit exhausted");
    }
    if (this.#active.size >= initialize.limits.max_active_invocations) {
      const outcome: InvocationOutcome = {
        kind: "runtime",
        failure: {
          kind: "resource_exhausted",
          capability: params.capability_id,
          operation: params.operation,
        },
      };
      await this.#settled(params, "completed");
      return { session: params.session, correlation_id: params.correlation_id, outcome };
    }
    const controller = new AbortController();
    const context = invocationContext(params.scope, params.correlation_id, controller.signal);
    const active = { params, controller, context };
    this.#active.set(params.correlation_id, active);
    this.#contexts.set(context, active);
    return this.#runInvocation(active);
  }

  async #runInvocation(active: ActiveInvocation): Promise<InvocationResult> {
    let state: Settlement["state"] = "completed";
    let outcome: InvocationOutcome;
    try {
      const provider = this.#providers.get(active.params.endpoint_id);
      if (provider === undefined) throw new Error("unknown admitted endpoint");
      const result = await provider.invokeRequest(
        active.params.operation,
        active.context,
        active.params.payload,
      );
      outcome = toWireOutcome(result, active.params);
      if (active.controller.signal.aborted) state = "cancelled";
    } catch (error) {
      state = "abandoned";
      outcome = {
        kind: "runtime",
        failure: { kind: "plugin_failure", detail: boundedError(error) },
      };
    }
    this.#active.delete(active.params.correlation_id);
    this.#retired.add(active.params.correlation_id);
    await this.#settled(active.params, state);
    return {
      session: active.params.session,
      correlation_id: active.params.correlation_id,
      outcome,
    };
  }

  async #settled(params: InvokeParams, state: Settlement["state"]): Promise<void> {
    await this.#callback("lenso.settled", {
      session: params.session,
      scope_id: params.scope.scope_id,
      correlation_id: params.correlation_id,
      state,
    } satisfies Settlement);
  }

  async #outboundCall(
    parent: ActiveInvocation,
    route: RouteDescriptor,
    operation: string,
    payload: unknown,
  ): Promise<ProviderDispatchOutcome> {
    const initialize = this.#requireInitialize();
    if (parent.controller.signal.aborted) {
      return { kind: "runtime", failure: { kind: "cancelled", detail: parent.params.correlation_id } };
    }
    if (this.#activeOutboundCalls >= initialize.limits.max_active_outbound_calls) {
      return { kind: "runtime", failure: { kind: "resource_exhausted", detail: operation } };
    }
    this.#activeOutboundCalls += 1;
    const correlationId = (this.#nextOutboundId++).toString();
    const request: OutboundCallParams = {
      session: initialize.identity.session,
      correlation_id: correlationId,
      requirement_id: route.requirement_id,
      route_id: route.route_id,
      operation,
      scope: {
        scope_id: `${parent.params.scope.scope_id}:outbound:${correlationId}`,
        parent_scope_id: parent.params.scope.scope_id,
        remaining_budget_nanos: remainingBudgetNanos(parent.context),
        permissions: parent.params.scope.permissions,
        extensions: parent.params.scope.extensions,
      },
      payload,
    };
    try {
      const result = await this.#callback("lenso.call", request) as OutboundCallResult;
      validateResultFor(result, initialize.identity, correlationId);
      return fromWireOutcome(result.outcome);
    } finally {
      this.#activeOutboundCalls -= 1;
    }
  }

  async #cancel(params: AuthoringCancelParams): Promise<CancelAck> {
    const initialize = this.#requireInitialize();
    validateAuthoringMessage(params, "cancel", initialize.identity);
    const active = this.#active.get(params.correlation_id);
    const accepted = active !== undefined && active.params.scope.scope_id === params.scope_id;
    if (accepted) active.controller.abort(params.reason);
    return {
      session: params.session,
      scope_id: params.scope_id,
      correlation_id: params.correlation_id,
      accepted,
    };
  }

  async #stop(params: StopParams): Promise<StoppedResult> {
    const initialize = this.#requireInitialize();
    validateAuthoringMessage(params, "stop", initialize.identity);
    if (this.#stopAttempted) throw new Error("Bun Plugin stopped more than once");
    this.#stopAttempted = true;
    if (this.#active.size > 0 || this.#activeOutboundCalls > 0) {
      throw new Error("Bun Plugin stopped with unfinished work");
    }
    if (this.#instance === undefined) throw new Error("Bun Plugin stopped before construction");
    let hook: StoppedResult["hook"] = "not_declared";
    const diagnostics: Array<{ readonly code: string; readonly detail: string }> = [];
    if (this.definition.stop !== undefined) {
      try {
        const controller = new AbortController();
        await this.definition.stop(
          this.#instance,
          lifecycleContext(params.remaining_budget_nanos, controller.signal),
        );
        hook = "completed";
      } catch (error) {
        hook = "failed";
        diagnostics.push({ code: "plugin_stop_failed", detail: boundedError(error) });
      }
    }
    const result: StoppedResult = {
      session: params.session,
      cleanup_scope_id: params.cleanup_scope_id,
      hook,
      diagnostics,
    };
    setTimeout(() => {
      this.#server?.stop();
      this.#finish?.();
    }, 0);
    return result;
  }

  async #callback(method: "lenso.call" | "lenso.settled", params: unknown): Promise<unknown> {
    const initialize = this.#requireInitialize();
    const proof = await createProof(
      this.#bootstrapSecret,
      authoringCallbackProofMessage(initialize.identity.session, method, params),
    );
    const response = await fetch(this.bootstrap.callback_origin, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        [BUN_AUTHORING_CALLBACK_PROOF_HEADER]: proof,
      },
      body: JSON.stringify({
        jsonrpc: "2.0",
        id: (this.#nextCallbackId++).toString(),
        method,
        params,
      }),
    });
    if (!response.ok) throw new Error(`Host callback returned HTTP ${response.status}`);
    const rpc = await response.json() as {
      readonly result?: unknown;
      readonly error?: { readonly message?: string };
    };
    if (rpc.error !== undefined) throw new Error(rpc.error.message ?? "Host callback failed");
    return rpc.result;
  }

  #requireInitialize(): InitializeParams {
    if (this.#initialize === undefined) throw new Error("Bun Plugin is not initialized");
    return this.#initialize;
  }
}

async function readBootstrap(): Promise<Bootstrap> {
  const lines = createInterface({ input: process.stdin, crlfDelay: Infinity });
  for await (const line of lines) {
    lines.close();
    if (new TextEncoder().encode(line).byteLength > 4_096) {
      throw new Error("Bun Authoring bootstrap exceeds its private-channel limit");
    }
    const value = JSON.parse(line) as Bootstrap;
    if (
      JSON.stringify(Object.keys(value).sort()) !==
        JSON.stringify(["bootstrap_secret", "callback_origin"]) ||
      typeof value.callback_origin !== "string" ||
      typeof value.bootstrap_secret !== "string"
    ) {
      throw new Error("invalid Bun Authoring bootstrap");
    }
    return value;
  }
  throw new Error("Bun Authoring bootstrap channel closed before initialization");
}

function writePrivateLine(value: unknown): Promise<void> {
  return new Promise((resolve, reject) => {
    process.stdout.write(`${JSON.stringify(value)}\n`, (error) =>
      error === null ? resolve() : reject(error));
  });
}

function boundedJsonResponse(value: unknown, maxBytes: number): Response {
  const body = JSON.stringify(value);
  if (new TextEncoder().encode(body).byteLength > maxBytes) {
    return new Response(null, { status: 507 });
  }
  return new Response(body, { headers: { "content-type": "application/json" } });
}

function sha256(value: Uint8Array): Uint8Array {
  return new Uint8Array(new Bun.CryptoHasher("sha256").update(value).digest());
}

async function createProof(secret: Uint8Array, message: Uint8Array): Promise<string> {
  const secretBytes = Uint8Array.from(secret);
  const messageBytes = Uint8Array.from(message);
  const key = await crypto.subtle.importKey(
    "raw",
    secretBytes,
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  return encodeBase64Url(
    new Uint8Array(await crypto.subtle.sign("HMAC", key, messageBytes)),
  );
}

async function verifyProof(
  secret: Uint8Array,
  message: Uint8Array,
  proof: string,
  name: string,
): Promise<void> {
  const expected = decodeBase64Url32(await createProof(secret, message), name);
  const received = decodeBase64Url32(proof, name);
  if (!timingSafeEqual(expected, received)) throw new Error(`${name} does not authenticate`);
}

function randomBase64Url32(): string {
  return encodeBase64Url(crypto.getRandomValues(new Uint8Array(32)));
}

function bindProviders<
  Instance extends object,
  Config extends import("./authoring.js").ConfigDeclaration<unknown> | undefined,
  Dependencies extends DependencyDeclarations | undefined,
>(
  definition: PluginDefinition<Instance, Config, Dependencies>,
  instance: Instance,
  initialize: InitializeParams,
): Map<string, CapabilityProviderBinding> {
  const bindings = new Map<string, CapabilityProviderBinding>();
  for (const endpoint of initialize.provided_endpoints) {
    const declaration = definition.providers.find(
      (provider) => provider.descriptor.capability_id === endpoint.capability_id,
    );
    if (declaration === undefined || !("kind" in declaration) || declaration.kind !== "lenso.provider") {
      throw new Error(`endpoint ${endpoint.endpoint_id} requires an instance binder`);
    }
    const binding = declaration.bind(instance);
    if (
      binding.descriptor.capability_id !== endpoint.capability_id ||
      binding.descriptor.descriptor_version !== endpoint.descriptor_version
    ) {
      throw new Error(`binder for ${endpoint.endpoint_id} returned another contract`);
    }
    bindings.set(endpoint.endpoint_id, binding);
  }
  return bindings;
}

function invocationContext(
  scope: InvocationScope,
  correlationId: string,
  signal: AbortSignal,
): InternalInvocationContext {
  const budget = remainingBudget(scope.remaining_budget_nanos);
  return Object.freeze({
    requestId: correlationId as InvocationContext["requestId"],
    get cancelled() { return signal.aborted; },
    signal,
    remainingTimeoutMs: budget.milliseconds,
    [remainingNanos]: budget.nanoseconds,
    ...(scope.extensions.length === 0
      ? {}
      : { extensions: Object.fromEntries(scope.extensions.map((value) => [value.key, value])) }),
  });
}

function lifecycleContext(remaining: string, signal: AbortSignal): LifecycleContext {
  const budget = remainingBudget(remaining);
  return Object.freeze({ signal, remainingTimeoutMs: budget.milliseconds });
}

function remainingBudget(initial: string): {
  readonly nanoseconds: () => string;
  readonly milliseconds: () => number;
} {
  const initialNanos = BigInt(initial);
  const started = performance.now();
  const nanoseconds = (): string => {
    const elapsed = BigInt(Math.max(0, Math.floor((performance.now() - started) * 1_000_000)));
    return (elapsed >= initialNanos ? 0n : initialNanos - elapsed).toString();
  };
  return {
    nanoseconds,
    milliseconds: () =>
      Math.max(0, Math.min(Number.MAX_SAFE_INTEGER, Number(BigInt(nanoseconds())) / 1_000_000)),
  };
}

function remainingBudgetNanos(context: BunInvocationContext): string {
  return (context as InternalInvocationContext)[remainingNanos]();
}

function toWireOutcome(
  outcome: ProviderDispatchOutcome,
  invocation: InvokeParams,
): InvocationOutcome {
  switch (outcome.kind) {
    case "success": return { kind: "success", value: outcome.value };
    case "domain": return { kind: "domain", error: outcome.value };
    case "runtime": return normalizeRuntimeFailure(outcome.failure, invocation);
  }
}

function normalizeRuntimeFailure(
  failure: import("@lenso/contract-runtime").RuntimeFailure,
  invocation: InvokeParams,
): InvocationOutcome {
  switch (failure.kind) {
    case "cancelled":
    case "deadline_exceeded":
      return { kind: "runtime", failure: { kind: failure.kind, request_id: invocation.correlation_id } };
    case "resource_exhausted":
      return { kind: "runtime", failure: { kind: "resource_exhausted", capability: invocation.capability_id, operation: invocation.operation } };
    case "unknown_operation":
      return { kind: "runtime", failure: { kind: "unknown_operation", capability: invocation.capability_id, operation: invocation.operation } };
    case "protocol_violation":
      return { kind: "runtime", failure: { kind: "protocol_violation", capability: invocation.capability_id } };
    case "admission_closed":
      return { kind: "runtime", failure: { kind: "admission_closed" } };
    default:
      return { kind: "runtime", failure: { kind: "plugin_failure", detail: boundedError(failure.detail ?? failure.kind) } };
  }
}

function fromWireOutcome(outcome: InvocationOutcome): ProviderDispatchOutcome {
  switch (outcome.kind) {
    case "success": return { kind: "success", value: outcome.value };
    case "domain": return { kind: "domain", value: outcome.error };
    case "runtime": return { kind: "runtime", failure: { kind: outcome.failure.kind, detail: outcome.failure } };
  }
}

function boundedError(error: unknown): string {
  const value = error instanceof Error ? error.message : String(error);
  const nonempty = value.length === 0 ? "Plugin returned an empty failure detail" : value;
  return new TextDecoder().decode(new TextEncoder().encode(nonempty).slice(0, 1_024));
}
