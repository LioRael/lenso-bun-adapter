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
  validateEventPublishResultFor,
  validateInitializeForRuntimeProfile,
  validateEventPublish,
  validateInvoke,
  validateStreamAction,
  validateStreamOpen,
  validateStreamOpenResultFor,
  validateStreamResultFor,
  validateResultFor,
  type AuthoringCancelParams,
  type CancelAck,
  type ConstructParams,
  type ConstructedResult,
  type EventPublishParams,
  type EventPublishResult,
  type InitializeParams,
  type InvocationOutcome,
  type InvocationResult,
  type InvocationScope,
  type InvokeParams,
  type OutboundCallParams,
  type OutboundCallResult,
  type OutboundEventPublishParams,
  type OutboundEventPublishResult,
  type OutboundStreamOpenParams,
  type OutboundStreamOpenResult,
  type RouteDescriptor,
  type Settlement,
  type StreamActionResult,
  type StreamOpenParams,
  type StreamOpenResult,
  type StreamReceiveParams,
  type StreamReceiveResult,
  type StreamSendParams,
  type StopParams,
  type StoppedResult,
} from "@lenso/process-protocol";

import type {
  BoundCapabilityClient,
  DependencyCardinality,
  DependencyDeclaration,
  DependencyDeclarations,
  InteractionDependencyInvoker,
  LifecycleContext,
  PluginDefinition,
  PluginInputs,
} from "./authoring.js";
import type {
  CapabilityProviderBinding,
  ProviderDispatchOutcome,
  ProviderEventPublishOutcome,
  ProviderStreamOpenOutcome,
  ProviderStreamSessionBinding,
} from "./index.js";

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

interface ActiveContext {
  readonly params: { readonly scope: InvocationScope };
  readonly controller: AbortController;
  readonly context: BunInvocationContext;
}

interface ActiveInvocation extends ActiveContext {
  readonly params: InvokeParams;
}

interface ActiveStreamOpen extends ActiveContext {
  readonly params: StreamOpenParams;
}

interface ActiveStream {
  readonly binding: ProviderStreamSessionBinding;
  readonly context: BunInvocationContext;
  nextSendSequence: bigint;
  nextReceiveSequence: bigint;
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
  readonly #active = new Map<string, ActiveInvocation | ActiveStreamOpen>();
  readonly #streams = new Map<string, ActiveStream>();
  readonly #outboundStreams = new Map<string, () => void>();
  readonly #contexts = new WeakMap<InvocationContext, ActiveContext>();
  readonly #retired = new Set<string>();
  readonly #bootstrapSecret: Uint8Array;
  #initialize: InitializeParams | undefined;
  #instance: Instance | undefined;
  #providers = new Map<string, CapabilityProviderBinding>();
  #constructionAttempted = false;
  #stopAttempted = false;
  #nextOutboundId = 1n;
  #nextCallbackId = 1n;
  #nextStreamId = 1n;
  #activeOutboundCalls = 0;
  #activeEventPublications = 0;
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
      const params = Array.isArray(rpc.params) && rpc.params.length === 1
        ? rpc.params[0]
        : rpc.params;
      const result = await this.#dispatch(rpc.method, params);
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
      case "lenso.event.publish": return this.#publishEvent(params as EventPublishParams);
      case "lenso.stream.open": return this.#openStream(params as StreamOpenParams);
      case "lenso.stream.send": return this.#sendStream(params as StreamSendParams);
      case "lenso.stream.receive": return this.#receiveStream(params as StreamReceiveParams);
      case "lenso.stream.close_send": return this.#closeStreamSend(params as StreamReceiveParams);
      case "lenso.stream.cancel": return this.#cancelStream(params as StreamReceiveParams);
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
    const names = Object.entries(declaredDependencies)
      .map(([name, raw]) =>
        (raw as DependencyDeclaration<unknown, DependencyCardinality>).id ?? name
      )
      .sort();
    const admittedNames = initialize.required_declarations.map((value) => value.requirement_id);
    if (JSON.stringify(names) !== JSON.stringify(admittedNames)) {
      throw new Error("admitted named requirements do not match the Plugin declaration");
    }
    for (const [name, raw] of Object.entries(declaredDependencies)) {
      const declaration = raw as DependencyDeclaration<unknown, DependencyCardinality>;
      const admitted = initialize.required_declarations.find(
        (candidate) => candidate.requirement_id === (declaration.id ?? name),
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
    const active = lifecycleActive(
      params.lifecycle_scope_id,
      params.remaining_budget_nanos,
    );
    this.#contexts.set(active.context, active);
    try {
      const inputs = this.#inputs(initialize);
      const instance = this.definition.create === undefined
        ? (inputs as unknown as Instance)
        : await this.definition.create(
          inputs,
          active.context,
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
    } finally {
      this.#contexts.delete(active.context);
    }
  }

  #inputs(initialize: InitializeParams): PluginInputs<Config, Dependencies> {
    const dependencies: Record<string, unknown> = {};
    for (const [name, raw] of Object.entries(this.definition.dependencies ?? {})) {
      const declaration = raw as DependencyDeclaration<unknown, DependencyCardinality>;
      const routes = initialize.routes.filter(
        (route) => route.requirement_id === (declaration.id ?? name),
      );
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
    const call = async (operation: string, context: InvocationContext, payload: unknown) => {
      const parent = this.#contexts.get(context);
      if (parent === undefined) throw new Error("Host calls require the active invocation context");
      return this.#outboundCall(parent, route, operation, payload);
    };
    const invoke = Object.assign(call, {
      providerInstance: route.provider_instance,
      openStream: async (operation: string, context: InvocationContext, payload: unknown) => {
        const parent = this.#contexts.get(context);
        if (parent === undefined) throw new Error("Host calls require the active invocation context");
        return this.#outboundStream(parent, route, operation, payload);
      },
      publishEvent: async (operation: string, context: InvocationContext, payload: unknown) => {
        const parent = this.#contexts.get(context);
        if (parent === undefined) throw new Error("Host calls require the active invocation context");
        return this.#outboundEvent(parent, route, operation, payload);
      },
    }) satisfies InteractionDependencyInvoker;
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
        this.#instance!,
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

  async #settled(
    params: { readonly session: string; readonly correlation_id: string; readonly scope: InvocationScope },
    state: Settlement["state"],
  ): Promise<void> {
    await this.#callback("lenso.settled", {
      session: params.session,
      scope_id: params.scope.scope_id,
      correlation_id: params.correlation_id,
      state,
    } satisfies Settlement);
  }

  async #publishEvent(params: EventPublishParams): Promise<EventPublishResult> {
    const initialize = this.#requireInitialize();
    validateEventPublish(params, initialize);
    if (this.#instance === undefined) throw new Error("Bun Plugin published to before construction");
    const controller = new AbortController();
    const context = invocationContext(params.scope, params.correlation_id, controller.signal);
    const active: ActiveContext = { params, controller, context };
    this.#contexts.set(context, active);
    const provider = this.#providers.get(params.endpoint_id);
    if (provider === undefined) throw new Error("unknown admitted endpoint");
    if (provider.publishEvent === undefined) throw new Error("Event operation has no provider binding");
    if (this.#activeEventPublications >= initialize.limits.max_queued_calls) {
      return {
        session: params.session,
        correlation_id: params.correlation_id,
        outcome: { kind: "runtime", failure: { kind: "resource_exhausted", capability: params.capability_id, operation: params.operation } },
      };
    }
    this.#activeEventPublications += 1;
    let outcome: EventPublishResult["outcome"];
    try {
      const result = await provider.publishEvent(
        params.operation,
        context,
        params.event,
        this.#instance,
      );
      outcome = result.kind === "accepted"
        ? result
        : { kind: "runtime", failure: wireFailure(result.failure, params) };
    } catch (error) {
      outcome = { kind: "runtime", failure: { kind: "plugin_failure", detail: boundedError(error) } };
    } finally {
      this.#activeEventPublications -= 1;
      this.#contexts.delete(context);
    }
    return { session: params.session, correlation_id: params.correlation_id, outcome };
  }

  async #openStream(params: StreamOpenParams): Promise<StreamOpenResult> {
    const initialize = this.#requireInitialize();
    validateStreamOpen(params, initialize);
    if (this.#instance === undefined) throw new Error("Bun Plugin streamed to before construction");
    if (this.#active.has(params.correlation_id) || this.#retired.has(params.correlation_id)) {
      throw new Error("Bun Plugin reused a correlation id");
    }
    if (
      this.#retired.size >= initialize.limits.max_retired_ids ||
      this.#active.size >= initialize.limits.max_active_invocations ||
      this.#streams.size >= initialize.limits.max_unfinished_executions
    ) {
      await this.#settled(params, "completed");
      return {
        session: params.session,
        correlation_id: params.correlation_id,
        outcome: { kind: "runtime", failure: { kind: "resource_exhausted", capability: params.capability_id, operation: params.operation } },
      };
    }
    const controller = new AbortController();
    const context = invocationContext(params.scope, params.correlation_id, controller.signal);
    const active: ActiveStreamOpen = { params, controller, context };
    this.#active.set(params.correlation_id, active);
    this.#contexts.set(context, active);
    let state: Settlement["state"] = "completed";
    let outcome: StreamOpenResult["outcome"];
    try {
      const provider = this.#providers.get(params.endpoint_id);
      if (provider === undefined) throw new Error("unknown admitted endpoint");
      if (provider.openStream === undefined) throw new Error("Stream operation has no provider binding");
      const result = await provider.openStream(
        params.operation,
        context,
        params.request,
        this.#instance,
      );
      if (result.kind === "opened" && controller.signal.aborted) {
        result.stream.cancel();
        outcome = {
          kind: "runtime",
          failure: { kind: "cancelled", request_id: params.correlation_id },
        };
      } else if (result.kind === "opened") {
        const streamId = (this.#nextStreamId++).toString();
        this.#streams.set(streamId, { binding: result.stream, context, nextSendSequence: 0n, nextReceiveSequence: 0n });
        outcome = { kind: "opened", stream_id: streamId };
      } else if (result.kind === "domain") {
        outcome = { kind: "domain", error: result.value };
      } else {
        outcome = { kind: "runtime", failure: wireFailure(result.failure, params) };
      }
      if (controller.signal.aborted) state = "cancelled";
    } catch (error) {
      state = "abandoned";
      outcome = { kind: "runtime", failure: { kind: "plugin_failure", detail: boundedError(error) } };
    }
    this.#active.delete(params.correlation_id);
    if (outcome.kind !== "opened") this.#contexts.delete(context);
    this.#retired.add(params.correlation_id);
    await this.#settled(params, state);
    return { session: params.session, correlation_id: params.correlation_id, outcome };
  }

  async #sendStream(params: StreamSendParams): Promise<StreamActionResult> {
    const initialize = this.#requireInitialize();
    validateStreamAction(params, initialize.identity, "stream_send");
    const stream = this.#streams.get(params.stream_id);
    if (stream !== undefined && params.sequence !== stream.nextSendSequence.toString()) {
      throw new Error("Bun Stream send sequence is not contiguous");
    }
    const outcome = stream === undefined
      ? { kind: "runtime", failure: { kind: "protocol_violation", capability: "lenso.bun-authoring" } } as const
      : await stream.binding.send(params.message).then((result) => result.kind === "accepted"
        ? result
        : { kind: "runtime" as const, failure: wireFailure(result.failure) });
    if (stream !== undefined && outcome.kind === "accepted") stream.nextSendSequence += 1n;
    return { session: params.session, correlation_id: params.correlation_id, stream_id: params.stream_id, outcome };
  }

  async #receiveStream(params: StreamReceiveParams): Promise<StreamReceiveResult> {
    const initialize = this.#requireInitialize();
    validateStreamAction(params, initialize.identity, "stream_receive");
    const stream = this.#streams.get(params.stream_id);
    let outcome: StreamReceiveResult["outcome"];
    if (stream === undefined) {
      outcome = { kind: "runtime", failure: { kind: "protocol_violation", capability: "lenso.bun-authoring" } };
    } else {
      const result = await stream.binding.receive();
      switch (result.kind) {
        case "message": outcome = { kind: "message", sequence: (stream.nextReceiveSequence++).toString(), message: result.value }; break;
        case "peer_half_closed": outcome = { kind: "peer_half_closed" }; break;
        case "terminal_success": outcome = { kind: "terminal", outcome: { kind: "success" } }; this.#streams.delete(params.stream_id); this.#contexts.delete(stream.context); break;
        case "terminal_domain": outcome = { kind: "terminal", outcome: { kind: "domain", error: result.value } }; this.#streams.delete(params.stream_id); this.#contexts.delete(stream.context); break;
        case "runtime": outcome = { kind: "runtime", failure: wireFailure(result.failure) }; break;
      }
    }
    return { session: params.session, correlation_id: params.correlation_id, stream_id: params.stream_id, outcome };
  }

  async #closeStreamSend(params: StreamReceiveParams): Promise<StreamActionResult> {
    const initialize = this.#requireInitialize();
    validateStreamAction(params, initialize.identity, "stream_close_send");
    const stream = this.#streams.get(params.stream_id);
    const outcome = stream === undefined
      ? { kind: "runtime", failure: { kind: "protocol_violation", capability: "lenso.bun-authoring" } } as const
      : await stream.binding.closeSend().then((result) => result.kind === "accepted"
        ? result
        : { kind: "runtime" as const, failure: wireFailure(result.failure) });
    return { session: params.session, correlation_id: params.correlation_id, stream_id: params.stream_id, outcome };
  }

  async #cancelStream(params: StreamReceiveParams): Promise<StreamActionResult> {
    const initialize = this.#requireInitialize();
    validateStreamAction(params, initialize.identity, "stream_cancel");
    const stream = this.#streams.get(params.stream_id);
    stream?.binding.cancel();
    if (stream !== undefined) this.#contexts.delete(stream.context);
    this.#streams.delete(params.stream_id);
    return { session: params.session, correlation_id: params.correlation_id, stream_id: params.stream_id, outcome: { kind: "accepted" } };
  }

  async #outboundCall(
    parent: ActiveContext,
    route: RouteDescriptor,
    operation: string,
    payload: unknown,
  ): Promise<ProviderDispatchOutcome> {
    const initialize = this.#requireInitialize();
    if (parent.controller.signal.aborted) {
      return {
        kind: "runtime",
        failure: { kind: "cancelled", detail: String(parent.context.requestId) },
      };
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
      scope: outboundScope(parent, correlationId),
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

  async #outboundEvent(
    parent: ActiveContext,
    route: RouteDescriptor,
    operation: string,
    event: unknown,
  ): Promise<ProviderEventPublishOutcome> {
    const initialize = this.#requireInitialize();
    if (parent.controller.signal.aborted) {
      return { kind: "runtime", failure: { kind: "cancelled", request_id: String(parent.context.requestId) } };
    }
    if (this.#activeOutboundCalls >= initialize.limits.max_active_outbound_calls) {
      return { kind: "runtime", failure: { kind: "resource_exhausted", capability: route.capability_id, operation } };
    }
    this.#activeOutboundCalls += 1;
    const correlationId = (this.#nextOutboundId++).toString();
    const request: OutboundEventPublishParams = {
      session: initialize.identity.session,
      correlation_id: correlationId,
      requirement_id: route.requirement_id,
      route_id: route.route_id,
      operation,
      scope: outboundScope(parent, correlationId),
      event,
    };
    try {
      const result = await this.#callback("lenso.event.publish", request) as OutboundEventPublishResult;
      validateEventPublishResultFor(result, initialize.identity, correlationId);
      return result.outcome;
    } finally {
      this.#activeOutboundCalls -= 1;
    }
  }

  async #outboundStream(
    parent: ActiveContext,
    route: RouteDescriptor,
    operation: string,
    requestPayload: unknown,
  ): Promise<ProviderStreamOpenOutcome> {
    const initialize = this.#requireInitialize();
    if (parent.controller.signal.aborted) {
      return { kind: "runtime", failure: { kind: "cancelled", request_id: String(parent.context.requestId) } };
    }
    if (
      this.#activeOutboundCalls >= initialize.limits.max_active_outbound_calls ||
      this.#outboundStreams.size >= initialize.limits.max_unfinished_executions
    ) {
      return { kind: "runtime", failure: { kind: "resource_exhausted", capability: route.capability_id, operation } };
    }
    this.#activeOutboundCalls += 1;
    const correlationId = (this.#nextOutboundId++).toString();
    const request: OutboundStreamOpenParams = {
      session: initialize.identity.session,
      correlation_id: correlationId,
      requirement_id: route.requirement_id,
      route_id: route.route_id,
      operation,
      scope: outboundScope(parent, correlationId),
      request: requestPayload,
    };
    try {
      const result = await this.#callback("lenso.stream.open", request) as OutboundStreamOpenResult;
      validateStreamOpenResultFor(result, initialize.identity, correlationId);
      if (result.outcome.kind === "domain") return { kind: "domain", value: result.outcome.error };
      if (result.outcome.kind === "runtime") return result.outcome;

      const streamId = result.outcome.stream_id;
      let nextSendSequence = 0n;
      let closed = false;
      const finish = () => {
        closed = true;
        this.#outboundStreams.delete(streamId);
      };
      const action = async (
        method: "lenso.stream.send" | "lenso.stream.close_send",
        params: StreamSendParams | StreamReceiveParams,
      ) => {
        if (closed) return { kind: "runtime", failure: { kind: "protocol_violation", capability: route.capability_id } } as const;
        if (this.#activeOutboundCalls >= initialize.limits.max_active_outbound_calls) {
          return { kind: "runtime", failure: { kind: "resource_exhausted", capability: route.capability_id } } as const;
        }
        this.#activeOutboundCalls += 1;
        try {
          const result = await this.#callback(method, params) as StreamActionResult;
          validateStreamResultFor(result, initialize.identity, params.correlation_id, streamId, "stream_action_result");
          if (result.outcome.kind === "runtime") finish();
          return result.outcome;
        } finally {
          this.#activeOutboundCalls -= 1;
        }
      };
      const binding: ProviderStreamSessionBinding = {
        send: async (message) => {
          const actionId = (this.#nextOutboundId++).toString();
          const outcome = await action("lenso.stream.send", {
            session: initialize.identity.session,
            correlation_id: actionId,
            stream_id: streamId,
            sequence: nextSendSequence.toString(),
            message,
          });
          if (outcome.kind === "accepted") nextSendSequence += 1n;
          return outcome;
        },
        receive: async () => {
          if (closed) return { kind: "runtime", failure: { kind: "protocol_violation", capability: route.capability_id } };
          if (this.#activeOutboundCalls >= initialize.limits.max_active_outbound_calls) {
            return { kind: "runtime", failure: { kind: "resource_exhausted", capability: route.capability_id } };
          }
          this.#activeOutboundCalls += 1;
          const actionId = (this.#nextOutboundId++).toString();
          try {
            const result = await this.#callback("lenso.stream.receive", {
              session: initialize.identity.session,
              correlation_id: actionId,
              stream_id: streamId,
            } satisfies StreamReceiveParams) as StreamReceiveResult;
            validateStreamResultFor(result, initialize.identity, actionId, streamId, "stream_receive_result");
            switch (result.outcome.kind) {
              case "message": return { kind: "message", value: result.outcome.message };
              case "peer_half_closed": return { kind: "peer_half_closed" };
              case "terminal":
                finish();
                return result.outcome.outcome.kind === "success"
                  ? { kind: "terminal_success" }
                  : { kind: "terminal_domain", value: result.outcome.outcome.error };
              case "runtime": finish(); return result.outcome;
            }
          } finally {
            this.#activeOutboundCalls -= 1;
          }
        },
        closeSend: async () => {
          const actionId = (this.#nextOutboundId++).toString();
          return action("lenso.stream.close_send", {
            session: initialize.identity.session,
            correlation_id: actionId,
            stream_id: streamId,
          });
        },
        cancel: () => {
          if (closed) return;
          finish();
          const actionId = (this.#nextOutboundId++).toString();
          void this.#callback("lenso.stream.cancel", {
            session: initialize.identity.session,
            correlation_id: actionId,
            stream_id: streamId,
          } satisfies StreamReceiveParams).catch(() => undefined);
        },
      };
      this.#outboundStreams.set(streamId, binding.cancel);
      return { kind: "opened", stream: binding };
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
    for (const cancel of this.#outboundStreams.values()) cancel();
    this.#outboundStreams.clear();
    for (const stream of this.#streams.values()) {
      stream.binding.cancel();
      this.#contexts.delete(stream.context);
    }
    this.#streams.clear();
    if (this.#instance === undefined) throw new Error("Bun Plugin stopped before construction");
    let hook: StoppedResult["hook"] = "not_declared";
    const diagnostics: Array<{ readonly code: string; readonly detail: string }> = [];
    if (this.definition.stop !== undefined) {
      const active = lifecycleActive(
        params.cleanup_scope_id,
        params.remaining_budget_nanos,
      );
      this.#contexts.set(active.context, active);
      try {
        await this.definition.stop(
          this.#instance,
          active.context,
        );
        hook = "completed";
      } catch (error) {
        hook = "failed";
        diagnostics.push({ code: "plugin_stop_failed", detail: boundedError(error) });
      } finally {
        this.#contexts.delete(active.context);
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

  async #callback(
    method:
      | "lenso.call"
      | "lenso.settled"
      | "lenso.event.publish"
      | "lenso.stream.open"
      | "lenso.stream.send"
      | "lenso.stream.receive"
      | "lenso.stream.close_send"
      | "lenso.stream.cancel",
    params: unknown,
  ): Promise<unknown> {
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

function lifecycleActive(scopeId: string, remaining: string): ActiveContext {
  const controller = new AbortController();
  const budget = remainingBudget(remaining);
  const context: InternalInvocationContext = Object.freeze({
    requestId: scopeId as InvocationContext["requestId"],
    get cancelled() { return controller.signal.aborted; },
    signal: controller.signal,
    remainingTimeoutMs: budget.milliseconds,
    [remainingNanos]: budget.nanoseconds,
  });
  return {
    params: {
      scope: {
        scope_id: scopeId,
        parent_scope_id: null,
        remaining_budget_nanos: remaining,
        permissions: [],
        extensions: [],
      },
    },
    controller,
    context,
  };
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

function outboundScope(parent: ActiveContext, correlationId: string): InvocationScope {
  return {
    scope_id: `${parent.params.scope.scope_id}:outbound:${correlationId}`,
    parent_scope_id: parent.params.scope.scope_id,
    remaining_budget_nanos: remainingBudgetNanos(parent.context),
    permissions: parent.params.scope.permissions,
    extensions: parent.params.scope.extensions,
  };
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

function wireFailure(
  failure: import("@lenso/contract-runtime").RuntimeFailure,
  invocation: Pick<InvokeParams, "correlation_id" | "capability_id" | "operation"> = {
    correlation_id: "0",
    capability_id: "lenso.bun-authoring",
    operation: "stream",
  },
): Extract<InvocationOutcome, { readonly kind: "runtime" }>["failure"] {
  return (normalizeRuntimeFailure(failure, invocation as InvokeParams) as Extract<
    InvocationOutcome,
    { readonly kind: "runtime" }
  >).failure;
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
