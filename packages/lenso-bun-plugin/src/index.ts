import type {
  InvocationContext,
  RuntimeFailure,
} from "@lenso/contract-runtime";

import type {
  ConfigDeclaration,
  DependencyDeclarations,
  DependencyCardinality,
  DependencyDeclaration,
  LifecycleContext,
  PluginDefinition,
  PluginOptionsWithCreate,
  PluginOptionsWithDefaultInstance,
  PluginProvider,
} from "./authoring.js";

export * from "./authoring.js";
export * from "./v2.js";

const PROTOCOL_VERSION = 1;
const VALUE_PROFILE = "lenso-json-value-v1";
const DEFAULT_MAX_FRAME_BYTES = 64 * 1024;
const DEFAULT_MAX_CONCURRENT_REQUESTS = 32;
const DEFAULT_MAX_RETIRED_REQUEST_IDS = 1024;

export interface CapabilityProviderDescriptor {
  readonly capability_id: string;
  readonly descriptor_version: string;
  readonly descriptor_digest?: string;
  readonly operations: ReadonlyArray<string>;
  readonly stream_operations: ReadonlyArray<string>;
  readonly event_operations: ReadonlyArray<string>;
}

interface CapabilityImportDescriptor extends CapabilityProviderDescriptor {
  readonly requirement_id: string;
}

export type ProviderDispatchOutcome =
  | { readonly kind: "success"; readonly value: unknown }
  | { readonly kind: "domain"; readonly value: unknown }
  | { readonly kind: "runtime"; readonly failure: RuntimeFailure };

export interface CapabilityProviderBinding<Instance = unknown> {
  readonly descriptor: CapabilityProviderDescriptor;
  invokeRequest(
    operation: string,
    context: InvocationContext,
    payload: unknown,
    instance: Instance,
  ): Promise<ProviderDispatchOutcome>;
}

export type BunPluginOptions = PluginOptionsWithDefaultInstance<undefined, undefined>;
export type BunPluginDefinition<
  Instance extends object = object,
  Config extends ConfigDeclaration<unknown> | undefined =
    | ConfigDeclaration<unknown>
    | undefined,
  Dependencies extends DependencyDeclarations | undefined =
    | DependencyDeclarations
    | undefined,
> = PluginDefinition<Instance, Config, Dependencies>;

export type DependencyTable = Readonly<
  Record<string, import("./authoring.js").CapabilityDependencyBinding<unknown>>
>;

type DependencyClients<Dependencies extends DependencyTable> = {
  readonly [Name in keyof Dependencies]: Dependencies[Name] extends import("./authoring.js").CapabilityDependencyBinding<
    infer Client
  >
    ? Client
    : never;
};

interface LegacyPluginInputs<Dependencies extends DependencyTable, Config> {
  readonly config: Config;
  readonly dependencies: DependencyClients<Dependencies>;
}

interface LegacyPluginOptions<
  Dependencies extends DependencyTable,
  Config,
  Instance,
> {
  readonly providers: ReadonlyArray<CapabilityProviderBinding<Instance>>;
  readonly dependencies?: Dependencies;
  readonly configurationSchema?: boolean | Readonly<Record<string, unknown>>;
  readonly decodeConfig?: (value: unknown) => Config;
  readonly create?: (
    inputs: LegacyPluginInputs<Dependencies, Config>,
  ) => Instance | Promise<Instance>;
  readonly stop?: (instance: Instance) => void | Promise<void>;
  readonly maxConcurrentRequests?: number;
}

interface LegacyPluginDefinition<
  Dependencies extends DependencyTable,
  Config,
  Instance,
> {
  readonly providers: ReadonlyArray<CapabilityProviderBinding<Instance>>;
  readonly dependencies: Dependencies;
  readonly configurationSchema: boolean | Readonly<Record<string, unknown>> | undefined;
  readonly decodeConfig: ((value: unknown) => Config) | undefined;
  readonly create: ((inputs: LegacyPluginInputs<Dependencies, Config>) => Instance | Promise<Instance>) | undefined;
  readonly stop: ((instance: Instance) => void | Promise<void>) | undefined;
  readonly maxConcurrentRequests: number;
}

interface RuntimePluginDefinition {
  readonly providers: ReadonlyArray<PluginProvider<object>>;
  readonly dependencies?: Readonly<Record<string, unknown>>;
  readonly config?: ConfigDeclaration<unknown>;
  readonly configurationSchema?: boolean | Readonly<Record<string, unknown>>;
  readonly decodeConfig?: (value: unknown) => unknown;
  readonly create?: (
    inputs: unknown,
    lifecycle?: LifecycleContext,
  ) => object | Promise<object>;
  readonly stop?: (
    instance: object,
    lifecycle?: LifecycleContext,
  ) => void | Promise<void>;
  readonly maxConcurrentRequests: number;
}

type AnyPluginDefinition = RuntimePluginDefinition;

/** Runtime-independent descriptor consumed by generated QuickJS and Bun wrappers. */
export interface PortablePluginDescriptor {
  readonly abi: "lenso.json-request@1";
  readonly configuration_schema?: boolean | Readonly<Record<string, unknown>>;
  readonly capabilities: ReadonlyArray<{
    readonly capability_id: string;
    readonly descriptor_version: string;
    readonly request_operations: ReadonlyArray<string>;
  }>;
  readonly required_capabilities: ReadonlyArray<{
    readonly requirement_id: string;
    readonly capability_id: string;
    readonly descriptor_version: string;
    readonly cardinality: "one" | "optional" | "many";
  }>;
}

export interface StartPluginOptions {
  readonly hostname?: string;
  readonly port?: number;
  readonly maxFrameBytes?: number;
  readonly expectedEndpoints?: ReadonlyArray<CapabilityProviderDescriptor>;
  readonly maxRetiredRequestIds?: number;
  readonly announceReady?: boolean;
  readonly managedLifecycle?: boolean;
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

interface ActivationRequest {
  readonly session?: string;
  readonly configuration: unknown;
  readonly imports_url: string;
  readonly imports_token: string;
  readonly imports: ReadonlyArray<CapabilityImportDescriptor>;
}

export function definePlugin<
  Factory extends (...arguments_: never[]) => object | Promise<object>,
  Config extends ConfigDeclaration<unknown> | undefined = undefined,
  Dependencies extends DependencyDeclarations | undefined = undefined,
>(
  options: PluginOptionsWithCreate<Config, Dependencies, Factory>,
): PluginDefinition<
  import("./authoring.js").CreatedInstance<Factory>,
  Config,
  Dependencies
>;
export function definePlugin<
  Config extends ConfigDeclaration<unknown> | undefined = undefined,
  Dependencies extends DependencyDeclarations | undefined = undefined,
>(
  options: PluginOptionsWithDefaultInstance<Config, Dependencies>,
): PluginDefinition<
  PluginOptionsInstance<Config, Dependencies>,
  Config,
  Dependencies
>;
export function definePlugin<
  Dependencies extends DependencyTable = Readonly<Record<never, never>>,
  Config = unknown,
  Instance extends object = LegacyPluginInputs<Dependencies, Config>,
>(
  options: LegacyPluginOptions<Dependencies, Config, Instance>,
): LegacyPluginDefinition<Dependencies, Config, Instance>;
export function definePlugin(
  options: object,
): object {
  const candidate = options as {
    readonly providers: ReadonlyArray<PluginProvider<object>>;
    readonly dependencies?: Readonly<Record<string, unknown>>;
    readonly config?: ConfigDeclaration<unknown>;
    readonly configurationSchema?: boolean | Readonly<Record<string, unknown>>;
    readonly decodeConfig?: (value: unknown) => unknown;
    readonly create?: (...arguments_: never[]) => object | Promise<object>;
    readonly stop?: (...arguments_: never[]) => void | Promise<void>;
    readonly maxConcurrentRequests?: number;
  };
  const seen = new Set<string>();
  for (const provider of candidate.providers) {
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
  if (candidate.dependencies !== undefined) {
    const dependencyIds = new Set<string>();
    for (const [name, raw] of Object.entries(candidate.dependencies)) {
      if (name.length === 0) throw new Error("dependency name must not be empty");
      if (typeof raw !== "object" || raw === null) {
        throw new Error(`dependency ${name} is invalid`);
      }
      if (!("kind" in raw)) {
        const legacy = raw as import("./authoring.js").CapabilityDependencyBinding<unknown>;
        validateDescriptor(legacy.descriptor);
        continue;
      }
      const declaration = raw as import("./authoring.js").DependencyDeclaration<
        unknown,
        import("./authoring.js").DependencyCardinality
      >;
      if (declaration.kind !== "lenso.dependency") {
        throw new Error(`dependency ${name} is not a dependency(...) declaration`);
      }
      if (dependencyIds.has(declaration.id)) throw new Error(`duplicate dependency id ${declaration.id}`);
      dependencyIds.add(declaration.id);
      validateDescriptor(declaration.contract.descriptor);
      if (
        declaration.cardinality !== "one" &&
        declaration.cardinality !== "optional" &&
        declaration.cardinality !== "many"
      ) {
        throw new Error(
          `dependency ${declaration.id} has invalid cardinality ${String(declaration.cardinality)}`,
        );
      }
    }
  }
  if (
    candidate.config !== undefined &&
    (candidate.config.kind !== "lenso.config" ||
      typeof candidate.config.parse !== "function" ||
      (typeof candidate.config.schema !== "boolean" &&
        (typeof candidate.config.schema !== "object" ||
          candidate.config.schema === null ||
          Array.isArray(candidate.config.schema))))
  ) {
    throw new Error("config must be a configuration(...) declaration");
  }
  const maxConcurrentRequests =
    candidate.maxConcurrentRequests ?? DEFAULT_MAX_CONCURRENT_REQUESTS;
  if (!Number.isSafeInteger(maxConcurrentRequests) || maxConcurrentRequests <= 0) {
    throw new Error("maxConcurrentRequests must be a positive safe integer");
  }
  if (
    candidate.configurationSchema !== undefined &&
    typeof candidate.configurationSchema !== "boolean" &&
    (typeof candidate.configurationSchema !== "object" ||
      candidate.configurationSchema === null ||
      Array.isArray(candidate.configurationSchema))
  ) {
    throw new Error("configurationSchema must be a JSON Schema object or boolean");
  }
  return Object.freeze({
    ...(candidate.config === undefined ? {} : { config: candidate.config }),
    ...(candidate.dependencies === undefined
      ? {}
      : { dependencies: Object.freeze({ ...candidate.dependencies }) }),
    ...(candidate.configurationSchema === undefined
      ? {}
      : { configurationSchema: candidate.configurationSchema }),
    ...(candidate.decodeConfig === undefined
      ? {}
      : { decodeConfig: candidate.decodeConfig }),
    providers: Object.freeze([...candidate.providers]),
    ...(candidate.create === undefined ? {} : { create: candidate.create }),
    ...(candidate.stop === undefined ? {} : { stop: candidate.stop }),
    maxConcurrentRequests,
  }) as AnyPluginDefinition;
}

type PluginOptionsInstance<
  Config extends ConfigDeclaration<unknown> | undefined,
  Dependencies extends DependencyDeclarations | undefined,
> = import("./authoring.js").PluginInputs<Config, Dependencies>;

function concreteProviders<Instance extends object>(
  providers: ReadonlyArray<PluginProvider<Instance>>,
): ReadonlyArray<CapabilityProviderBinding> {
  const declarations = providers.filter(
    (provider) => "kind" in provider && provider.kind === "lenso.provider",
  );
  if (declarations.length > 0) {
    throw new Error("instance-bound providers require the Bun runtime profile v2");
  }
  return providers as ReadonlyArray<CapabilityProviderBinding>;
}

function runtimeDefinition(value: unknown): RuntimePluginDefinition {
  return value as RuntimePluginDefinition;
}

function dependencyDefinition(
  name: string,
  raw: unknown,
): {
  readonly requirementId: string;
  readonly binding: import("./authoring.js").CapabilityDependencyBinding<unknown>;
  readonly cardinality: DependencyCardinality;
} {
  if (typeof raw === "object" && raw !== null && "kind" in raw) {
    const declaration = raw as DependencyDeclaration<
      unknown,
      DependencyCardinality
    >;
    return {
      requirementId: declaration.id,
      binding: declaration.contract,
      cardinality: declaration.cardinality,
    };
  }
  return {
    requirementId: name,
    binding: raw as import("./authoring.js").CapabilityDependencyBinding<unknown>,
    cardinality: "one",
  };
}

function legacyLifecycleContext(requestId: string): LifecycleContext {
  const controller = new AbortController();
  return Object.freeze({
    requestId: requestId as InvocationContext["requestId"],
    cancelled: false,
    signal: controller.signal,
    remainingTimeoutMs: () => Number.MAX_SAFE_INTEGER,
  }) as LifecycleContext;
}

/** Describes the same Plugin definition without touching any Bun global API. */
export function describePortablePlugin<
  Dependencies extends DependencyTable,
  Config,
  Instance extends object,
>(
  definition: LegacyPluginDefinition<Dependencies, Config, Instance>,
): PortablePluginDescriptor;
export function describePortablePlugin<
  Instance extends object,
  Config extends ConfigDeclaration<unknown> | undefined,
  Dependencies extends DependencyDeclarations | undefined,
>(
  definition: BunPluginDefinition<Instance, Config, Dependencies>,
): PortablePluginDescriptor;
export function describePortablePlugin(definition: unknown): PortablePluginDescriptor {
  const runtime = runtimeDefinition(definition);
  const providers = concreteProviders(runtime.providers);
  const requiredCapabilities = Object.entries(runtime.dependencies ?? {}).map(
    ([name, raw]) => {
      const declaration = dependencyDefinition(name, raw);
      return {
        requirement_id: declaration.requirementId,
        capability_id: declaration.binding.descriptor.capability_id,
        descriptor_version: declaration.binding.descriptor.descriptor_version,
        cardinality: declaration.cardinality,
      };
    },
  );
  return {
    abi: "lenso.json-request@1",
    ...(runtime.configurationSchema === undefined
      ? {}
      : { configuration_schema: runtime.configurationSchema }),
    capabilities: providers.map(({ descriptor }) => ({
      capability_id: descriptor.capability_id,
      descriptor_version: descriptor.descriptor_version,
      request_operations: [...descriptor.operations],
    })),
    required_capabilities: requiredCapabilities,
  };
}

/** Dispatches the portable QuickJS ABI through the same authored Provider binding. */
export async function invokePortablePlugin<
  Instance extends object,
  Config extends ConfigDeclaration<unknown> | undefined,
  Dependencies extends DependencyDeclarations | undefined,
>(
  definition: BunPluginDefinition<Instance, Config, Dependencies>,
  capability: string,
  operation: string,
  requestJson: string,
): Promise<string>;
export async function invokePortablePlugin<
  Dependencies extends DependencyTable,
  Config,
  Instance extends object,
>(
  definition: LegacyPluginDefinition<Dependencies, Config, Instance>,
  capability: string,
  operation: string,
  requestJson: string,
): Promise<string>;
export async function invokePortablePlugin(
  definition: unknown,
  capability: string,
  operation: string,
  requestJson: string,
): Promise<string> {
  const runtime = runtimeDefinition(definition);
  const provider = concreteProviders(runtime.providers).find(
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
    Object.freeze({ config: Object.freeze({}), dependencies: Object.freeze({}) }),
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

export function serve<
  Instance extends object,
  Config extends ConfigDeclaration<unknown> | undefined,
  Dependencies extends DependencyDeclarations | undefined,
>(
  definition: BunPluginDefinition<Instance, Config, Dependencies>,
): BunPluginServer;
export function serve<
  Dependencies extends DependencyTable,
  Config,
  Instance extends object,
>(definition: LegacyPluginDefinition<Dependencies, Config, Instance>): BunPluginServer;
export function serve(definition: unknown): BunPluginServer {
  const runtime = runtimeDefinition(definition);
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
    concreteProviders(runtime.providers).map(({ descriptor }) => descriptor),
  );
  return startPlugin(runtime as BunPluginDefinition<object>, {
    announceReady: true,
    managedLifecycle: true,
    expectedEndpoints,
    maxFrameBytes,
    port,
  });
}

export function startPlugin<
  Instance extends object,
  Config extends ConfigDeclaration<unknown> | undefined,
  Dependencies extends DependencyDeclarations | undefined,
>(
  definition: BunPluginDefinition<Instance, Config, Dependencies>,
  options?: StartPluginOptions,
): BunPluginServer;
export function startPlugin<
  Dependencies extends DependencyTable,
  Config,
  Instance extends object,
>(
  definition: LegacyPluginDefinition<Dependencies, Config, Instance>,
  options?: StartPluginOptions,
): BunPluginServer;
export function startPlugin(
  input: unknown,
  options: StartPluginOptions = {},
): BunPluginServer {
  const definition = runtimeDefinition(input);
  const maxFrameBytes = options.maxFrameBytes ?? DEFAULT_MAX_FRAME_BYTES;
  const maxRetiredRequestIds =
    options.maxRetiredRequestIds ?? DEFAULT_MAX_RETIRED_REQUEST_IDS;
  if (!Number.isSafeInteger(maxFrameBytes) || maxFrameBytes <= 0) {
    throw new Error("maxFrameBytes must be a positive safe integer");
  }
  if (!Number.isSafeInteger(maxRetiredRequestIds) || maxRetiredRequestIds <= 0) {
    throw new Error("maxRetiredRequestIds must be a positive safe integer");
  }

  const providers = concreteProviders(definition.providers);
  const providerByCapability = new Map(
    providers.map((provider) => [
      provider.descriptor.capability_id,
      provider,
    ]),
  );
  const pluginEndpoints = providers.map(({ descriptor }) => descriptor);
  const expectedEndpoints = options.expectedEndpoints ?? pluginEndpoints;
  const activeRequests = new Map<number, RequestState>();
  const retiredRequestIds = new Set<number>();
  let session: string | undefined;
  let admittedHandshake: Handshake | undefined;
  let accepting = true;
  const managedLifecycle = options.managedLifecycle ?? false;
  let instance: object | undefined =
    !managedLifecycle &&
    Object.keys(definition.dependencies ?? {}).length === 0 && definition.create === undefined
      ? Object.freeze({ config: Object.freeze({}), dependencies: Object.freeze({}) })
      : undefined;

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
          managed_lifecycle: managedLifecycle,
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

      if (envelope.method === "lenso.activate") {
        const activation = objectParam(params) as ActivationRequest | undefined;
        if (
          !activation ||
          !managedLifecycle ||
          !hasSession(activation.session, session, admittedHandshake) ||
          instance !== undefined ||
          activeRequests.size !== 0
        ) {
          return jsonRpcResult(id, protocolViolation("invalid activation"));
        }
        try {
          const dependencies = createDependencyClients(definition, activation);
          const rawConfig = activation.configuration;
          const config = definition.decodeConfig?.(rawConfig)
            ?? definition.config?.parse(rawConfig)
            ?? rawConfig;
          const inputs = Object.freeze({ config, dependencies });
          instance = definition.create
            ? await definition.create(inputs, legacyLifecycleContext("activate"))
            : inputs;
          if (instance === undefined) {
            throw new Error("Plugin create must return a complete instance");
          }
          return jsonRpcResult(id, true);
        } catch (error) {
          return jsonRpcResult(id, {
            kind: "runtime",
            failure: { kind: "plugin_failure", detail: errorMessage(error) },
          });
        }
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
        if (instance !== undefined && definition.stop !== undefined) {
          try {
            await definition.stop(instance, legacyLifecycleContext("shutdown"));
          } catch (error) {
            return jsonRpcResult(id, {
              kind: "runtime",
              failure: { kind: "plugin_failure", detail: errorMessage(error) },
            });
          }
        }
        setTimeout(() => server.stop(false), 0);
        return jsonRpcResult(id, true);
      }

      if (
        envelope.method !== "lenso.request" ||
        !accepting ||
        !admittedHandshake ||
        instance === undefined
      ) {
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
            instance,
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

function createDependencyClients(
  definition: RuntimePluginDefinition,
  activation: ActivationRequest,
): Readonly<Record<string, unknown>> {
  if (
    typeof activation.imports_url !== "string" ||
    !activation.imports_url.startsWith("http://127.0.0.1:") ||
    typeof activation.imports_token !== "string" ||
    activation.imports_token.length < 16 ||
    !Array.isArray(activation.imports)
  ) {
    throw new Error("Host imports activation data is invalid");
  }
  const admitted = new Map(
    activation.imports.map((descriptor) => [descriptor.requirement_id, descriptor]),
  );
  const admittedByCapability = new Map<string, CapabilityImportDescriptor[]>();
  for (const descriptor of activation.imports) {
    const matches = admittedByCapability.get(descriptor.capability_id) ?? [];
    matches.push(descriptor);
    admittedByCapability.set(descriptor.capability_id, matches);
  }
  const claimed = new Set<string>();
  const clients: Record<string, unknown> = {};
  let nextImportRequestId = 1;
  for (const [name, raw] of Object.entries(definition.dependencies ?? {})) {
    const dependency = dependencyDefinition(name, raw);
    if (dependency.cardinality !== "one") {
      throw new Error(
        `legacy Bun activation does not support ${dependency.cardinality} dependency ${dependency.requirementId}`,
      );
    }
    const exact = admitted.get(dependency.requirementId);
    const legacy = admittedByCapability.get(
      dependency.binding.descriptor.capability_id,
    );
    const descriptor = exact ?? (legacy?.length === 1 ? legacy[0] : undefined);
    if (!descriptor || !sameEndpoints([descriptor], [dependency.binding.descriptor])) {
      throw new Error(`Host did not admit dependency ${name}`);
    }
    if (claimed.has(descriptor.requirement_id)) {
      throw new Error(`Host import ${descriptor.requirement_id} was claimed more than once`);
    }
    claimed.add(descriptor.requirement_id);
    clients[name] = dependency.binding.createClient(async (operation, context, payload) => {
      const requestId = nextImportRequestId;
      nextImportRequestId = requestId >= Number.MAX_SAFE_INTEGER ? 1 : requestId + 1;
      if (!descriptor.operations.includes(operation)) {
        return {
          kind: "runtime",
          failure: { kind: "unknown_operation", operation },
        };
      }
      const response = await fetch(activation.imports_url, {
        method: "POST",
        headers: {
          authorization: `Bearer ${activation.imports_token}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          jsonrpc: "2.0",
          id: requestId,
          method: "lenso.import",
          params: [{
            request_id: requestId,
            requirement_id: descriptor.requirement_id,
            capability_id: descriptor.capability_id,
            operation,
            deadline_nanos: (context as InvocationContext & { readonly deadlineNanos?: number }).deadlineNanos,
            caller_instance: context.callerInstance,
            extensions: (context as InvocationContext & {
              readonly wireExtensions?: ReadonlyArray<WireInvocationExtension>;
            }).wireExtensions,
            payload,
          }],
        }),
      });
      if (!response.ok) {
        return {
          kind: "runtime",
          failure: { kind: "plugin_failure", detail: `Host import returned HTTP ${response.status}` },
        };
      }
      const envelope = (await response.json()) as { result?: ProviderDispatchOutcome };
      return envelope.result ?? {
        kind: "runtime",
        failure: { kind: "protocol_violation" },
      };
    });
  }
  return Object.freeze(clients);
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
  if (
    descriptor.descriptor_digest !== undefined &&
    !/^sha256:[0-9a-f]{64}$/u.test(descriptor.descriptor_digest)
  ) {
    throw new Error(
      `Capability Provider ${descriptor.capability_id} has an invalid descriptor digest`,
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
  return left.length === right.length && left.every((endpoint, index) => {
    const expected = right[index];
    return expected !== undefined &&
      endpoint.capability_id === expected.capability_id &&
      endpoint.descriptor_version === expected.descriptor_version &&
      sameStrings(endpoint.operations, expected.operations) &&
      sameStrings(endpoint.stream_operations, expected.stream_operations) &&
      sameStrings(endpoint.event_operations, expected.event_operations);
  });
}

function sameStrings(
  left: ReadonlyArray<string>,
  right: ReadonlyArray<string>,
): boolean {
  return left.length === right.length && left.every((value, index) => value === right[index]);
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
    ...(request.deadline_nanos == null
      ? {}
      : { deadlineNanos: request.deadline_nanos }),
    wireRequestId: request.request_id,
    ...(request.extensions === undefined
      ? {}
      : { wireExtensions: request.extensions }),
    ...(request.caller_instance == null
      ? {}
      : { callerInstance: request.caller_instance }),
    ...(extensions === undefined ? {} : { extensions }),
  } as unknown as InvocationContext;
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
