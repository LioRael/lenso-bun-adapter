import type { InvocationContext } from "@lenso/contract-runtime";

import type {
  CapabilityProviderBinding,
  CapabilityProviderDescriptor,
  ProviderDispatchOutcome,
} from "./index.js";

/** A finite Host-owned scope used while constructing or stopping one instance. */
export interface LifecycleContext extends InvocationContext {
  readonly signal: AbortSignal;
  remainingTimeoutMs(): number;
}

/** Runtime projection emitted beside a generated request Capability client. */
export interface CapabilityDependencyBinding<Client> {
  readonly descriptor: CapabilityProviderDescriptor;
  createClient(invoke: DependencyInvoker): Client;
}

export type DependencyInvoker = (
  operation: string,
  context: InvocationContext,
  payload: unknown,
) => Promise<ProviderDispatchOutcome>;

export type DependencyCardinality = "one" | "optional" | "many";

export interface BoundCapabilityClient<Client> {
  readonly providerInstance: string;
  readonly client: Client;
}

export interface DependencyDeclaration<
  Client,
  Cardinality extends DependencyCardinality = "one",
> {
  readonly kind: "lenso.dependency";
  readonly id: string;
  readonly contract: CapabilityDependencyBinding<Client>;
  readonly cardinality: Cardinality;
}

export function dependency<
  Client,
  Cardinality extends DependencyCardinality = "one",
>(options: {
  readonly id: string;
  readonly contract: CapabilityDependencyBinding<Client>;
  readonly cardinality?: Cardinality;
}): DependencyDeclaration<Client, Cardinality> {
  if (options.id.length === 0) throw new Error("dependency id must not be empty");
  return Object.freeze({
    kind: "lenso.dependency" as const,
    id: options.id,
    contract: options.contract,
    cardinality: options.cardinality ?? ("one" as Cardinality),
  });
}

/** A generated declaration that validates and defaults Plugin configuration. */
export interface ConfigDeclaration<Config> {
  readonly kind: "lenso.config";
  parse(input: unknown): Config;
}

export interface ProviderDeclaration<Instance extends object> {
  readonly kind: "lenso.provider";
  readonly descriptor: CapabilityProviderDescriptor;
  readonly bind: (instance: Instance) => CapabilityProviderBinding;
}

/** Adapts a generated instance binder to the generic Plugin declaration. */
export function provider<Instance extends object>(
  descriptor: CapabilityProviderDescriptor,
  bind: (instance: Instance) => CapabilityProviderBinding,
): ProviderDeclaration<Instance> {
  return Object.freeze({ kind: "lenso.provider" as const, descriptor, bind });
}

export type DependencyDeclarations = Readonly<
  Record<string, DependencyDeclaration<unknown, DependencyCardinality>>
>;

type ConfigValue<Declaration> = Declaration extends ConfigDeclaration<infer Value>
  ? Value
  : never;

type DependencyValue<Declaration> =
  Declaration extends DependencyDeclaration<infer Client, infer Cardinality>
    ? Cardinality extends "optional"
      ? Client | undefined
      : Cardinality extends "many"
        ? ReadonlyArray<BoundCapabilityClient<Client>>
        : Client
    : never;

export type PluginInputs<
  Config extends ConfigDeclaration<unknown> | undefined,
  Dependencies extends DependencyDeclarations | undefined,
> = (Config extends ConfigDeclaration<unknown>
  ? { readonly config: ConfigValue<Config> }
  : object) &
  (Dependencies extends DependencyDeclarations
    ? {
        readonly dependencies: {
          readonly [Name in keyof Dependencies]: DependencyValue<Dependencies[Name]>;
        };
      }
    : object);

export type PluginProvider<Instance extends object> =
  | ProviderDeclaration<Instance>
  | CapabilityProviderBinding;

export type PluginDefinition<
  Instance extends object,
  Config extends ConfigDeclaration<unknown> | undefined =
    | ConfigDeclaration<unknown>
    | undefined,
  Dependencies extends DependencyDeclarations | undefined =
    | DependencyDeclarations
    | undefined,
> = DeclaredInputs<Config, Dependencies> & {
  readonly providers: ReadonlyArray<PluginProvider<Instance>>;
  readonly create?: (
    inputs: object,
    lifecycle: LifecycleContext,
  ) => Instance | Promise<Instance>;
  readonly stop?: (
    instance: Instance,
    lifecycle: LifecycleContext,
  ) => void | Promise<void>;
  readonly maxConcurrentRequests: number;
};

type DeclaredInputs<
  Config extends ConfigDeclaration<unknown> | undefined,
  Dependencies extends DependencyDeclarations | undefined,
> = (Config extends ConfigDeclaration<unknown>
  ? { readonly config: Config }
  : { readonly config?: undefined }) &
  (Dependencies extends DependencyDeclarations
    ? { readonly dependencies: Dependencies }
    : { readonly dependencies?: undefined });

export type PluginOptionsWithCreate<
  Config extends ConfigDeclaration<unknown> | undefined,
  Dependencies extends DependencyDeclarations | undefined,
  Factory extends (...arguments_: never[]) => object | Promise<object>,
> = DeclaredInputs<Config, Dependencies> & {
  readonly providers: ReadonlyArray<
    PluginProvider<NoInfer<CreatedInstance<Factory>>>
  >;
  readonly create: Factory &
    ((
      inputs: PluginInputs<Config, Dependencies>,
      lifecycle: LifecycleContext,
    ) => object | Promise<object>);
  readonly stop?: (
    instance: NoInfer<CreatedInstance<Factory>>,
    lifecycle: LifecycleContext,
  ) => void | Promise<void>;
  readonly maxConcurrentRequests?: number;
};

export type CreatedInstance<
  Factory extends (...arguments_: never[]) => object | Promise<object>,
> = Awaited<ReturnType<Factory>>;

export type PluginOptionsWithDefaultInstance<
  Config extends ConfigDeclaration<unknown> | undefined,
  Dependencies extends DependencyDeclarations | undefined,
> = DeclaredInputs<Config, Dependencies> & {
  readonly providers: ReadonlyArray<
    PluginProvider<NoInfer<PluginInputs<Config, Dependencies>>>
  >;
  readonly create?: undefined;
  readonly stop?: (
    instance: NoInfer<PluginInputs<Config, Dependencies>>,
    lifecycle: LifecycleContext,
  ) => void | Promise<void>;
  readonly maxConcurrentRequests?: number;
};
