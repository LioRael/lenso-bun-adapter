import { createHash } from "node:crypto";

export const BUILD_API_VERSION = 1 as const;
export const MAX_GENERATED_FILES = 256;
export const MAX_GENERATED_BYTES = 16 * 1024 * 1024;

export interface SourceSpan {
  readonly file: string;
  readonly start: number;
  readonly end: number;
}

export interface BuildPackageIdentity {
  readonly name: string;
  readonly version: string;
  readonly integrity: string;
}

/** Static package.json contract used to discover trusted product lowerings. */
export interface BuildPackageMetadata {
  readonly api_version: typeof BUILD_API_VERSION;
  readonly export: string;
  readonly declarations: Readonly<Record<string, ReadonlyArray<string>>>;
  readonly handler_parameters?: Readonly<
    Record<string, Readonly<Record<string, ReadonlyArray<number>>>>
  >;
  readonly provider_contracts: ReadonlyArray<BuildProviderContract>;
}

export interface BuildProviderContract extends AllowedProvider {
  readonly request_operations: ReadonlyArray<string>;
}

export type PortableValue =
  | null
  | boolean
  | number
  | string
  | ReadonlyArray<PortableValue>
  | { readonly [key: string]: PortableValue };

export type BuildValue =
  | null
  | boolean
  | number
  | string
  | ReadonlyArray<BuildValue | BuildArgument>
  | { readonly [key: string]: BuildValue | BuildArgument };

export interface ValueArgument {
  readonly kind: "value";
  readonly value: BuildValue;
  readonly span: SourceSpan;
}

export interface ContractArgument {
  readonly kind: "contract";
  readonly capability_id: string;
  readonly descriptor_version: string;
  readonly descriptor_digest: string;
  readonly generated_module: string;
  readonly generated_export: string;
  readonly span: SourceSpan;
}

export interface HandlerArgument {
  readonly kind: "handler";
  readonly reference: string;
  readonly span: SourceSpan;
}

export interface DeclarationArgument {
  readonly kind: "declaration";
  readonly package: BuildPackageIdentity;
  readonly export_name: string;
  readonly arguments: ReadonlyArray<BuildArgument>;
  readonly span: SourceSpan;
}

export type BuildArgument =
  | ValueArgument
  | ContractArgument
  | HandlerArgument
  | DeclarationArgument;

export interface LoweringInput {
  readonly api_version: typeof BUILD_API_VERSION;
  readonly package: BuildPackageIdentity;
  readonly export_name: string;
  readonly arguments: ReadonlyArray<BuildArgument>;
  readonly span: SourceSpan;
}

export interface GeneratedSymbol {
  readonly module: string;
  readonly export_name: string;
}

export interface LoweredProvider {
  readonly capability_id: string;
  readonly descriptor_version: string;
  readonly descriptor_digest: string;
  readonly binder: GeneratedSymbol;
  readonly handler_references: ReadonlyArray<string>;
}

export interface GeneratedFile {
  readonly path: string;
  readonly contents: string;
}

export interface BuildDiagnostic {
  readonly severity: "error" | "warning";
  readonly message: string;
  readonly span: SourceSpan;
}

export interface LoweringOutput {
  readonly api_version: typeof BUILD_API_VERSION;
  readonly providers: ReadonlyArray<LoweredProvider>;
  readonly files: ReadonlyArray<GeneratedFile>;
  readonly diagnostics: ReadonlyArray<BuildDiagnostic>;
}

export type Lowering = (
  input: LoweringInput,
) => LoweringOutput | Promise<LoweringOutput>;

export interface AllowedProvider {
  readonly capability_id: string;
  readonly descriptor_version: string;
  readonly descriptor_digest: string;
}

export interface LoweringConstraints {
  readonly allowedProviders: ReadonlyArray<AllowedProvider>;
}

export class BuildOutputError extends Error {
  readonly span: SourceSpan;

  constructor(message: string, span: SourceSpan) {
    super(message);
    this.name = "BuildOutputError";
    this.span = span;
  }
}

/** Validates the product-neutral `lenso.build` package metadata contract. */
export function validateBuildPackageMetadata(raw: unknown): BuildPackageMetadata {
  const span = EMPTY_SPAN;
  const metadata = record(raw, "lenso.build", span);
  const allowedKeys = new Set([
    "api_version",
    "export",
    "declarations",
    "handler_parameters",
    "provider_contracts",
  ]);
  const unknown = Object.keys(metadata).find((key) => !allowedKeys.has(key));
  if (unknown !== undefined) {
    throw new BuildOutputError(`lenso.build contains unknown field ${unknown}`, span);
  }
  for (const required of ["api_version", "export", "declarations", "provider_contracts"]) {
    if (!(required in metadata)) {
      throw new BuildOutputError(`lenso.build is missing field ${required}`, span);
    }
  }
  if (metadata.api_version !== BUILD_API_VERSION) {
    throw new BuildOutputError(
      `unsupported build metadata API ${String(metadata.api_version)}`,
      span,
    );
  }
  const buildExport = packageExport(metadata.export, "lenso.build.export", span);
  const declarationsRecord = record(metadata.declarations, "lenso.build.declarations", span);
  const declarations: Record<string, ReadonlyArray<string>> = {};
  for (const [subpath, rawNames] of Object.entries(declarationsRecord)) {
    packageExport(subpath, "declaration subpath", span, true);
    const names = array(rawNames, `declarations.${subpath}`, span).map((name, index) =>
      nonemptyString(name, `declarations.${subpath}[${index}]`, span),
    );
    if (new Set(names).size !== names.length) {
      throw new BuildOutputError(`declarations.${subpath} contains a duplicate export`, span);
    }
    declarations[subpath] = Object.freeze(names);
  }
  const handlerParameters: Record<string, Readonly<Record<string, ReadonlyArray<number>>>> = {};
  if (metadata.handler_parameters !== undefined) {
    const subpaths = record(metadata.handler_parameters, "lenso.build.handler_parameters", span);
    for (const [subpath, rawExports] of Object.entries(subpaths)) {
      const declared = declarations[subpath];
      if (declared === undefined) {
        throw new BuildOutputError(
          `handler_parameters.${subpath} has no declared package subpath`,
          span,
        );
      }
      const exports = record(rawExports, `handler_parameters.${subpath}`, span);
      const normalized: Record<string, ReadonlyArray<number>> = {};
      for (const [exportName, rawIndexes] of Object.entries(exports)) {
        if (!declared.includes(exportName)) {
          throw new BuildOutputError(
            `handler_parameters.${subpath}.${exportName} is not a declared export`,
            span,
          );
        }
        const indexes = array(
          rawIndexes,
          `handler_parameters.${subpath}.${exportName}`,
          span,
        ).map((value, index) =>
          nonnegativeInteger(
            value,
            `handler_parameters.${subpath}.${exportName}[${index}]`,
            span,
          ),
        );
        if (new Set(indexes).size !== indexes.length) {
          throw new BuildOutputError(
            `handler_parameters.${subpath}.${exportName} contains a duplicate index`,
            span,
          );
        }
        normalized[exportName] = Object.freeze(indexes);
      }
      handlerParameters[subpath] = Object.freeze(normalized);
    }
  }
  const providerContracts = array(
    metadata.provider_contracts,
    "lenso.build.provider_contracts",
    span,
  ).map((rawContract, index) => {
    const subject = `provider_contracts[${index}]`;
    const contract = record(rawContract, subject, span);
    exactKeys(
      contract,
      [
        "capability_id",
        "descriptor_version",
        "descriptor_digest",
        "request_operations",
      ],
      subject,
      span,
    );
    const operations = array(
      contract.request_operations,
      `${subject}.request_operations`,
      span,
    ).map((operation, operationIndex) =>
      nonemptyString(operation, `${subject}.request_operations[${operationIndex}]`, span),
    );
    if (operations.length === 0 || new Set(operations).size !== operations.length) {
      throw new BuildOutputError(
        `${subject}.request_operations must be a nonempty unique list`,
        span,
      );
    }
    return Object.freeze({
      capability_id: nonemptyString(contract.capability_id, `${subject}.capability_id`, span),
      descriptor_version: nonemptyString(
        contract.descriptor_version,
        `${subject}.descriptor_version`,
        span,
      ),
      descriptor_digest: canonicalDigest(
        contract.descriptor_digest,
        `${subject}.descriptor_digest`,
        span,
      ),
      request_operations: Object.freeze(operations),
    });
  });
  return Object.freeze({
    api_version: BUILD_API_VERSION,
    export: buildExport,
    declarations: Object.freeze(declarations),
    ...(metadata.handler_parameters === undefined
      ? {}
      : { handler_parameters: Object.freeze(handlerParameters) }),
    provider_contracts: Object.freeze(providerContracts),
  });
}

/**
 * Runs one trusted SDK lowering and validates its complete untrusted result.
 * The caller resolves and loads the exact locked build dependency.
 */
export async function runLowering(
  lower: Lowering,
  input: LoweringInput,
  constraints: LoweringConstraints,
): Promise<LoweringOutput> {
  if (input.api_version !== BUILD_API_VERSION) {
    throw new BuildOutputError(
      `unsupported build input API ${String(input.api_version)}`,
      input.span,
    );
  }
  const raw: unknown = await lower(input);
  const output = validateLoweringOutput(raw, input);
  validateAllowedProviders(output.providers, constraints.allowedProviders, input.span);
  return output;
}

export interface BuildFingerprintInput {
  readonly sourceClosure: ReadonlyArray<{
    readonly path: string;
    readonly sha256: string;
  }>;
  readonly contractArtifacts: ReadonlyArray<{
    readonly path: string;
    readonly sha256: string;
  }>;
  readonly lockedPackages: ReadonlyArray<BuildPackageIdentity>;
  readonly target: string;
}

export function fingerprintBuildInputs(input: BuildFingerprintInput): string {
  const canonical = {
    source_closure: canonicalFiles(input.sourceClosure, "sourceClosure"),
    contract_artifacts: canonicalFiles(input.contractArtifacts, "contractArtifacts"),
    locked_packages: [...input.lockedPackages]
      .map((value) => ({
        name: nonemptyString(value.name, "locked package name", EMPTY_SPAN),
        version: nonemptyString(value.version, "locked package version", EMPTY_SPAN),
        integrity: nonemptyString(value.integrity, "locked package integrity", EMPTY_SPAN),
      }))
      .sort((left, right) =>
        `${left.name}\u0000${left.version}\u0000${left.integrity}`.localeCompare(
          `${right.name}\u0000${right.version}\u0000${right.integrity}`,
        ),
      ),
    target: nonemptyString(input.target, "build target", EMPTY_SPAN),
  };
  return `sha256:${createHash("sha256").update(JSON.stringify(canonical)).digest("hex")}`;
}

export function verifyBuildFingerprint(
  expected: string,
  input: BuildFingerprintInput,
): void {
  const actual = fingerprintBuildInputs(input);
  if (actual !== expected) {
    throw new BuildOutputError(
      `declaration/artifact fingerprint drift: expected ${expected}, received ${actual}`,
      EMPTY_SPAN,
    );
  }
}

export function validateLoweringOutput(
  raw: unknown,
  input: LoweringInput,
): LoweringOutput {
  const output = record(raw, "lowering output", input.span);
  exactKeys(
    output,
    ["api_version", "providers", "files", "diagnostics"],
    "lowering output",
    input.span,
  );
  if (output.api_version !== BUILD_API_VERSION) {
    throw new BuildOutputError(
      `unsupported build output API ${String(output.api_version)}`,
      input.span,
    );
  }

  const providers = array(output.providers, "providers", input.span).map(
    (value, index) => validateProvider(value, index, input.span),
  );
  const files = validateFiles(output.files, input.span);
  const diagnostics = array(output.diagnostics, "diagnostics", input.span).map(
    (value, index) => validateDiagnostic(value, index, input.span),
  );

  const providerIds = new Set<string>();
  for (const provider of providers) {
    const identity = `${provider.capability_id}\u0000${provider.descriptor_version}`;
    if (providerIds.has(identity)) {
      throw new BuildOutputError(
        `duplicate generated provider ${provider.capability_id} ${provider.descriptor_version}`,
        input.span,
      );
    }
    providerIds.add(identity);
  }

  const expectedHandlers = collectHandlerReferences(input.arguments);
  const usedHandlers = new Set<string>();
  for (const provider of providers) {
    for (const reference of provider.handler_references) {
      if (!expectedHandlers.has(reference)) {
        throw new BuildOutputError(
          `lowering used unknown handler reference ${reference}`,
          input.span,
        );
      }
      if (usedHandlers.has(reference)) {
        throw new BuildOutputError(
          `lowering used handler reference ${reference} more than once`,
          input.span,
        );
      }
      usedHandlers.add(reference);
    }
  }
  for (const reference of expectedHandlers) {
    if (!usedHandlers.has(reference)) {
      throw new BuildOutputError(
        `lowering did not use handler reference ${reference}`,
        input.span,
      );
    }
  }

  return Object.freeze({
    api_version: BUILD_API_VERSION,
    providers: Object.freeze(providers),
    files: Object.freeze(files),
    diagnostics: Object.freeze(diagnostics),
  });
}

function validateProvider(
  raw: unknown,
  index: number,
  span: SourceSpan,
): LoweredProvider {
  const subject = `providers[${index}]`;
  const provider = record(raw, subject, span);
  exactKeys(
    provider,
    [
      "capability_id",
      "descriptor_version",
      "descriptor_digest",
      "binder",
      "handler_references",
    ],
    subject,
    span,
  );
  const binder = record(provider.binder, `${subject}.binder`, span);
  exactKeys(binder, ["module", "export_name"], `${subject}.binder`, span);
  return Object.freeze({
    capability_id: nonemptyString(provider.capability_id, `${subject}.capability_id`, span),
    descriptor_version: nonemptyString(
      provider.descriptor_version,
      `${subject}.descriptor_version`,
      span,
    ),
    descriptor_digest: canonicalDigest(
      provider.descriptor_digest,
      `${subject}.descriptor_digest`,
      span,
    ),
    binder: Object.freeze({
      module: safeRelativePath(binder.module, `${subject}.binder.module`, span),
      export_name: nonemptyString(
        binder.export_name,
        `${subject}.binder.export_name`,
        span,
      ),
    }),
    handler_references: Object.freeze(
      array(provider.handler_references, `${subject}.handler_references`, span).map(
        (value, handlerIndex) =>
          nonemptyString(
            value,
            `${subject}.handler_references[${handlerIndex}]`,
            span,
          ),
      ),
    ),
  });
}

function validateAllowedProviders(
  providers: ReadonlyArray<LoweredProvider>,
  allowed: ReadonlyArray<AllowedProvider>,
  span: SourceSpan,
): void {
  const identities = new Set(
    allowed.map((provider) => {
      const capability = nonemptyString(
        provider.capability_id,
        "allowed provider capability_id",
        span,
      );
      const version = nonemptyString(
        provider.descriptor_version,
        "allowed provider descriptor_version",
        span,
      );
      const digest = canonicalDigest(
        provider.descriptor_digest,
        "allowed provider descriptor_digest",
        span,
      );
      return `${capability}\u0000${version}\u0000${digest}`;
    }),
  );
  for (const provider of providers) {
    const identity = `${provider.capability_id}\u0000${provider.descriptor_version}\u0000${provider.descriptor_digest}`;
    if (!identities.has(identity)) {
      throw new BuildOutputError(
        `generated provider ${provider.capability_id} does not match an allowed exact contract`,
        span,
      );
    }
  }
}

const EMPTY_SPAN: SourceSpan = Object.freeze({ file: "<build>", start: 0, end: 0 });

function canonicalFiles(
  files: BuildFingerprintInput["sourceClosure"],
  subject: string,
): ReadonlyArray<{ readonly path: string; readonly sha256: string }> {
  const seen = new Set<string>();
  return [...files]
    .map((file) => {
      const path = safeRelativePath(file.path, `${subject} path`, EMPTY_SPAN);
      if (seen.has(path)) {
        throw new BuildOutputError(`${subject} contains duplicate path ${path}`, EMPTY_SPAN);
      }
      seen.add(path);
      if (!/^sha256:[0-9a-f]{64}$/u.test(file.sha256)) {
        throw new BuildOutputError(`${subject} digest must be canonical SHA-256`, EMPTY_SPAN);
      }
      return { path, sha256: file.sha256 };
    })
    .sort((left, right) => left.path.localeCompare(right.path));
}

function validateFiles(raw: unknown, span: SourceSpan): GeneratedFile[] {
  const values = array(raw, "files", span);
  if (values.length > MAX_GENERATED_FILES) {
    throw new BuildOutputError(
      `lowering generated ${values.length} files; limit is ${MAX_GENERATED_FILES}`,
      span,
    );
  }
  const paths = new Set<string>();
  let totalBytes = 0;
  return values.map((value, index) => {
    const subject = `files[${index}]`;
    const file = record(value, subject, span);
    exactKeys(file, ["path", "contents"], subject, span);
    const path = safeRelativePath(file.path, `${subject}.path`, span);
    if (paths.has(path)) {
      throw new BuildOutputError(`duplicate generated path ${path}`, span);
    }
    paths.add(path);
    const contents = string(file.contents, `${subject}.contents`, span);
    totalBytes += new TextEncoder().encode(contents).byteLength;
    if (totalBytes > MAX_GENERATED_BYTES) {
      throw new BuildOutputError(
        `generated files exceed ${MAX_GENERATED_BYTES} bytes`,
        span,
      );
    }
    return Object.freeze({ path, contents });
  });
}

function validateDiagnostic(
  raw: unknown,
  index: number,
  fallbackSpan: SourceSpan,
): BuildDiagnostic {
  const subject = `diagnostics[${index}]`;
  const diagnostic = record(raw, subject, fallbackSpan);
  exactKeys(diagnostic, ["severity", "message", "span"], subject, fallbackSpan);
  if (diagnostic.severity !== "error" && diagnostic.severity !== "warning") {
    throw new BuildOutputError(`${subject}.severity is invalid`, fallbackSpan);
  }
  return Object.freeze({
    severity: diagnostic.severity,
    message: nonemptyString(diagnostic.message, `${subject}.message`, fallbackSpan),
    span: validateSpan(diagnostic.span, `${subject}.span`, fallbackSpan),
  });
}

function collectHandlerReferences(
  arguments_: ReadonlyArray<BuildArgument>,
  references = new Set<string>(),
): Set<string> {
  for (const argument of arguments_) {
    if (argument.kind === "handler") references.add(argument.reference);
    if (argument.kind === "declaration") {
      collectHandlerReferences(argument.arguments, references);
    }
  }
  return references;
}

function validateSpan(
  raw: unknown,
  subject: string,
  fallbackSpan: SourceSpan,
): SourceSpan {
  const span = record(raw, subject, fallbackSpan);
  exactKeys(span, ["file", "start", "end"], subject, fallbackSpan);
  const file = nonemptyString(span.file, `${subject}.file`, fallbackSpan);
  const start = nonnegativeInteger(span.start, `${subject}.start`, fallbackSpan);
  const end = nonnegativeInteger(span.end, `${subject}.end`, fallbackSpan);
  if (end < start) throw new BuildOutputError(`${subject}.end precedes start`, fallbackSpan);
  return Object.freeze({ file, start, end });
}

function safeRelativePath(raw: unknown, subject: string, span: SourceSpan): string {
  const path = nonemptyString(raw, subject, span);
  if (
    path.startsWith("/") ||
    path.startsWith("\\") ||
    /^[A-Za-z]:[\\/]/u.test(path) ||
    path.split(/[\\/]/u).some((part) => part === ".." || part.length === 0)
  ) {
    throw new BuildOutputError(`${subject} must be a normalized relative path`, span);
  }
  return path.replaceAll("\\", "/");
}

function packageExport(
  raw: unknown,
  subject: string,
  span: SourceSpan,
  allowRoot = false,
): string {
  const value = nonemptyString(raw, subject, span);
  if ((allowRoot && value === ".") || /^\.\/[A-Za-z0-9][A-Za-z0-9._/-]*$/u.test(value)) {
    if (!value.split("/").some((part) => part === ".." || part.length === 0)) {
      return value;
    }
  }
  throw new BuildOutputError(`${subject} must be a static package export subpath`, span);
}

function exactKeys(
  value: Readonly<Record<string, unknown>>,
  expected: ReadonlyArray<string>,
  subject: string,
  span: SourceSpan,
): void {
  const allowed = new Set(expected);
  const unknown = Object.keys(value).find((key) => !allowed.has(key));
  if (unknown !== undefined) {
    throw new BuildOutputError(`${subject} contains unknown field ${unknown}`, span);
  }
  const missing = expected.find((key) => !(key in value));
  if (missing !== undefined) {
    throw new BuildOutputError(`${subject} is missing field ${missing}`, span);
  }
}

function record(
  value: unknown,
  subject: string,
  span: SourceSpan,
): Readonly<Record<string, unknown>> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new BuildOutputError(`${subject} must be an object`, span);
  }
  return value as Readonly<Record<string, unknown>>;
}

function array(value: unknown, subject: string, span: SourceSpan): unknown[] {
  if (!Array.isArray(value)) throw new BuildOutputError(`${subject} must be an array`, span);
  return value;
}

function string(value: unknown, subject: string, span: SourceSpan): string {
  if (typeof value !== "string") throw new BuildOutputError(`${subject} must be a string`, span);
  return value;
}

function nonemptyString(value: unknown, subject: string, span: SourceSpan): string {
  const result = string(value, subject, span);
  if (result.length === 0) throw new BuildOutputError(`${subject} must not be empty`, span);
  return result;
}

function canonicalDigest(value: unknown, subject: string, span: SourceSpan): string {
  const result = nonemptyString(value, subject, span);
  if (!/^sha256:[0-9a-f]{64}$/u.test(result)) {
    throw new BuildOutputError(`${subject} must be a canonical SHA-256 digest`, span);
  }
  return result;
}

function nonnegativeInteger(value: unknown, subject: string, span: SourceSpan): number {
  if (!Number.isSafeInteger(value) || (value as number) < 0) {
    throw new BuildOutputError(`${subject} must be a nonnegative safe integer`, span);
  }
  return value as number;
}
