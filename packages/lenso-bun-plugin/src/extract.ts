import { dirname } from "node:path";
import * as ts from "typescript/unstable/ast";
import {
  API,
  SymbolFlags,
  type Checker,
  type Diagnostic,
  type Symbol as TypeScriptSymbol,
} from "typescript/unstable/async";

import type {
  BuildArgument,
  BuildPackageIdentity,
  BuildValue,
  ContractArgument,
  DeclarationArgument,
  HandlerArgument,
  SourceSpan,
  ValueArgument,
} from "./build.js";

export interface SymbolOrigin {
  readonly file: string;
  readonly name: string;
}

export type SymbolMeaning =
  | { readonly kind: "plugin_definition" }
  | {
      readonly kind: "declaration";
      readonly package: BuildPackageIdentity;
      readonly export_name: string;
      readonly handler_parameters?: ReadonlyArray<number>;
    }
  | {
      readonly kind: "contract";
      readonly capability_id: string;
      readonly descriptor_version: string;
      readonly descriptor_digest: string;
      readonly generated_module: string;
      readonly generated_export: string;
    };

export interface ExtractionOptions {
  readonly entryFile: string;
  readonly classifySymbol: (origin: SymbolOrigin) => SymbolMeaning | undefined;
}

export interface ExtractedPluginDefinition {
  readonly span: SourceSpan;
  readonly config?: BuildArgument;
  readonly dependencies?: Readonly<Record<string, BuildArgument>>;
  readonly providers: ReadonlyArray<BuildArgument>;
  readonly create?: HandlerArgument;
  readonly stop?: HandlerArgument;
  readonly sourceFiles: ReadonlyArray<string>;
}

export class DeclarationExtractionError extends Error {
  readonly span: SourceSpan;

  constructor(message: string, span: SourceSpan) {
    super(message);
    this.name = "DeclarationExtractionError";
    this.span = span;
  }
}

/** Parses a Plugin declaration without importing or evaluating its module. */
export async function extractPluginDefinition(
  options: ExtractionOptions,
): Promise<ExtractedPluginDefinition> {
  const api = new API({ cwd: dirname(options.entryFile) });
  try {
    const snapshot = await api.updateSnapshot({ openFiles: [options.entryFile] });
    const project = await snapshot.getDefaultProjectForFile(options.entryFile);
    if (project === undefined) {
      throw new DeclarationExtractionError(
        `cannot resolve a TypeScript project for ${options.entryFile}`,
        { file: options.entryFile, start: 0, end: 0 },
      );
    }
    const program = project.program;
    const source = await program.getSourceFile(options.entryFile);
    if (source === undefined) {
      throw new DeclarationExtractionError(
        `cannot read Plugin entry ${options.entryFile}`,
        { file: options.entryFile, start: 0, end: 0 },
      );
    }
    const sourceDiagnostic = (
      await program.getSyntacticDiagnostics(options.entryFile)
    )[0];
    if (sourceDiagnostic !== undefined) {
      throw diagnosticError(sourceDiagnostic, source);
    }

    const evaluator = new StaticEvaluator(project.checker, options.classifySymbol);
    const assignment = source.statements.find(
      (statement): statement is ts.ExportAssignment =>
        ts.isExportAssignment(statement) && !statement.isExportEquals,
    );
    if (assignment === undefined) {
      throw evaluator.error(
        source,
        "Plugin module must have one default export",
        source,
      );
    }
    const call = unwrap(assignment.expression);
    if (!ts.isCallExpression(call)) {
      throw evaluator.error(call, "default export must call definePlugin", source);
    }
    const meaning = await evaluator.meaning(call.expression);
    if (meaning?.kind !== "plugin_definition") {
      throw evaluator.error(
        call.expression,
        "default export must call the generic definePlugin export",
        source,
      );
    }
    if (call.arguments.length !== 1) {
      throw evaluator.error(
        call,
        "definePlugin requires exactly one declaration object",
        source,
      );
    }
    const declaration = await evaluator.object(call.arguments[0]!, {
      create: "handler",
      stop: "handler",
    });
    const allowed = new Set([
      "config",
      "dependencies",
      "providers",
      "create",
      "stop",
      "maxConcurrentRequests",
    ]);
    for (const key of declaration.keys()) {
      if (!allowed.has(key)) {
        throw evaluator.error(
          call.arguments[0]!,
          `unknown definePlugin field ${key}`,
          source,
        );
      }
    }
    const providers = requiredArray(
      declaration.get("providers"),
      "providers",
      evaluator,
      source,
    );
    const dependenciesValue = declaration.get("dependencies");
    const dependencies =
      dependenciesValue === undefined
        ? undefined
        : requiredObject(dependenciesValue, "dependencies", evaluator, source);
    const create = optionalHandler(
      declaration.get("create"),
      "create",
      evaluator,
      source,
    );
    const stop = optionalHandler(
      declaration.get("stop"),
      "stop",
      evaluator,
      source,
    );

    return Object.freeze({
      span: span(call, source),
      ...(declaration.get("config") === undefined
        ? {}
        : { config: declaration.get("config")! }),
      ...(dependencies === undefined
        ? {}
        : { dependencies: Object.freeze(Object.fromEntries(dependencies)) }),
      providers: Object.freeze(providers),
      ...(create === undefined ? {} : { create }),
      ...(stop === undefined ? {} : { stop }),
      sourceFiles: Object.freeze(evaluator.sourceFiles(source.fileName)),
    });
  } finally {
    await api.close();
  }
}

type ValueMode = "declaration" | "handler";

class StaticEvaluator {
  readonly #checker: Checker;
  readonly #classify: ExtractionOptions["classifySymbol"];
  readonly #active = new Set<TypeScriptSymbol>();
  readonly #sourceFiles = new Set<string>();

  constructor(
    checker: Checker,
    classify: ExtractionOptions["classifySymbol"],
  ) {
    this.#checker = checker;
    this.#classify = classify;
  }

  async value(
    expression: ts.Expression,
    mode: ValueMode = "declaration",
  ): Promise<BuildArgument> {
    const value = unwrap(expression);
    const source = value.getSourceFile();
    this.#sourceFiles.add(source.fileName);
    if (mode === "handler") return this.handler(value);
    if (ts.isStringLiteralLikeNode(value)) return this.literal(value.text, value);
    if (ts.isNumericLiteral(value)) return this.literal(Number(value.text), value);
    if (value.kind === ts.SyntaxKind.TrueKeyword) return this.literal(true, value);
    if (value.kind === ts.SyntaxKind.FalseKeyword) return this.literal(false, value);
    if (value.kind === ts.SyntaxKind.NullKeyword) return this.literal(null, value);
    if (
      ts.isPrefixUnaryExpression(value) &&
      value.operator === ts.SyntaxKind.MinusToken &&
      ts.isNumericLiteral(value.operand)
    ) {
      return this.literal(-Number(value.operand.text), value);
    }
    if (ts.isArrayLiteralExpression(value)) {
      const elements: Array<BuildArgument> = [];
      for (const element of value.elements) {
        if (ts.isSpreadElement(element)) {
          const spread = await this.value(element.expression);
          const spreadValue = valuePayload(spread, "array spread", this, source);
          if (!Array.isArray(spreadValue)) {
            throw this.error(element, "array spread must resolve to a static array", source);
          }
          elements.push(...spreadValue.map((item) => asArgument(item, element, source)));
        } else {
          elements.push(await this.value(element));
        }
      }
      return this.literal(elements, value);
    }
    if (ts.isObjectLiteralExpression(value)) {
      return this.literal(Object.fromEntries(await this.object(value)), value);
    }
    if (ts.isCallExpression(value)) return this.call(value);
    if (ts.isIdentifier(value) || ts.isPropertyAccessExpression(value)) {
      const meaning = await this.meaning(value);
      if (meaning?.kind === "contract") return contractArgument(meaning, value, source);
      if (meaning !== undefined) {
        throw this.error(value, "a declaration export must be called", source);
      }
      if (ts.isPropertyAccessExpression(value)) {
        throw this.error(value, "runtime property access is not supported", source);
      }
      return this.constant(value);
    }
    throw this.error(value, "expression is not in the static declaration subset", source);
  }

  async object(
    expression: ts.Expression,
    modes: Readonly<Record<string, ValueMode>> = {},
  ): Promise<Map<string, BuildArgument>> {
    const value = unwrap(expression);
    const source = value.getSourceFile();
    if (!ts.isObjectLiteralExpression(value)) {
      const resolved = await this.value(value);
      return requiredObject(resolved, "object", this, source);
    }
    const entries = new Map<string, BuildArgument>();
    for (const property of value.properties) {
      if (ts.isSpreadAssignment(property)) {
        const spread = requiredObject(
          await this.value(property.expression),
          "object spread",
          this,
          source,
        );
        for (const [key, item] of spread) this.add(entries, key, item, property, source);
        continue;
      }
      if (
        ts.isGetAccessorDeclaration(property) ||
        ts.isSetAccessorDeclaration(property) ||
        ts.isMethodDeclaration(property)
      ) {
        throw this.error(property, "methods and accessors are not declaration values", source);
      }
      if (!ts.isPropertyAssignment(property) && !ts.isShorthandPropertyAssignment(property)) {
        throw this.error(property, "unsupported object declaration member", source);
      }
      const key = propertyName(property.name, this, source);
      const initializer: ts.Expression = ts.isShorthandPropertyAssignment(property)
        ? (property.name as ts.Identifier)
        : property.initializer;
      this.add(
        entries,
        key,
        await this.value(initializer, modes[key]),
        property,
        source,
      );
    }
    return entries;
  }

  async meaning(expression: ts.Expression): Promise<SymbolMeaning | undefined> {
    const target = ts.isPropertyAccessExpression(expression) ? expression.name : expression;
    const symbol = await this.resolveSymbol(await this.symbolFor(target));
    if (symbol === undefined) return undefined;
    const declaration = symbol.valueDeclaration
      ? await symbol.valueDeclaration.resolve()
      : symbol.declarations[0]
        ? await symbol.declarations[0].resolve()
        : undefined;
    if (declaration === undefined) return undefined;
    this.#sourceFiles.add(declaration.getSourceFile().fileName);
    return this.#classify({
      file: declaration.getSourceFile().fileName,
      name: symbol.name,
    });
  }

  error(node: ts.Node, message: string, fallback: ts.SourceFile): DeclarationExtractionError {
    return new DeclarationExtractionError(message, span(node, node.getSourceFile() ?? fallback));
  }

  sourceFiles(entryFile: string): ReadonlyArray<string> {
    this.#sourceFiles.add(entryFile);
    return [...this.#sourceFiles].sort();
  }

  private literal(value: BuildValue, node: ts.Node): ValueArgument {
    return Object.freeze({ kind: "value", value, span: span(node, node.getSourceFile()) });
  }

  private async call(call: ts.CallExpression): Promise<DeclarationArgument> {
    const source = call.getSourceFile();
    const meaning = await this.meaning(call.expression);
    if (meaning?.kind !== "declaration") {
      throw this.error(call.expression, "unsupported call in declaration", source);
    }
    const handlerParameters = new Set(meaning.handler_parameters ?? []);
    const arguments_ = await Promise.all(
      call.arguments.map((argument, index) =>
        this.value(
          argument,
          handlerParameters.has(index) ? "handler" : "declaration",
        ),
      ),
    );
    return Object.freeze({
      kind: "declaration",
      package: meaning.package,
      export_name: meaning.export_name,
      arguments: Object.freeze(arguments_),
      span: span(call, source),
    });
  }

  private async constant(
    expression: ts.Identifier | ts.PropertyAccessExpression,
  ): Promise<BuildArgument> {
    const source = expression.getSourceFile();
    const target = ts.isPropertyAccessExpression(expression) ? expression.name : expression;
    const symbol = await this.resolveSymbol(await this.symbolFor(target));
    if (symbol === undefined) throw this.error(expression, "unresolved declaration reference", source);
    if (this.#active.has(symbol)) throw this.error(expression, "cyclic declaration constant", source);
    const declaration = symbol.valueDeclaration
      ? await symbol.valueDeclaration.resolve()
      : undefined;
    if (declaration === undefined) throw this.error(expression, "declaration reference has no static value", source);
    if (ts.isVariableDeclaration(declaration)) {
      const list = declaration.parent;
      if (!ts.isVariableDeclarationList(list) || (list.flags & ts.NodeFlags.Const) === 0) {
        throw this.error(declaration, "mutable declaration values are not supported", source);
      }
      if (declaration.initializer === undefined) {
        throw this.error(declaration, "declaration constant has no initializer", source);
      }
      this.#active.add(symbol);
      try {
        return await this.value(declaration.initializer);
      } finally {
        this.#active.delete(symbol);
      }
    }
    throw this.error(declaration, "reference is not a static const declaration", source);
  }

  private async handler(expression: ts.Expression): Promise<HandlerArgument> {
    const value = unwrap(expression);
    const source = value.getSourceFile();
    let target: ts.Node = value;
    if (ts.isIdentifier(value)) {
      const symbol = await this.resolveSymbol(await this.symbolFor(value));
      const resolvedDeclaration = symbol?.valueDeclaration
        ? await symbol.valueDeclaration.resolve()
        : undefined;
      const declaration =
        resolvedDeclaration !== undefined &&
        ts.isIdentifier(resolvedDeclaration) &&
        (ts.isFunctionDeclaration(resolvedDeclaration.parent) ||
          ts.isVariableDeclaration(resolvedDeclaration.parent))
          ? resolvedDeclaration.parent
          : resolvedDeclaration;
      if (declaration === undefined) throw this.error(value, "handler reference is unresolved", source);
      if (ts.isVariableDeclaration(declaration)) {
        const list = declaration.parent;
        if (!ts.isVariableDeclarationList(list) || (list.flags & ts.NodeFlags.Const) === 0) {
          throw this.error(declaration, "handler reference must be a static function or const", source);
        }
        if (
          declaration.initializer === undefined ||
          (!ts.isArrowFunction(declaration.initializer) &&
            !ts.isFunctionExpression(declaration.initializer))
        ) {
          throw this.error(declaration, "handler const must contain a function", source);
        }
        target = declaration.initializer;
      } else if (ts.isFunctionDeclaration(declaration)) {
        target = declaration;
      } else {
        throw this.error(declaration, "handler reference must name a function", source);
      }
    } else if (!ts.isArrowFunction(value) && !ts.isFunctionExpression(value)) {
      throw this.error(value, "handler position requires a function", source);
    }
    const handlerSpan = span(target, target.getSourceFile());
    return Object.freeze({
      kind: "handler",
      reference: `${handlerSpan.file}:${handlerSpan.start}:${handlerSpan.end}`,
      span: handlerSpan,
    });
  }

  private async resolveSymbol(
    symbol: TypeScriptSymbol | undefined,
  ): Promise<TypeScriptSymbol | undefined> {
    let resolved = symbol;
    const seen = new Set<TypeScriptSymbol>();
    while (resolved !== undefined && (resolved.flags & SymbolFlags.Alias) !== 0) {
      if (seen.has(resolved)) return undefined;
      seen.add(resolved);
      resolved = await this.#checker.getAliasedSymbol(resolved);
      if (await this.#checker.isUnknownSymbol(resolved)) return undefined;
    }
    return resolved;
  }

  private async symbolFor(node: ts.Node): Promise<TypeScriptSymbol | undefined> {
    if (
      ts.isIdentifier(node) &&
      ts.isShorthandPropertyAssignment(node.parent)
    ) {
      return this.#checker.getShorthandAssignmentValueSymbol(node.parent);
    }
    return this.#checker.getSymbolAtLocation(node);
  }

  private add(
    entries: Map<string, BuildArgument>,
    key: string,
    value: BuildArgument,
    node: ts.Node,
    source: ts.SourceFile,
  ): void {
    if (entries.has(key)) throw this.error(node, `duplicate object key ${key}`, source);
    entries.set(key, value);
  }
}

function unwrap(expression: ts.Expression): ts.Expression {
  let value = expression;
  while (
    ts.isParenthesizedExpression(value) ||
    ts.isAsExpression(value) ||
    ts.isAssertionExpression(value) ||
    ts.isSatisfiesExpression(value) ||
    ts.isNonNullExpression(value)
  ) {
    value = value.expression;
  }
  return value;
}

function propertyName(
  name: ts.PropertyName,
  evaluator: StaticEvaluator,
  source: ts.SourceFile,
): string {
  if (
    ts.isIdentifier(name) ||
    ts.isStringLiteralLikeNode(name) ||
    ts.isNumericLiteral(name)
  ) {
    return name.text;
  }
  throw evaluator.error(name, "computed declaration keys are not supported", source);
}

function requiredArray(
  argument: BuildArgument | undefined,
  subject: string,
  evaluator: StaticEvaluator,
  source: ts.SourceFile,
): BuildArgument[] {
  if (argument === undefined) {
    throw evaluator.error(source, `definePlugin requires ${subject}`, source);
  }
  const value = valuePayload(argument, subject, evaluator, source);
  if (!Array.isArray(value)) throw evaluator.error(source, `${subject} must be a static array`, source);
  return value.map((item) => asArgument(item, source, source));
}

function requiredObject(
  argument: BuildArgument,
  subject: string,
  evaluator: StaticEvaluator,
  source: ts.SourceFile,
): Map<string, BuildArgument> {
  const value = valuePayload(argument, subject, evaluator, source);
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw evaluator.error(source, `${subject} must be a static object`, source);
  }
  return new Map(
    Object.entries(value).map(([key, item]) => [key, asArgument(item, source, source)]),
  );
}

function valuePayload(
  argument: BuildArgument,
  subject: string,
  evaluator: StaticEvaluator,
  source: ts.SourceFile,
): BuildValue {
  if (argument.kind !== "value") {
    throw evaluator.error(source, `${subject} must resolve to a literal value`, source);
  }
  return argument.value;
}

function asArgument(value: BuildValue | BuildArgument, node: ts.Node, source: ts.SourceFile): BuildArgument {
  if (
    typeof value === "object" &&
    value !== null &&
    !Array.isArray(value) &&
    "kind" in value &&
    "span" in value
  ) {
    return value as BuildArgument;
  }
  return Object.freeze({ kind: "value", value, span: span(node, source) });
}

function optionalHandler(
  argument: BuildArgument | undefined,
  subject: string,
  evaluator: StaticEvaluator,
  source: ts.SourceFile,
): HandlerArgument | undefined {
  if (argument === undefined) return undefined;
  if (argument.kind !== "handler") {
    throw evaluator.error(source, `${subject} must be a static function`, source);
  }
  return argument;
}

function contractArgument(
  meaning: Extract<SymbolMeaning, { readonly kind: "contract" }>,
  node: ts.Node,
  source: ts.SourceFile,
): ContractArgument {
  return Object.freeze({
    kind: "contract",
    capability_id: meaning.capability_id,
    descriptor_version: meaning.descriptor_version,
    descriptor_digest: meaning.descriptor_digest,
    generated_module: meaning.generated_module,
    generated_export: meaning.generated_export,
    span: span(node, source),
  });
}

function span(node: ts.Node, source: ts.SourceFile): SourceSpan {
  return Object.freeze({
    file: source.fileName,
    start: node.getStart(source),
    end: node.getEnd(),
  });
}

function diagnosticError(
  diagnostic: Diagnostic,
  fallback: ts.SourceFile,
): DeclarationExtractionError {
  const file = diagnostic.fileName ?? fallback.fileName;
  const start = diagnostic.pos;
  return new DeclarationExtractionError(
    diagnostic.text,
    { file, start, end: diagnostic.end },
  );
}
