export function definePlugin(value: unknown): unknown {
  throw new Error(`must not execute definePlugin: ${String(value)}`);
}

export function tools(value: unknown): unknown {
  throw new Error(`must not execute tools: ${String(value)}`);
}

export function tool(value: unknown, handler: unknown): unknown {
  throw new Error(`must not execute tool: ${String(value)} ${String(handler)}`);
}

export function schemaString(): unknown {
  throw new Error("must not execute schemaString");
}

export function dependency(value: unknown): unknown {
  throw new Error(`must not execute dependency: ${String(value)}`);
}

export const Store = Symbol("must not read Store");
