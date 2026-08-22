/** W3C Trace Context propagation for a Bun Module Adapter. */

export const TRACE_CONTEXT_EXTENSION_KEY = "lenso.otel.trace-context";

export type WireExtension = {
  key: string;
  value: number[];
  issuer?: string;
  audience?: string[];
  proof?: string;
  sealed?: boolean;
};

export type TraceContext = {
  traceparent: string;
  tracestate?: string;
};

export class TraceContextError extends Error {}

const encoder = new TextEncoder();
const decoder = new TextDecoder();

/** Adds one target-scoped sealed Trace Context without replacing an extension. */
export async function injectTraceContext(
  extensions: readonly WireExtension[] | undefined,
  trace: TraceContext,
  issuer: string,
  signingKey: string,
  audiences: readonly string[],
): Promise<WireExtension[]> {
  if (!issuer || !signingKey || audiences.length === 0 || audiences.some((value) => !value)) {
    throw new TraceContextError("trace-context provenance or audience is invalid");
  }
  const existing = extensions ?? [];
  if (existing.some((extension) => extension.key === TRACE_CONTEXT_EXTENSION_KEY)) {
    throw new TraceContextError("trace-context extension is already set");
  }
  const value = serializeTraceContext(trace);
  const proof = await sign(issuer, audiences, value, signingKey);
  return [
    ...existing,
    {
      key: TRACE_CONTEXT_EXTENSION_KEY,
      value: Array.from(value),
      issuer,
      audience: [...audiences],
      proof,
      sealed: true,
    },
  ];
}

/** Verifies and parses a target-scoped sealed Trace Context. */
export async function extractTraceContext(
  extensions: readonly WireExtension[] | undefined,
  capabilityId: string,
  operation: string,
  expectedIssuer: string,
  signingKey: string,
): Promise<TraceContext | undefined> {
  const extension = extensions?.find(
    (candidate) => candidate.key === TRACE_CONTEXT_EXTENSION_KEY,
  );
  if (!extension) return undefined;
  const expectedAudience = `${capabilityId}:${operation}`;
  if (
    !extension.sealed ||
    extension.issuer !== expectedIssuer ||
    !extension.proof ||
    !extension.audience?.includes(expectedAudience)
  ) {
    throw new TraceContextError("trace-context extension is not target-bound");
  }
  const value = Uint8Array.from(extension.value);
  const expectedProof = await sign(
    expectedIssuer,
    extension.audience,
    value,
    signingKey,
  );
  if (!constantTimeEqual(expectedProof, extension.proof)) {
    throw new TraceContextError("trace-context proof is invalid");
  }
  return parseTraceContext(decoder.decode(value));
}

function serializeTraceContext(trace: TraceContext): Uint8Array {
  const parsed = parseTraceContext(trace.traceparent + (trace.tracestate ? `\n${trace.tracestate}` : ""));
  return encoder.encode(parsed.traceparent + (parsed.tracestate ? `\n${parsed.tracestate}` : ""));
}

function parseTraceContext(value: string): TraceContext {
  const newline = value.indexOf("\n");
  const traceparent = newline < 0 ? value : value.slice(0, newline);
  const tracestate = newline < 0 ? undefined : value.slice(newline + 1);
  const fields = traceparent.split("-");
  if (
    !/^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$/.test(traceparent) ||
    fields[1] === "0".repeat(32) ||
    fields[2] === "0".repeat(16)
  ) {
    throw new TraceContextError("traceparent is invalid");
  }
  if (
    tracestate !== undefined &&
    (!tracestate || tracestate.length > 512 || /[\u0000-\u001f\u007f]/.test(tracestate))
  ) {
    throw new TraceContextError("tracestate is invalid");
  }
  return tracestate === undefined ? { traceparent } : { traceparent, tracestate };
}

async function sign(
  issuer: string,
  audiences: readonly string[],
  value: Uint8Array,
  signingKey: string,
): Promise<string> {
  const payload = signingPayload(issuer, audiences, value);
  const key = await crypto.subtle.importKey(
    "raw",
    encoder.encode(signingKey),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  return base64Url(
    new Uint8Array(await crypto.subtle.sign("HMAC", key, payload)),
  );
}

function signingPayload(
  issuer: string,
  audiences: readonly string[],
  value: Uint8Array,
): Uint8Array {
  const parts = [encoder.encode(issuer), ...audiences.map((audience) => encoder.encode(audience))];
  const size = parts.reduce((total, part) => total + 8 + part.length, 0) + 8 + value.length;
  const payload = new Uint8Array(size);
  const view = new DataView(payload.buffer);
  let offset = 0;
  for (const part of parts) {
    view.setBigUint64(offset, BigInt(part.length));
    offset += 8;
    payload.set(part, offset);
    offset += part.length;
  }
  view.setBigUint64(offset, BigInt(value.length));
  payload.set(value, offset + 8);
  return payload;
}

function base64Url(bytes: Uint8Array): string {
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary).replaceAll("+", "-").replaceAll("/", "_").replaceAll("=", "");
}

function constantTimeEqual(left: string, right: string): boolean {
  if (left.length !== right.length) return false;
  let difference = 0;
  for (let index = 0; index < left.length; index += 1) {
    difference |= left.charCodeAt(index) ^ right.charCodeAt(index);
  }
  return difference === 0;
}
