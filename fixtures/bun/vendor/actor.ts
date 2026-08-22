export type WireExtension = {
  key: string;
  value: number[];
  issuer?: string;
  audience?: string[];
  proof?: string;
  sealed?: boolean;
};

export type ActorAssertion = {
  actor_kind: string;
  assurance: string;
  audience: string[];
  claims?: Record<string, unknown>;
  expires_at: string;
  issued_at: string;
  issuer: string;
  parent_provenance?: string;
  proof: string;
  subject: string;
};

export class ActorBindingError extends Error {}

const base64Url = (bytes: ArrayBuffer): string =>
  Buffer.from(bytes).toString("base64url");

export async function bindActor(
  extensions: WireExtension[] | undefined,
  capabilityId: string,
  operation: string,
  expectedIssuer: string,
  signingKey: string,
  actorKind: string,
  now = new Date(),
): Promise<ActorAssertion> {
  const expectedAudience = `${capabilityId}:${operation}`;
  const extension = extensions?.find(
    (candidate) => candidate.key === "lenso.auth.actor-assertion",
  );
  if (
    !extension?.sealed ||
    extension.issuer !== expectedIssuer ||
    !extension.audience?.includes(expectedAudience) ||
    !extension.proof ||
    extension.value.length === 0
  ) {
    throw new ActorBindingError("actor assertion is missing or not target-bound");
  }

  let assertion: ActorAssertion;
  try {
    assertion = JSON.parse(
      new TextDecoder().decode(new Uint8Array(extension.value)),
    ) as ActorAssertion;
  } catch {
    throw new ActorBindingError("actor assertion is not valid JSON");
  }
  if (
    assertion.actor_kind !== actorKind ||
    typeof assertion.assurance !== "string" ||
    !Array.isArray(assertion.audience) ||
    !assertion.audience.every((entry) => typeof entry === "string") ||
    !assertion.audience.includes(expectedAudience) ||
    assertion.issuer !== expectedIssuer ||
    typeof assertion.subject !== "string" ||
    assertion.subject.length === 0 ||
    typeof assertion.proof !== "string" ||
    assertion.proof !== extension.proof ||
    typeof assertion.issued_at !== "string" ||
    typeof assertion.expires_at !== "string"
  ) {
    throw new ActorBindingError("actor assertion shape is invalid");
  }
  const issuedAt = Date.parse(assertion.issued_at);
  const expiresAt = Date.parse(assertion.expires_at);
  if (!Number.isFinite(issuedAt) || !Number.isFinite(expiresAt) || issuedAt >= expiresAt) {
    throw new ActorBindingError("actor assertion validity is invalid");
  }
  if (now.getTime() < issuedAt || now.getTime() >= expiresAt) {
    throw new ActorBindingError("actor assertion is outside its validity interval");
  }

  const signingPayload = JSON.stringify({
    actor_kind: assertion.actor_kind,
    assurance: assertion.assurance,
    audience: assertion.audience,
    claims: assertion.claims ?? null,
    expires_at: assertion.expires_at,
    issued_at: assertion.issued_at,
    issuer: assertion.issuer,
    parent_provenance: assertion.parent_provenance ?? null,
    subject: assertion.subject,
  });
  const key = await crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(signingKey),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const proof = base64Url(
    await crypto.subtle.sign("HMAC", key, new TextEncoder().encode(signingPayload)),
  );
  if (proof !== assertion.proof) {
    throw new ActorBindingError("actor assertion proof is invalid");
  }
  return assertion;
}
