import { expect, test } from "bun:test";

import {
  BUILD_API_VERSION,
  BuildOutputError,
  fingerprintBuildInputs,
  runLowering,
  verifyBuildFingerprint,
  type HandlerArgument,
  type LoweringInput,
  type LoweringOutput,
} from "../src/build.ts";

const span = { file: "src/plugin.ts", start: 10, end: 20 } as const;
const descriptorDigest = `sha256:${"b".repeat(64)}`;
const handler: HandlerArgument = {
  kind: "handler",
  reference: "handler:sync",
  span,
};
const input: LoweringInput = {
  api_version: BUILD_API_VERSION,
  package: {
    name: "third-party-sdk",
    version: "1.2.3",
    integrity: "sha512-locked",
  },
  export_name: "operation",
  arguments: [handler],
  span,
};
const constraints = {
  allowedProviders: [
    {
      capability_id: "example.operation@1",
      descriptor_version: "1.0.0",
      descriptor_digest: descriptorDigest,
    },
  ],
};

function validOutput(): LoweringOutput {
  return {
    api_version: BUILD_API_VERSION,
    providers: [
      {
        capability_id: "example.operation@1",
        descriptor_version: "1.0.0",
        descriptor_digest: descriptorDigest,
        binder: { module: "generated/operation.ts", export_name: "bindOperation" },
        handler_references: [handler.reference],
      },
    ],
    files: [{ path: "generated/operation.ts", contents: "export {};" }],
    diagnostics: [],
  };
}

test("accepts a product-neutral third-party lowering", async () => {
  const output = await runLowering(async (received) => {
    expect(received).toBe(input);
    return validOutput();
  }, input, constraints);
  expect(output.providers[0]?.capability_id).toBe("example.operation@1");
});

test("rejects hidden fields and generated path escape", async () => {
  await expect(
    runLowering(
      async () => ({ ...validOutput(), dependencies: [{ id: "hidden" }] }) as never,
      input,
      constraints,
    ),
  ).rejects.toThrow("unknown field dependencies");

  const escaped = validOutput();
  await expect(
    runLowering(
      async () => ({
        ...escaped,
        files: [{ path: "../outside.ts", contents: "" }],
      }),
      input,
      constraints,
    ),
  ).rejects.toThrow("normalized relative path");
});

test("rejects missing, unknown, and repeated handler references", async () => {
  const missing = validOutput();
  await expect(
    runLowering(async () => ({ ...missing, providers: [] }), input, constraints),
  ).rejects.toThrow("did not use handler reference handler:sync");

  const unknown = validOutput();
  await expect(
    runLowering(
      async () => ({
        ...unknown,
        providers: [
          { ...unknown.providers[0]!, handler_references: ["handler:other"] },
        ],
      }),
      input,
      constraints,
    ),
  ).rejects.toThrow("unknown handler reference handler:other");

  const repeated = validOutput();
  await expect(
    runLowering(
      async () => ({
        ...repeated,
        providers: [
          {
            ...repeated.providers[0]!,
            handler_references: [handler.reference, handler.reference],
          },
        ],
      }),
      input,
      constraints,
    ),
  ).rejects.toThrow("more than once");
});

test("reports source-located output failures", async () => {
  try {
    await runLowering(
      async () => ({ ...validOutput(), api_version: 2 as never }),
      input,
      constraints,
    );
    throw new Error("expected validation failure");
  } catch (error) {
    expect(error).toBeInstanceOf(BuildOutputError);
    expect((error as BuildOutputError).span).toEqual(span);
  }
});

test("rejects a provider outside the SDK's exact contract set", async () => {
  await expect(
    runLowering(async () => validOutput(), input, {
      allowedProviders: [
        {
          capability_id: "example.operation@1",
          descriptor_version: "1.0.0",
          descriptor_digest: `sha256:${"c".repeat(64)}`,
        },
      ],
    }),
  ).rejects.toThrow("does not match an allowed exact contract");
});

test("fingerprints source, contract, package, and target inputs canonically", () => {
  const digest = `sha256:${"a".repeat(64)}`;
  const fingerprintInput = {
    sourceClosure: [{ path: "src/plugin.ts", sha256: digest }],
    contractArtifacts: [{ path: "generated/store.ts", sha256: digest }],
    lockedPackages: [input.package],
    target: "bun-darwin-arm64",
  };
  const fingerprint = fingerprintBuildInputs(fingerprintInput);
  expect(fingerprint).toMatch(/^sha256:[0-9a-f]{64}$/);
  expect(() => verifyBuildFingerprint(fingerprint, fingerprintInput)).not.toThrow();
  expect(() =>
    verifyBuildFingerprint(fingerprint, {
      ...fingerprintInput,
      target: "bun-linux-x64",
    }),
  ).toThrow("fingerprint drift");
});
