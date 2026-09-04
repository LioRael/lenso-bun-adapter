import { expect, test } from "bun:test";

import {
  BUILD_API_VERSION,
  BuildOutputError,
  runLowering,
  type HandlerArgument,
  type LoweringInput,
  type LoweringOutput,
} from "../src/build.ts";

const span = { file: "src/plugin.ts", start: 10, end: 20 } as const;
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

function validOutput(): LoweringOutput {
  return {
    api_version: BUILD_API_VERSION,
    providers: [
      {
        capability_id: "example.operation@1",
        descriptor_version: "1.0.0",
        descriptor_digest: "sha256:descriptor",
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
  }, input);
  expect(output.providers[0]?.capability_id).toBe("example.operation@1");
});

test("rejects hidden fields and generated path escape", async () => {
  await expect(
    runLowering(
      async () => ({ ...validOutput(), dependencies: [{ id: "hidden" }] }) as never,
      input,
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
    ),
  ).rejects.toThrow("normalized relative path");
});

test("rejects missing, unknown, and repeated handler references", async () => {
  const missing = validOutput();
  await expect(
    runLowering(async () => ({ ...missing, providers: [] }), input),
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
    ),
  ).rejects.toThrow("more than once");
});

test("reports source-located output failures", async () => {
  try {
    await runLowering(async () => ({ ...validOutput(), api_version: 2 as never }), input);
    throw new Error("expected validation failure");
  } catch (error) {
    expect(error).toBeInstanceOf(BuildOutputError);
    expect((error as BuildOutputError).span).toEqual(span);
  }
});
