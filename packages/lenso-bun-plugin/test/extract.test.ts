import { expect, test } from "bun:test";
import { join } from "node:path";

import {
  DeclarationExtractionError,
  extractPluginDefinition,
  type SymbolOrigin,
} from "../src/extract.ts";

const fixtures = join(import.meta.dir, "fixtures", "extraction");
const packageIdentity = {
  name: "@example/agent-sdk",
  version: "1.0.0",
  integrity: "sha512-locked",
} as const;

function classify(origin: SymbolOrigin) {
  switch (origin.name) {
    case "definePlugin":
      return { kind: "plugin_definition" } as const;
    case "Store":
      return {
        kind: "contract",
        capability_id: "lenso.store@1",
        descriptor_version: "1.0.0",
        descriptor_digest: `sha256:${"a".repeat(64)}`,
        generated_module: "generated/store.ts",
        generated_export: "bindDependency",
      } as const;
    case "tool":
      return {
        kind: "declaration",
        package: packageIdentity,
        export_name: "tool",
        handler_parameters: [1],
      } as const;
    case "tools":
    case "schemaString":
    case "dependency":
      return {
        kind: "declaration",
        package: packageIdentity,
        export_name: origin.name,
      } as const;
    default:
      return undefined;
  }
}

test("extracts aliases, re-exports, constants, and spreads without module evaluation", async () => {
  const extracted = await extractPluginDefinition({
    entryFile: join(fixtures, "plugin.ts"),
    classifySymbol: classify,
  });

  expect(extracted.providers).toHaveLength(1);
  expect(extracted.providers[0]).toMatchObject({
    kind: "declaration",
    export_name: "tools",
  });
  expect(extracted.dependencies?.source).toMatchObject({
    kind: "declaration",
    export_name: "dependency",
  });
  expect(extracted.create?.kind).toBe("handler");
  expect(extracted.stop?.kind).toBe("handler");
  expect(extracted.sourceFiles.some((file) => file.endsWith("reexports.ts"))).toBe(true);
});

for (const [fixture, message] of [
  ["dynamic.ts", "runtime property access is not supported"],
  ["mutable.ts", "mutable declaration values are not supported"],
  ["cycle.ts", "cyclic declaration constant"],
  ["arbitrary-call.ts", "unsupported call in declaration"],
  ["duplicate-spread.ts", "duplicate object key name"],
] as const) {
  test(`rejects ${fixture} with a source span`, async () => {
    try {
      await extractPluginDefinition({
        entryFile: join(fixtures, fixture),
        classifySymbol: classify,
      });
      throw new Error("expected extraction failure");
    } catch (error) {
      expect(error).toBeInstanceOf(DeclarationExtractionError);
      expect((error as DeclarationExtractionError).message).toContain(message);
      expect((error as DeclarationExtractionError).span.file).toEndWith(fixture);
    }
  });
}
