import { definePlugin, dependency, schemaString, tool } from "./sdk.ts";
import { common, groupedTools as grouped } from "./reexports.ts";
import { Store as RenamedStore } from "./sdk.ts";

const extraTools = [
  tool({ ...common, input: schemaString() }, (input: string) => input),
] as const;

async function create() {
  return { calls: 0 };
}

const stop = async (_instance: { calls: number }) => {};

throw new Error("module side effects must not run during extraction");

export default definePlugin({
  dependencies: {
    source: dependency({ id: "source", contract: RenamedStore }),
  },
  create,
  providers: [grouped([...extraTools])],
  stop,
});
