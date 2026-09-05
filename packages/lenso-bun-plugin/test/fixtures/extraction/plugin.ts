import { definePlugin, dependency, schemaString, tool } from "./sdk.ts";
import { common, groupedTools as grouped } from "./reexports.ts";
import { Store as RenamedStore } from "./sdk.ts";

const extraTools = [
  tool({ ...common, input: schemaString() }, (input: string) => input),
] as const;

throw new Error("module side effects must not run during extraction");

export default definePlugin({
  dependencies: {
    source: dependency({ id: "source", contract: RenamedStore }),
  },
  async create() {
    return { calls: 0 };
  },
  providers: [grouped([...extraTools])],
  stop(_instance: { calls: number }) {},
});
