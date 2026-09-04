import { definePlugin, tools } from "./sdk.ts";

let declarations: unknown[] = [];

export default definePlugin({ providers: [tools(declarations)] });
