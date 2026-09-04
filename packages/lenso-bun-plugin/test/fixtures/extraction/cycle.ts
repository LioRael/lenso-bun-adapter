import { definePlugin, tools } from "./sdk.ts";

const first = second;
const second = first;

export default definePlugin({ providers: [tools(first)] });
