import { definePlugin } from "./sdk.ts";
import { Conversation } from "../generated/conversation.ts";
import { Profile } from "../generated/profile.ts";

export default definePlugin({
  provides: [Conversation],
  dependencies: { profile: Profile.required() },
  create() {
    return {};
  },
});
