# Versioned conformance inputs

These files are immutable test inputs copied at the ADR 0064 extraction point
`LioRael/lenso@67d21499548d07e92c2f6529d7c8345e58c067d9`:

- `greeting.ts`: `lenso-capability-greeting` 0.1.0 generated binding;
- `secure-greeting.ts`: `lenso-capability-secure-greeting` 0.1.0 generated binding;
- `actor.ts`: `lenso-auth-sdk` 0.1.1 TypeScript helper;
- `trace-context.ts`: `lenso-otel-module` 0.1.1 TypeScript helper.

They are conformance snapshots, not source ownership. Refresh them only from a
released owner package and review the resulting wire change.
