# Agent instructions

This checkout retains pre-consolidation history. New Rust Adapter work belongs
in `LioRael/lenso`, and Bun SDK work in `LioRael/lenso-js`, under ADR 0077;
the rules below cover maintenance here.

This repository owns the Bun Execution Adapter and its transport conformance.
Keep product Capability semantics in their owning repositories and consume the
published portable core through versioned dependencies.

Use Conventional Commits and locked workspace checks.
