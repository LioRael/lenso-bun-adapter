# Lenso Bun Adapter

The Bun child-process Execution Adapter, its wire implementation, and
cross-runtime conformance fixtures. This repository consumes released Lenso
core packages and does not own Kernel semantics or product Modules.

The source was extracted from `LioRael/lenso` at monorepo commit
`67d21499548d07e92c2f6529d7c8345e58c067d9` under ADR 0064. Imported subtrees
retain their relevant Git history.

## Validation

```sh
cargo fmt --all -- --check
cargo check --locked --workspace --all-targets
cargo test --locked --workspace
bun build --target bun fixtures/bun/request-provider.ts --outdir /tmp/lenso-bun-fixtures
cargo test --locked -p lenso-bun-adapter --test bun_cross_runtime -- --ignored --test-threads=1
```
