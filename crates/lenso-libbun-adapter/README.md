# Lenso libbun Adapter experiment

This unpublished crate proves a separate `lenso.bun-embedded@1` Execution
Class without changing the portable Kernel or the production
`lenso.bun-process@1` Adapter.

The experiment is deliberately narrow:

- one embedded Bun Plugin Instance per resolved Plan;
- request Capabilities only;
- leaf Providers with no required Capabilities or Host Imports;
- one bounded serial queue feeding one affine Bun VM thread; and
- an exact, host-selected `libbun` native plugin path.

The selected entrypoint must export `lensoInvoke`. It receives:

```json
{
  "capability": "example.greeting@1",
  "operation": "greet",
  "request": { "name": "Ada" },
  "configuration": {}
}
```

It must resolve to one of these envelopes:

```json
{ "kind": "ok", "value": { "message": "Hello, Ada" } }
{ "kind": "domain_error", "value": "empty_name" }
```

Thrown exceptions, rejected promises, malformed envelopes, oversized values,
and dynamic runtime failures are Runtime Failures. They are never converted to
Capability Domain Errors.

This is trusted in-process execution, not a sandbox. Cancellation stops result
delivery but cannot preempt a synchronous JavaScript call inside JSC. Keep the
process Adapter as the production default until the experiment's conformance,
shutdown, performance, platform, and distribution gates have passed.

The native plugin carries Bun/JSC/WebKit redistribution requirements. Product
bundles must keep it replaceable and pass through the matching libbun source,
notice, license inventory, and checksum artifacts.

Run the real dynamic-plugin smoke explicitly after installing a matching
release asset:

```sh
LIBBUN_PLUGIN_PATH=/absolute/path/to/liblibbun_plugin_native.dylib \
  cargo test -p lenso-libbun-adapter --lib \
  tests::real_dynamic_plugin_smoke --locked -- --ignored --exact
```
