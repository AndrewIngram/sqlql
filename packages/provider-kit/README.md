# `@tupl/provider-kit`

Provider contracts, entity binding helpers, and reusable shape utilities for `tupl` adapters.

Use this package when authoring custom providers or adapter-style integrations.

Stable adapter-authoring surfaces:

- `@tupl/provider-kit`: adapter contracts, request/row types, entity handles, capability helpers
- `@tupl/provider-kit/shapes`: reusable provider-shape analysis and relational pushdown helpers
- `@tupl/provider-kit/testing`: framework-neutral adapter conformance cases

Ordinary adapter code should not need to import `@tupl/schema-model` directly.

For ordinary SQL-like adapters, the main path is `createSqlRelationalProviderAdapter(...)` on the
package root. It owns recursive rel compilation and keeps provider packages focused on backend
query-builder hooks plus runtime binding.

Use `createRelationalProviderAdapter(...)` when an adapter is unusual enough that it cannot fit the
ordinary SQL-like path cleanly.
