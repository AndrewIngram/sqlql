# `@tupl/provider-kit`

Provider contracts, adapter-authoring helpers, entity binding helpers, and reusable shape
utilities for `tupl`.

Terminology in this package:

- `provider`: the runtime object the planner/runtime talks to
- `adapter`: the code or helper that constructs a provider
- `backend`: the wrapped external system or query builder

Use this package when authoring custom providers or adapter-style integrations.

Stable provider/adapter authoring surfaces:

- `@tupl/provider-kit`: provider contracts, request/row types, entity handles, capability helpers
- `@tupl/provider-kit/shapes`: reusable provider-shape analysis and relational pushdown helpers
- `@tupl/provider-kit/testing`: framework-neutral adapter conformance cases

Ordinary adapter code should not need to import `@tupl/schema-model` directly.

For ordinary SQL-like adapters, the main path is `createSqlRelationalProviderAdapter(...)` on the
package root. It owns recursive rel compilation and keeps provider packages focused on backend
query-builder hooks plus runtime binding. Adapter authors provide entity configs and a
`resolveEntity(...)` callback; provider-kit derives the internal resolved-entity map itself.

Use `createRelationalProviderAdapter(...)` when an adapter is unusual enough that it cannot fit the
ordinary SQL-like path cleanly.
