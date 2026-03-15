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
- `@tupl/provider-kit/relational-sql`: advanced SQL-relational translation types and helper internals
- `@tupl/provider-kit/shapes`: reusable provider-shape analysis and relational pushdown helpers
- `@tupl/provider-kit/testing`: framework-neutral adapter conformance cases

Ordinary adapter code should not need to import `@tupl/schema-model` directly.

For ordinary SQL-like adapters, the main path is `createSqlRelationalProviderAdapter(...)` on the
package root. It keeps provider roots close to the manual provider lifecycle:

- top-level lifecycle/config hooks such as `resolveRuntime(...)`
- one nested `queryBackend` that owns backend query translation and execution
- optional `advanced` escape hatch for unusual scan-binding or strategy overrides

Adapter authors can usually rely on defaults for resolved entities, scan bindings, and strategy
selection; `advanced` is only needed for unusual SQL-like backends. If an adapter needs the
backend translation contracts themselves, import those from `@tupl/provider-kit/relational-sql`
rather than widening the package root surface.

Use `createRelationalProviderAdapter(...)` when an adapter is unusual enough that it cannot fit the
ordinary SQL-like path cleanly.
