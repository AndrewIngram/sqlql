# SQL Provider Deepening

Status: completed

## Outcome

- `createSqlRelationalProviderAdapter(...)` now exposes one normal-path `queryBackend` plus an
  explicit `advanced` escape hatch for unusual scan-binding or strategy overrides.
- Shared SQL-relational orchestration stays in provider-kit; first-party SQL providers now supply
  backend query-builder primitives instead of their own root-to-leaf orchestration loops.
- Drizzle, Kysely, and Objection provider roots stay on the canonical SQL helper path.
- Code-level design comments were added at the provider semantic core.
- The thin runtime alias module `packages/runtime/src/runtime/plan-graph.ts` was deleted.

## Notes

- Drizzle remains the one first-party SQL adapter that needs `advanced` because its binding shape
  and single-query validation are materially different from the default SQL helper path.
- Kysely and Objection now stay on the normal path with only backend query translation and runtime
  binding differences.
