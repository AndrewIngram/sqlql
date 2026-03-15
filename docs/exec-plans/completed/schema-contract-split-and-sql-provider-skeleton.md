# Schema Contract Split and SQL Provider Skeleton

## Summary

This tranche finished the `TableMethods` boundary split, narrowed the remaining
`@tupl/schema-model` root leakage around that split, and extracted the last clearly shared
Kysely/Objection SQL-provider skeleton into `@tupl/provider-kit`.

## What Changed

- `TableMethods` on `@tupl/schema-model` root now exposes only ordinary schema behavior:
  `scan`, optional `lookup`, and optional `aggregate`.
- Planning hooks moved to the explicit `@tupl/schema-model/table-planning` subpath via:
  - `TablePlanningMethods`
  - `TablePlanningMethodsMap`
  - `TablePlanningMethodsForSchema`
- DSL-token and view-shape detail moved off the root into `@tupl/schema-model/dsl`.
- Public import tests now enforce:
  - no planning-hook contracts on `@tupl/schema-model` root
  - no view-node DSL types on `@tupl/schema-model` root
  - explicit access through `@tupl/schema-model/dsl`, `@tupl/schema-model/planning`,
    `@tupl/schema-model/normalized`, and `@tupl/schema-model/table-planning`
- Kysely and Objection now share more SQL-provider skeleton through `@tupl/provider-kit`:
  - shared SQL-relational strategy/plan helpers
  - shared scan-binding types
  - shared `lookupMany`-via-scan execution scaffolding

## Design Notes

- Ordinary schema consumers should stay on `@tupl/schema` or `@tupl/schema-model` root.
- Planning hooks remain supported, but they are intentionally a lower-level extension seam for
  runtime, test-support, and advanced tooling.
- The Kysely/Objection extraction is still intentionally backend-primitive based: provider-kit owns
  shared orchestration and scaffolding, while backend query operations remain provider-owned.

## Verification

- `pnpm lint`
- `pnpm typecheck`
- `pnpm test`
- `pnpm fmt`
