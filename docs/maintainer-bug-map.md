# Maintainer Bug Map

This guide is the fastest route to the owning module when a bug report lands.

## Start Here

- SQL-like provider authoring should start at `createSqlRelationalProviderAdapter(...)` in `@tupl/provider-kit`.
- Lower-level provider work that does not fit the ordinary SQL-like path should start at `createRelationalProviderAdapter(...)`.
- Application-facing schema/query issues should usually start at `@tupl/schema`, then move down only if the public facade is too thin to explain the behavior.

## Bug Map

- Provider pushdown shape accepted/rejected incorrectly:
  start in `@tupl/provider-kit` relational SQL helpers and `@tupl/provider-kit/shapes`
- Kysely/Objection backend query-builder translation bug:
  start in the provider package `planning/rel-builder.ts` backend hooks
- Drizzle pushdown or SQL-expression translation bug:
  start in `packages/provider-drizzle/src/planning`
- Scan/lookup execution bug inside one provider:
  start in that provider package `execution/`
- SQL lowering bug for joins, pushed filters, or semi-joins:
  start in `packages/planner/src/planner/select/select-join-tree.ts`
- SQL lowering bug for expressions, subqueries, or literal filters:
  start in `packages/planner/src/planner/sql-expr-lowering.ts` and `packages/planner/src/planner/where-lowering.ts`
- View expansion or normalized schema/view mismatch:
  start in `packages/planner/src/planner/view-expansion.ts` and `packages/schema-model/src/normalization`
- Runtime fallback, provider-fragment preference, or guardrail behavior:
  start in `packages/runtime/src/runtime/query-runner.ts` and `packages/runtime/src/runtime/provider/`
- Session/explain output bug:
  start in `packages/runtime/src/runtime/session/` and `packages/runtime/src/runtime/execution/explain-shaping.ts`

## Adapter Authoring Boundaries

- Ordinary SQL-like adapters should stay on `@tupl/provider-kit`, `@tupl/provider-kit/shapes`, and `@tupl/provider-kit/testing`.
- They should not need `@tupl/schema-model` or planner internals.
- If a provider needs planner-specific knowledge to compile ordinary `scan` / `join` / `aggregate` / `set_op` / `with` fragments, that is a design bug in `provider-kit`, not a cue to reach downward.
