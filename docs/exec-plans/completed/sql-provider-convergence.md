# SQL Provider Convergence

## Summary

This tranche made `createSqlRelationalProviderAdapter(...)` the real canonical path for ordinary SQL-like adapters and aligned the first-party SQL-like providers with that path.

## Key decisions

- The SQL-relational helper now presents top-level lifecycle/config fields plus one nested `queryBackend`.
- Resolved-entity defaults come from `config.table ?? entity`; ordinary SQL-like adapters no longer need to duplicate entity-map setup in provider roots.
- Drizzle, Kysely, and Objection package roots now use `createSqlRelationalProviderAdapter(...)` directly.
- Runtime binding validation stays provider-owned, but provider-facing compile/execute/lookup paths surface those failures through adapter operation results rather than provider-root `throw new Error(...)` logic.

## Follow-up

- The narrower `@tupl/schema-model` root cleanup is intentionally deferred into its own execution plan.
