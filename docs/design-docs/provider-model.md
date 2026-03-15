# Provider Model

Providers are rel-subtree compilers.

## Primary contract

The canonical provider surface is:

- `canExecute(rel, context)`
- `compile(rel, context)`
- `describeCompiledPlan?(plan, context)`
- `execute(plan, context)`

Compiled plans are provider-specific payloads. `tupl` does not assume SQL text is available or executable.

## Design intent

- Providers do not define semantics.
- Providers do not need planner-internal knowledge beyond canonical rel shapes.
- Provider authoring should prefer rel-shape and field-policy helpers over secondary capability vocabularies.
- Optional helper layers may exist for narrow patterns like keyed scans or lookup optimizations, but they are not the main semantic contract.

## Non-relational sources

- Non-relational backends should still present a rel-first surface.
- Redis-like providers may support only narrow scan shapes, but they should still answer support in terms of rel subtrees.
- Helper APIs should make field-sensitive `canExecute(...)` easy to author without forcing providers to hand-walk trees.

## Compile vs execute

- `compile(...)` lowers canonical rel into a provider-owned compiled plan.
- `execute(...)` runs that compiled plan.
- This separation exists to support explainability, plan descriptions, and future caching/replay opportunities without exposing provider internals upstream.

## Explain descriptions

- Runtime owns two explain modes internally:
  - basic fragment descriptions that never compile provider plans
  - enriched provider descriptions that may compile supported provider fragments
- Providers do not need a separate public explain-only method in the current model.

## SQL-like adapter path

- Ordinary SQL-like adapters should start with `createSqlRelationalProviderAdapter(...)`.
- The helper should feel like manual provider authoring:
  - top-level lifecycle/config fields
  - one nested `queryBackend` for backend-specific query translation
  - optional `advanced` overrides only for real backend exceptions
- `createRelationalProviderAdapter(...)` remains the lower-level escape hatch for unusual adapters.
