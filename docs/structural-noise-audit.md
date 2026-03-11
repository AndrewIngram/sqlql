# Structural Noise Audit

Current package-structure score: `8/10`.

This audit tracks shallow wrapper layers that add interface surface without hiding complexity.

## Cleaned up

- `@tupl/provider-kit/shapes` now points directly at `packages/provider-kit/src/provider/shapes/index.ts`; the extra `packages/provider-kit/src/shapes/index.ts` wrapper is removed.
- `@tupl/runtime/executor` now points directly at `packages/runtime/src/runtime/executor.ts`; the extra `packages/runtime/src/executor.ts` wrapper is removed.
- `@tupl/runtime` no longer exposes a fake `runtime/errors` module that only re-exported `@tupl/foundation`.
- `@tupl/schema-model` now places its real module at `packages/schema-model/src/index.ts` instead of hiding it under `packages/schema-model/src/schema/index.ts`.

## Deferred

- `packages/provider-kit/src/provider/shapes/index.ts` remains because it is the real semantic root for the shapes module; it aggregates multiple leaf modules and is not a one-hop alias.
- Package-root barrels in `foundation`, `planner`, `provider-kit`, and `runtime` remain because they aggregate multiple real concepts at the package boundary.

## Completed Since This Audit Started

- The temporary `core` package was removed entirely.
- Planner implementation is no longer concentrated behind one fake root file; `planning.ts` and `sql-lowering.ts` are now thin surfaces over real internal planner modules.
- `@tupl/provider-kit` is now the normal adapter-authoring facade for first-party providers instead of requiring direct `@tupl/schema-model` imports for routine request and row contracts.

## Guardrails

- No non-root wrapper file should exist solely to re-export one deeper module.
- Public subpath exports should point at the real module that owns the concept.
- Single-concept packages should keep their implementation at `src/index.ts` unless a second top-level concept justifies a subtree.
