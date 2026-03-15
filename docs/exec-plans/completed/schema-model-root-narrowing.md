# Schema-Model Root Narrowing

## Summary

This tranche narrowed the `@tupl/schema-model` package root so it stays focused on schema DSL entrypoints, core schema/query contracts, timestamps, and DDL helpers.

## Key decisions

- Normalization, mapping, definition helpers, enum-link resolution, and validation no longer live on the `@tupl/schema-model` root surface.
- Those deeper seams now resolve through explicit public subpaths:
  - `@tupl/schema-model/normalization`
  - `@tupl/schema-model/mapping`
  - `@tupl/schema-model/definition`
  - `@tupl/schema-model/enums`
  - `@tupl/schema-model/constraints`
  - `@tupl/schema-model/ddl`
- Planner, runtime, examples, and test-support code now import from the narrowest schema-model subpath that owns the concept instead of relying on the root barrel.
- The application-facing `@tupl/schema` facade remains unchanged and still re-exports the canonical schema-authoring workflow.

## Follow-up

- Reassess whether any remaining schema-model helper groups should be narrowed further once downstream consumers settle on the explicit subpaths.
