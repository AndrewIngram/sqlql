# Package Architecture

`tupl` uses six semantic packages with one public application-facing facade:

- `@tupl/foundation`: relational model primitives, diagnostics, and value helpers
- `@tupl/provider-kit`: adapter contracts, entity binding, and reusable provider-shape helpers
- `@tupl/schema-model`: schema DSL, normalization, and provider binding validation
- `@tupl/planner`: SQL parsing, lowering, and physical planning
- `@tupl/runtime`: query execution, guardrails, constraints, and sessions
- `@tupl/schema`: canonical application-facing surface for schema authors

Allowed dependency directions:

- `provider-kit` -> `foundation`
- `schema-model` -> `foundation`, `provider-kit`
- `planner` -> `foundation`, `provider-kit`, `schema-model`
- `runtime` -> `foundation`, `provider-kit`, `schema-model`, `planner`
- `schema` -> `schema-model`, `runtime`

Guidelines:

- Import from the narrowest package that owns the concept.
- Do not import upward within the six-package graph.
- Provider implementations should prefer `@tupl/provider-kit`, `@tupl/foundation`, and `@tupl/schema`.
- Application docs and examples should prefer `@tupl/schema` and first-party provider packages.
- Public subpaths should resolve directly to real modules, not to one-hop wrapper files.
- Package roots may aggregate real concepts; internal alias layers should be deleted instead of documented.
