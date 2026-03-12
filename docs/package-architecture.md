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

Layer invariants:

- `@tupl/foundation` owns the relational vocabulary, diagnostics, and value helpers. Callers may rely on its data model, but must not assume execution or schema-building behavior.
- `@tupl/provider-kit` owns adapter contracts, entity handles, capability reporting, provider-shape analysis, and adapter testing helpers. Callers may build adapters against it, but must not assume how schemas are normalized or how runtime sessions are orchestrated.
- `@tupl/schema-model` owns logical schema authoring and normalization. Callers may rely on schema DSL behavior and normalized bindings, but must not assume planner or executor internals.
- `@tupl/planner` owns SQL lowering and physical planning. Callers may rely on relational planning output, but must not assume runtime guardrail policy or provider execution semantics.
- `@tupl/runtime` owns executable-schema construction, guardrails, query orchestration, and sessions. Callers may rely on execution contracts, but must not depend on planner-internal shapes beyond the published explain surface.
- `@tupl/schema` owns the application-facing facade. It should expose the documented schema/runtime workflow, not mirror the full lower-layer export surface.

Cross-module rules:

- Import from the narrowest package that owns the concept.
- Do not import upward within the six-package graph.
- Package roots may aggregate real concepts; internal alias layers should be deleted instead of documented.
- Public subpaths should resolve directly to real modules, not to one-hop wrapper files.
- Package-local tests and support helpers should import from their owning package or lower layers, not from `@tupl/schema`.
- `@tupl/foundation` must remain product-only and must not absorb test fixtures, harnesses, or adapter conformance logic.
- Internal cross-package test infrastructure lives in the private `@tupl/test-support` workspace package.
- External adapter authors should use `@tupl/provider-kit/testing` instead of importing repo-only helpers.

Consumer guidance:

- Provider implementations should prefer `@tupl/provider-kit`, `@tupl/provider-kit/shapes`, and `@tupl/provider-kit/testing` for ordinary adapter work.
- Ordinary SQL-like adapters should start with `createSqlRelationalProviderAdapter(...)`; lower-level `createRelationalProviderAdapter(...)` remains for unusual adapters that do not fit that shape.
- `@tupl/foundation` remains available for primitive relational helpers, but it is not the primary extension boundary.
- Provider implementations should not normally import `@tupl/schema-model`; that package owns schema internals, not the ordinary adapter-authoring surface.
- Adapter conformance belongs on `@tupl/provider-kit/testing`; internal test fixtures do not.
- Application docs and examples should prefer `@tupl/schema` and first-party provider packages.
- Maintainers should use [`maintainer-bug-map.md`](./maintainer-bug-map.md) as the starting point for bug triage across provider, planner, and runtime layers.
