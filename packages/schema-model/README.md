# `@tupl/schema-model`

Logical schema DSL and core schema/query contracts for `tupl`.

This package backs `@tupl/schema`; most application code should prefer `@tupl/schema`.

Use this package directly when you need lower-level schema authoring or the raw schema/query types.
Use explicit subpaths for deeper helpers:

- `@tupl/schema-model/normalization`
- `@tupl/schema-model/mapping`
- `@tupl/schema-model/definition`
- `@tupl/schema-model/enums`
- `@tupl/schema-model/constraints`
- `@tupl/schema-model/ddl`

It is not the normal adapter-authoring entrypoint; third-party providers should usually use `@tupl/provider-kit` instead.
