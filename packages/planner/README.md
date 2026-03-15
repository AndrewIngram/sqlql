# `@tupl/planner`

SQL parsing, relational lowering, and physical planning for `tupl`.

Use this package for planner-focused tooling, debugging, or advanced integrations.

The public root stays intentionally small:

- `lowerSqlToRelResult`
- `expandRelViewsResult`
- `planPhysicalQueryResult`
- `buildLogicalQueryPlanResult`
- `buildPhysicalQueryPlanResult`

Internal planner implementation is split across planner-owned modules; consumers should treat the package root as the stable entrypoint rather than depending on internal file layout.
