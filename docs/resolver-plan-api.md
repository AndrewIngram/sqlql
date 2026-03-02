# Resolver and Planning API

`sqlql` keeps planning internal, but table methods can now participate in pushdown decisions.

## Runtime surface

Users of the library still write:

1. `defineSchema(...)`
2. `defineTableMethods(schema, ...)`
3. `query({ sql, ... })`

## Schema policy model

- Column-level capabilities (default `true`):
  - `filterable`
  - `sortable`
- Table-level non-column policy:
  - `maxRows`
  - `reject` (`requiresLimit`, `forbidFullScan`, `requireAnyFilterOn`)
  - `fallback` (`allow_local` or `require_pushdown` per category)

Static schema policy is enforced before table method execution.

## Planner hooks

Table methods may define optional hooks:

- `planScan(request, context)`
- `planLookup(request, context)`
- `planAggregate(request, context)`

Planned requests carry stable IDs:

- `where: [{ id, clause }]`
- `orderBy: [{ id, term }]`
- `metrics: [{ id, metric }]`

Hooks can return:

- ID-based pushdown decisions (`whereIds`, `orderByIds`, `metricIds`, etc.)
- explicit `mode: "remote_residual"` with separate `remote` and `residual` sections
- `reject: { code, message }`
- `notes`

Residual work is executed locally unless blocked by fallback policy (`require_pushdown`).

## Enums and checks

- Column `enum` metadata is first-class.
- DDL includes generated `CHECK ... IN (...)` for enums.
- Structured table checks are supported via `constraints.checks` (`kind: "in"`).
- Optional runtime constraint validation (`warn`/`error`) reports enum/check violations in returned rows.
