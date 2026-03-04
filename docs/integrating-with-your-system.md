# Integrating With Your System (v1)

This guide covers the provider-first runtime in `sqlql` v1.

## Runtime surface

1. `defineSchema(...)` with `provider` per table.
2. `defineProviders(...)` with one adapter per provider name.
3. `query({ schema, providers, context, sql, queryGuardrails? })`.

`query` / `createQuerySession` are providers-only in v1; legacy `methods` execution is not part of the runtime path.

You can also use source-neutral entity handles (`createDataEntityHandle`) and map logical SQL tables
to provider-owned entities via the schema DSL (`table({ from, columns })`).
When using the DSL callback form, `table(...)` values can be reused as typed references in
`rel.scan(...)` and `col(tableRef, \"column\")`.

## Provider interface

A provider implements:

- `canExecute(fragment, context)`
- `compile(fragment, context)`
- `execute(compiled, context)`
- optional `lookupMany(request, context)`
- optional `estimate(fragment, context)`

Provider fragments are `rel`, `scan`, and `aggregate` (no `sql_query` fragment kind).

Source-neutral naming:

- `DataSourceAdapter` is an alias of the provider contract.
- `DataEntityHandle` represents a provider-owned physical entity (table/index/keyspace/collection).

`lookupMany` enables cross-provider lookup joins (`INNER`/`LEFT`) without full scans on the lookup side.

## Minimal provider example

```ts
import { defineProviders, type ProviderAdapter } from "sqlql";

const warehouseProvider: ProviderAdapter<{ tenantId: string }> = {
  canExecute() {
    return true;
  },
  async compile(fragment) {
    return {
      provider: "warehouse",
      kind: fragment.kind,
      payload: fragment,
    };
  },
  async execute(_compiled, _context) {
    return [];
  },
  async lookupMany(_request, _context) {
    return [];
  },
};

const providers = defineProviders({
  warehouse: warehouseProvider,
});
```

## Guardrails

Use `queryGuardrails` for global execution safety:

- `maxPlannerNodes`
- `maxExecutionRows`
- `maxLookupKeysPerBatch`
- `maxLookupBatches`
- `timeoutMs`

Example:

```ts
const rows = await query({
  schema,
  providers,
  context,
  sql,
  queryGuardrails: {
    maxExecutionRows: 100_000,
    timeoutMs: 10_000,
  },
});
```

## Provider packages

- `@sqlql/drizzle`
- `@sqlql/objection`
- `@sqlql/kysely`

These implement the same provider contract with backend-specific execution behavior.

Adapter example commands:

```bash
pnpm example:drizzle:build
pnpm example:drizzle:start
pnpm example:kysely:build
pnpm example:kysely:start
pnpm example:objection:build
pnpm example:objection:start
```

## Facade-over-downstream pattern

A practical integration pattern is:

1. Keep a larger downstream data model (for example operational tables in Postgres/Drizzle).
2. Expose a narrower, user-centric facade schema in `sqlql`.
3. Apply mandatory context scope in provider table config (`scope(context)`).
4. Let users query only the facade tables.

This is exactly how the playground demo is wired:

- downstream DB: pglite + Drizzle raw tables (`orgs`, `users`, `vendors`, `products`, `orders`, `order_items`)
- facade authoring: TypeScript schema module (`export const schema = defineSchema(...)`) in Monaco
- facade tables: scoped lenses + derived view (`my_orders`, `my_order_items`, `vendors_for_org`, `active_products`, `my_order_lines`)
- additional non-relational provider: in-memory KV (`product_view_counts`)
- context controls: `orgId` and `userId` (via Query tab context popover)
- explain UI: execution plan graph plus provider step inspection
