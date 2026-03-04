# Upgrading to sqlql v1

`sqlql` v1 is intentionally breaking. This guide covers required changes.

## 1) Replace table methods with providers

v0-style:

```ts
query({ schema, methods, context, sql });
```

v1:

```ts
query({ schema, providers, context, sql });
```

Use `defineProviders(...)` and implement `ProviderAdapter` (`canExecute`, `compile`, `execute`, optional `lookupMany`).

## 2) Add `provider` to every table

Each table must declare a non-empty `provider` string:

```ts
tables: {
  orders: {
    provider: "warehouse",
    columns: { ... },
  },
}
```

`defineSchema(...)` now validates provider bindings.

## 3) Move safety limits to `queryGuardrails`

Use global guardrails on `query(...)`:

- `maxPlannerNodes`
- `maxExecutionRows`
- `maxLookupKeysPerBatch`
- `maxLookupBatches`
- `timeoutMs`

Table-level fallback/reject policy is no longer the core execution-control model in v1.

## 4) Adopt provider fragments and rel pushdown

Providers can now receive:

- `scan`
- `rel`
- `aggregate`

For robust pushdown, implement `rel` support in adapters.

## 5) Session/explain step kinds changed

Expect v1 step kinds/routes such as:

- `remote_fragment`
- `lookup_join`
- local operator steps (`local_*`)
- routes: `provider_fragment`, `lookup_join`, `local`

## 6) First-party provider packages

v1 packages:

- `@sqlql/drizzle`
- `@sqlql/objection`
- `@sqlql/kysely`

Prisma is intentionally deferred; use a custom provider with raw SQL hooks if needed.
