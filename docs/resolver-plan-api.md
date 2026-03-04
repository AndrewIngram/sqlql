# Provider and Planner API (v1)

`sqlql` v1 uses a provider-first planner/runtime.

`query(...)` / `createQuerySession(...)` execute through `providers` only (no legacy `methods` execution path).

## Provider contract

```ts
interface ProviderAdapter<TContext = unknown> {
  canExecute(fragment, context): boolean | ProviderCapabilityReport | Promise<boolean | ProviderCapabilityReport>;
  compile(fragment, context): ProviderCompiledPlan | Promise<ProviderCompiledPlan>;
  execute(plan, context): Promise<QueryRow[]>;
  lookupMany?(request, context): Promise<QueryRow[]>;
  estimate?(fragment, context): { rows: number; cost: number } | Promise<{ rows: number; cost: number }>;
}
```

## Fragment kinds

- `rel`
- `scan`
- `aggregate`

`sql_query` is not part of the active planner/provider contract in v1.

The planner can emit mixed physical plans with:

- `remote_fragment`
- `lookup_join`
- local fallback operators

## Relational IR exports

`sqlql` exports relational and physical planning types:

- `RelNode`, `RelExpr`
- `PhysicalPlan`, `PhysicalStep`
- `ProviderFragment`

## Guardrails

`query(...)` accepts global guardrails:

- `maxPlannerNodes`
- `maxExecutionRows`
- `maxLookupKeysPerBatch`
- `maxLookupBatches`
- `timeoutMs`

These are planner/executor safety limits and are provider-agnostic.
