# Creating a New Adapter

This guide is the practical path for shipping a provider without implementing a full relational engine up front.

The core contract is intentionally small:

- `canExecute(rel)`: the source of truth for whether the provider can own a rel subtree
- `compile(rel)`: lowers that canonical rel subtree to a provider-specific plan payload
- `describeCompiledPlan?(plan)`: optional explain/debug surface
- `execute(plan)`: runs the provider-specific plan and returns canonical rows

Provider-kit helpers should work directly with canonical rel shapes and field policies. Provider authors should not need a second capability language to describe supported subtrees.

## Minimal Adapter Skeleton

This is the minimum useful shape for a provider. It supports a narrow single-source rel subtree and rejects broader pushdown with a structured reason.

```ts
import type {
  ProviderAdapter,
  ProviderCapabilityReport,
  ProviderCompiledPlan,
  QueryRow,
} from "@tupl/provider-kit";
import { AdapterResult } from "@tupl/provider-kit";
import { checkSimpleRelScanCapability } from "@tupl/provider-kit/shapes";
import type { RelNode } from "@tupl/foundation";

type DbContext = {
  tenantId: string;
};

type CompiledPlan = {
  kind: "simple_scan";
  sql: string;
  params: unknown[];
  rel: RelNode;
};

export function createExampleSqlAdapter(): ProviderAdapter<DbContext> {
  return {
    name: "example-sql",

    canExecute(rel): boolean | ProviderCapabilityReport {
      const capability = checkSimpleRelScanCapability(rel);
      return capability.isOk() ? true : capability.error;
    },

    async compile(rel) {
      const capability = checkSimpleRelScanCapability(rel);
      if (capability.isError()) {
        return AdapterResult.err(
          new Error(capability.error.reason ?? "Unsupported simple scan pipeline."),
        );
      }
      const request = capability.value;

      return AdapterResult.ok({
        provider: "example-sql",
        kind: "query",
        payload: compileScanRequest(rel, request),
      } satisfies ProviderCompiledPlan);
    },

    async execute(compiled, context) {
      if (compiled.kind !== "query") {
        return AdapterResult.err(new Error(`Unsupported compiled plan kind: ${compiled.kind}`));
      }

      const plan = compiled.payload as CompiledPlan;
      return AdapterResult.ok(await runQuery(plan.sql, plan.params, context));
    },
  };
}
```

That provider is already useful because unsupported joins, aggregates, windows, and computed expressions can fall back to local logical execution.

## SQL-Like Adapters Should Start With `createSqlRelationalProviderAdapter`

For SQL-like backends, the normal path is `createSqlRelationalProviderAdapter(...)` from `@tupl/provider-kit`.

That helper owns the repeated plumbing:

- entity handle creation and binding
- ordinary SQL-like subtree strategy selection
- provider-specific compiled-plan wiring
- runtime binding orchestration
- optional lookup optimization hooks

Your provider package should keep only backend-specific work:

- entity config details (`table`, `base`, scoping hooks)
- field-sensitive support checks
- optional strategy overrides
- query-builder translation
- lookup optimization

Sketch:

```ts
import { AdapterResult, createSqlRelationalProviderAdapter } from "@tupl/provider-kit";

export function createExampleRelationalProvider(options: CreateExampleProviderOptions) {
  return createSqlRelationalProviderAdapter({
    name: "example-sql",
    entities: options.entities,
    resolveRuntime(context) {
      return resolveExampleRuntime(options, context);
    },
    queryBackend: exampleQueryBackend,
    advanced: {
      resolveRelCompileStrategy(rel, resolvedEntities) {
        return resolveExampleStrategy(rel, resolvedEntities);
      },
    },
    async lookupMany({ request, context, resolvedEntities, runtime }) {
      return AdapterResult.tryPromise({
        try: () => executeExampleLookup(runtime, resolvedEntities, request, context),
        catch: (error) => (error instanceof Error ? error : new Error(String(error))),
      });
    },
  });
}
```

Defaults:

- `resolveEntity(...)` is optional; the helper uses `config.table ?? entity`
- `advanced.createScanBinding(...)` is optional for ordinary SQL-like adapters
- `advanced.resolveRelCompileStrategy(...)` is optional when the default relational strategy rules are sufficient
- most adapters should only supply `queryBackend` plus runtime/entity wiring; `advanced` is the escape hatch, not the normal path

Reach for `createRelationalProviderAdapter(...)` only when the backend does not fit the ordinary SQL-like path cleanly.

## Optional Helpers

`@tupl/provider-kit/shapes` exists for optional helpers, not for the primary semantic contract.

Use it when it materially simplifies provider code:

- `checkSimpleRelScanCapability(...)` for narrow single-source scan pipelines with structured capability reports
- `extractSimpleRelScanRequest(...)` if you only need raw extraction
- field-sensitive validation helpers for projected, filtered, or sorted columns
- optional lookup optimization helpers for keyed backends

If a provider never needs these helpers, that is fine.

## Unsupported Reports

Use provider-kit report builders from shape helpers instead of inventing provider-local error shapes.

The preferred pattern is:

- extract a supported rel shape
- validate backend-specific field rules
- return the helper-produced unsupported report when that fails

That keeps `canExecute(...)` as the source of truth without introducing extra metadata layers.

## Wiring the Adapter Into a Facade Schema

Once a provider exposes typed `entities`, the current schema API is:

1. `createSchemaBuilder<TContext>()`
2. `builder.table(...)` and `builder.view(...)`
3. `createExecutableSchema(builder)`

Example:

```ts
import { createExecutableSchema, createSchemaBuilder } from "@tupl/schema";
import { createExampleProvider } from "./provider";

const provider = createExampleProvider();
const builder = createSchemaBuilder<DbContext>();

builder.table("orders", provider.entities.orders, {
  columns: {
    id: "text",
    total_cents: "integer",
    created_at: "timestamp",
  },
});

const executableSchema = createExecutableSchema(builder);
```

If your provider already returns typed `entities`, use those handles directly rather than building entity handles by hand.

## Deep-Module Rule

If you find yourself importing planner or schema internals into a provider package to decide whether a subtree is pushdownable, stop and move that knowledge into `provider-kit`.

The provider package should own:

- backend-specific execution semantics
- backend-specific payloads
- backend-specific field support checks
- backend-specific runtime binding

It should not own generic rel-shape interrogation that other providers need too.
