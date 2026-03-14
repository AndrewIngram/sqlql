# Creating a New Adapter

This guide is the practical path for shipping a provider without implementing a full relational engine up front.

The core contract is intentionally small:

- `canExecute(rel)`: the source of truth for whether the provider can own a rel subtree
- `compile(rel)`: lowers that canonical rel subtree to a provider-specific plan payload
- `describeCompiledPlan?(plan)`: optional explain/debug surface
- `execute(plan)`: runs the provider-specific plan and returns canonical rows

Capability atoms are optional metadata only. They can help explain or cheaply prefilter, but they are not the semantic contract.

## Minimal Adapter Skeleton

This is the minimum useful shape for a provider. It supports a narrow single-source rel subtree and rejects broader pushdown with a structured reason.

```ts
import type {
  ProviderAdapter,
  ProviderCapabilityReport,
  ProviderCompiledPlan,
  QueryRow,
} from "@tupl/provider-kit";
import { AdapterResult, extractSimpleRelScanRequest } from "@tupl/provider-kit";
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
      if (extractSimpleRelScanRequest(rel)) {
        return true;
      }

      return {
        supported: false,
        routeFamily: "rel-core",
        reason: "This adapter only supports simple single-source scan pipelines.",
      };
    },

    async compile(rel) {
      const request = extractSimpleRelScanRequest(rel);
      if (!request) {
        return AdapterResult.err(
          new Error("This adapter only supports simple single-source scan pipelines."),
        );
      }

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
- provider-specific payload construction hooks
- compiled-plan execution wiring
- optional lookup optimization hooks

Your provider package should keep only backend-specific work:

- entity config details (`table`, `base`, scoping hooks)
- field-sensitive support checks
- strategy selection
- provider-specific payload building
- compiled-plan execution

Sketch:

```ts
import { AdapterResult, createSqlRelationalProviderAdapter } from "@tupl/provider-kit";

export function createExampleRelationalProvider(options: CreateExampleProviderOptions) {
  return createSqlRelationalProviderAdapter({
    name: "example-sql",
    entities: options.entities,
    resolveSqlStrategy({ rel }) {
      return resolveExampleStrategy(rel, options.entities);
    },
    buildPlanPayload({ rel, strategy }) {
      return { strategy, rel };
    },
    executePlan({ plan, context }) {
      return AdapterResult.tryPromise({
        try: () => executeExamplePlan(plan, options, context),
        catch: (error) => (error instanceof Error ? error : new Error(String(error))),
      });
    },
  });
}
```

Reach for `createRelationalProviderAdapter(...)` only when the backend does not fit the ordinary SQL-like path cleanly.

## Optional Helpers

`@tupl/provider-kit/shapes` exists for optional helpers, not for the primary semantic contract.

Use it when it materially simplifies provider code:

- `extractSimpleRelScanRequest(...)` for narrow single-source scan pipelines
- field-sensitive validation helpers for projected, filtered, or sorted columns
- optional lookup optimization helpers for keyed backends

If a provider never needs these helpers, that is fine.

## Optional Atoms

Atoms are coarse metadata only.

They are useful for:

- explain diagnostics
- cheap prefiltering before a deeper `canExecute` check
- documenting broad provider intent

They should stay broad. If support depends on entity, field, operator, or operator combination, that logic belongs in `canExecute(...)`.

If atoms stop pulling their weight for a provider, omit them.

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
