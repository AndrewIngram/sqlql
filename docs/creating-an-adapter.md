# Creating a New Adapter

This guide is the practical path for shipping an adapter without implementing a full relational engine up front.

The core model now has three layers:

- route families: coarse maturity stages for docs and onboarding
- capability atoms: the actual planner/runtime contract
- fallback policy: whether unsupported or expensive shapes fall back locally, warn, or fail

## Minimal Adapter Skeleton

This is the minimum useful shape for a relational adapter. It supports `scan` and rejects `rel` pushdown with structured capability info.

```ts
import type {
  ProviderAdapter,
  ProviderCapabilityAtom,
  ProviderCapabilityReport,
  ProviderCompiledPlan,
  ProviderFragment,
  QueryRow,
  TableScanRequest,
} from "sqlql";

type DbContext = {
  tenantId: string;
};

type CompiledScanPlan = {
  kind: "scan";
  sql: string;
  params: unknown[];
  request: TableScanRequest;
};

const declaredAtoms: readonly ProviderCapabilityAtom[] = [
  "scan.project",
  "scan.filter.basic",
  "scan.filter.set_membership",
  "scan.sort",
  "scan.limit_offset",
];

export function createExampleSqlAdapter(): ProviderAdapter<DbContext> {
  return {
    name: "example-sql",
    routeFamilies: ["scan"],
    capabilityAtoms: [...declaredAtoms],

    canExecute(fragment): boolean | ProviderCapabilityReport {
      switch (fragment.kind) {
        case "scan":
          return true;
        case "rel":
          return {
            supported: false,
            routeFamily: "rel-core",
            requiredAtoms: ["join.inner"],
            missingAtoms: ["join.inner"],
            reason: "This adapter only supports scan pushdown.",
          };
        default:
          return false;
      }
    },

    async compile(fragment): Promise<ProviderCompiledPlan> {
      if (fragment.kind !== "scan") {
        throw new Error(`Unsupported fragment kind: ${fragment.kind}`);
      }

      return {
        provider: "example-sql",
        kind: "scan",
        payload: compileScanRequest(fragment.request),
      };
    },

    async execute(compiled, context): Promise<QueryRow[]> {
      if (compiled.kind !== "scan") {
        throw new Error(`Unsupported compiled plan kind: ${compiled.kind}`);
      }

      const plan = compiled.payload as CompiledScanPlan;
      return runQuery(plan.sql, plan.params, context);
    },
  };
}

function compileScanRequest(request: TableScanRequest): CompiledScanPlan {
  const params: unknown[] = [];
  const select = request.select.map(quoteIdent).join(", ");
  const where = (request.where ?? [])
    .map((clause) => toSqlWhereClause(clause, params))
    .join(" AND ");
  const orderBy = (request.orderBy ?? [])
    .map((term) => `${quoteIdent(term.column)} ${term.direction.toUpperCase()}`)
    .join(", ");

  const sql = [
    `select ${select}`,
    `from ${quoteIdent(request.table)}`,
    where.length > 0 ? `where ${where}` : "",
    orderBy.length > 0 ? `order by ${orderBy}` : "",
    request.limit != null ? `limit ${request.limit}` : "",
    request.offset != null ? `offset ${request.offset}` : "",
  ].filter(Boolean).join(" ");

  return {
    kind: "scan",
    sql,
    params,
    request,
  };
}

function toSqlWhereClause(
  clause: TableScanRequest["where"][number],
  params: unknown[],
): string {
  switch (clause.op) {
    case "eq":
      params.push(clause.value);
      return `${quoteIdent(clause.column)} = ?`;
    case "neq":
      params.push(clause.value);
      return `${quoteIdent(clause.column)} <> ?`;
    case "gt":
    case "gte":
    case "lt":
    case "lte": {
      const op = { gt: ">", gte: ">=", lt: "<", lte: "<=" }[clause.op];
      params.push(clause.value);
      return `${quoteIdent(clause.column)} ${op} ?`;
    }
    case "in":
    case "not_in": {
      const op = clause.op === "in" ? "in" : "not in";
      const placeholders = clause.values.map(() => "?").join(", ");
      params.push(...clause.values);
      return `${quoteIdent(clause.column)} ${op} (${placeholders})`;
    }
    case "like":
    case "not_like": {
      const op = clause.op === "like" ? "like" : "not like";
      params.push(clause.value);
      return `${quoteIdent(clause.column)} ${op} ?`;
    }
    case "is_null":
      return `${quoteIdent(clause.column)} is null`;
    case "is_not_null":
      return `${quoteIdent(clause.column)} is not null`;
    case "is_distinct_from":
    case "is_not_distinct_from": {
      const op = clause.op === "is_distinct_from" ? "is distinct from" : "is not distinct from";
      params.push(clause.value);
      return `${quoteIdent(clause.column)} ${op} ?`;
    }
  }
}

function quoteIdent(name: string): string {
  return `\"${name.replaceAll('\"', '\"\"')}\"`;
}

async function runQuery(sql: string, params: unknown[], _context: DbContext): Promise<QueryRow[]> {
  void sql;
  void params;
  return [];
}
```

That adapter is already usable because unsupported joins, aggregates, and computed expressions can fall back to local logical execution.

## Wiring the Adapter Into a Facade Schema

Once an adapter exposes typed `entities`, the current schema API is:

1. `createSchemaBuilder<TContext>()`
2. `builder.table(...)` and `builder.view(...)`
3. `createExecutableSchema(builder)`

Example:

```ts
import {
  createDataEntityHandle,
  createExecutableSchema,
  createSchemaBuilder,
} from "sqlql";

const ordersEntity = createDataEntityHandle<"id" | "total_cents" | "created_at">({
  entity: "orders",
  provider: "example-sql",
});

const builder = createSchemaBuilder<DbContext>();

const myOrders = builder.table("myOrders", ordersEntity, {
  columns: ({ col }) => ({
    id: col.id("id"),
    totalCents: col.integer("total_cents"),
    createdAt: col.timestamp("created_at"),
  }),
});

builder.view(
  "recentOrders",
  ({ scan }) => scan(myOrders),
  {
    columns: ({ col }) => ({
      id: col.id(myOrders, "id"),
      totalCents: col.integer(myOrders, "totalCents", { nullable: false }),
      createdAt: col.timestamp(myOrders, "createdAt", { nullable: false }),
    }),
  },
);

const executableSchema = createExecutableSchema(builder);
```

If your adapter returns typed `entities` the way the first-party adapters do, use those handles here instead of calling `createDataEntityHandle(...)` directly.
If you are documenting or testing an adapter, prefer this builder flow over the older callback-style examples.

## Route Families

Route families are intentionally coarse:

- `scan`
- `lookup`
- `aggregate`
- `rel-core`
- `rel-advanced`

Use them to describe where an adapter is in its progression. Do not treat them as all-or-nothing promises.

Example:

- an adapter can be `rel-advanced` in broad terms while only supporting `cte.non_recursive` and `window.rank_basic`
- it does not need to support every CTE, window, or expression shape in that family

## Capability Atoms

Capability atoms are the planner contract. `canExecute(fragment, context)` should be able to explain support in terms of missing atoms.

Useful starter atoms:

- `scan.project`
- `scan.filter.basic`
- `scan.filter.set_membership`
- `scan.sort`
- `scan.limit_offset`
- `lookup.bulk`
- `aggregate.group_by`
- `aggregate.having`
- `join.inner`
- `join.left`
- `join.right_full`
- `set_op.union_all`
- `set_op.union_distinct`
- `set_op.intersect`
- `set_op.except`
- `cte.non_recursive`
- `subquery.from`
- `subquery.correlated`
- `window.rank_basic`
- `window.aggregate_default_frame`
- `window.frame_explicit`
- `window.navigation`
- `expr.compare_basic`
- `expr.like`
- `expr.in_not_in`
- `expr.null_distinct`
- `expr.arithmetic`
- `expr.case_simple`
- `expr.case_searched`
- `expr.string_basic`
- `expr.numeric_basic`
- `expr.cast_basic`

## Minimum Viable Adapter

Start with `scan` support:

- projection
- filter
- sort
- limit/offset

For unsupported `rel` fragments, return an unsupported capability report from `canExecute`.

That is enough to make a provider work because `sqlql` can fall back to local logical execution.

## Real Build Order

A practical implementation usually looks like this:

1. get a scan-only adapter working
2. make `canExecute` return structured unsupported reports for `rel`
3. add `lookupMany` if the backend has efficient keyed access
4. add same-provider `rel` compilation atom-by-atom

That progression keeps the codebase honest: the docs-facing `routeFamilies` are broad, but the implementation work happens in small, testable pieces.

## Recommended Relational Progression

For relational backends, this is the default order:

1. `scan`
2. `lookup`
3. `aggregate`
4. `rel-core`
5. selective `rel-advanced`

The important point is granularity:

- add one capability atom at a time
- do not block on complete `rel-core`
- do not block on complete `rel-advanced`

Practical sequence:

1. `scan.*`
2. `lookup.bulk`
3. `aggregate.group_by`
4. `join.inner`, `join.left`
5. set ops and non-recursive CTEs
6. window atoms and richer expression atoms

## `canExecute` Contract

`canExecute` is the source of truth for a concrete fragment decision.

This is the shape to aim for once you start supporting more than `scan`:

```ts
canExecute(fragment): boolean | ProviderCapabilityReport {
  if (fragment.kind === "scan") {
    return true;
  }

  const requiredAtoms = collectCapabilityAtomsForFragment(fragment);
  const missingAtoms = requiredAtoms.filter((atom) => !declaredAtoms.includes(atom));

  if (missingAtoms.length > 0) {
    return {
      supported: false,
      routeFamily: inferRouteFamilyForFragment(fragment),
      requiredAtoms,
      missingAtoms,
      reason: "Rel fragment is not supported for single-query pushdown.",
    };
  }

  return true;
}
```

The important behavior is:

- declared atoms are metadata
- the fragment shape still needs a real compile path
- `missingAtoms` should explain why pushdown stopped

For unsupported fragments, return a structured report where possible:

- `supported: false`
- `routeFamily`
- `requiredAtoms`
- `missingAtoms`
- `reason`
- optional `diagnostics`
- optional estimates such as `estimatedRows` or `estimatedCost`

This gives the planner enough information to:

- push down supported fragments
- fall back locally when allowed
- reject expensive or forbidden fallback
- explain the decision to users and tooling

## Declared Metadata vs Dynamic Checks

Adapters can optionally declare:

- `routeFamilies`
- `capabilityAtoms`
- `fallbackPolicy`

Treat those as metadata for docs, tooling, and compatibility reporting.

Do not rely on declared atoms alone. The actual fragment decision still belongs in `canExecute`.

## Fallback Policy

Default behavior is `allow + annotate`:

- local fallback is allowed
- fallback emits diagnostics

Adapters can tighten this with `fallbackPolicy`.

Important knobs:

- `allowFallback`
- `warnOnFallback`
- `rejectOnMissingAtom`
- `rejectOnEstimatedCost`
- `maxLocalRows`
- `maxLookupFanout`
- `maxJoinExpansionRisk`

Use this when a backend can technically fall back locally but the shape is too expensive to allow silently.

## Diagnostics

Use stable diagnostics instead of ad hoc strings.

Current structure:

- `code`: stable `SQLQL_*`
- `class`: SQLSTATE-like category
- `severity`: `error | warning | note`
- `message`
- `details`

Use SQLSTATE-like classes as categories only. `sqlql` planner and fallback failures are not standard engine SQLSTATEs.

Recommended failure details:

- provider name
- fragment kind
- route family
- required atoms
- missing atoms
- estimate fields when available

## Expression Support

The core runtime now supports a first local expression layer for read queries, including:

- `LIKE`, `NOT LIKE`
- `NOT IN`
- `IS DISTINCT FROM`, `IS NOT DISTINCT FROM`
- arithmetic `+ - * / %`
- string concat
- `LOWER`, `UPPER`, `TRIM`, `LENGTH`, `SUBSTR`
- `COALESCE`, `NULLIF`
- `ABS`, `ROUND`
- `CAST`
- searched `CASE`

Adapters do not need to push all of these down immediately.

Practical recommendation:

- support the scan filter subset first
- add computed-expression pushdown atom-by-atom later

If your backend cannot compile computed projections yet, reject them in `canExecute` and let the runtime handle them locally.

## Testing Strategy

Minimum coverage:

1. scan tests
- filter, sort, projection, limit/offset

2. capability tests
- unsupported fragments return stable missing atoms and reasons

3. fallback tests
- unsupported pushdown still produces correct local results when fallback is allowed

4. rejection tests
- expensive or forbidden fallback fails with diagnostics

5. conformance tests
- parity with other first-party adapters for shared atoms

## Related Docs

- [provider-capability-matrix.md](./provider-capability-matrix.md)
- [building-a-non-relational-adapter.md](./building-a-non-relational-adapter.md)
