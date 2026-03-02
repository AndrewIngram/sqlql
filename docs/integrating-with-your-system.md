# Integrating With Your System

This guide covers how to connect `sqlql` to your domain/storage layer.

## Method surface

For each table, define methods with `defineTableMethods`:

- required: `scan`
- optional: `lookup`
- optional: `aggregate`
- optional planner hooks: `planScan`, `planLookup`, `planAggregate`

```ts
import { defineTableMethods } from "sqlql";

const methods = defineTableMethods(schema, {
  orders: {
    async scan(request, context) {
      return context.ordersRepo.scan(request);
    },
    async lookup(request, context) {
      return context.ordersRepo.lookup(request);
    },
    async aggregate(request, context) {
      return context.ordersRepo.aggregate(request);
    },
  },
});
```

## Running queries

```ts
import { query } from "sqlql";

const rows = await query({
  schema,
  methods,
  context: { tenantId: "org_1" },
  sql: `
    SELECT o.id, u.email
    FROM orders o
    JOIN users u ON o.user_id = u.id
    WHERE o.status = 'paid'
    ORDER BY o.ordered_at DESC
    LIMIT 50
  `,
});
```

`context` is passed to your methods unchanged. Use it for tenancy/auth scoping.

## Implementing `scan`

`scan` receives normalized request data:

- `table`, `alias`
- `select` projected columns
- `where` normalized filters
- `orderBy`
- `limit`, `offset`

Typical pattern:

1. map normalized filters to your ORM/service query API
2. apply projection, sorting, pagination where possible
3. return plain row objects

```ts
orders: {
  async scan(req, ctx) {
    return ctx.db.orders.findMany({
      select: req.select,
      where: req.where,
      orderBy: req.orderBy,
      limit: req.limit,
      offset: req.offset,
      tenantId: ctx.tenantId,
    });
  },
}
```

## When to add `lookup`

`lookup` helps with key-based fetch patterns (for example dependency-driven `IN (...)` joins) and can reduce overfetch compared with generic scans.

If omitted, `sqlql` falls back to `scan` + local processing.

## When to add `aggregate`

`aggregate` lets your backend run grouped metrics directly.

If omitted (or not applicable for a query), `sqlql` can run aggregates locally unless table fallback policy requires pushdown.

## Planner hooks (optional optimization)

Hooks let adapters decide what can be pushed down:

- `planScan`
- `planLookup`
- `planAggregate`

Each hook gets planned term IDs so you can select pushdown terms deterministically.

ID-based example:

```ts
planScan(request) {
  return {
    whereIds: request.where
      ?.filter((term) => term.clause.column === "status")
      .map((term) => term.id),
    orderByIds: request.orderBy
      ?.filter((term) => term.term.column === "ordered_at")
      .map((term) => term.id),
  };
}
```

Escape-hatch example (`remote_residual`):

```ts
planScan() {
  return {
    mode: "remote_residual",
    remote: { where: [{ op: "eq", column: "status", value: "paid" }] },
    residual: { orderBy: [{ column: "score", direction: "desc" }] },
  };
}
```

You can also reject from hooks:

```ts
planScan() {
  return {
    reject: {
      code: "UNSUPPORTED_QUERY_SHAPE",
      message: "This backend requires a status filter.",
    },
  };
}
```

## Fallback policy interaction

Schema `query.fallback` controls whether local residual work is allowed:

- `allow_local`: planner may do residual filters/sorts/aggregates in `sqlql`
- `require_pushdown`: queries that need residual local work are rejected

Use this per table to enforce strict backend execution where needed.

## Runtime constraint validation (optional)

`query(...)` accepts:

```ts
constraintValidation: {
  mode: "off" | "warn" | "error";
  onViolation?: (violation) => void;
}
```

Current checks for returned rows include:

- not-null columns
- primary key uniqueness (within returned batch)
- unique constraint uniqueness (within returned batch)
- enum/check (`kind: "in"`) validation

FK runtime lookups are intentionally not performed.

## Debugging plans

For introspection, use sessions:

```ts
import { createQuerySession } from "sqlql";

const session = createQuerySession({ schema, methods, context, sql });
const plan = session.getPlan();
const result = await session.runToCompletion();
```

This is useful for explain UIs and adapter debugging.

## Common integration checklist

- enforce auth/tenant scope in your methods
- push down `select`, `where`, and `orderBy` wherever possible
- add `lookup` for key-based fetch patterns
- add `aggregate` for high-cardinality grouped queries
- use planner hooks only where pushdown capabilities are conditional
- keep method return values as plain JSON-like row objects
