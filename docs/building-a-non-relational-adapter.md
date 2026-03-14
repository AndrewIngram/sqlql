# Building a Non-Relational Adapter

This guide covers Redis, KV stores, document stores, and index-backed adapters that can expose
rows relationally but usually support only a narrow pushdown envelope.

For a concrete Redis implementation, see `@tupl/provider-ioredis` in this repo.
To expose those entities in a facade schema, use `createSchemaBuilder(...)` and finish with
`createExecutableSchema(builder)`.

The key rule is that non-relational adapters still compile canonical rel subtrees. Their
non-relational nature shows up in a narrow `canExecute` envelope, not in a separate semantic model.

## Narrow-Scan Skeleton

This is the shape to aim for when the backend is naturally keyed and only supports constrained scans.

```ts
import type {
  LookupManyCapableProviderAdapter,
  ProviderAdapter,
  ProviderCapabilityReport,
  ProviderCompiledPlan,
  ProviderLookupManyRequest,
  ProviderCapabilityAtom,
  QueryRow,
  TableScanRequest,
} from "@tupl/provider-kit";
import { AdapterResult, extractSimpleRelScanRequest } from "@tupl/provider-kit";

type KvContext = {
  namespace: string;
};

type KvRecord = {
  key: string;
  value: unknown;
};

type CompiledKvPlan = {
  kind: "scan";
  request: TableScanRequest;
};

const declaredAtoms: readonly ProviderCapabilityAtom[] = ["lookup.bulk"];

export function createExampleKvAdapter(
  rows: KvRecord[],
): ProviderAdapter<KvContext> & LookupManyCapableProviderAdapter<KvContext> {
  return {
    name: "example-kv",
    capabilityAtoms: [...declaredAtoms],
    fallbackPolicy: {
      maxLookupFanout: 1000,
      rejectOnEstimatedCost: true,
    },

    canExecute(rel): boolean | ProviderCapabilityReport {
      return extractSimpleRelScanRequest(rel)
        ? true
        : {
            supported: false,
            routeFamily: rel.kind === "scan" ? "scan" : "rel-core",
            reason: "This KV adapter only supports simple single-entity scan pipelines.",
          };
    },

    async compile(rel) {
      const request = extractSimpleRelScanRequest(rel);
      if (!request) {
        return AdapterResult.err(
          new Error("This KV adapter only supports simple single-entity scan pipelines."),
        );
      }

      return AdapterResult.ok({
        provider: "example-kv",
        kind: "rel",
        payload: {
          kind: "scan",
          request,
        } satisfies CompiledKvPlan,
      } satisfies ProviderCompiledPlan);
    },

    async execute(compiled, _context) {
      if (compiled.kind !== "rel") {
        return AdapterResult.err(new Error(`Unsupported compiled plan kind: ${compiled.kind}`));
      }

      const plan = compiled.payload as CompiledKvPlan;
      const materialized = rows.map(materializeRow);
      return AdapterResult.ok(applyScanRequest(materialized, plan.request));
    },

    async lookupMany(request, _context) {
      const keys = new Set(request.keys.map(String));
      return AdapterResult.ok(rows.filter((row) => keys.has(row.key)).map(materializeRow));
    },
  };
}

function materializeRow(row: KvRecord): QueryRow {
  return {
    id: row.key,
    value: row.value,
  };
}

function applyScanRequest(rows: QueryRow[], request: TableScanRequest): QueryRow[] {
  let out = [...rows];

  for (const clause of request.where ?? []) {
    out = out.filter((row) => {
      if (clause.op === "eq") {
        return row[clause.column] === clause.value;
      }
      if (clause.op === "in") {
        return clause.values.includes(row[clause.column]);
      }
      return true;
    });
  }

  if (request.limit != null) {
    out = out.slice(0, request.limit);
  }

  return out.map((row) =>
    Object.fromEntries(request.select.map((column) => [column, row[column] ?? null])),
  );
}
```

That adapter already participates in cross-provider joins through `lookupMany`, but its primary
provider contract is still rel compile/execute.

For normal adapter work, `@tupl/provider-kit` is the extension facade. Keep `@tupl/schema-model`
out of adapter code unless you are intentionally working on lower-level planner/schema internals.

## Stage 1: Strong Lookup Path

Implement `lookupMany` only as an optimization when your backend is naturally keyed.

This gives you:

- efficient keyed fetch
- cross-provider lookup joins
- a good baseline for point lookups and fanout joins

Relevant atom:

- `lookup.bulk`

If your system is truly key-driven, this is often more valuable than trying to emulate a full table scan.

In practice, `lookupMany` is the first optional optimization hook that should be fast, predictable,
and capacity-aware.

## Stage 2: Optional Scan

Only add `scan` if your backend has a rational way to do it.

Typical scan atoms:

- `scan.project`
- `scan.filter.basic`
- `scan.filter.set_membership`
- `scan.sort`
- `scan.limit_offset`

If the backend cannot support these without pathological behavior, leave them unsupported and rely on explicit rejection or carefully controlled fallback.

If you do add scan support later, do it as a narrow slice:

- a constrained scan over one entity
- only the filter operators your indexes actually support
- explicit rejection for everything else

## Stage 3: Selective Aggregates and Rel Pushdown

Do not treat `rel-core` or `rel-advanced` as mandatory milestones.

Instead, add atoms that map cleanly to backend features:

- `aggregate.group_by`
- `join.inner` only if the backend has a real indexed join-like primitive
- `set_op.union_all` only if it is natural and reliable

Skip atoms that would force expensive emulation in the provider.

## Rejection and Fallback

Non-relational adapters should be explicit about expensive shapes.

Good reasons to reject:

- unbounded scan over a high-cardinality keyspace
- large local join expansion driven by lookup fanout
- unsupported aggregate semantics
- unsupported window or CTE behavior

Use capability diagnostics to explain the decision:

- `missingAtoms`
- route family
- estimate fields when available

Default `tupl` behavior allows local fallback with diagnostics, but providers should tighten this when the cost profile is unacceptable.

Useful policy knobs:

- `allowFallback`
- `rejectOnMissingAtom`
- `rejectOnEstimatedCost`
- `maxLookupFanout`
- `maxLocalRows`

A good KV adapter is usually stricter than a relational one here. Silent fallback from an accidental broad keyspace access is rarely the right default.

## Practical Capability Shape

A healthy KV adapter might declare:

- atoms:
  - `lookup.bulk`
  - maybe `scan.project`
  - maybe `scan.filter.basic`
  - maybe `scan.limit_offset`

That is already enough to participate in mixed-provider queries.

## Expression Expectations

The core runtime can now execute a first batch of scalar expressions locally. That means a non-relational adapter does not need immediate pushdown support for:

- `LIKE`
- `NOT IN`
- arithmetic
- `CASE`
- basic string and numeric functions

Use that to keep the adapter simple:

- return unsupported for computed-expression pushdown
- let the runtime evaluate those expressions locally when policy allows

## Testing Strategy

Minimum tests:

1. lookupMany correctness
2. scan correctness, if scan exists
3. fallback correctness for unsupported relational shapes
4. rejection tests for expensive keyspace access
5. diagnostics stability for unsupported atoms

## Related Docs

- [creating-an-adapter.md](./creating-an-adapter.md)
- [provider-capability-matrix.md](./provider-capability-matrix.md)
