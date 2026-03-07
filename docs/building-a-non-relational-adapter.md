# Building a Non-Relational Adapter

This guide covers lookup-first systems such as Redis, KV stores, document stores, and index-backed adapters.

The key rule is that non-relational adapters do not need to mimic the relational progression exactly.

## Lookup-First Skeleton

This is the shape to aim for when the backend is naturally keyed and may never offer a rational full scan.

```ts
import type {
  ProviderAdapter,
  ProviderCapabilityReport,
  ProviderCompiledPlan,
  ProviderLookupManyRequest,
  ProviderCapabilityAtom,
  QueryRow,
  TableScanRequest,
} from "sqlql";

type KvContext = {
  namespace: string;
};

type KvRecord = {
  key: string;
  value: unknown;
};

type CompiledKvPlan =
  | {
      kind: "scan";
      request: TableScanRequest;
    }
  | {
      kind: "lookupMany";
      request: ProviderLookupManyRequest;
    };

const declaredAtoms: readonly ProviderCapabilityAtom[] = [
  "lookup.bulk",
];

export function createExampleKvAdapter(rows: KvRecord[]): ProviderAdapter<KvContext> {
  return {
    name: "example-kv",
    routeFamilies: ["lookup"],
    capabilityAtoms: [...declaredAtoms],
    fallbackPolicy: {
      maxLookupFanout: 1000,
      rejectOnEstimatedCost: true,
    },

    canExecute(fragment): boolean | ProviderCapabilityReport {
      switch (fragment.kind) {
        case "scan":
          return {
            supported: false,
            routeFamily: "scan",
            requiredAtoms: ["scan.project"],
            missingAtoms: ["scan.project"],
            reason: "This KV adapter is lookup-first and does not support general scan pushdown.",
          };
        case "rel":
          return {
            supported: false,
            routeFamily: "rel-core",
            reason: "Complex relational pushdown is not supported for this KV adapter.",
          };
        default:
          return false;
      }
    },

    async compile(fragment): Promise<ProviderCompiledPlan> {
      if (fragment.kind === "scan") {
        return {
          provider: "example-kv",
          kind: "scan",
          payload: {
            kind: "scan",
            request: fragment.request,
          } satisfies CompiledKvPlan,
        };
      }

      throw new Error(`Unsupported fragment kind: ${fragment.kind}`);
    },

    async execute(compiled, _context): Promise<QueryRow[]> {
      if (compiled.kind !== "scan") {
        throw new Error(`Unsupported compiled plan kind: ${compiled.kind}`);
      }

      const plan = compiled.payload as Extract<CompiledKvPlan, { kind: "scan" }>;
      const materialized = rows.map(materializeRow);
      return applyScanRequest(materialized, plan.request);
    },

    async lookupMany(request, _context): Promise<QueryRow[]> {
      const keys = new Set(request.keys.map(String));
      return rows
        .filter((row) => keys.has(row.key))
        .map(materializeRow);
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

That adapter already participates in cross-provider joins through `lookupMany`, even though it rejects `scan` and `rel` pushdown.

## Route Family Progression

For lookup-first or KV-style backends, the recommended progression is:

1. `lookup`
2. optional `scan`
3. optional `aggregate`
4. selective `rel`

That means a provider can be useful before it supports `scan` at all.

## Stage 1: Strong Lookup Path

Implement `lookupMany` first when your backend is naturally keyed.

This gives you:

- efficient keyed fetch
- cross-provider lookup joins
- a good baseline for point lookups and fanout joins

Relevant atom:

- `lookup.bulk`

If your system is truly key-driven, this is often more valuable than trying to emulate a full table scan.

In practice, `lookupMany` is the first method that should be fast, predictable, and capacity-aware.

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

Default `sqlql` behavior allows local fallback with diagnostics, but providers should tighten this when the cost profile is unacceptable.

Useful policy knobs:

- `allowFallback`
- `rejectOnMissingAtom`
- `rejectOnEstimatedCost`
- `maxLookupFanout`
- `maxLocalRows`

A good KV adapter is usually stricter than a relational one here. Silent fallback from an accidental broad keyspace access is rarely the right default.

## Practical Capability Shape

A healthy KV adapter might declare:

- route families: `lookup`, optionally `scan`
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
