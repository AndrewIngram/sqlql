# Building a Non-Relational Adapter

This guide covers Redis, KV stores, document stores, and index-backed providers that can expose rows relationally but usually support only a narrow pushdown envelope.

For a concrete Redis implementation, see `@tupl/provider-ioredis` in this repo.

The key rule is that non-relational providers still compile canonical rel subtrees. Their non-relational nature shows up in a narrow `canExecute` envelope, not in a separate semantic model.

## Narrow-Scan Skeleton

This is the shape to aim for when the backend is naturally keyed and only supports constrained scans.

```ts
import type {
  ProviderAdapter,
  ProviderCapabilityReport,
  ProviderCompiledPlan,
  QueryRow,
  TableScanRequest,
} from "@tupl/provider-kit";
import { AdapterResult } from "@tupl/provider-kit";
import { checkSimpleRelScanCapability, prepareKeyedSimpleRelScan } from "@tupl/provider-kit/shapes";

type KvContext = {
  namespace: string;
};

type KvRecord = {
  key: string;
  value: unknown;
};

type CompiledKvPlan = {
  kind: "keyed_scan";
  request: TableScanRequest;
};

export function createExampleKvAdapter(rows: KvRecord[]): ProviderAdapter<KvContext> {
  return {
    name: "example-kv",
    fallbackPolicy: {
      maxLookupFanout: 1000,
      rejectOnEstimatedCost: true,
    },

    canExecute(rel): boolean | ProviderCapabilityReport {
      const capability = checkSimpleRelScanCapability(rel, {
        unsupportedShapeReason:
          "This KV adapter only supports simple single-entity scan pipelines.",
      });
      return capability.isOk() ? true : capability.error;
    },

    async compile(rel) {
      const capability = checkSimpleRelScanCapability(rel, {
        unsupportedShapeReason:
          "This KV adapter only supports simple single-entity scan pipelines.",
      });
      if (capability.isError()) {
        return AdapterResult.err(new Error(capability.error.reason ?? "Unsupported keyed scan."));
      }
      const request = capability.value;

      return AdapterResult.ok({
        provider: "example-kv",
        kind: "rel",
        payload: {
          kind: "keyed_scan",
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
  };
}
```

That provider is already useful even though its public surface is only rel compile/execute.

## Optional Lookup Optimization

If your backend has a genuinely valuable keyed bulk-fetch path, keep it in the optional helper layer under `@tupl/provider-kit/shapes`, not in the primary provider contract.

That keeps the public authoring model small while still allowing runtime optimizations for specific backends.

For keyed single-entity scans, prefer `prepareKeyedSimpleRelScan(...)` over stitching together:

- simple scan extraction
- field-sensitive validation
- lookup-key predicate inference
- fetch-column collection
- structured unsupported reports

## Stage 1: Optional Narrow Scan

Only add scan support if your backend has a rational way to do it.

The preferred implementation is:

- extract a simple single-source scan pipeline
- validate projected, filtered, and sorted fields
- reject unsupported shapes explicitly with a structured helper report

If the backend cannot support that without pathological behavior, leave it unsupported and rely on explicit rejection or controlled fallback.

## Stage 2: Selective Aggregates and Broader Rel Pushdown

Do not treat `rel-core` or `rel-advanced` as mandatory milestones.

Instead, add support where it maps cleanly to backend features:

- `aggregate.group_by`
- `join.inner` only if the backend has a real indexed join-like primitive
- `set_op.union_all` only if it is natural and reliable

Skip anything that would force expensive emulation inside the provider.

## Rejection and Fallback

Non-relational providers should be explicit about expensive shapes.

Good reasons to reject:

- unbounded scan over a high-cardinality keyspace
- large local join expansion driven by lookup fanout
- unsupported aggregate semantics
- unsupported window or CTE behavior

Useful policy knobs:

- `allowFallback`
- `rejectOnEstimatedCost`
- `maxLookupFanout`
- `maxLocalRows`

A good KV provider is usually stricter than a relational one here. Silent fallback from an accidental broad keyspace access is rarely the right default.

## Practical Capability Shape

A healthy KV provider should only need:

- rel-shape extraction helpers
- field-policy validation
- structured unsupported reports

`canExecute(...)` remains the source of truth.

## Expression Expectations

The runtime can execute a broad batch of scalar expressions locally. That means a non-relational provider does not need immediate pushdown support for:

- `LIKE`
- `NOT IN`
- arithmetic
- `CASE`
- basic string and numeric functions

Use that to keep the provider simple:

- return unsupported for computed-expression pushdown
- let the runtime evaluate those expressions locally when policy allows
