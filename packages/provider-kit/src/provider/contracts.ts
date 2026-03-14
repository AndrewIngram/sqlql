import type {
  DataEntityHandle as FoundationDataEntityHandle,
  QueryRow,
  RelNode,
  ScanFilterClause,
  ScanOrderBy,
  TableAggregateMetric,
  TableAggregateRequest,
  TableLookupRequest,
  TableScanRequest,
} from "@tupl/foundation";

import type {
  ProviderCapabilityAtom,
  ProviderCapabilityReport,
  ProviderEstimate,
  ProviderRouteFamily,
  QueryFallbackPolicy,
} from "./capabilities";
import type { AdapterResult, MaybePromise, ProviderRuntimeBinding } from "./operations";

export interface ProviderPlanOperationDescription {
  kind: string;
  summary?: string;
  sql?: string;
  lookup?: unknown;
  target?: string;
  variables?: unknown;
  raw?: unknown;
}

export interface ProviderPlanDescription {
  kind: string;
  summary: string;
  operations: ProviderPlanOperationDescription[];
  raw?: unknown;
}

/**
 * Provider compiled plans are provider-owned execution payloads produced from a fragment.
 * Runtime treats `payload` as opaque and must not assume any backend-specific structure.
 */
export interface ProviderCompiledPlan {
  provider: string;
  kind: string;
  payload: unknown;
}

/**
 * Provider lookup-many requests represent runtime-driven batched key lookups against one entity.
 * They are a provider-facing optimization contract, not part of the public query API.
 */
export interface ProviderLookupManyRequest {
  table: string;
  alias?: string;
  key: string;
  keys: unknown[];
  select: string[];
  where?: ScanFilterClause[];
}

/** Rel fragments ask a provider to execute a provider-normalized relational subtree. */
export interface RelProviderFragment {
  kind: "rel";
  provider: string;
  rel: RelNode;
}

/** Scan fragments ask a provider to execute a single physical scan request directly. */
export interface ScanProviderFragment {
  kind: "scan";
  provider: string;
  table: string;
  request: TableScanRequest;
}

/** Aggregate fragments ask a provider to execute one grouped aggregate request directly. */
export interface AggregateProviderFragment {
  kind: "aggregate";
  provider: string;
  table: string;
  request: TableAggregateRequest;
}

/**
 * Provider fragments are the only units of work the runtime asks a provider to reason about.
 * Providers must not assume they can inspect planner internals beyond this fragment shape.
 */
export type ProviderFragment =
  | RelProviderFragment
  | ScanProviderFragment
  | AggregateProviderFragment;

export type ProviderOperationResult<T, E = Error> = AdapterResult<T, E>;
export type { ProviderRuntimeBinding };

/**
 * ProviderBase is the runtime provider contract.
 * This is the provider object registered with the runtime.
 * Capability and estimate methods describe what can be run remotely; they do not execute work.
 */
export interface ProviderBase<TContext = unknown> {
  name: string;
  /** Declared route families summarize the kinds of provider work this provider is designed for. */
  routeFamilies?: ProviderRouteFamily[];
  /** Capability atoms describe pushdown features supported by the provider. */
  capabilityAtoms?: ProviderCapabilityAtom[];
  /** Provider-local fallback policy is merged with per-query runtime overrides. */
  fallbackPolicy?: QueryFallbackPolicy;
  /**
   * `canExecute` answers whether the provider can run a fragment remotely.
   * Returning a report is preferred when the provider can explain unsupported atoms or cost.
   */
  canExecute(
    fragment: ProviderFragment,
    context: TContext,
  ): MaybePromise<boolean | ProviderCapabilityReport>;
  /** `estimate` is optional advisory metadata used when choosing remote vs local execution. */
  estimate?(fragment: ProviderFragment, context: TContext): MaybePromise<ProviderEstimate>;
  /**
   * Optional source-neutral physical entity handles owned by this provider.
   */
  entities?: Record<string, FoundationDataEntityHandle<string>>;
}

/**
 * Fragment providers support full fragment compile/execute flows.
 * `compile` must be pure with respect to runtime state; `execute` runs the compiled provider plan.
 */
export interface FragmentProvider<TContext = unknown> extends ProviderBase<TContext> {
  compile(
    fragment: ProviderFragment,
    context: TContext,
  ): MaybePromise<ProviderOperationResult<ProviderCompiledPlan>>;
  describeCompiledPlan?(
    plan: ProviderCompiledPlan,
    context: TContext,
  ): MaybePromise<ProviderPlanDescription>;
  execute(
    plan: ProviderCompiledPlan,
    context: TContext,
  ): MaybePromise<ProviderOperationResult<QueryRow[]>>;
}

/**
 * Lookup providers support batched key lookups used by runtime lookup joins.
 * They complement, rather than replace, fragment execution support.
 */
export interface LookupProvider<TContext = unknown> extends ProviderBase<TContext> {
  lookupMany(
    request: ProviderLookupManyRequest,
    context: TContext,
  ): MaybePromise<ProviderOperationResult<QueryRow[]>>;
}

/**
 * Providers may support fragment execution, lookup execution, or both.
 * Callers should feature-detect optional capabilities rather than assume one concrete shape.
 */
export type Provider<TContext = unknown> =
  | FragmentProvider<TContext>
  | LookupProvider<TContext>
  | (FragmentProvider<TContext> & LookupProvider<TContext>);

/** Provider maps are runtime lookup tables keyed by the provider name exposed on the provider. */
export type ProviderMap<TContext = unknown> = Record<string, Provider<TContext>>;
export type DataSourceProvider<TContext = unknown> = Provider<TContext>;

export type {
  QueryRow,
  ScanFilterClause,
  ScanOrderBy,
  TableAggregateMetric,
  TableAggregateRequest,
  TableLookupRequest,
  TableScanRequest,
};
