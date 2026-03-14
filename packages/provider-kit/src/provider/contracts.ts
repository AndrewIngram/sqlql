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

export type ProviderOperationResult<T, E = Error> = AdapterResult<T, E>;
export type { ProviderRuntimeBinding };

/**
 * Provider adapter base is the minimum contract every provider implements.
 * Capability and estimate methods describe what can be run remotely; they do not execute work.
 */
export interface ProviderAdapterBase<TContext = unknown> {
  name: string;
  /** Capability atoms describe pushdown features supported by the adapter. */
  capabilityAtoms?: ProviderCapabilityAtom[];
  /** Provider-local fallback policy is merged with per-query runtime overrides. */
  fallbackPolicy?: QueryFallbackPolicy;
  /**
   * `canExecute` answers whether the provider can run a provider-normalized rel subtree remotely.
   * Returning a report is preferred when the adapter can explain unsupported atoms or cost.
   */
  canExecute(rel: RelNode, context: TContext): MaybePromise<boolean | ProviderCapabilityReport>;
  /** `estimate` is optional advisory metadata used when choosing remote vs local execution. */
  estimate?(rel: RelNode, context: TContext): MaybePromise<ProviderEstimate>;
  /**
   * Optional source-neutral physical entity handles owned by this adapter.
   */
  entities?: Record<string, FoundationDataEntityHandle<string>>;
}

/**
 * Fragment providers support full rel-compile/execute flows.
 * `compile` must be pure with respect to runtime state; `execute` runs the compiled provider plan.
 */
export interface FragmentProviderAdapter<TContext = unknown> extends ProviderAdapterBase<TContext> {
  compile(
    rel: RelNode,
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
 * Provider adapters compile and execute canonical relational subtrees.
 */
export type ProviderAdapter<TContext = unknown> = FragmentProviderAdapter<TContext>;

/** Providers maps are runtime lookup tables keyed by the provider name exposed on the adapter. */
export type ProvidersMap<TContext = unknown> = Record<string, ProviderAdapter<TContext>>;
export type DataSourceAdapter<TContext = unknown> = ProviderAdapter<TContext>;

export type {
  QueryRow,
  ScanFilterClause,
  ScanOrderBy,
  TableAggregateMetric,
  TableAggregateRequest,
  TableLookupRequest,
  TableScanRequest,
};
