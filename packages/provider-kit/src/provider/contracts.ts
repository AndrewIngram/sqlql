import type {
  DataEntityHandle as FoundationDataEntityHandle,
  QueryRow,
  RelNode,
  ScanFilterClause,
  TableAggregateRequest,
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

export interface ProviderCompiledPlan {
  provider: string;
  kind: string;
  payload: unknown;
}

export interface ProviderLookupManyRequest {
  table: string;
  alias?: string;
  key: string;
  keys: unknown[];
  select: string[];
  where?: ScanFilterClause[];
}

export interface RelProviderFragment {
  kind: "rel";
  provider: string;
  rel: RelNode;
}

export interface ScanProviderFragment {
  kind: "scan";
  provider: string;
  table: string;
  request: TableScanRequest;
}

export interface AggregateProviderFragment {
  kind: "aggregate";
  provider: string;
  table: string;
  request: TableAggregateRequest;
}

export type ProviderFragment =
  | RelProviderFragment
  | ScanProviderFragment
  | AggregateProviderFragment;

export type ProviderOperationResult<T, E = Error> = AdapterResult<T, E>;
export type { ProviderRuntimeBinding };

export interface ProviderAdapterBase<TContext = unknown> {
  name: string;
  routeFamilies?: ProviderRouteFamily[];
  capabilityAtoms?: ProviderCapabilityAtom[];
  fallbackPolicy?: QueryFallbackPolicy;
  canExecute(
    fragment: ProviderFragment,
    context: TContext,
  ): MaybePromise<boolean | ProviderCapabilityReport>;
  estimate?(fragment: ProviderFragment, context: TContext): MaybePromise<ProviderEstimate>;
  /**
   * Optional source-neutral physical entity handles owned by this adapter.
   */
  entities?: Record<string, FoundationDataEntityHandle<string>>;
}

export interface FragmentProviderAdapter<TContext = unknown> extends ProviderAdapterBase<TContext> {
  compile(
    fragment: ProviderFragment,
    context: TContext,
  ): MaybePromise<ProviderOperationResult<ProviderCompiledPlan>>;
  execute(
    plan: ProviderCompiledPlan,
    context: TContext,
  ): MaybePromise<ProviderOperationResult<QueryRow[]>>;
}

export interface LookupProviderAdapter<TContext = unknown> extends ProviderAdapterBase<TContext> {
  lookupMany(
    request: ProviderLookupManyRequest,
    context: TContext,
  ): MaybePromise<ProviderOperationResult<QueryRow[]>>;
}

export type ProviderAdapter<TContext = unknown> =
  | FragmentProviderAdapter<TContext>
  | LookupProviderAdapter<TContext>
  | (FragmentProviderAdapter<TContext> & LookupProviderAdapter<TContext>);

export type ProvidersMap<TContext = unknown> = Record<string, ProviderAdapter<TContext>>;
export type DataSourceAdapter<TContext = unknown> = ProviderAdapter<TContext>;
