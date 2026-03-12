import type { DataEntityColumnMap, DataEntityHandle, DataEntityShape } from "../entity-handles";
import type {
  FragmentProviderAdapter,
  LookupProviderAdapter,
  ProviderCompiledPlan,
  ProviderFragment,
  ProviderLookupManyRequest,
  QueryRow,
} from "../contracts";
import type {
  ProviderCapabilityAtom,
  ProviderCapabilityReport,
  ProviderRouteFamily,
  QueryFallbackPolicy,
} from "../capabilities";
import type { AdapterResult, MaybePromise } from "../operations";

/**
 * Relational provider helpers own the ordinary adapter-authoring path for SQL-like sources.
 * Backend-specific SQL compilation still lives in provider packages; this surface only absorbs
 * repeated entity binding, capability reporting, and fragment wiring.
 */
export interface RelationalProviderEntityConfig<TColumns extends string = string> {
  shape?: DataEntityShape<TColumns>;
}

export type RelationalProviderRelCompileStrategy = string;

export interface RelationalProviderCapabilityContext<
  TContext,
  TEntities extends Record<string, RelationalProviderEntityConfig>,
  TStrategy extends RelationalProviderRelCompileStrategy,
> {
  context: TContext;
  entities: TEntities;
  fragment: Extract<ProviderFragment, { kind: "rel" }>;
  routeFamily: ProviderRouteFamily;
  requiredAtoms: ProviderCapabilityAtom[];
  missingAtoms: ProviderCapabilityAtom[];
  strategy: TStrategy | null;
}

export interface RelationalProviderCompileScanArgs<
  TContext,
  TEntities extends Record<string, RelationalProviderEntityConfig>,
> {
  context: TContext;
  entities: TEntities;
  fragment: Extract<ProviderFragment, { kind: "scan" }>;
  name: string;
}

export interface RelationalProviderCompileRelArgs<
  TContext,
  TEntities extends Record<string, RelationalProviderEntityConfig>,
  TStrategy extends RelationalProviderRelCompileStrategy,
> {
  context: TContext;
  entities: TEntities;
  fragment: Extract<ProviderFragment, { kind: "rel" }>;
  name: string;
  strategy: TStrategy;
}

export interface RelationalProviderExecuteArgs<
  TContext,
  TEntities extends Record<string, RelationalProviderEntityConfig>,
> {
  context: TContext;
  entities: TEntities;
  plan: ProviderCompiledPlan;
  name: string;
}

export interface RelationalProviderLookupArgs<
  TContext,
  TEntities extends Record<string, RelationalProviderEntityConfig>,
> {
  context: TContext;
  entities: TEntities;
  name: string;
  request: ProviderLookupManyRequest;
}

export interface RelationalProviderEntityColumnsArgs<
  TEntities extends Record<string, RelationalProviderEntityConfig>,
  TEntityName extends Extract<keyof TEntities, string>,
> {
  config: TEntities[TEntityName];
  entity: TEntityName;
  name: string;
}

export interface RelationalProviderSupportArgs<
  TContext,
  TEntities extends Record<string, RelationalProviderEntityConfig>,
  TStrategy extends RelationalProviderRelCompileStrategy,
> extends RelationalProviderCapabilityContext<TContext, TEntities, TStrategy> {}

export const DEFAULT_RELATIONAL_CAPABILITY_ATOMS = [
  "scan.project",
  "scan.filter.basic",
  "scan.filter.set_membership",
  "scan.sort",
  "scan.limit_offset",
  "aggregate.group_by",
  "join.inner",
  "join.left",
  "join.right_full",
  "set_op.union_all",
  "set_op.union_distinct",
  "set_op.intersect",
  "set_op.except",
  "cte.non_recursive",
  "window.rank_basic",
] as const satisfies readonly ProviderCapabilityAtom[];

interface RelationalProviderAdapterOptionsBase<
  TContext,
  TEntities extends Record<string, RelationalProviderEntityConfig>,
  TStrategy extends RelationalProviderRelCompileStrategy,
> {
  name: string;
  declaredAtoms?: readonly ProviderCapabilityAtom[];
  entities: TEntities;
  fallbackPolicy?: QueryFallbackPolicy;
  routeFamilies?: readonly ProviderRouteFamily[];
  resolveEntityColumns?<TEntityName extends Extract<keyof TEntities, string>>(
    args: RelationalProviderEntityColumnsArgs<TEntities, TEntityName>,
  ): DataEntityColumnMap<string> | undefined;
  resolveRelCompileStrategy(args: {
    context: TContext;
    entities: TEntities;
    fragment: Extract<ProviderFragment, { kind: "rel" }>;
  }): MaybePromise<TStrategy | null>;
  unsupportedRelReason?(
    args: RelationalProviderCapabilityContext<TContext, TEntities, TStrategy>,
  ): string;
  unsupportedRelReasonMessage?: string;
  unsupportedRelCompileMessage?: string;
  isRelStrategySupported?(
    args: RelationalProviderSupportArgs<TContext, TEntities, TStrategy>,
  ): MaybePromise<true | string | ProviderCapabilityReport>;
  compileScanFragment?(
    args: RelationalProviderCompileScanArgs<TContext, TEntities>,
  ): MaybePromise<AdapterResult<ProviderCompiledPlan>>;
  compileRelFragment?(
    args: RelationalProviderCompileRelArgs<TContext, TEntities, TStrategy>,
  ): MaybePromise<AdapterResult<ProviderCompiledPlan>>;
  buildRelPlanPayload?(
    args: RelationalProviderCompileRelArgs<TContext, TEntities, TStrategy>,
  ): unknown;
  executeCompiledPlan(
    args: RelationalProviderExecuteArgs<TContext, TEntities>,
  ): MaybePromise<AdapterResult<QueryRow[]>>;
}

export interface RelationalProviderAdapterOptions<
  TContext,
  TEntities extends Record<string, RelationalProviderEntityConfig>,
  TStrategy extends RelationalProviderRelCompileStrategy,
> extends RelationalProviderAdapterOptionsBase<TContext, TEntities, TStrategy> {
  lookupMany?: undefined;
}

export interface RelationalProviderAdapterOptionsWithLookup<
  TContext,
  TEntities extends Record<string, RelationalProviderEntityConfig>,
  TStrategy extends RelationalProviderRelCompileStrategy,
> extends RelationalProviderAdapterOptionsBase<TContext, TEntities, TStrategy> {
  lookupMany(
    args: RelationalProviderLookupArgs<TContext, TEntities>,
  ): MaybePromise<AdapterResult<QueryRow[]>>;
}

export type RelationalProviderHandles<
  TEntities extends Record<string, RelationalProviderEntityConfig>,
> = {
  [K in keyof TEntities]: DataEntityHandle<string>;
};

export type RelationalProviderAdapter<
  TContext,
  TEntities extends Record<string, RelationalProviderEntityConfig>,
> = FragmentProviderAdapter<TContext> & {
  entities: RelationalProviderHandles<TEntities>;
};

export type RelationalProviderAdapterWithLookup<
  TContext,
  TEntities extends Record<string, RelationalProviderEntityConfig>,
> = FragmentProviderAdapter<TContext> &
  LookupProviderAdapter<TContext> & {
    entities: RelationalProviderHandles<TEntities>;
  };
