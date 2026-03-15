import type { RelNode } from "@tupl/foundation";
import type { DataEntityColumnMap, DataEntityHandle, DataEntityShape } from "../entity-handles";
import type {
  FragmentProviderAdapter,
  ProviderCompiledPlan,
  ProviderPlanDescription,
  QueryRow,
} from "../contracts";
import type {
  LookupManyCapableProviderAdapter,
  ProviderLookupManyRequest,
} from "../shapes/lookup-optimization";
import type { ProviderCapabilityReport, QueryFallbackPolicy } from "../capabilities";
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
  rel: RelNode;
  strategy: TStrategy | null;
}

export interface RelationalProviderCompileRelArgs<
  TContext,
  TEntities extends Record<string, RelationalProviderEntityConfig>,
  TStrategy extends RelationalProviderRelCompileStrategy,
> {
  context: TContext;
  entities: TEntities;
  rel: RelNode;
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

export interface RelationalProviderDescribeArgs<
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

interface RelationalProviderAdapterOptionsBase<
  TContext,
  TEntities extends Record<string, RelationalProviderEntityConfig>,
  TStrategy extends RelationalProviderRelCompileStrategy,
> {
  name: string;
  entities: TEntities;
  fallbackPolicy?: QueryFallbackPolicy;
  resolveEntityColumns?<TEntityName extends Extract<keyof TEntities, string>>(
    args: RelationalProviderEntityColumnsArgs<TEntities, TEntityName>,
  ): DataEntityColumnMap<string> | undefined;
  resolveRelCompileStrategy(args: {
    context: TContext;
    entities: TEntities;
    rel: RelNode;
  }): MaybePromise<TStrategy | null>;
  unsupportedRelReason?(
    args: RelationalProviderCapabilityContext<TContext, TEntities, TStrategy>,
  ): string | ProviderCapabilityReport;
  unsupportedRelReasonMessage?: string;
  unsupportedRelCompileMessage?: string;
  isRelStrategySupported?(
    args: RelationalProviderSupportArgs<TContext, TEntities, TStrategy>,
  ): MaybePromise<true | string | ProviderCapabilityReport>;
  compileRelFragment?(
    args: RelationalProviderCompileRelArgs<TContext, TEntities, TStrategy>,
  ): MaybePromise<AdapterResult<ProviderCompiledPlan>>;
  buildRelPlanPayload?(
    args: RelationalProviderCompileRelArgs<TContext, TEntities, TStrategy>,
  ): unknown;
  describeCompiledPlan?(
    args: RelationalProviderDescribeArgs<TContext, TEntities>,
  ): MaybePromise<ProviderPlanDescription>;
  executeCompiledPlan(
    args: RelationalProviderExecuteArgs<TContext, TEntities>,
  ): MaybePromise<AdapterResult<QueryRow[]>>;
}

export interface RelationalProviderAdapterOptions<
  TContext,
  TEntities extends Record<string, RelationalProviderEntityConfig>,
  TStrategy extends RelationalProviderRelCompileStrategy,
> extends RelationalProviderAdapterOptionsBase<TContext, TEntities, TStrategy> {}

export interface RelationalLookupProviderAdapterOptions<
  TContext,
  TEntities extends Record<string, RelationalProviderEntityConfig>,
  TStrategy extends RelationalProviderRelCompileStrategy,
> extends RelationalProviderAdapterOptionsBase<TContext, TEntities, TStrategy> {
  lookupMany?(
    this: void,
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

export type LookupCapableRelationalProviderAdapter<
  TContext,
  TEntities extends Record<string, RelationalProviderEntityConfig>,
> = RelationalProviderAdapter<TContext, TEntities> & LookupManyCapableProviderAdapter<TContext>;
