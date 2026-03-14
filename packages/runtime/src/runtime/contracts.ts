import type { PhysicalPlan } from "@tupl/planner";
import type {
  ProviderPlanDescription,
  QueryFallbackPolicy,
  ProvidersMap,
  TuplDiagnostic,
} from "@tupl/provider-kit";
import type { RelConvention, RelNode, TuplResult } from "@tupl/foundation";

import type { ConstraintValidationOptions } from "./constraints";
import type { QueryRow, SchemaDefinition } from "@tupl/schema-model";

export type { QueryFallbackPolicy, TuplDiagnostic } from "@tupl/provider-kit";

/**
 * Runtime contracts define the public query/explain surface exposed by the runtime package root.
 */
export interface QueryGuardrails {
  /** Hard upper bound on the size of the lowered relational tree. */
  maxPlannerNodes: number;
  /** Hard upper bound on rows materialized by local or remote execution. */
  maxExecutionRows: number;
  /** Maximum lookup keys sent in a single provider batch request. */
  maxLookupKeysPerBatch: number;
  /** Maximum number of lookup batches a query may fan out into. */
  maxLookupBatches: number;
  /** End-to-end timeout applied to provider execution and local evaluation phases. */
  timeoutMs: number;
}

/**
 * Default runtime guardrails favor successful local execution while still preventing accidental
 * unbounded planning, lookup fanout, and runaway result sizes.
 */
export const DEFAULT_QUERY_GUARDRAILS: QueryGuardrails = {
  maxPlannerNodes: 50_000,
  maxExecutionRows: 1_000_000,
  maxLookupKeysPerBatch: 1000,
  maxLookupBatches: 100,
  timeoutMs: 30_000,
};

/**
 * Default fallback policy allows local execution when provider pushdown is incomplete.
 * Callers that need strict pushdown must override these defaults explicitly.
 */
export const DEFAULT_QUERY_FALLBACK_POLICY: Required<QueryFallbackPolicy> = {
  allowFallback: true,
  warnOnFallback: true,
  rejectOnMissingAtom: false,
  rejectOnEstimatedCost: false,
  maxLocalRows: Number.POSITIVE_INFINITY,
  maxLookupFanout: Number.POSITIVE_INFINITY,
  maxJoinExpansionRisk: Number.POSITIVE_INFINITY,
};

/**
 * Query input is the fully bound runtime request shape used by top-level query helpers.
 * It requires an already finalized schema plus the concrete provider map that owns execution.
 */
export interface QueryInput<TContext> {
  schema: SchemaDefinition;
  providers: ProvidersMap<TContext>;
  context: TContext;
  sql: string;
  queryGuardrails?: Partial<QueryGuardrails>;
  fallbackPolicy?: QueryFallbackPolicy;
  constraintValidation?: ConstraintValidationOptions;
}

/**
 * Executable-schema query input is the caller-facing request shape once schema and providers are
 * already bound into an executable schema instance.
 */
export interface ExecutableSchemaQueryInput<TContext> {
  context: TContext;
  sql: string;
  queryGuardrails?: Partial<QueryGuardrails>;
  fallbackPolicy?: QueryFallbackPolicy;
  constraintValidation?: ConstraintValidationOptions;
}

/**
 * Executable schemas bind a finalized schema to concrete providers and expose the stable runtime API.
 * They own execution wiring so callers do not need to pass schema/provider maps on each query.
 */
export interface ExecutableSchema<TContext, TSchema extends SchemaDefinition = SchemaDefinition> {
  schema: TSchema;
  query(input: ExecutableSchemaQueryInput<TContext>): Promise<TuplResult<QueryRow[]>>;
  explain(input: ExecutableSchemaQueryInput<TContext>): Promise<TuplResult<ExplainResult>>;
}

/**
 * Explain diagnostics are attached to a concrete translation stage so query tooling can render
 * planner/provider warnings next to the artifact that produced them.
 */
export interface ExplainDiagnostic {
  stage: "lowering" | "rewriting" | "physical_planning" | "provider_planning";
  diagnostic: TuplDiagnostic;
}

export interface ExplainFragment {
  id: string;
  convention: RelConvention;
  provider?: string;
  rel: RelNode;
}

export interface ExplainProviderPlan {
  fragmentId: string;
  provider: string;
  kind: string;
  rel: RelNode;
  description?: ProviderPlanDescription;
  descriptionUnavailable?: true;
}

/**
 * Explain results expose the staged translation pipeline from SQL to logical and physical plans.
 * They are introspection artifacts and never imply that the query has executed. Provider plan
 * descriptions may use either basic fragment metadata or enriched compiled descriptions depending
 * on the runtime's internal explain mode.
 */
export interface ExplainResult {
  sql: string;
  initialRel: RelNode;
  rewrittenRel: RelNode;
  physicalPlan: PhysicalPlan;
  fragments: ExplainFragment[];
  providerPlans: ExplainProviderPlan[];
  plannerNodeCount: number;
  diagnostics: ExplainDiagnostic[];
}
