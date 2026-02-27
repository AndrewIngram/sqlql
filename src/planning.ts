export type ScalarValue = string | number | boolean | null;

export type ComparisonOperator = "=" | "!=" | ">" | ">=" | "<" | "<=";

export interface ColumnRef {
  table: string;
  column: string;
}

export interface LiteralValue {
  kind: "literal";
  value: ScalarValue;
}

export interface ColumnValue {
  kind: "column";
  ref: ColumnRef;
}

export type Expression = ColumnValue | LiteralValue;

export interface ComparisonPredicate {
  kind: "comparison";
  operator: ComparisonOperator;
  left: Expression;
  right: Expression;
}

export interface InPredicate {
  kind: "in";
  left: Expression;
  values: ScalarValue[];
}

export interface NullPredicate {
  kind: "null_check";
  expression: Expression;
  negated: boolean;
}

export interface AndPredicate {
  kind: "and";
  predicates: Predicate[];
}

export interface OrPredicate {
  kind: "or";
  predicates: Predicate[];
}

export interface NotPredicate {
  kind: "not";
  predicate: Predicate;
}

export type Predicate =
  | ComparisonPredicate
  | InPredicate
  | NullPredicate
  | AndPredicate
  | OrPredicate
  | NotPredicate;

export interface SelectItem {
  expression: Expression;
  alias?: string;
}

export interface OrderByTerm {
  expression: Expression;
  direction: "asc" | "desc";
}

export interface JoinSpec {
  type: "inner" | "left";
  sourceTable: string;
  sourceAlias?: string;
  on: Predicate;
}

export interface AggregateExpression {
  function: "count" | "sum" | "avg" | "min" | "max";
  argument?: Expression;
  distinct?: boolean;
}

export interface AggregateItem {
  alias: string;
  expression: AggregateExpression;
}

export interface CteBinding {
  name: string;
  query: QueryIR;
  recursive?: boolean;
}

export interface FromSource {
  table: string;
  alias?: string;
}

export interface QueryIR {
  ctes?: CteBinding[];
  from: FromSource;
  joins?: JoinSpec[];
  select: SelectItem[];
  where?: Predicate;
  groupBy?: Expression[];
  aggregates?: AggregateItem[];
  orderBy?: OrderByTerm[];
  limit?: number;
  offset?: number;
}

export interface MutationIR {
  type: "insert" | "update" | "delete";
  table: string;
}

export interface QueryCapabilities {
  filterOperators: ComparisonOperator[];
  filterableColumns: string[];
  sortableColumns: string[];
  maxLimit: number;
  requiresLimit: boolean;
  supportsOr: boolean;
  supportsNot: boolean;
  supportsAggregates: boolean;
  supportsCtes: boolean;
  supportsRecursiveCtes: boolean;
}

export interface ScanRequest {
  table: string;
  alias?: string;
  projection: ColumnRef[];
  predicate?: Predicate;
  orderBy?: OrderByTerm[];
  limit?: number;
  offset?: number;
}

export interface AggregateRequest {
  table: string;
  alias?: string;
  predicate?: Predicate;
  groupBy: Expression[];
  aggregates: AggregateItem[];
  orderBy?: OrderByTerm[];
  limit?: number;
}

export type Row = Record<string, unknown>;

export interface RowSet {
  rows: Row[];
}

export interface ResolverContext {
  requestId: string;
  actorId?: string;
}

export interface TableResolver<TContext extends ResolverContext = ResolverContext> {
  table: string;
  capabilities: QueryCapabilities;
  scan: (request: ScanRequest, context: TContext) => Promise<RowSet>;
  aggregate?: (request: AggregateRequest, context: TContext) => Promise<RowSet>;
}

export interface ResolverRegistry<TContext extends ResolverContext = ResolverContext> {
  getResolver: (table: string) => TableResolver<TContext> | undefined;
}

export interface PlanStepBase {
  id: string;
  dependsOn: string[];
}

export interface ScanStep extends PlanStepBase {
  kind: "scan";
  table: string;
  resolver: string;
  request: ScanRequest;
  pushdown: {
    predicate: boolean;
    projection: boolean;
    orderBy: boolean;
    limit: boolean;
  };
}

export interface FilterStep extends PlanStepBase {
  kind: "filter";
  predicate: Predicate;
}

export interface JoinStep extends PlanStepBase {
  kind: "join";
  join: JoinSpec;
}

export interface AggregateStep extends PlanStepBase {
  kind: "aggregate";
  request: AggregateRequest;
}

export interface ProjectStep extends PlanStepBase {
  kind: "project";
  select: SelectItem[];
}

export interface CteStep extends PlanStepBase {
  kind: "cte";
  binding: CteBinding;
  strategy: "inline" | "materialize";
}

export type PlanStep = ScanStep | FilterStep | JoinStep | AggregateStep | ProjectStep | CteStep;

export interface QueryPlan {
  rootStepId: string;
  steps: PlanStep[];
}

export interface ExplainDecision {
  stepId: string;
  decision: string;
}

export interface ExplainResult {
  query: QueryIR;
  plan: QueryPlan;
  decisions: ExplainDecision[];
}

export function defineTableResolver<TContext extends ResolverContext = ResolverContext>(
  resolver: TableResolver<TContext>,
): TableResolver<TContext> {
  return resolver;
}
