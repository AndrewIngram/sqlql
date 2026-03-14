import type { RelColumnRef, RelExpr, RelNode } from "@tupl/foundation";
import type { ScanFilterClause } from "@tupl/foundation";
import type {
  CorrelatedExistsFilter,
  CorrelatedInSubqueryFilter,
  CorrelatedScalarAggregateProjection,
  CorrelatedScalarAggregateFilter,
} from "./subqueries/correlated-predicate-types";

export interface Binding {
  table: string;
  alias: string;
  index: number;
  sourceKind: "table" | "cte";
}

export interface ParsedJoin {
  alias: string;
  joinType: "inner" | "left" | "right" | "full";
  leftAlias: string;
  leftColumn: string;
  rightAlias: string;
  rightColumn: string;
}

export interface SelectColumnProjection {
  kind: "column";
  source: RelColumnRef;
  output: string;
}

export interface SelectWindowProjection {
  kind: "window";
  output: string;
  function: Extract<RelNode, { kind: "window" }>["functions"][number];
}

export interface SelectExprProjection {
  kind: "expr";
  output: string;
  expr: RelExpr;
  source?: RelColumnRef;
}

export interface SelectCorrelatedScalarProjection {
  kind: "correlated_scalar";
  output: string;
  projection: CorrelatedScalarAggregateProjection;
}

export type SelectProjection =
  | SelectColumnProjection
  | SelectWindowProjection
  | SelectExprProjection
  | SelectCorrelatedScalarProjection;

export interface ResolvedOrderTerm {
  source: {
    alias?: string;
    column: string;
  };
  direction: "asc" | "desc";
}

export interface ParsedAggregateGroupProjection {
  kind: "group";
  output: string;
  source?: RelColumnRef;
  expr?: RelExpr;
}

export interface ParsedAggregateMetricProjection {
  kind: "metric";
  output: string;
  metric: {
    fn: "count" | "sum" | "avg" | "min" | "max";
    as: string;
    column?: RelColumnRef;
    distinct?: boolean;
  };
}

export type ParsedAggregateProjection =
  | ParsedAggregateGroupProjection
  | ParsedAggregateMetricProjection;

export interface ParsedGroupByRefTerm {
  kind: "ref";
  ref: RelColumnRef;
}

export interface ParsedGroupByOrdinalTerm {
  kind: "ordinal";
  position: number;
}

export type ParsedGroupByTerm = ParsedGroupByRefTerm | ParsedGroupByOrdinalTerm;

export interface ParsedOrderByRefTerm {
  kind: "ref";
  source: ResolvedOrderTerm["source"];
  direction: "asc" | "desc";
}

export interface ParsedOrderByOutputTerm {
  kind: "output";
  output: string;
  direction: "asc" | "desc";
}

export interface ParsedOrderByOrdinalTerm {
  kind: "ordinal";
  position: number;
  direction: "asc" | "desc";
}

export type ParsedOrderByTerm =
  | ParsedOrderByRefTerm
  | ParsedOrderByOutputTerm
  | ParsedOrderByOrdinalTerm;

export interface LiteralFilter {
  alias: string;
  clause: ScanFilterClause;
}

export interface InSubqueryFilter {
  negated?: boolean;
  alias: string;
  column: string;
  subquery: import("./sqlite-parser/ast").SelectAst;
}

export interface ParsedWhereFilters {
  literals: LiteralFilter[];
  inSubqueries: InSubqueryFilter[];
  existsSubqueries: CorrelatedExistsFilter[];
  correlatedInSubqueries: CorrelatedInSubqueryFilter[];
  correlatedScalarAggregates: CorrelatedScalarAggregateFilter[];
  residualExpr?: RelExpr;
}

export type ViewAliasColumnMap = Record<string, RelColumnRef>;
export type AliasToSourceMap = Map<string, Record<string, string>>;
