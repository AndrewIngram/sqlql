import type { SelectAst } from "../sqlite-parser/ast";

/**
 * Correlated predicate types capture supported correlated-subquery shapes before decorrelation.
 */
export interface CorrelatedExistsFilter {
  negated: boolean;
  outer: {
    alias: string;
    column: string;
  };
  inner: {
    alias: string;
    column: string;
  };
  subquery: SelectAst;
}

export interface CorrelatedInSubqueryFilter {
  negated: boolean;
  outer: {
    alias: string;
    column: string;
  };
  inner: {
    alias: string;
    column: string;
  };
  subquery: SelectAst;
}

export interface CorrelatedScalarAggregateFilter {
  outerCompare: {
    alias: string;
    column: string;
  };
  outerKey: {
    alias: string;
    column: string;
  };
  innerKey: {
    alias: string;
    column: string;
  };
  operator: string;
  subquery: SelectAst;
  correlationOutput: string;
  metricOutput: string;
}

export interface CorrelatedScalarAggregateProjection {
  outerKey: {
    alias: string;
    column: string;
  };
  innerKey: {
    alias: string;
    column: string;
  };
  subquery: SelectAst;
  correlationOutput: string;
  metricOutput: string;
}
