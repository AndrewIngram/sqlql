import type { RelExpr } from "@tupl/foundation";

import type { Binding, InSubqueryFilter, LiteralFilter, ParsedWhereFilters } from "./planner-types";
import type { SqlExprLoweringContext } from "./sql-expr-lowering";
import { lowerSqlAstToRelExpr } from "./sql-expr-lowering";
import { flattenConjunctiveWhere, parseLiteralFilter } from "./expr/literal-filter-parser";
import {
  parseSupportedCorrelatedExistsSubquery,
  parseSupportedCorrelatedInSubquery,
  parseSupportedCorrelatedScalarAggregateSubquery,
} from "./subqueries/analysis";
import { literalFilterToRelExpr } from "./expr/literal-filter-operators";
import type {
  CorrelatedExistsFilter,
  CorrelatedInSubqueryFilter,
  CorrelatedScalarAggregateFilter,
} from "./subqueries/correlated-predicate-types";

/**
 * Literal filter lowering owns pushdown-friendly predicate extraction and residual expr fallback.
 */
export function parseWhereFilters(
  where: unknown,
  bindings: Binding[],
  aliasToBinding: Map<string, Binding>,
  lowerExprContext: SqlExprLoweringContext,
): ParsedWhereFilters | null {
  if (!where) {
    return {
      literals: [],
      inSubqueries: [],
      existsSubqueries: [],
      correlatedInSubqueries: [],
      correlatedScalarAggregates: [],
    };
  }

  const parts = flattenConjunctiveWhere(where);
  if (parts == null) {
    const residualExpr = lowerSqlAstToRelExpr(where, bindings, aliasToBinding, lowerExprContext);
    if (!residualExpr) {
      return null;
    }
    return {
      literals: [],
      inSubqueries: [],
      existsSubqueries: [],
      correlatedInSubqueries: [],
      correlatedScalarAggregates: [],
      residualExpr,
    };
  }

  const literals: LiteralFilter[] = [];
  const inSubqueries: InSubqueryFilter[] = [];
  const existsSubqueries: CorrelatedExistsFilter[] = [];
  const correlatedInSubqueries: CorrelatedInSubqueryFilter[] = [];
  const correlatedScalarAggregates: CorrelatedScalarAggregateFilter[] = [];
  const residualParts: RelExpr[] = [];
  const outerAliases = new Set(bindings.map((binding) => binding.alias));
  for (const part of parts) {
    const correlatedExists = parseSupportedCorrelatedExistsSubquery(part, outerAliases);
    if (correlatedExists) {
      existsSubqueries.push({
        negated: correlatedExists.negated,
        outer: correlatedExists.outer,
        inner: correlatedExists.inner,
        subquery: correlatedExists.rewrittenSubquery,
      });
      continue;
    }

    const correlatedIn = parseSupportedCorrelatedInSubquery(part, outerAliases);
    if (correlatedIn) {
      correlatedInSubqueries.push({
        negated: correlatedIn.negated,
        outer: correlatedIn.outer,
        inner: correlatedIn.inner,
        subquery: correlatedIn.rewrittenSubquery,
      });
      continue;
    }

    const correlatedScalarAggregate = parseSupportedCorrelatedScalarAggregateSubquery(
      part,
      outerAliases,
    );
    if (correlatedScalarAggregate) {
      correlatedScalarAggregates.push({
        outerCompare: correlatedScalarAggregate.outerCompare,
        outerKey: correlatedScalarAggregate.outerKey,
        innerKey: correlatedScalarAggregate.innerKey,
        operator: correlatedScalarAggregate.operator,
        subquery: correlatedScalarAggregate.rewrittenSubquery,
        correlationOutput: correlatedScalarAggregate.correlationOutput,
        metricOutput: correlatedScalarAggregate.metricOutput,
      });
      continue;
    }

    const parsed = parseLiteralFilter(part, bindings, aliasToBinding);
    if (!parsed) {
      const residual = lowerSqlAstToRelExpr(part, bindings, aliasToBinding, lowerExprContext);
      if (!residual) {
        return null;
      }
      residualParts.push(residual);
      continue;
    }
    if ("subquery" in parsed) {
      inSubqueries.push(parsed);
      continue;
    }
    literals.push(parsed);
  }

  const residualExpr = residualParts.reduce<RelExpr | null>(
    (acc, current) =>
      acc
        ? {
            kind: "function",
            name: "and",
            args: [acc, current],
          }
        : current,
    null,
  );

  return residualExpr
    ? {
        literals,
        inSubqueries,
        existsSubqueries,
        correlatedInSubqueries,
        correlatedScalarAggregates,
        residualExpr,
      }
    : {
        literals,
        inSubqueries,
        existsSubqueries,
        correlatedInSubqueries,
        correlatedScalarAggregates,
      };
}

export { literalFilterToRelExpr } from "./expr/literal-filter-operators";
