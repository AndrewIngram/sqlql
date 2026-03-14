import type { RelNode } from "@tupl/foundation";

import { nextRelId } from "../physical/planner-ids";
import type { ParsedWhereFilters } from "../planner-types";
import type { SelectAst } from "../sqlite-parser/ast";

/**
 * Correlate lowering owns explicit logical representation of supported correlated predicates.
 */
export function attachCorrelatedPredicates(
  input: RelNode,
  whereFilters: ParsedWhereFilters,
  tryLowerSelect: (ast: SelectAst) => RelNode | null,
): RelNode | null {
  let current = input;

  for (const inFilter of whereFilters.correlatedInSubqueries) {
    const subqueryRel = tryLowerSelect(inFilter.subquery);
    if (!subqueryRel || subqueryRel.output.length !== 1) {
      return null;
    }

    current = {
      id: nextRelId("correlate"),
      kind: "correlate",
      convention: "logical",
      left: current,
      right: subqueryRel,
      correlation: {
        outer: {
          alias: inFilter.outer.alias,
          column: inFilter.outer.column,
        },
        inner: {
          alias: inFilter.inner.alias,
          column: inFilter.inner.column,
        },
      },
      apply: {
        kind: inFilter.negated ? "anti" : "semi",
      },
      output: current.output,
    };
  }

  for (const existsFilter of whereFilters.existsSubqueries) {
    const subqueryRel = tryLowerSelect(existsFilter.subquery);
    if (!subqueryRel) {
      return null;
    }

    current = {
      id: nextRelId("correlate"),
      kind: "correlate",
      convention: "logical",
      left: current,
      right: subqueryRel,
      correlation: {
        outer: {
          alias: existsFilter.outer.alias,
          column: existsFilter.outer.column,
        },
        inner: {
          alias: existsFilter.inner.alias,
          column: existsFilter.inner.column,
        },
      },
      apply: {
        kind: existsFilter.negated ? "anti" : "semi",
      },
      output: current.output,
    };
  }

  for (const scalarFilter of whereFilters.correlatedScalarAggregates) {
    const subqueryRel = tryLowerSelect(scalarFilter.subquery);
    if (!subqueryRel || subqueryRel.output.length !== 2) {
      return null;
    }

    current = {
      id: nextRelId("correlate"),
      kind: "correlate",
      convention: "logical",
      left: current,
      right: subqueryRel,
      correlation: {
        outer: {
          alias: scalarFilter.outerKey.alias,
          column: scalarFilter.outerKey.column,
        },
        inner: {
          alias: scalarFilter.innerKey.alias,
          column: scalarFilter.innerKey.column,
        },
      },
      apply: {
        kind: "scalar_filter",
        comparison: scalarFilter.operator,
        outerCompare: {
          alias: scalarFilter.outerCompare.alias,
          column: scalarFilter.outerCompare.column,
        },
        correlationColumn: scalarFilter.correlationOutput,
        metricColumn: scalarFilter.metricOutput,
      },
      output: current.output,
    };
  }

  return current;
}
