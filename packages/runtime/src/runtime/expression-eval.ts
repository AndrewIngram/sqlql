import { Result, type Result as BetterResult } from "better-result";

import { TuplExecutionError, type RelExpr } from "@tupl/foundation";

import { evaluateScalarFunctionResult } from "./execution/expression-scalar-functions";
import { readRowValue, toColumnKey, type InternalRow } from "./row-ops";
export { evaluateAggregateMetricResult } from "./execution/aggregate-metric-eval";

/**
 * Expression eval owns recursive local relational expression execution and delegates scalar/aggregate semantics.
 */
export function evaluateRelExprResult(
  expr: RelExpr,
  row: InternalRow,
  subqueryResults: Map<string, unknown>,
): BetterResult<unknown, TuplExecutionError> {
  switch (expr.kind) {
    case "literal":
      return Result.ok(expr.value);
    case "column":
      return Result.ok(readRowValue(row, toColumnKey(expr.ref)) ?? null);
    case "subquery":
      return Result.ok(subqueryResults.get(expr.id) ?? null);
    case "function": {
      const args: unknown[] = [];
      for (const arg of expr.args) {
        const argResult = evaluateRelExprResult(arg, row, subqueryResults);
        if (Result.isError(argResult)) {
          return argResult;
        }
        args.push(argResult.value);
      }

      return evaluateScalarFunctionResult(expr.name, args);
    }
  }
}
