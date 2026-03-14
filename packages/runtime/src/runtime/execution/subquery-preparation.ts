import { Result } from "better-result";

import type { RelExpr, RelNode } from "@tupl/foundation";

import { executeRelNodeResult, type RelExecutionContext } from "./local-execution";
import type { TuplExecutionError, TuplGuardrailError, TuplPlanningError } from "@tupl/foundation";
import type { Result as BetterResult } from "better-result";

/**
 * Subquery preparation owns pre-execution and memoization of scalar and EXISTS subqueries.
 */
export async function prepareSubqueryResultsResult<TContext>(
  node: RelNode,
  context: RelExecutionContext<TContext>,
): Promise<BetterResult<void, TuplPlanningError | TuplExecutionError | TuplGuardrailError>> {
  const visited = new Set<string>();

  const prepareExpr = async (
    expr: RelExpr,
  ): Promise<BetterResult<void, TuplPlanningError | TuplExecutionError | TuplGuardrailError>> => {
    switch (expr.kind) {
      case "literal":
      case "column":
        return Result.ok(undefined);
      case "function":
        for (const arg of expr.args) {
          const argResult = await prepareExpr(arg);
          if (Result.isError(argResult)) {
            return argResult;
          }
        }
        return Result.ok(undefined);
      case "subquery": {
        if (visited.has(expr.id)) {
          return Result.ok(undefined);
        }
        visited.add(expr.id);

        const nestedResult = await prepareSubqueryResultsResult(expr.rel, context);
        if (Result.isError(nestedResult)) {
          return nestedResult;
        }

        const rowsResult = await executeRelNodeResult(expr.rel, context);
        if (Result.isError(rowsResult)) {
          return rowsResult;
        }

        const rows = rowsResult.value;
        const value =
          expr.mode === "exists"
            ? rows.length > 0
            : rows.length === 0
              ? null
              : (rows[0]?.[expr.outputColumn ?? ""] ?? null);
        context.subqueryResults.set(expr.id, value);
        return Result.ok(undefined);
      }
    }
  };

  switch (node.kind) {
    case "scan":
    case "values":
    case "sql":
      return Result.ok(undefined);
    case "filter": {
      const inputResult = await prepareSubqueryResultsResult(node.input, context);
      if (Result.isError(inputResult)) {
        return inputResult;
      }
      return node.expr ? prepareExpr(node.expr) : Result.ok(undefined);
    }
    case "project": {
      const inputResult = await prepareSubqueryResultsResult(node.input, context);
      if (Result.isError(inputResult)) {
        return inputResult;
      }
      for (const column of node.columns) {
        if (!("expr" in column)) {
          continue;
        }
        const exprResult = await prepareExpr(column.expr);
        if (Result.isError(exprResult)) {
          return exprResult;
        }
      }
      return Result.ok(undefined);
    }
    case "aggregate":
    case "window":
    case "sort":
    case "limit_offset":
      return prepareSubqueryResultsResult(node.input, context);
    case "join": {
      const leftResult = await prepareSubqueryResultsResult(node.left, context);
      if (Result.isError(leftResult)) {
        return leftResult;
      }
      return prepareSubqueryResultsResult(node.right, context);
    }
    case "set_op": {
      const leftResult = await prepareSubqueryResultsResult(node.left, context);
      if (Result.isError(leftResult)) {
        return leftResult;
      }
      return prepareSubqueryResultsResult(node.right, context);
    }
    case "with": {
      for (const cte of node.ctes) {
        const cteResult = await prepareSubqueryResultsResult(cte.query, context);
        if (Result.isError(cteResult)) {
          return cteResult;
        }
      }
      return prepareSubqueryResultsResult(node.body, context);
    }
    case "repeat_union": {
      const seedResult = await prepareSubqueryResultsResult(node.seed, context);
      if (Result.isError(seedResult)) {
        return seedResult;
      }
      return prepareSubqueryResultsResult(node.iterative, context);
    }
  }
}
