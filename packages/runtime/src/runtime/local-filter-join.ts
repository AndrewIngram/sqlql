import { Result } from "better-result";

import type { RelJoinNode, RelNode } from "@tupl/foundation";

import { evaluateRelExprResult } from "./expression-eval";
import { maybeExecuteLookupJoinResult, applyLocalHashJoin } from "./lookup-join";
import {
  executeRelNodeResult,
  type RelExecutionContext,
  type RelExecutionResult,
} from "./local-execution";
import { matchesClause, type InternalRow } from "./row-ops";

/**
 * Local filter/join execution owns in-memory filtering and join materialization once inputs are available.
 */
export async function executeFilterResult<TContext>(
  filter: Extract<RelNode, { kind: "filter" }>,
  context: RelExecutionContext<TContext>,
): Promise<RelExecutionResult> {
  const rowsResult = await executeRelNodeResult(filter.input, context);
  if (Result.isError(rowsResult)) {
    return rowsResult;
  }

  let out = [...(rowsResult.value as InternalRow[])];
  for (const clause of filter.where ?? []) {
    out = out.filter((row) => matchesClause(row, clause));
  }

  if (!filter.expr) {
    return Result.ok(out);
  }

  const filtered: InternalRow[] = [];
  for (const row of out) {
    const exprResult = evaluateRelExprResult(filter.expr, row, context.subqueryResults);
    if (Result.isError(exprResult)) {
      return exprResult;
    }
    if (exprResult.value) {
      filtered.push(row);
    }
  }

  return Result.ok(filtered);
}

export async function executeJoinResult<TContext>(
  join: RelJoinNode,
  context: RelExecutionContext<TContext>,
): Promise<RelExecutionResult> {
  const leftRowsResult = await executeRelNodeResult(join.left, context);
  if (Result.isError(leftRowsResult)) {
    return leftRowsResult;
  }

  const lookupResult = await maybeExecuteLookupJoinResult(
    join,
    leftRowsResult.value as InternalRow[],
    context,
  );
  if (Result.isError(lookupResult)) {
    return lookupResult;
  }
  if (lookupResult.value) {
    return Result.ok(lookupResult.value);
  }

  const rightRowsResult = await executeRelNodeResult(join.right, context);
  if (Result.isError(rightRowsResult)) {
    return rightRowsResult;
  }

  return Result.ok(
    applyLocalHashJoin(
      join,
      leftRowsResult.value as InternalRow[],
      rightRowsResult.value as InternalRow[],
    ),
  );
}
