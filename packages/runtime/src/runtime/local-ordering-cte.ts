import { Result } from "better-result";

import type { RelNode } from "@tupl/foundation";

import {
  executeRelNodeResult,
  type RelExecutionContext,
  type RelExecutionResult,
} from "./local-execution";
import {
  compareNullableValues,
  dedupeRows,
  readRowValue,
  stableRowKey,
  toColumnKey,
} from "./row-ops";

/**
 * Local ordering/CTE execution owns in-memory sort, limit/offset, set-op, and WITH materialization.
 */
export async function executeSortResult<TContext>(
  sort: Extract<RelNode, { kind: "sort" }>,
  context: RelExecutionContext<TContext>,
): Promise<RelExecutionResult> {
  const rowsResult = await executeRelNodeResult(sort.input, context);
  if (Result.isError(rowsResult)) {
    return rowsResult;
  }

  const sorted = [...rowsResult.value];
  sorted.sort((left, right) => {
    for (const term of sort.orderBy) {
      const comparison = compareNullableValues(
        readRowValue(left, toColumnKey(term.source)),
        readRowValue(right, toColumnKey(term.source)),
      );
      if (comparison !== 0) {
        return term.direction === "asc" ? comparison : -comparison;
      }
    }
    return 0;
  });

  return Result.ok(sorted);
}

export async function executeLimitOffsetResult<TContext>(
  limitOffset: Extract<RelNode, { kind: "limit_offset" }>,
  context: RelExecutionContext<TContext>,
): Promise<RelExecutionResult> {
  const rowsResult = await executeRelNodeResult(limitOffset.input, context);
  if (Result.isError(rowsResult)) {
    return rowsResult;
  }

  let rows = rowsResult.value;
  if (limitOffset.offset != null) {
    rows = rows.slice(limitOffset.offset);
  }
  if (limitOffset.limit != null) {
    rows = rows.slice(0, limitOffset.limit);
  }

  return Result.ok(rows);
}

export async function executeSetOpResult<TContext>(
  setOp: Extract<RelNode, { kind: "set_op" }>,
  context: RelExecutionContext<TContext>,
): Promise<RelExecutionResult> {
  const leftRowsResult = await executeRelNodeResult(setOp.left, context);
  if (Result.isError(leftRowsResult)) {
    return leftRowsResult;
  }
  const rightRowsResult = await executeRelNodeResult(setOp.right, context);
  if (Result.isError(rightRowsResult)) {
    return rightRowsResult;
  }

  const leftRows = leftRowsResult.value;
  const rightRows = rightRowsResult.value;

  switch (setOp.op) {
    case "union_all":
      return Result.ok([...leftRows, ...rightRows]);
    case "union":
      return Result.ok(dedupeRows([...leftRows, ...rightRows]));
    case "intersect": {
      const rightKeys = new Set(rightRows.map((row) => stableRowKey(row)));
      return Result.ok(dedupeRows(leftRows.filter((row) => rightKeys.has(stableRowKey(row)))));
    }
    case "except": {
      const rightKeys = new Set(rightRows.map((row) => stableRowKey(row)));
      return Result.ok(dedupeRows(leftRows.filter((row) => !rightKeys.has(stableRowKey(row)))));
    }
  }
}

export async function executeWithResult<TContext>(
  withNode: Extract<RelNode, { kind: "with" }>,
  context: RelExecutionContext<TContext>,
): Promise<RelExecutionResult> {
  const cteRows = new Map(context.cteRows);
  const nested: RelExecutionContext<TContext> = {
    ...context,
    cteRows,
  };

  for (const cte of withNode.ctes) {
    const rowsResult = await executeRelNodeResult(cte.query, nested);
    if (Result.isError(rowsResult)) {
      return rowsResult;
    }
    cteRows.set(cte.name, rowsResult.value);
  }

  return executeRelNodeResult(withNode.body, nested);
}
