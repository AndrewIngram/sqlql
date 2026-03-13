import { Result } from "better-result";

import type { RelNode } from "@tupl/foundation";

import { evaluateAggregateMetricResult } from "./expression-eval";
import {
  executeRelNodeResult,
  type RelExecutionContext,
  type RelExecutionResult,
} from "./local-execution";
import { compareNullableValues, readRowValue, toColumnKey, type InternalRow } from "./row-ops";

/**
 * Window execution owns local window-function evaluation and partition ordering semantics.
 */
export async function executeWindowResult<TContext>(
  windowNode: Extract<RelNode, { kind: "window" }>,
  context: RelExecutionContext<TContext>,
): Promise<RelExecutionResult> {
  const rowsResult = await executeRelNodeResult(windowNode.input, context);
  if (Result.isError(rowsResult)) {
    return rowsResult;
  }

  const rows = rowsResult.value as InternalRow[];
  if (windowNode.functions.length === 0) {
    return Result.ok(rows);
  }

  let current = rows.map((row) => ({ ...row }));
  for (const fn of windowNode.functions) {
    current = applyWindowFunction(current, fn);
  }
  return Result.ok(current);
}

function applyWindowFunction(
  rows: InternalRow[],
  fn: Extract<RelNode, { kind: "window" }>["functions"][number],
): InternalRow[] {
  const partitioned = new Map<string, Array<{ row: InternalRow; index: number }>>();
  for (let index = 0; index < rows.length; index += 1) {
    const row = rows[index];
    if (!row) {
      continue;
    }
    const key = JSON.stringify(
      fn.partitionBy.map((ref) => readRowValue(row, toColumnKey(ref)) ?? null),
    );
    const bucket = partitioned.get(key) ?? [];
    bucket.push({ row, index });
    partitioned.set(key, bucket);
  }

  const out = rows.map((row) => ({ ...row }));

  for (const entries of partitioned.values()) {
    entries.sort((left, right) => compareWindowEntries(left.row, right.row, fn.orderBy));

    let denseRank = 0;
    let rank = 1;

    for (let idx = 0; idx < entries.length; idx += 1) {
      const entry = entries[idx];
      if (!entry) {
        continue;
      }
      const row = out[entry.index];
      if (!row) {
        continue;
      }

      if (fn.fn === "row_number") {
        row[fn.as] = idx + 1;
        continue;
      }

      if (fn.fn === "rank" || fn.fn === "dense_rank") {
        const prev = idx > 0 ? entries[idx - 1] : undefined;
        const isPeer = prev ? compareWindowEntries(prev.row, entry.row, fn.orderBy) === 0 : false;
        if (!isPeer) {
          denseRank += 1;
          rank = idx + 1;
        }

        row[fn.as] = fn.fn === "dense_rank" ? denseRank : rank;
        continue;
      }

      const aggregateFn = fn as Extract<typeof fn, { fn: "count" | "sum" | "avg" | "min" | "max" }>;
      const frameEntries = resolveFrameEntries(entries, idx, aggregateFn);
      const values = aggregateFn.column
        ? frameEntries.map(
            (current) => readRowValue(current.row, toColumnKey(aggregateFn.column!)) ?? null,
          )
        : frameEntries.map(() => 1);
      const metricValues = aggregateFn.distinct
        ? [...new Map(values.map((value) => [JSON.stringify(value), value])).values()]
        : values;

      const metricResult = evaluateAggregateMetricResult(
        aggregateFn.fn,
        metricValues,
        frameEntries.length,
        aggregateFn.column != null,
      );
      if (Result.isError(metricResult)) {
        throw metricResult.error;
      }
      row[fn.as] = metricResult.value;
    }
  }

  return out;
}

function resolveFrameEntries(
  entries: Array<{ row: InternalRow; index: number }>,
  idx: number,
  fn: Extract<RelNode, { kind: "window" }>["functions"][number],
) {
  const frame = fn.frame;
  if (!frame) {
    return fn.orderBy.length > 0 ? entries.slice(0, idx + 1) : entries;
  }

  const startIndex = frame.start === "current_row" ? idx : 0;
  const endIndex =
    frame.end === "current_row"
      ? idx + 1
      : frame.end === "unbounded_following"
        ? entries.length
        : idx + 1;
  return entries.slice(startIndex, endIndex);
}

function compareWindowEntries(
  left: InternalRow,
  right: InternalRow,
  orderBy: Extract<RelNode, { kind: "window" }>["functions"][number]["orderBy"],
): number {
  for (const term of orderBy) {
    const comparison = compareNullableValues(
      readRowValue(left, toColumnKey(term.source)),
      readRowValue(right, toColumnKey(term.source)),
    );
    if (comparison !== 0) {
      return term.direction === "asc" ? comparison : -comparison;
    }
  }
  return 0;
}
