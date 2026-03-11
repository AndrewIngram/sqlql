import { Result } from "better-result";

import { isRelProjectColumnMapping, type RelNode, type RelProjectNode } from "@tupl/foundation";
import type { QueryRow } from "@tupl/schema-model";

import { evaluateAggregateMetricResult, evaluateRelExprResult } from "./expression-eval";
import {
  executeRelNodeResult,
  type RelExecutionContext,
  type RelExecutionResult,
} from "./local-execution";
import { readRowValue, toColumnKey, type InternalRow } from "./row-ops";

/**
 * Local projection/aggregation owns in-memory project and aggregate operators over materialized rows.
 */
export async function executeProjectResult<TContext>(
  project: RelProjectNode,
  context: RelExecutionContext<TContext>,
): Promise<RelExecutionResult> {
  const rowsResult = await executeRelNodeResult(project.input, context);
  if (Result.isError(rowsResult)) {
    return rowsResult;
  }

  const out: QueryRow[] = [];
  for (const row of rowsResult.value as InternalRow[]) {
    const projected: QueryRow = {};
    for (const mapping of project.columns) {
      if (isRelProjectColumnMapping(mapping)) {
        projected[mapping.output] = readRowValue(row, toColumnKey(mapping.source)) ?? null;
        continue;
      }

      const exprResult = evaluateRelExprResult(mapping.expr, row, context.subqueryResults);
      if (Result.isError(exprResult)) {
        return exprResult;
      }
      projected[mapping.output] = exprResult.value;
    }
    out.push(projected);
  }

  return Result.ok(out);
}

export async function executeAggregateResult<TContext>(
  aggregate: Extract<RelNode, { kind: "aggregate" }>,
  context: RelExecutionContext<TContext>,
): Promise<RelExecutionResult> {
  const rowsResult = await executeRelNodeResult(aggregate.input, context);
  if (Result.isError(rowsResult)) {
    return rowsResult;
  }

  const rows = rowsResult.value as InternalRow[];
  const groups = new Map<string, InternalRow[]>();

  for (const row of rows) {
    const key = JSON.stringify(
      aggregate.groupBy.map((ref) => readRowValue(row, toColumnKey(ref)) ?? null),
    );
    const bucket = groups.get(key) ?? [];
    bucket.push(row);
    groups.set(key, bucket);
  }

  if (groups.size === 0 && aggregate.groupBy.length === 0) {
    groups.set("__all__", []);
  }

  const out: QueryRow[] = [];
  for (const [groupKey, bucket] of groups.entries()) {
    const row: QueryRow = {};

    if (aggregate.groupBy.length > 0) {
      const values = JSON.parse(groupKey) as unknown[];
      aggregate.groupBy.forEach((ref, index) => {
        const outputName = aggregate.output[index]?.name ?? ref.column;
        row[outputName] = values[index] ?? null;
      });
    }

    for (const metric of aggregate.metrics) {
      const values = metric.column
        ? bucket.map((entry) => readRowValue(entry, toColumnKey(metric.column!)) ?? null)
        : bucket.map(() => 1);
      const metricValues = metric.distinct
        ? [...new Map(values.map((value) => [JSON.stringify(value), value])).values()]
        : values;

      const metricResult = evaluateAggregateMetricResult(
        metric.fn,
        metricValues,
        bucket.length,
        metric.column != null,
      );
      if (Result.isError(metricResult)) {
        return metricResult;
      }
      row[metric.as] = metricResult.value;
    }

    out.push(row);
  }

  return Result.ok(out);
}
