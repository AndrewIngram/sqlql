import { Result } from "better-result";

import type { RelNode } from "@tupl/foundation";

import type { RelExecutionResult } from "./local-execution";

/**
 * Values execution owns in-memory literal row materialization for SELECT cores without backing scans.
 */
export function executeValuesResult(
  values: Extract<RelNode, { kind: "values" }>,
): RelExecutionResult {
  return Result.ok(
    values.rows.map((row) =>
      Object.fromEntries(values.output.map((column, index) => [column.name, row[index] ?? null])),
    ),
  );
}
