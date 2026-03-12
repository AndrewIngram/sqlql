import { Result, type Result as BetterResult } from "better-result";

import type { ConstraintValidationOptions } from "../constraints";
import {
  executeAggregateResult,
  executeFilterResult,
  executeJoinResult,
  executeLimitOffsetResult,
  executeProjectResult,
  executeSetOpResult,
  executeSortResult,
  executeWithResult,
} from "./local-operators";
import { tryExecuteRemoteSubtreeResult } from "./remote-subtree";
import { executeScanResult } from "./scan-execution";
import { prepareSubqueryResultsResult } from "./subquery-preparation";
import { executeWindowResult } from "./window-execution";
import type { InternalRow } from "./row-ops";
import {
  TuplExecutionError,
  TuplGuardrailError,
  type RelNode,
  type TuplPlanningError,
} from "@tupl/foundation";
import type { ProvidersMap } from "@tupl/provider-kit";
import type { QueryRow, SchemaDefinition } from "@tupl/schema-model";

export interface RelExecutionGuardrails {
  maxExecutionRows: number;
  maxLookupKeysPerBatch: number;
  maxLookupBatches: number;
}

export interface RelExecutionContext<TContext> {
  schema: SchemaDefinition;
  providers: ProvidersMap<TContext>;
  context: TContext;
  guardrails: RelExecutionGuardrails;
  constraintValidation?: ConstraintValidationOptions;
  lookupBatches: number;
  cteRows: Map<string, QueryRow[]>;
  subqueryResults: Map<string, unknown>;
}

export type RelExecutionResult = BetterResult<
  QueryRow[] | InternalRow[],
  TuplPlanningError | TuplExecutionError | TuplGuardrailError
>;

export type RemoteExecutionResult = BetterResult<
  QueryRow[] | null,
  TuplPlanningError | TuplExecutionError | TuplGuardrailError
>;

function toTuplExecutionError(error: unknown, operation: string) {
  if (TuplExecutionError.is(error) || TuplGuardrailError.is(error)) {
    return error;
  }

  return new TuplExecutionError({
    operation,
    message: error instanceof Error ? error.message : String(error),
    cause: error,
  });
}

export function tryExecutionStep<T>(operation: string, fn: () => T) {
  return Result.try({
    try: () => fn() as Awaited<T>,
    catch: (error) => toTuplExecutionError(error, operation),
  });
}

export async function tryExecutionStepAsync<T>(operation: string, fn: () => Promise<T>) {
  return Result.tryPromise({
    try: fn,
    catch: (error) => toTuplExecutionError(error, operation),
  });
}

export async function executeRelLocallyResult<TContext>(
  rel: RelNode,
  executionContext: RelExecutionContext<TContext>,
): Promise<BetterResult<QueryRow[], TuplPlanningError | TuplExecutionError | TuplGuardrailError>> {
  // Scalar and EXISTS subqueries are prepared once up front so downstream node execution can treat
  // them as memoized inputs instead of recursively re-running subtrees at each expression use site.
  const subqueryPrepResult = await prepareSubqueryResultsResult(rel, executionContext);
  if (Result.isError(subqueryPrepResult)) {
    return subqueryPrepResult;
  }

  const rowsResult = await executeRelNodeResult(rel, executionContext);
  if (Result.isError(rowsResult)) {
    return rowsResult;
  }

  const rows = rowsResult.value as QueryRow[];
  if (rows.length > executionContext.guardrails.maxExecutionRows) {
    return Result.err(
      new TuplGuardrailError({
        guardrail: "maxExecutionRows",
        limit: executionContext.guardrails.maxExecutionRows,
        actual: rows.length,
        message: `Query exceeded maxExecutionRows guardrail (${executionContext.guardrails.maxExecutionRows}). Received ${rows.length} rows.`,
      }),
    );
  }

  return Result.ok(rows);
}

export async function executeRelNodeResult<TContext>(
  node: RelNode,
  context: RelExecutionContext<TContext>,
): Promise<RelExecutionResult> {
  // Each subtree gets a remote-execution chance first so provider pushdown can absorb work even
  // after higher-level orchestration has decided the full query cannot stay remote end to end.
  const remoteRowsResult = await tryExecuteRemoteSubtreeResult(node, context);
  if (Result.isError(remoteRowsResult)) {
    return remoteRowsResult;
  }
  if (remoteRowsResult.value) {
    return Result.ok(remoteRowsResult.value);
  }

  switch (node.kind) {
    case "scan":
      return executeScanResult(node, context);
    case "join":
      return executeJoinResult(node, context);
    case "filter":
      return executeFilterResult(node, context);
    case "project":
      return executeProjectResult(node, context);
    case "aggregate":
      return executeAggregateResult(node, context);
    case "window":
      return executeWindowResult(node, context);
    case "sort":
      return executeSortResult(node, context);
    case "limit_offset":
      return executeLimitOffsetResult(node, context);
    case "set_op":
      return executeSetOpResult(node, context);
    case "with":
      return executeWithResult(node, context);
    case "sql":
      return Result.err(
        new TuplExecutionError({
          operation: "execute relational node",
          message: "SQL-shaped rel nodes are not executable in the current provider runtime.",
        }),
      );
  }
}
