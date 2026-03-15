import { Result, type Result as BetterResult } from "better-result";

import type { RelNode } from "@tupl/foundation";
import { unwrapProviderOperationResult, type FragmentProviderAdapter } from "@tupl/provider-kit";
import type { ProviderRelTarget } from "@tupl/planner";
import type { QueryRow } from "@tupl/schema-model";
import { mapProviderRowsToRelOutput } from "@tupl/schema-model/mapping";

import type { QueryGuardrails, TuplDiagnostic } from "../contracts";
import type { QuerySessionInput, QueryStepEvent, QueryStepState } from "../session/contracts";
import { tryQueryStep, tryQueryStepAsync } from "../diagnostics";
import { failProviderFragmentState } from "./provider-fragment-errors";
import { withTimeoutResult } from "./provider-execution";
import { enforceExecutionRowLimitResult } from "../policy";

export interface ProviderFragmentRunFailure {
  error: import("@tupl/foundation").TuplError;
  state: QueryStepState;
}

/**
 * Provider fragment replay owns one-shot remote fragment execution and done-event shaping.
 */
export async function runProviderFragmentOnceResult<TContext>(input: {
  provider: FragmentProviderAdapter<TContext>;
  providerName: string;
  fragment: ProviderRelTarget;
  rel: RelNode;
  sessionInput: QuerySessionInput<TContext>;
  guardrails: QueryGuardrails;
  state: QueryStepState;
}): Promise<BetterResult<{ rows: QueryRow[]; state: QueryStepState }, ProviderFragmentRunFailure>> {
  const startedAt = Date.now();
  let state: QueryStepState = {
    ...input.state,
    status: "running",
    startedAt,
  };

  const compiledResult = await tryQueryStepAsync("compile provider fragment", async () =>
    unwrapProviderOperationResult(
      await Promise.resolve(input.provider.compile(input.fragment.rel, input.sessionInput.context)),
    ),
  );
  if (Result.isError(compiledResult)) {
    return Result.err({
      error: compiledResult.error,
      state: failProviderFragmentState(state, compiledResult.error, Date.now()),
    });
  }

  const executeRowsResult = await withTimeoutResult(
    "execute provider fragment",
    async () =>
      unwrapProviderOperationResult(
        await input.provider.execute(compiledResult.value, input.sessionInput.context),
      ),
    input.guardrails.timeoutMs,
  );
  if (Result.isError(executeRowsResult)) {
    return Result.err({
      error: executeRowsResult.error,
      state: failProviderFragmentState(state, executeRowsResult.error, Date.now()),
    });
  }

  let rows: QueryRow[] = executeRowsResult.value;
  const mappedRowsResult = tryQueryStep("map provider rows to logical rel output rows", () =>
    mapProviderRowsToRelOutput(rows, input.rel, input.sessionInput.preparedSchema.schema),
  );
  if (Result.isError(mappedRowsResult)) {
    return Result.err({
      error: mappedRowsResult.error,
      state: failProviderFragmentState(state, mappedRowsResult.error, Date.now()),
    });
  }
  rows = mappedRowsResult.value;

  const limitedRowsResult = enforceExecutionRowLimitResult(rows, input.guardrails);
  if (Result.isError(limitedRowsResult)) {
    return Result.err({
      error: limitedRowsResult.error,
      state: failProviderFragmentState(state, limitedRowsResult.error, Date.now()),
    });
  }

  const endedAt = Date.now();
  state = {
    ...state,
    status: "done",
    routeUsed: "provider_fragment",
    rowCount: rows.length,
    outputRowCount: rows.length,
    startedAt,
    endedAt,
    durationMs: endedAt - startedAt,
    ...(input.sessionInput.options?.captureRows === "full" ? { rows } : {}),
  };

  return Result.ok({ rows, state });
}

export function buildProviderFragmentDoneEvent(input: {
  state: QueryStepState;
  rows: QueryRow[];
  captureRows: "full" | undefined;
  diagnostics: TuplDiagnostic[];
}): QueryStepEvent {
  return {
    id: input.state.id,
    kind: "remote_fragment",
    status: "done",
    summary: input.state.summary,
    dependsOn: [],
    executionIndex: 1,
    startedAt: input.state.startedAt ?? Date.now(),
    endedAt: input.state.endedAt ?? Date.now(),
    durationMs: input.state.durationMs ?? 0,
    routeUsed: "provider_fragment",
    ...(input.state.rowCount != null ? { rowCount: input.state.rowCount } : {}),
    ...(input.state.outputRowCount != null ? { outputRowCount: input.state.outputRowCount } : {}),
    ...(input.captureRows === "full" ? { rows: input.rows } : {}),
    ...(input.diagnostics.length > 0 ? { diagnostics: input.diagnostics } : {}),
  };
}
