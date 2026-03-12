import { Result } from "better-result";

import { TuplRuntimeError, type RelNode } from "@tupl/foundation";
import {
  supportsFragmentExecution,
  type ProviderAdapter,
  type ProviderFragment,
} from "@tupl/provider-kit";
import { type QueryRow } from "@tupl/schema-model";

import type {
  QueryExecutionPlan,
  QueryGuardrails,
  QuerySession,
  QuerySessionInput,
  QueryStepEvent,
  QueryStepState,
  TuplDiagnostic,
} from "../contracts";
import { unwrapQueryResult } from "../diagnostics";
import {
  buildProviderFragmentDoneEvent,
  runProviderFragmentOnceResult,
} from "./provider-fragment-replay";
import {
  createInitialProviderFragmentState,
  createProviderFragmentPlan,
} from "./provider-session-lifecycle";

/**
 * Provider-fragment sessions own sync provider-fragment session creation and execution.
 */
export function createProviderFragmentSession<TContext>(
  input: QuerySessionInput<TContext>,
  guardrails: QueryGuardrails,
  provider: ProviderAdapter<TContext>,
  providerName: string,
  fragment: ProviderFragment,
  rel: RelNode,
  diagnostics: TuplDiagnostic[] = [],
): QuerySession {
  if (!supportsFragmentExecution(provider)) {
    throw new TuplRuntimeError({
      operation: "create provider fragment session",
      message: `Provider ${providerName} does not support compiled fragment execution.`,
    });
  }

  let executed = false;
  let result: QueryRow[] | null = null;
  let eventDispatched = false;
  const plan: QueryExecutionPlan = createProviderFragmentPlan(providerName, fragment, diagnostics);
  let state: QueryStepState = createInitialProviderFragmentState(providerName, diagnostics);

  const runResult = async () => {
    // Provider-fragment sessions intentionally collapse remote execution into one stable step:
    // one compile/execute run updates one piece of session state and can be replayed idempotently.
    if (executed) {
      return Result.ok(result ?? []);
    }

    executed = true;
    const executedResult = await runProviderFragmentOnceResult({
      provider,
      providerName,
      fragment,
      rel,
      sessionInput: input,
      guardrails,
      state,
    });
    if (Result.isError(executedResult)) {
      state = executedResult.error.state;
      return Result.err(executedResult.error.error);
    }

    state = {
      ...executedResult.value.state,
      ...(diagnostics.length > 0 ? { diagnostics } : {}),
    };
    result = executedResult.value.rows;
    return Result.ok(result);
  };

  const run = async (): Promise<QueryRow[]> => unwrapQueryResult(await runResult());

  return {
    getPlan: () => plan,
    next: async () => {
      await run();
      // Sessions emit exactly one step event for the remote fragment, then switch to the terminal
      // `{ done: true, result }` shape used by the general query-session interface.
      if (!eventDispatched) {
        eventDispatched = true;
        const event: QueryStepEvent = buildProviderFragmentDoneEvent({
          state,
          rows: result ?? [],
          captureRows: input.options?.captureRows,
          diagnostics,
        });

        input.options?.onEvent?.(event);
        return event;
      }

      return {
        done: true as const,
        result: result ?? [],
      };
    },
    runToCompletion: async () => run(),
    getResult: () => result,
    getStepState: (id: string) => (id === "remote_fragment_1" ? state : undefined),
  };
}
