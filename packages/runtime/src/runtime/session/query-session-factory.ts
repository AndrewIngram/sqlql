import { Result, type Result as BetterResult } from "better-result";

import type { TuplError } from "@tupl/foundation";

import type { QuerySession, QuerySessionInput } from "./contracts";
import { tryQueryStep, unwrapQueryResult } from "../diagnostics";
import { createRelExecutionSession } from "./rel-execution-session";
import {
  type PreparedSession,
  resolveSessionPreparationResult,
} from "../provider/provider-session-lifecycle";

/**
 * Query-session factory owns end-to-end session preparation and chooses the concrete session kind.
 */
export function createQuerySessionResult<TContext>(
  input: QuerySessionInput<TContext>,
): BetterResult<QuerySession, TuplError> {
  const preparationResult = resolveSessionPreparationResult(input);
  if (Result.isError(preparationResult)) {
    return preparationResult;
  }

  const prepared: PreparedSession<TContext> = preparationResult.value;
  if (prepared.providerSession) {
    return Result.ok(prepared.providerSession);
  }

  return tryQueryStep("create relational execution session", () =>
    createRelExecutionSession(
      prepared.resolvedInput,
      prepared.guardrails,
      prepared.executableRel,
      prepared.diagnostics,
    ),
  );
}

export function createQuerySessionInternal<TContext>(
  input: QuerySessionInput<TContext>,
): QuerySession {
  return unwrapQueryResult(createQuerySessionResult(input));
}
