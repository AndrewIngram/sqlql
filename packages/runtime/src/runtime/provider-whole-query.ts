import { Result, type Result as BetterResult } from "better-result";

import { TuplExecutionError, type RelNode, type TuplError } from "@tupl/foundation";
import { supportsFragmentExecution, unwrapProviderOperationResult } from "@tupl/provider-kit";
import {
  getNormalizedTableBinding,
  mapProviderRowsToLogical,
  mapProviderRowsToRelOutput,
  type QueryRow,
} from "@tupl/schema-model";

import type { QueryInput } from "./contracts";
import { validateTableConstraintRows } from "./constraints";
import { maybeRejectFallbackResult } from "./provider-fallback";
import { resolveProviderCapabilityForRel } from "./provider-capability";

/**
 * Provider whole-query execution owns provider fragment compile/execute flows for fully pushdownable queries.
 */
export async function maybeExecuteWholeQueryFragmentResult<TContext>(
  input: QueryInput<TContext>,
  rel: RelNode,
): Promise<BetterResult<QueryRow[] | null, TuplError>> {
  const resolutionResult = await resolveProviderCapabilityForRel(input, rel);
  if (Result.isError(resolutionResult)) {
    return resolutionResult;
  }

  const resolution = resolutionResult.value;
  if (!resolution.fragment || !resolution.provider || !resolution.report) {
    return Result.ok(null);
  }

  if (!resolution.report.supported) {
    const fallbackResult = maybeRejectFallbackResult(input, resolution);
    if (Result.isError(fallbackResult)) {
      return fallbackResult;
    }

    return Result.ok(null);
  }

  if (!supportsFragmentExecution(resolution.provider)) {
    return Result.err(
      new TuplExecutionError({
        operation: "execute provider fragment",
        message: `Provider ${resolution.fragment.provider} does not support compiled fragment execution.`,
      }),
    );
  }

  const compiled = unwrapProviderOperationResult(
    await resolution.provider.compile(resolution.fragment, input.context),
  );
  const executed = await resolution.provider.execute(compiled, input.context);
  const rows = unwrapProviderOperationResult(executed);

  if (resolution.fragment.kind === "rel") {
    return Result.ok(mapProviderRowsToRelOutput(rows, rel, input.schema));
  }

  if (resolution.fragment.kind === "scan" && rel.kind === "scan") {
    const binding = getNormalizedTableBinding(input.schema, rel.table);
    const mappedRows = mapProviderRowsToLogical(
      rows,
      rel.select,
      binding?.kind === "physical" ? binding : null,
      input.schema.tables[rel.table],
      {
        enforceNotNull: !input.constraintValidation || input.constraintValidation.mode === "off",
        enforceEnum: !input.constraintValidation || input.constraintValidation.mode === "off",
      },
    );
    validateTableConstraintRows({
      schema: input.schema,
      tableName: rel.table,
      rows: mappedRows,
      ...(input.constraintValidation ? { options: input.constraintValidation } : {}),
    });
    return Result.ok(mappedRows);
  }

  return Result.ok(rows);
}
