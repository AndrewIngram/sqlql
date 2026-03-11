import { Result, type Result as BetterResult } from "better-result";

import { validateTableConstraintRows } from "./constraints";
import {
  TuplDiagnosticError,
  TuplExecutionError,
  TuplTimeoutError,
  type RelNode,
  type TuplError,
} from "@tupl/foundation";
import {
  normalizeCapability,
  supportsFragmentExecution,
  unwrapProviderOperationResult,
  type ProviderAdapter,
  type ProviderCapabilityReport,
  type ProviderFragment,
} from "@tupl/provider-kit";
import { buildProviderFragmentForRelResult } from "@tupl/planner";
import {
  getNormalizedTableBinding,
  mapProviderRowsToLogical,
  mapProviderRowsToRelOutput,
  type QueryRow,
} from "@tupl/schema-model";

import type { QueryInput, TuplDiagnostic } from "./contracts";
import {
  buildCapabilityDiagnostics,
  makeDiagnostic,
  summarizeCapabilityReason,
  tryQueryStep,
  tryQueryStepAsync,
} from "./diagnostics";
import { isPromiseLike, resolveFallbackPolicy } from "./policy";

/**
 * Provider execution owns whole-query provider pushdown and provider-fragment session setup.
 */
export interface QueryCapabilityResolution<TContext> {
  fragment: ProviderFragment | null;
  provider: ProviderAdapter<TContext> | null;
  report: ProviderCapabilityReport | null;
  diagnostics: TuplDiagnostic[];
}

export async function withTimeoutResult<T>(
  operation: string,
  promiseFactory: () => Promise<T>,
  timeoutMs: number,
): Promise<BetterResult<T, TuplError>> {
  if (timeoutMs <= 0 || !Number.isFinite(timeoutMs)) {
    return tryQueryStepAsync(operation, promiseFactory);
  }

  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeoutPromise = new Promise<never>((_, reject) => {
    timer = setTimeout(() => {
      reject(
        new TuplTimeoutError({
          operation,
          timeoutMs,
          message: `Query timed out after ${timeoutMs}ms.`,
        }),
      );
    }, timeoutMs);
  });

  try {
    return await tryQueryStepAsync(operation, () =>
      Promise.race([promiseFactory(), timeoutPromise]),
    );
  } finally {
    if (timer) {
      clearTimeout(timer);
    }
  }
}

export async function resolveProviderCapabilityForRel<TContext>(
  input: QueryInput<TContext>,
  rel: RelNode,
): Promise<BetterResult<QueryCapabilityResolution<TContext>, TuplError>> {
  const fragmentResult = buildProviderFragmentForRelResult(rel, input.schema, input.context);
  if (Result.isError(fragmentResult)) {
    return fragmentResult;
  }

  const fragment = fragmentResult.value;
  if (!fragment) {
    return Result.ok({
      fragment: null,
      provider: null,
      report: null,
      diagnostics: [],
    });
  }

  const provider = input.providers[fragment.provider] ?? null;
  if (!provider) {
    return Result.ok({
      fragment,
      provider: null,
      report: null,
      diagnostics: [],
    });
  }

  const capabilityResult = await tryQueryStepAsync("resolve provider capability", () =>
    Promise.resolve(provider.canExecute(fragment, input.context)),
  );
  if (Result.isError(capabilityResult)) {
    return capabilityResult;
  }

  const report = normalizeCapability(capabilityResult.value);
  return Result.ok({
    fragment,
    provider,
    report,
    diagnostics: buildCapabilityDiagnostics(provider, fragment, report, input.fallbackPolicy),
  });
}

export function resolveSyncProviderCapabilityForRel<TContext>(
  input: QueryInput<TContext>,
  rel: RelNode,
): BetterResult<QueryCapabilityResolution<TContext> | null, TuplError> {
  const fragmentResult = buildProviderFragmentForRelResult(rel, input.schema, input.context);
  if (Result.isError(fragmentResult)) {
    return fragmentResult;
  }

  const fragment = fragmentResult.value;
  if (!fragment) {
    return Result.ok({
      fragment: null,
      provider: null,
      report: null,
      diagnostics: [],
    });
  }

  const provider = input.providers[fragment.provider] ?? null;
  if (!provider) {
    return Result.ok({
      fragment,
      provider: null,
      report: null,
      diagnostics: [],
    });
  }

  const capabilityResult = tryQueryStep("resolve provider capability", () =>
    provider.canExecute(fragment, input.context),
  );
  if (Result.isError(capabilityResult)) {
    return capabilityResult;
  }

  const capability = capabilityResult.value;
  if (isPromiseLike(capability)) {
    return Result.ok(null);
  }

  const report = normalizeCapability(capability);
  return Result.ok({
    fragment,
    provider,
    report,
    diagnostics: buildCapabilityDiagnostics(provider, fragment, report, input.fallbackPolicy),
  });
}

export function maybeRejectFallbackResult<TContext>(
  input: QueryInput<TContext>,
  resolution: QueryCapabilityResolution<TContext>,
): BetterResult<QueryCapabilityResolution<TContext>, TuplDiagnosticError> {
  if (!resolution.provider || !resolution.report || resolution.report.supported) {
    return Result.ok(resolution);
  }

  const policy = resolveFallbackPolicy(input.fallbackPolicy, resolution.provider.fallbackPolicy);
  const exceedsEstimatedCost =
    policy.rejectOnEstimatedCost &&
    resolution.report.estimatedCost != null &&
    Number.isFinite(policy.maxJoinExpansionRisk) &&
    resolution.report.estimatedCost > policy.maxJoinExpansionRisk;

  if (!policy.allowFallback || policy.rejectOnMissingAtom || exceedsEstimatedCost) {
    const diagnostics =
      resolution.diagnostics.length > 0
        ? resolution.diagnostics
        : [
            makeDiagnostic(
              "TUPL_ERR_FALLBACK",
              "error",
              summarizeCapabilityReason(resolution.report),
              {
                provider: resolution.provider.name,
                fragment: resolution.fragment?.kind,
                missingAtoms: resolution.report.missingAtoms,
              },
              "42000",
            ),
          ];

    return Result.err(
      new TuplDiagnosticError({
        message: summarizeCapabilityReason(resolution.report),
        diagnostics,
      }),
    );
  }

  return Result.ok(resolution);
}

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

export function resolveSyncProviderCapabilityForRelResult<TContext>(
  input: QueryInput<TContext>,
  rel: RelNode,
): BetterResult<QueryCapabilityResolution<TContext> | null, TuplError> {
  return Result.gen(function* () {
    const resolution = yield* resolveSyncProviderCapabilityForRel(input, rel);
    if (resolution) {
      yield* maybeRejectFallbackResult(input, resolution);
    }
    return Result.ok(resolution);
  });
}
