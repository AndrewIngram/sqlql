import { Result, type Result as BetterResult } from "better-result";

import { type TuplError, TuplRuntimeError } from "@tupl/foundation";
import { buildProviderFragmentForRelResult } from "@tupl/planner";
import { normalizeCapability } from "@tupl/provider-kit";

import type { ExplainFragment, ExplainProviderPlan, QueryInput } from "../contracts";
import { toTuplRuntimeError } from "../diagnostics";

export type ExplainProviderDescriptionMode = "basic" | "enriched";

/**
 * Provider plan descriptions own the provider-facing seam used by explain(). Basic mode never
 * compiles provider plans; enriched mode may compile supported provider fragments for richer
 * backend-native descriptions.
 */
export async function describeExplainProviderPlansResult<TContext>(
  input: QueryInput<TContext>,
  fragments: ExplainFragment[],
  mode: ExplainProviderDescriptionMode,
): Promise<BetterResult<ExplainProviderPlan[], TuplError>> {
  return Result.gen(async function* () {
    const providerPlans: ExplainProviderPlan[] = [];

    for (const fragment of fragments) {
      if (!fragment.provider) {
        continue;
      }

      const adapter = input.providers[fragment.provider];
      if (!adapter) {
        return Result.err(
          new TuplRuntimeError({
            operation: "describe explain provider plans",
            message: `Missing provider adapter: ${fragment.provider}`,
          }),
        );
      }

      const providerFragment = yield* buildProviderFragmentForRelResult(
        fragment.rel,
        input.schema,
        input.context,
      );
      if (!providerFragment) {
        continue;
      }

      const capabilityResult = yield* Result.await(
        Result.tryPromise({
          try: () => Promise.resolve(adapter.canExecute(providerFragment.rel, input.context)),
          catch: (error) => toTuplRuntimeError(error, "describe explain provider plans"),
        }),
      );
      const capability = normalizeCapability(capabilityResult);
      if (!capability.supported) {
        providerPlans.push({
          fragmentId: fragment.id,
          provider: fragment.provider,
          kind: "unsupported_fragment",
          rel: fragment.rel,
          descriptionUnavailable: true as const,
        });
        continue;
      }

      if (mode === "basic") {
        providerPlans.push({
          fragmentId: fragment.id,
          provider: fragment.provider,
          kind: "rel_fragment",
          rel: fragment.rel,
          descriptionUnavailable: true as const,
        });
        continue;
      }

      const compileResult = yield* Result.await(
        Result.tryPromise({
          try: () => Promise.resolve(adapter.compile(providerFragment.rel, input.context)),
          catch: (error) => toTuplRuntimeError(error, "compile explain provider plan"),
        }),
      );
      const compiledPlan = Result.isOk(compileResult)
        ? compileResult.value
        : yield* Result.err(
            toTuplRuntimeError(compileResult.error, "compile explain provider plan"),
          );
      const description =
        typeof adapter.describeCompiledPlan === "function"
          ? yield* Result.await(
              Result.tryPromise({
                try: () =>
                  Promise.resolve(adapter.describeCompiledPlan?.(compiledPlan, input.context)),
                catch: (error) =>
                  toTuplRuntimeError(error, "describe compiled explain provider plan"),
              }),
            )
          : undefined;

      providerPlans.push({
        fragmentId: fragment.id,
        provider: fragment.provider,
        kind: compiledPlan.kind,
        rel: fragment.rel,
        ...(description ? { description } : { descriptionUnavailable: true as const }),
      });
    }

    return Result.ok(providerPlans);
  });
}
