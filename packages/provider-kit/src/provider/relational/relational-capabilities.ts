import type { RelNode } from "@tupl/foundation";
import type { ProviderCapabilityReport } from "../capabilities";
import type { MaybePromise } from "../operations";
import type {
  RelationalProviderAdapterOptions,
  RelationalProviderCapabilityContext,
  RelationalProviderEntityConfig,
} from "./relational-adapter-types";

export function canExecuteRelationalFragment<
  TContext,
  TEntities extends Record<string, RelationalProviderEntityConfig>,
  TStrategy extends string,
>(
  options: RelationalProviderAdapterOptions<TContext, TEntities, TStrategy>,
  rel: RelNode,
  context: TContext,
): MaybePromise<boolean | ProviderCapabilityReport> {
  return evaluateRelationalCapability(options, rel, context);
}

function isPromiseLike<T>(value: unknown): value is PromiseLike<T> {
  return (
    (typeof value === "object" || typeof value === "function") &&
    value !== null &&
    typeof (value as { then?: unknown }).then === "function"
  );
}

export async function resolveRelationalCapabilityContext<
  TContext,
  TEntities extends Record<string, RelationalProviderEntityConfig>,
  TStrategy extends string,
>(
  options: RelationalProviderAdapterOptions<TContext, TEntities, TStrategy>,
  rel: RelNode,
  context: TContext,
): Promise<RelationalProviderCapabilityContext<TContext, TEntities, TStrategy>> {
  const strategy = await options.resolveRelCompileStrategy({
    context,
    entities: options.entities,
    rel,
  });
  const capabilityContext: RelationalProviderCapabilityContext<TContext, TEntities, TStrategy> = {
    context,
    entities: options.entities,
    rel,
    strategy,
  };

  return capabilityContext;
}

function evaluateRelationalCapability<
  TContext,
  TEntities extends Record<string, RelationalProviderEntityConfig>,
  TStrategy extends string,
>(
  options: RelationalProviderAdapterOptions<TContext, TEntities, TStrategy>,
  rel: RelNode,
  context: TContext,
): MaybePromise<boolean | ProviderCapabilityReport> {
  const strategy = options.resolveRelCompileStrategy({
    context,
    entities: options.entities,
    rel,
  });
  if (isPromiseLike<TStrategy | null>(strategy)) {
    return strategy.then((resolvedStrategy) =>
      evaluateRelationalCapabilityWithContext(options, {
        context,
        entities: options.entities,
        rel,
        strategy: resolvedStrategy,
      }),
    );
  }

  return evaluateRelationalCapabilityWithContext(options, {
    context,
    entities: options.entities,
    rel,
    strategy,
  });
}

function evaluateRelationalCapabilityWithContext<
  TContext,
  TEntities extends Record<string, RelationalProviderEntityConfig>,
  TStrategy extends string,
>(
  options: RelationalProviderAdapterOptions<TContext, TEntities, TStrategy>,
  capabilityContext: RelationalProviderCapabilityContext<TContext, TEntities, TStrategy>,
): MaybePromise<boolean | ProviderCapabilityReport> {
  if (!capabilityContext.strategy) {
    const unsupported =
      options.unsupportedRelReason?.(capabilityContext) ??
      options.unsupportedRelReasonMessage ??
      "Rel fragment is not supported for this provider.";
    if (typeof unsupported !== "string") {
      return unsupported;
    }
    return {
      supported: false,
      reason: unsupported,
    };
  }

  const support = options.isRelStrategySupported?.(capabilityContext);
  if (isPromiseLike<true | string | ProviderCapabilityReport>(support)) {
    return support.then((resolvedSupport) =>
      normalizeCapabilitySupport(capabilityContext, resolvedSupport),
    );
  }
  return normalizeCapabilitySupport(capabilityContext, support);
}

function normalizeCapabilitySupport<
  TContext,
  TEntities extends Record<string, RelationalProviderEntityConfig>,
  TStrategy extends string,
>(
  capabilityContext: RelationalProviderCapabilityContext<TContext, TEntities, TStrategy>,
  support: true | string | ProviderCapabilityReport | undefined,
): boolean | ProviderCapabilityReport {
  if (support === undefined || support === true) {
    return true;
  }
  if (typeof support === "string") {
    return {
      supported: false,
      reason: support,
    };
  }
  return support;
}
