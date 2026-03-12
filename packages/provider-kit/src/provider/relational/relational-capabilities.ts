import { relContainsSqlNode } from "@tupl/foundation";

import {
  collectCapabilityAtomsForFragment,
  inferRouteFamilyForFragment,
  type ProviderCapabilityReport,
} from "../capabilities";
import type { ProviderFragment } from "../contracts";
import type { MaybePromise } from "../operations";
import type {
  RelationalProviderCapabilityContext,
  RelationalProviderEntityConfig,
  RelationalProviderOptions,
} from "./relational-adapter-types";
import { DEFAULT_RELATIONAL_CAPABILITY_ATOMS } from "./relational-adapter-types";

export function canExecuteRelationalFragment<
  TContext,
  TEntities extends Record<string, RelationalProviderEntityConfig>,
  TStrategy extends string,
>(
  options: RelationalProviderOptions<TContext, TEntities, TStrategy>,
  fragment: ProviderFragment,
  context: TContext,
): MaybePromise<boolean | ProviderCapabilityReport> {
  switch (fragment.kind) {
    case "scan":
      return Object.hasOwn(options.entities, fragment.table);
    case "rel":
      return evaluateRelationalCapability(options, fragment, context);
    default:
      return false;
  }
}

function isPromiseLike<T>(value: unknown): value is PromiseLike<T> {
  return (
    (typeof value === "object" || typeof value === "function") &&
    value !== null &&
    typeof (value as { then?: unknown }).then === "function"
  );
}

function getDeclaredAtoms<
  TContext,
  TEntities extends Record<string, RelationalProviderEntityConfig>,
  TStrategy extends string,
>(options: RelationalProviderOptions<TContext, TEntities, TStrategy>) {
  return options.declaredAtoms ?? DEFAULT_RELATIONAL_CAPABILITY_ATOMS;
}

export async function resolveRelationalCapabilityContext<
  TContext,
  TEntities extends Record<string, RelationalProviderEntityConfig>,
  TStrategy extends string,
>(
  options: RelationalProviderOptions<TContext, TEntities, TStrategy>,
  fragment: Extract<ProviderFragment, { kind: "rel" }>,
  context: TContext,
): Promise<RelationalProviderCapabilityContext<TContext, TEntities, TStrategy>> {
  const requiredAtoms = collectCapabilityAtomsForFragment(fragment);
  const declaredAtoms = getDeclaredAtoms(options);
  const missingAtoms = requiredAtoms.filter((atom) => !declaredAtoms.includes(atom));
  const routeFamily = inferRouteFamilyForFragment(fragment);
  const strategy = await options.resolveRelCompileStrategy({
    context,
    entities: options.entities,
    fragment,
  });
  const capabilityContext: RelationalProviderCapabilityContext<TContext, TEntities, TStrategy> = {
    context,
    entities: options.entities,
    fragment,
    routeFamily,
    requiredAtoms,
    missingAtoms,
    strategy,
  };

  return capabilityContext;
}

function evaluateRelationalCapability<
  TContext,
  TEntities extends Record<string, RelationalProviderEntityConfig>,
  TStrategy extends string,
>(
  options: RelationalProviderOptions<TContext, TEntities, TStrategy>,
  fragment: Extract<ProviderFragment, { kind: "rel" }>,
  context: TContext,
): MaybePromise<boolean | ProviderCapabilityReport> {
  const requiredAtoms = collectCapabilityAtomsForFragment(fragment);
  const declaredAtoms = getDeclaredAtoms(options);
  const missingAtoms = requiredAtoms.filter((atom) => !declaredAtoms.includes(atom));
  const routeFamily = inferRouteFamilyForFragment(fragment);
  const strategy = options.resolveRelCompileStrategy({
    context,
    entities: options.entities,
    fragment,
  });
  if (isPromiseLike<TStrategy | null>(strategy)) {
    return strategy.then((resolvedStrategy) =>
      evaluateRelationalCapabilityWithContext(options, {
        context,
        entities: options.entities,
        fragment,
        routeFamily,
        requiredAtoms,
        missingAtoms,
        strategy: resolvedStrategy,
      }),
    );
  }

  return evaluateRelationalCapabilityWithContext(options, {
    context,
    entities: options.entities,
    fragment,
    routeFamily,
    requiredAtoms,
    missingAtoms,
    strategy,
  });
}

function evaluateRelationalCapabilityWithContext<
  TContext,
  TEntities extends Record<string, RelationalProviderEntityConfig>,
  TStrategy extends string,
>(
  options: RelationalProviderOptions<TContext, TEntities, TStrategy>,
  capabilityContext: RelationalProviderCapabilityContext<TContext, TEntities, TStrategy>,
): MaybePromise<boolean | ProviderCapabilityReport> {
  if (!capabilityContext.strategy) {
    return {
      supported: false,
      routeFamily: capabilityContext.routeFamily,
      requiredAtoms: capabilityContext.requiredAtoms,
      missingAtoms: capabilityContext.missingAtoms,
      reason:
        options.unsupportedRelReason?.(capabilityContext) ??
        (relContainsSqlNode(capabilityContext.fragment.rel)
          ? "rel fragment must not contain sql nodes."
          : (options.unsupportedRelReasonMessage ??
            "Rel fragment is not supported for this provider.")),
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
      routeFamily: capabilityContext.routeFamily,
      requiredAtoms: capabilityContext.requiredAtoms,
      missingAtoms: capabilityContext.missingAtoms,
      reason: support,
    };
  }
  return support;
}
