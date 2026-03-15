import { bindProviderEntities } from "../entity-handles";
import { AdapterResult } from "../operations";
import { TuplExecutionError, TuplProviderBindingError, type RelNode } from "@tupl/foundation";
import type { ProviderCompiledPlan, ProviderPlanDescription } from "../contracts";
import type {
  LookupManyCapableProviderAdapter,
  ProviderLookupManyRequest,
} from "../shapes/lookup-optimization";
import {
  canExecuteRelationalFragment,
  resolveRelationalCapabilityContext,
} from "./relational-capabilities";
import { buildRelationalEntityHandles } from "./relational-entities";
import { type RelationalProviderCompileRelArgs } from "./relational-adapter-types";
import type {
  LookupCapableRelationalProviderAdapter,
  RelationalProviderAdapter,
  RelationalProviderAdapterOptions,
  RelationalLookupProviderAdapterOptions,
  RelationalProviderEntityConfig,
  RelationalProviderHandles,
  RelationalProviderRelCompileStrategy,
} from "./relational-adapter-types";

/**
 * Relational provider adapters own the boundary between canonical rel fragments and provider-owned
 * compile/execute hooks. Callers can rely on Result-shaped boundary errors and rel-first support
 * checks; strategy selection and payload construction stay hidden behind the adapter.
 */
export type {
  LookupCapableRelationalProviderAdapter,
  RelationalProviderAdapterOptions,
  RelationalProviderCapabilityContext,
  RelationalProviderCompileRelArgs,
  RelationalProviderDescribeArgs,
  RelationalProviderEntityColumnsArgs,
  RelationalProviderEntityConfig,
  RelationalProviderExecuteArgs,
  RelationalLookupProviderAdapterOptions,
  RelationalProviderLookupArgs,
  RelationalProviderRelCompileStrategy,
  RelationalProviderSupportArgs,
} from "./relational-adapter-types";

function resolveLookupManyHandler<
  TContext,
  TEntities extends Record<string, RelationalProviderEntityConfig>,
  TStrategy extends RelationalProviderRelCompileStrategy,
>(
  options:
    | RelationalProviderAdapterOptions<TContext, TEntities, TStrategy>
    | RelationalLookupProviderAdapterOptions<TContext, TEntities, TStrategy>,
) {
  if (!("lookupMany" in options) || typeof options.lookupMany !== "function") {
    return undefined;
  }

  return options.lookupMany;
}

function describeRelationalCompiledPlan(
  name: string,
  plan: ProviderCompiledPlan,
): ProviderPlanDescription {
  switch (plan.kind) {
    case "rel": {
      const payload = plan.payload as { strategy?: unknown; sql?: unknown } | null;
      const strategy =
        payload && typeof payload === "object" && typeof payload.strategy === "string"
          ? payload.strategy
          : undefined;
      const sql =
        payload && typeof payload === "object" && typeof payload.sql === "string"
          ? payload.sql
          : undefined;
      return {
        kind: "rel_fragment",
        summary: strategy ? `${name} rel fragment (${strategy})` : `${name} rel fragment`,
        operations: [
          {
            kind: sql ? "sql" : "rel",
            ...(sql ? { sql } : {}),
            target: name,
            ...(strategy ? { summary: strategy } : {}),
            raw: plan.payload,
          },
        ],
        raw: plan.payload,
      };
    }
    default:
      return {
        kind: "compiled_plan",
        summary: `${name} compiled plan`,
        operations: [
          {
            kind: "compiled_plan",
            target: name,
            raw: plan.payload,
          },
        ],
        raw: plan.payload,
      };
  }
}

function normalizeRelationalProviderError(
  error: unknown,
  operation: string,
): TuplExecutionError | TuplProviderBindingError | Error {
  if (error instanceof TuplExecutionError || error instanceof TuplProviderBindingError) {
    return error;
  }

  if (error instanceof Error) {
    return new TuplExecutionError({
      operation,
      message: error.message,
      cause: error,
    });
  }

  return new TuplExecutionError({
    operation,
    message: String(error),
    cause: error,
  });
}

export function createRelationalProviderAdapter<
  TContext,
  TEntities extends Record<string, RelationalProviderEntityConfig>,
  TStrategy extends RelationalProviderRelCompileStrategy,
  THandles extends RelationalProviderHandles<TEntities> = RelationalProviderHandles<TEntities>,
>(
  options: RelationalLookupProviderAdapterOptions<TContext, TEntities, TStrategy>,
): LookupCapableRelationalProviderAdapter<TContext, TEntities, THandles>;
export function createRelationalProviderAdapter<
  TContext,
  TEntities extends Record<string, RelationalProviderEntityConfig>,
  TStrategy extends RelationalProviderRelCompileStrategy,
  THandles extends RelationalProviderHandles<TEntities> = RelationalProviderHandles<TEntities>,
>(
  options:
    | RelationalProviderAdapterOptions<TContext, TEntities, TStrategy>
    | RelationalLookupProviderAdapterOptions<TContext, TEntities, TStrategy>,
): RelationalProviderAdapter<TContext, TEntities, THandles> {
  const lookupManyHandler = resolveLookupManyHandler(options);
  const adapter = {
    name: options.name,
    ...(options.fallbackPolicy ? { fallbackPolicy: options.fallbackPolicy } : {}),
    canExecute(rel: RelNode, context: TContext) {
      return canExecuteRelationalFragment(options, rel, context);
    },
    async compile(rel: RelNode, context: TContext) {
      try {
        const capabilityContext = await resolveRelationalCapabilityContext(options, rel, context);
        if (!capabilityContext.strategy) {
          return AdapterResult.err(
            new TuplExecutionError({
              operation: "compile provider fragment",
              message:
                options.unsupportedRelCompileMessage ??
                `Unsupported relational fragment for ${options.name} provider.`,
            }),
          );
        }

        const support = await options.isRelStrategySupported?.(capabilityContext);
        if (support !== undefined && support !== true) {
          return AdapterResult.err(
            new TuplExecutionError({
              operation: "compile provider fragment",
              message: typeof support === "string" ? support : (support.reason ?? "Unsupported."),
            }),
          );
        }

        const strategy = capabilityContext.strategy;
        const compileArgs = {
          context,
          entities: options.entities,
          rel,
          name: options.name,
          strategy,
        };

        if (options.compileRelFragment) {
          return options.compileRelFragment(compileArgs);
        }

        return AdapterResult.ok({
          provider: options.name,
          kind: "rel",
          payload: options.buildRelPlanPayload?.(compileArgs) ?? {
            strategy,
            rel,
          },
        } satisfies ProviderCompiledPlan);
      } catch (error) {
        return AdapterResult.err(
          normalizeRelationalProviderError(error, "compile provider fragment"),
        );
      }
    },
    async execute(plan: ProviderCompiledPlan, context: TContext) {
      try {
        return await options.executeCompiledPlan({
          context,
          entities: options.entities,
          name: options.name,
          plan,
        });
      } catch (error) {
        return AdapterResult.err(
          normalizeRelationalProviderError(error, "execute provider compiled plan"),
        );
      }
    },
    async describeCompiledPlan(plan: ProviderCompiledPlan, context: TContext) {
      return (
        options.describeCompiledPlan?.({
          context,
          entities: options.entities,
          name: options.name,
          plan,
        }) ?? describeRelationalCompiledPlan(options.name, plan)
      );
    },
  };

  const entities = buildRelationalEntityHandles<TContext, TEntities, TStrategy, THandles>(
    adapter,
    options,
  );
  const boundAdapter = {
    ...adapter,
    entities,
    ...(lookupManyHandler
      ? {
          async lookupMany(request: ProviderLookupManyRequest, context: TContext) {
            try {
              return await lookupManyHandler({
                context,
                entities: options.entities,
                name: options.name,
                request,
              });
            } catch (error) {
              return AdapterResult.err(
                normalizeRelationalProviderError(error, "execute provider lookupMany"),
              );
            }
          },
        }
      : {}),
  };

  return bindProviderEntities(boundAdapter) as RelationalProviderAdapter<
    TContext,
    TEntities,
    THandles
  > &
    Partial<LookupManyCapableProviderAdapter<TContext>>;
}
