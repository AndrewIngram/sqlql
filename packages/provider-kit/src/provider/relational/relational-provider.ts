import { bindAdapterEntities } from "../entity-handles";
import { AdapterResult } from "../operations";
import type { RelNode } from "@tupl/foundation";
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
  RelationalProviderRelCompileStrategy,
} from "./relational-adapter-types";

const LOOKUP_CAPABILITY_ATOM = "lookup.bulk" as const;

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

function getCapabilityAtoms<TAtoms extends readonly string[] | undefined>(
  declaredAtoms: TAtoms,
  includeLookupAtom: boolean,
) {
  const atoms = [
    ...new Set([...(declaredAtoms ?? []), ...(includeLookupAtom ? [LOOKUP_CAPABILITY_ATOM] : [])]),
  ];
  return atoms.length > 0 ? atoms : undefined;
}

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

export function createRelationalProviderAdapter<
  TContext,
  TEntities extends Record<string, RelationalProviderEntityConfig>,
  TStrategy extends RelationalProviderRelCompileStrategy,
>(
  options: RelationalLookupProviderAdapterOptions<TContext, TEntities, TStrategy>,
): LookupCapableRelationalProviderAdapter<TContext, TEntities>;
export function createRelationalProviderAdapter<
  TContext,
  TEntities extends Record<string, RelationalProviderEntityConfig>,
  TStrategy extends RelationalProviderRelCompileStrategy,
>(
  options:
    | RelationalProviderAdapterOptions<TContext, TEntities, TStrategy>
    | RelationalLookupProviderAdapterOptions<TContext, TEntities, TStrategy>,
): RelationalProviderAdapter<TContext, TEntities> {
  const lookupManyHandler = resolveLookupManyHandler(options);
  const hasLookupMany = lookupManyHandler != null;
  const capabilityAtoms = getCapabilityAtoms(options.declaredAtoms, hasLookupMany);
  const adapter = {
    name: options.name,
    ...(capabilityAtoms ? { capabilityAtoms } : {}),
    ...(options.fallbackPolicy ? { fallbackPolicy: options.fallbackPolicy } : {}),
    canExecute(rel: RelNode, context: TContext) {
      return canExecuteRelationalFragment(options, rel, context);
    },
    async compile(rel: RelNode, context: TContext) {
      const capabilityContext = await resolveRelationalCapabilityContext(options, rel, context);
      if (!capabilityContext.strategy) {
        return AdapterResult.err(
          new Error(
            options.unsupportedRelCompileMessage ??
              `Unsupported relational fragment for ${options.name} provider.`,
          ),
        );
      }

      const support = await options.isRelStrategySupported?.(capabilityContext);
      if (support !== undefined && support !== true) {
        return AdapterResult.err(
          new Error(typeof support === "string" ? support : (support.reason ?? "Unsupported.")),
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
    },
    async execute(plan: ProviderCompiledPlan, context: TContext) {
      return options.executeCompiledPlan({
        context,
        entities: options.entities,
        name: options.name,
        plan,
      });
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

  const entities = buildRelationalEntityHandles(adapter, options);
  const boundAdapter = {
    ...adapter,
    entities,
    ...(lookupManyHandler
      ? {
          async lookupMany(request: ProviderLookupManyRequest, context: TContext) {
            return lookupManyHandler({
              context,
              entities: options.entities,
              name: options.name,
              request,
            });
          },
        }
      : {}),
  };

  return bindAdapterEntities(boundAdapter) as RelationalProviderAdapter<TContext, TEntities> &
    Partial<LookupManyCapableProviderAdapter<TContext>>;
}
