import { bindProviderEntities } from "../entity-handles";
import { AdapterResult } from "../operations";
import type {
  ProviderCompiledPlan,
  ProviderFragment,
  ProviderLookupManyRequest,
  ProviderPlanDescription,
} from "../contracts";
import {
  canExecuteRelationalFragment,
  resolveRelationalCapabilityContext,
} from "./relational-capabilities";
import { buildRelationalEntityHandles } from "./relational-entities";
import {
  DEFAULT_RELATIONAL_CAPABILITY_ATOMS,
  type RelationalProviderCompileRelArgs,
} from "./relational-adapter-types";
import type {
  RelationalProvider,
  RelationalProviderEntityConfig,
  RelationalProviderOptions,
  RelationalProviderRelCompileStrategy,
  RelationalProviderWithLookup,
} from "./relational-adapter-types";

const DEFAULT_RELATIONAL_ROUTE_FAMILIES = [
  "scan",
  "aggregate",
  "rel-core",
  "rel-advanced",
] as const;
const LOOKUP_CAPABILITY_ATOM = "lookup.bulk" as const;

export { DEFAULT_RELATIONAL_CAPABILITY_ATOMS } from "./relational-adapter-types";

export type {
  RelationalProviderCapabilityContext,
  RelationalProviderCompileRelArgs,
  RelationalProviderCompileScanArgs,
  RelationalProviderDescribeArgs,
  RelationalProviderEntityColumnsArgs,
  RelationalProviderEntityConfig,
  RelationalProviderExecuteArgs,
  RelationalProviderLookupArgs,
  RelationalProviderOptions,
  RelationalProviderRelCompileStrategy,
} from "./relational-adapter-types";

type RelationalProviderOptionsWithLookup<
  TContext,
  TEntities extends Record<string, RelationalProviderEntityConfig>,
  TStrategy extends RelationalProviderRelCompileStrategy,
> = RelationalProviderOptions<TContext, TEntities, TStrategy> & {
  lookupMany: NonNullable<RelationalProviderOptions<TContext, TEntities, TStrategy>["lookupMany"]>;
};
function describeRelationalCompiledPlan(
  name: string,
  plan: ProviderCompiledPlan,
): ProviderPlanDescription {
  switch (plan.kind) {
    case "scan": {
      const payload = plan.payload as { table?: unknown; request?: unknown } | null;
      const table =
        payload && typeof payload === "object" && typeof payload.table === "string"
          ? payload.table
          : undefined;
      return {
        kind: "scan_fragment",
        summary: table ? `${name} scan on ${table}` : `${name} scan fragment`,
        operations: [
          {
            kind: "scan",
            ...(table ? { target: table } : {}),
            raw: plan.payload,
          },
        ],
        raw: plan.payload,
      };
    }
    case "aggregate":
      return {
        kind: "aggregate_fragment",
        summary: `${name} aggregate fragment`,
        operations: [
          {
            kind: "aggregate",
            raw: plan.payload,
          },
        ],
        raw: plan.payload,
      };
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
  options: RelationalProviderOptionsWithLookup<TContext, TEntities, TStrategy>,
): RelationalProviderWithLookup<TContext, TEntities>;
export function createRelationalProviderAdapter<
  TContext,
  TEntities extends Record<string, RelationalProviderEntityConfig>,
  TStrategy extends RelationalProviderRelCompileStrategy,
>(
  options: RelationalProviderOptions<TContext, TEntities, TStrategy> & {
    lookupMany?: undefined;
  },
): RelationalProvider<TContext, TEntities>;
export function createRelationalProviderAdapter<
  TContext,
  TEntities extends Record<string, RelationalProviderEntityConfig>,
  TStrategy extends RelationalProviderRelCompileStrategy,
>(
  options: RelationalProviderOptions<TContext, TEntities, TStrategy>,
): RelationalProvider<TContext, TEntities> | RelationalProviderWithLookup<TContext, TEntities> {
  const adapter = {
    name: options.name,
    routeFamilies: [
      ...(options.routeFamilies ??
        (options.lookupMany
          ? [...DEFAULT_RELATIONAL_ROUTE_FAMILIES, "lookup"]
          : DEFAULT_RELATIONAL_ROUTE_FAMILIES)),
    ],
    capabilityAtoms: [
      ...new Set([
        ...(options.declaredAtoms ?? DEFAULT_RELATIONAL_CAPABILITY_ATOMS),
        ...(options.lookupMany ? [LOOKUP_CAPABILITY_ATOM] : []),
      ]),
    ],
    ...(options.fallbackPolicy ? { fallbackPolicy: options.fallbackPolicy } : {}),
    canExecute(fragment: ProviderFragment, context: TContext) {
      return canExecuteRelationalFragment(options, fragment, context);
    },
    async compile(fragment: ProviderFragment, context: TContext) {
      switch (fragment.kind) {
        case "scan":
          if (!Object.hasOwn(options.entities, fragment.table)) {
            return AdapterResult.err(
              new Error(`Unknown ${options.name} entity config: ${fragment.table}`),
            );
          }
          return (
            options.compileScanFragment?.({
              context,
              entities: options.entities,
              fragment,
              name: options.name,
            }) ??
            AdapterResult.ok({
              provider: options.name,
              kind: "scan",
              payload: fragment,
            } satisfies ProviderCompiledPlan)
          );
        case "rel": {
          const capabilityContext = await resolveRelationalCapabilityContext(
            options,
            fragment,
            context,
          );
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

          const compileArgs = {
            context,
            entities: options.entities,
            fragment,
            name: options.name,
            strategy: capabilityContext.strategy,
          } satisfies RelationalProviderCompileRelArgs<TContext, TEntities, TStrategy>;

          if (options.compileRelFragment) {
            return options.compileRelFragment(compileArgs);
          }

          return AdapterResult.ok({
            provider: options.name,
            kind: "rel",
            payload: options.buildRelPlanPayload?.(compileArgs) ?? {
              strategy: capabilityContext.strategy,
              rel: fragment.rel,
            },
          } satisfies ProviderCompiledPlan);
        }
        default:
          return AdapterResult.err(
            new Error(`Unsupported ${options.name} fragment kind: ${fragment.kind}`),
          );
      }
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
  const lookupMany = options.lookupMany;
  const boundAdapter = {
    ...adapter,
    entities,
    ...(lookupMany
      ? {
          async lookupMany(request: ProviderLookupManyRequest, context: TContext) {
            return lookupMany({
              context,
              entities: options.entities,
              name: options.name,
              request,
            });
          },
        }
      : {}),
  };

  return bindProviderEntities(boundAdapter);
}
