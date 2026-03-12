import { bindAdapterEntities } from "../entity-handles";
import { AdapterResult } from "../operations";
import type {
  ProviderCompiledPlan,
  ProviderFragment,
  ProviderLookupManyRequest,
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
  RelationalProviderAdapter,
  RelationalProviderAdapterOptions,
  RelationalProviderAdapterOptionsWithLookup,
  RelationalProviderAdapterWithLookup,
  RelationalProviderEntityConfig,
  RelationalProviderRelCompileStrategy,
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
  RelationalProviderAdapterOptions,
  RelationalProviderAdapterOptionsWithLookup,
  RelationalProviderCapabilityContext,
  RelationalProviderCompileRelArgs,
  RelationalProviderCompileScanArgs,
  RelationalProviderEntityColumnsArgs,
  RelationalProviderEntityConfig,
  RelationalProviderExecuteArgs,
  RelationalProviderLookupArgs,
  RelationalProviderRelCompileStrategy,
  RelationalProviderSupportArgs,
} from "./relational-adapter-types";

export function createRelationalProviderAdapter<
  TContext,
  TEntities extends Record<string, RelationalProviderEntityConfig>,
  TStrategy extends RelationalProviderRelCompileStrategy,
>(
  options: RelationalProviderAdapterOptions<TContext, TEntities, TStrategy>,
): RelationalProviderAdapter<TContext, TEntities>;
export function createRelationalProviderAdapter<
  TContext,
  TEntities extends Record<string, RelationalProviderEntityConfig>,
  TStrategy extends RelationalProviderRelCompileStrategy,
>(
  options: RelationalProviderAdapterOptionsWithLookup<TContext, TEntities, TStrategy>,
): RelationalProviderAdapterWithLookup<TContext, TEntities>;
export function createRelationalProviderAdapter<
  TContext,
  TEntities extends Record<string, RelationalProviderEntityConfig>,
  TStrategy extends RelationalProviderRelCompileStrategy,
>(
  options:
    | RelationalProviderAdapterOptions<TContext, TEntities, TStrategy>
    | RelationalProviderAdapterOptionsWithLookup<TContext, TEntities, TStrategy>,
):
  | RelationalProviderAdapter<TContext, TEntities>
  | RelationalProviderAdapterWithLookup<TContext, TEntities> {
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
  };

  const entities = buildRelationalEntityHandles(adapter, options);
  const boundAdapter = {
    ...adapter,
    entities,
    ...(options.lookupMany
      ? {
          async lookupMany(request: ProviderLookupManyRequest, context: TContext) {
            return options.lookupMany({
              context,
              entities: options.entities,
              name: options.name,
              request,
            });
          },
        }
      : {}),
  };

  return bindAdapterEntities(boundAdapter);
}
