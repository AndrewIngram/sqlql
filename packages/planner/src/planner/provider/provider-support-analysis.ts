import { Result, type Result as BetterResult } from "better-result";

import { PhysicalPlanningError, type RelNode, type TuplError } from "@tupl/foundation";
import {
  normalizeCapability,
  type ProviderCapabilityReport,
  type ProvidersMap,
} from "@tupl/provider-kit";
import type { SchemaDefinition } from "@tupl/schema-model";

import { buildProviderFragmentForNodeResult, type ProviderRelTarget } from "../provider-fragments";
import { toPhysicalPlanningError } from "../planner-errors";
import { resolveSingleProvider } from "./provider-ownership";

export interface ProviderSupportDecision {
  provider: string | null;
  fragment: ProviderRelTarget | null;
  capability: ProviderCapabilityReport | null;
  supported: boolean;
}

export interface ProviderSupportAnalysis {
  byNodeId: Map<string, ProviderSupportDecision>;
}

/**
 * Provider support analysis owns bottom-up support discovery for physical planning. It memoizes
 * one support decision per rel node so fragment cutting can stay maximal-first without repeatedly
 * probing broad unsupported parents.
 */
export async function analyzeProviderSupportResult<TContext>(
  node: RelNode,
  schema: SchemaDefinition,
  providers: ProvidersMap<TContext>,
  context: TContext,
): Promise<BetterResult<ProviderSupportAnalysis, TuplError>> {
  return Result.gen(async function* () {
    const byNodeId = new Map<string, ProviderSupportDecision>();

    const visit = async (current: RelNode): Promise<BetterResult<void, TuplError>> =>
      Result.gen(async function* () {
        for (const child of getSupportAnalysisChildren(current)) {
          yield* Result.await(visit(child));
        }

        const provider = resolveSingleProvider(current, schema);
        if (!provider) {
          byNodeId.set(current.id, {
            provider: null,
            fragment: null,
            capability: null,
            supported: false,
          });
          return Result.ok(undefined);
        }

        const adapter = providers[provider];
        if (!adapter) {
          return Result.err(
            new PhysicalPlanningError({
              operation: "analyze provider support",
              message: `Missing provider adapter: ${provider}`,
            }),
          );
        }

        const fragment = yield* buildProviderFragmentForNodeResult(current, schema, provider);
        const capabilityResult = yield* Result.await(
          Result.tryPromise({
            try: () => Promise.resolve(adapter.canExecute(fragment.rel, context)),
            catch: (error) => toPhysicalPlanningError(error, "analyze provider support"),
          }),
        );
        const capability = normalizeCapability(capabilityResult);

        byNodeId.set(current.id, {
          provider,
          fragment,
          capability,
          supported: capability.supported,
        });

        return Result.ok(undefined);
      });

    yield* Result.await(visit(node));
    return Result.ok({ byNodeId });
  });
}

export function getProviderSupportDecision(
  analysis: ProviderSupportAnalysis,
  node: RelNode,
): ProviderSupportDecision | null {
  return analysis.byNodeId.get(node.id) ?? null;
}

/**
 * Child order here is semantic, not scheduler-related: support is discovered bottom-up so every
 * parent decision sees already-resolved child support state.
 */
function getSupportAnalysisChildren(node: RelNode): RelNode[] {
  switch (node.kind) {
    case "values":
    case "scan":
    case "cte_ref":
      return [];
    case "filter":
    case "project":
    case "aggregate":
    case "window":
    case "sort":
    case "limit_offset":
      return [node.input];
    case "correlate":
      return [node.left, node.right];
    case "join":
    case "set_op":
      return [node.left, node.right];
    case "with":
      return [...node.ctes.map((cte) => cte.query), node.body];
    case "repeat_union":
      return [node.seed, node.iterative];
  }
}
