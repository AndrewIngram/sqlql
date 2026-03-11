import { Result, type Result as BetterResult } from "better-result";

import { TuplPlanningError, type RelNode } from "@tupl/foundation";
import type { PhysicalPlan, PhysicalStep } from "./physical";
import type { ProvidersMap } from "@tupl/provider-kit";
import { normalizeCapability } from "@tupl/provider-kit";
import type { SchemaDefinition } from "@tupl/schema-model";
import { nextPhysicalStepId } from "./planner-ids";
import { toTuplPlanningError } from "./planner-errors";
import {
  assignConventions,
  resolveLookupJoinCandidate,
  resolveSingleProvider,
} from "./conventions";
import { buildProviderFragmentForNodeResult } from "./provider-fragments";
import { expandRelViewsResult } from "./view-expansion";

/**
 * Physical planning owns remote-fragment planning and the step graph built from conventioned RelNode trees.
 */
export async function planPhysicalQuery<TContext>(
  rel: RelNode,
  schema: SchemaDefinition,
  providers: ProvidersMap<TContext>,
  context: TContext,
  _sql: string,
): Promise<PhysicalPlan> {
  const result = await planPhysicalQueryResult(rel, schema, providers, context, _sql);
  if (Result.isError(result)) {
    throw result.error;
  }
  return result.value;
}

export async function planPhysicalQueryResult<TContext>(
  rel: RelNode,
  schema: SchemaDefinition,
  providers: ProvidersMap<TContext>,
  context: TContext,
  _sql: string,
) {
  return Result.gen(async function* () {
    const expandedRel = yield* expandRelViewsResult(rel, schema, context);
    const plannedRel = assignConventions(expandedRel, schema);
    const state: { steps: PhysicalStep[] } = { steps: [] };

    const rootStepId = yield* Result.await(
      planPhysicalNodeResult(plannedRel, schema, providers, context, state),
    );

    return Result.ok({
      rel: plannedRel,
      rootStepId,
      steps: state.steps,
    });
  });
}

async function planPhysicalNodeResult<TContext>(
  node: RelNode,
  schema: SchemaDefinition,
  providers: ProvidersMap<TContext>,
  context: TContext,
  state: { steps: PhysicalStep[] },
): Promise<BetterResult<string, TuplPlanningError>> {
  return Result.gen(async function* () {
    const remoteStepId = yield* Result.await(
      tryPlanRemoteFragmentResult(node, schema, providers, context, state),
    );
    if (remoteStepId) {
      return Result.ok(remoteStepId);
    }

    switch (node.kind) {
      case "scan": {
        const step: PhysicalStep = {
          id: nextPhysicalStepId("local_project"),
          kind: "local_project",
          dependsOn: [],
          summary: `Local fallback scan for ${node.table}`,
        };
        state.steps.push(step);
        return Result.ok(step.id);
      }
      case "filter":
      case "project":
      case "aggregate":
      case "sort":
      case "limit_offset": {
        const input = yield* Result.await(
          planPhysicalNodeResult(node.input, schema, providers, context, state),
        );
        const kind =
          node.kind === "filter"
            ? "local_filter"
            : node.kind === "project"
              ? "local_project"
              : node.kind === "aggregate"
                ? "local_aggregate"
                : node.kind === "sort"
                  ? "local_sort"
                  : "local_limit_offset";

        const step: PhysicalStep = {
          id: nextPhysicalStepId(kind),
          kind,
          dependsOn: [input],
          summary: `Local ${node.kind} execution`,
        };
        state.steps.push(step);
        return Result.ok(step.id);
      }
      case "join": {
        const lookup = resolveLookupJoinCandidate(node, schema, providers);
        if (lookup) {
          const left = yield* Result.await(
            planPhysicalNodeResult(node.left, schema, providers, context, state),
          );
          const step: PhysicalStep = {
            id: nextPhysicalStepId("lookup_join"),
            kind: "lookup_join",
            dependsOn: [left],
            summary: `Lookup join ${lookup.leftScan.table}.${lookup.leftKey} -> ${lookup.rightScan.table}.${lookup.rightKey}`,
            leftProvider: lookup.leftProvider,
            rightProvider: lookup.rightProvider,
            leftTable: lookup.leftScan.table,
            rightTable: lookup.rightScan.table,
            leftKey: lookup.leftKey,
            rightKey: lookup.rightKey,
            joinType: lookup.joinType,
          };
          state.steps.push(step);
          return Result.ok(step.id);
        }

        const left = yield* Result.await(
          planPhysicalNodeResult(node.left, schema, providers, context, state),
        );
        const right = yield* Result.await(
          planPhysicalNodeResult(node.right, schema, providers, context, state),
        );
        const step: PhysicalStep = {
          id: nextPhysicalStepId("local_hash_join"),
          kind: "local_hash_join",
          dependsOn: [left, right],
          summary: `Local ${node.joinType} join execution`,
        };
        state.steps.push(step);
        return Result.ok(step.id);
      }
      case "window": {
        const input = yield* Result.await(
          planPhysicalNodeResult(node.input, schema, providers, context, state),
        );
        const step: PhysicalStep = {
          id: nextPhysicalStepId("local_window"),
          kind: "local_window",
          dependsOn: [input],
          summary: "Local window execution",
        };
        state.steps.push(step);
        return Result.ok(step.id);
      }
      case "set_op": {
        const left = yield* Result.await(
          planPhysicalNodeResult(node.left, schema, providers, context, state),
        );
        const right = yield* Result.await(
          planPhysicalNodeResult(node.right, schema, providers, context, state),
        );
        const step: PhysicalStep = {
          id: nextPhysicalStepId("local_set_op"),
          kind: "local_set_op",
          dependsOn: [left, right],
          summary: `Local ${node.op} execution`,
        };
        state.steps.push(step);
        return Result.ok(step.id);
      }
      case "with": {
        const dependencies: string[] = [];
        for (const cte of node.ctes) {
          dependencies.push(
            yield* Result.await(
              planPhysicalNodeResult(cte.query, schema, providers, context, state),
            ),
          );
        }
        dependencies.push(
          yield* Result.await(planPhysicalNodeResult(node.body, schema, providers, context, state)),
        );

        const step: PhysicalStep = {
          id: nextPhysicalStepId("local_with"),
          kind: "local_with",
          dependsOn: dependencies,
          summary: "Local WITH materialization",
        };
        state.steps.push(step);
        return Result.ok(step.id);
      }
      case "sql": {
        const step: PhysicalStep = {
          id: nextPhysicalStepId("local_project"),
          kind: "local_project",
          dependsOn: [],
          summary: "Local SQL fallback execution",
        };
        state.steps.push(step);
        return Result.ok(step.id);
      }
    }
  });
}

async function tryPlanRemoteFragmentResult<TContext>(
  node: RelNode,
  schema: SchemaDefinition,
  providers: ProvidersMap<TContext>,
  context: TContext,
  state: { steps: PhysicalStep[] },
): Promise<BetterResult<string | null, TuplPlanningError>> {
  const provider = resolveSingleProvider(node, schema);
  if (!provider) {
    return Result.ok(null);
  }

  const adapter = providers[provider];
  if (!adapter) {
    return Result.err(
      new TuplPlanningError({
        operation: "plan remote fragment",
        message: `Missing provider adapter: ${provider}`,
      }),
    );
  }

  const fragmentResult = buildProviderFragmentForNodeResult(node, schema, provider);
  if (Result.isError(fragmentResult)) {
    return fragmentResult;
  }

  const capabilityResult = await Result.tryPromise({
    try: () => Promise.resolve(adapter.canExecute(fragmentResult.value, context)),
    catch: (error) => toTuplPlanningError(error, "plan remote fragment"),
  });
  if (Result.isError(capabilityResult)) {
    return capabilityResult;
  }

  const capability = normalizeCapability(capabilityResult.value);
  if (!capability.supported) {
    return Result.ok(null);
  }

  const step: PhysicalStep = {
    id: nextPhysicalStepId("remote_fragment"),
    kind: "remote_fragment",
    dependsOn: [],
    summary: `Execute provider fragment (${provider})`,
    provider,
    fragment: fragmentResult.value,
  };

  state.steps.push(step);
  return Result.ok(step.id);
}
