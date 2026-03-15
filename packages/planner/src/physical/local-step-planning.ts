import { Result, type Result as BetterResult } from "better-result";

import { type RelNode, type TuplError } from "@tupl/foundation";
import type { ProvidersMap } from "@tupl/provider-kit";
import type { SchemaDefinition } from "@tupl/schema-model";

import { nextPhysicalStepId } from "../physical/planner-ids";
import { recordPhysicalStep, type PhysicalPlanningState } from "./physical-plan-state";
import { tryPlanRemoteFragmentResult } from "./remote-fragment-planning";

/**
 * Local step planning owns recursive physical-step construction for conventioned RelNode trees.
 */
export async function planPhysicalNodeResult<TContext>(
  node: RelNode,
  schema: SchemaDefinition,
  providers: ProvidersMap<TContext>,
  context: TContext,
  state: PhysicalPlanningState,
): Promise<BetterResult<string, TuplError>> {
  return Result.gen(async function* () {
    const remoteStepId = yield* Result.await(tryPlanRemoteFragmentResult(node, state));
    if (remoteStepId) {
      return Result.ok(remoteStepId);
    }

    switch (node.kind) {
      case "values":
      case "cte_ref":
        return Result.ok(
          recordPhysicalStep(state, {
            id: nextPhysicalStepId("local_project"),
            kind: "local_project",
            dependsOn: [],
            summary:
              node.kind === "values"
                ? "Local literal row materialization"
                : `Local CTE read (${node.name})`,
          }),
        );
      case "scan":
        return Result.ok(
          recordPhysicalStep(state, {
            id: nextPhysicalStepId("local_project"),
            kind: "local_project",
            dependsOn: [],
            summary: `Local fallback scan for ${node.table}`,
          }),
        );
      case "correlate": {
        const left = yield* Result.await(
          planPhysicalNodeResult(node.left, schema, providers, context, state),
        );
        const right = yield* Result.await(
          planPhysicalNodeResult(node.right, schema, providers, context, state),
        );
        return Result.ok(
          recordPhysicalStep(state, {
            id: nextPhysicalStepId("local_filter"),
            kind: "local_filter",
            dependsOn: [left, right],
            summary: "Local correlated subquery rewrite",
          }),
        );
      }
      case "filter":
      case "project":
      case "aggregate":
      case "sort":
      case "limit_offset":
        return planUnaryLocalNodeResult(node, schema, providers, context, state);
      case "join":
        return planJoinNodeResult(node, schema, providers, context, state);
      case "window": {
        const input = yield* Result.await(
          planPhysicalNodeResult(node.input, schema, providers, context, state),
        );
        return Result.ok(
          recordPhysicalStep(state, {
            id: nextPhysicalStepId("local_window"),
            kind: "local_window",
            dependsOn: [input],
            summary: "Local window execution",
          }),
        );
      }
      case "set_op": {
        const left = yield* Result.await(
          planPhysicalNodeResult(node.left, schema, providers, context, state),
        );
        const right = yield* Result.await(
          planPhysicalNodeResult(node.right, schema, providers, context, state),
        );
        return Result.ok(
          recordPhysicalStep(state, {
            id: nextPhysicalStepId("local_set_op"),
            kind: "local_set_op",
            dependsOn: [left, right],
            summary: `Local ${node.op} execution`,
          }),
        );
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

        return Result.ok(
          recordPhysicalStep(state, {
            id: nextPhysicalStepId("local_with"),
            kind: "local_with",
            dependsOn: dependencies,
            summary: "Local WITH materialization",
          }),
        );
      }
      case "repeat_union": {
        const seed = yield* Result.await(
          planPhysicalNodeResult(node.seed, schema, providers, context, state),
        );
        const iterative = yield* Result.await(
          planPhysicalNodeResult(node.iterative, schema, providers, context, state),
        );
        return Result.ok(
          recordPhysicalStep(state, {
            id: nextPhysicalStepId("local_with"),
            kind: "local_with",
            dependsOn: [seed, iterative],
            summary: `Local recursive CTE execution (${node.cteName})`,
          }),
        );
      }
    }
  });
}

async function planUnaryLocalNodeResult<TContext>(
  node: Extract<RelNode, { kind: "filter" | "project" | "aggregate" | "sort" | "limit_offset" }>,
  schema: SchemaDefinition,
  providers: ProvidersMap<TContext>,
  context: TContext,
  state: PhysicalPlanningState,
): Promise<BetterResult<string, TuplError>> {
  return Result.gen(async function* () {
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

    return Result.ok(
      recordPhysicalStep(state, {
        id: nextPhysicalStepId(kind),
        kind,
        dependsOn: [input],
        summary: `Local ${node.kind} execution`,
      }),
    );
  });
}

async function planJoinNodeResult<TContext>(
  node: Extract<RelNode, { kind: "join" }>,
  schema: SchemaDefinition,
  providers: ProvidersMap<TContext>,
  context: TContext,
  state: PhysicalPlanningState,
): Promise<BetterResult<string, TuplError>> {
  return Result.gen(async function* () {
    const left = yield* Result.await(
      planPhysicalNodeResult(node.left, schema, providers, context, state),
    );
    const right = yield* Result.await(
      planPhysicalNodeResult(node.right, schema, providers, context, state),
    );
    return Result.ok(
      recordPhysicalStep(state, {
        id: nextPhysicalStepId("local_hash_join"),
        kind: "local_hash_join",
        dependsOn: [left, right],
        summary: `Local ${node.joinType} join execution`,
      }),
    );
  });
}
