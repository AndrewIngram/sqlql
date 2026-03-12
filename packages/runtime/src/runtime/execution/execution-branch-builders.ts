import type { RelNode } from "@tupl/foundation";

import type { QuerySessionInput } from "../session/contracts";
import type { PlanBuildState } from "./explain-shaping";
import { nextPlanId } from "./explain-shaping";
import { resolveSyncLookupJoinCandidate } from "./lookup-join-planning";
import { formatColumnRef } from "./step-routing";

type VisitExecutionNode = (node: RelNode, scopeId?: string) => string;

/**
 * Execution branch builders own join, set-op, and WITH scope/step construction.
 */
export function buildJoinStep<TContext>(
  state: PlanBuildState,
  input: QuerySessionInput<TContext>,
  node: Extract<RelNode, { kind: "join" }>,
  scopeId: string,
  visit: VisitExecutionNode,
): string {
  const lookupJoin = resolveSyncLookupJoinCandidate(node, input);
  if (lookupJoin) {
    const leftId = visit(node.left, scopeId);
    const id = nextPlanId(state, "lookup_join");
    state.steps.push({
      id,
      kind: "lookup_join",
      dependsOn: [leftId],
      summary: `Lookup join ${lookupJoin.leftTable}.${lookupJoin.leftKey} -> ${lookupJoin.rightTable}.${lookupJoin.rightKey}`,
      phase: "fetch",
      operation: {
        name: "lookup_join",
        details: {
          leftProvider: lookupJoin.leftProvider,
          rightProvider: lookupJoin.rightProvider,
          joinType: lookupJoin.joinType,
          on: `${lookupJoin.leftTable}.${lookupJoin.leftKey} = ${lookupJoin.rightTable}.${lookupJoin.rightKey}`,
        },
      },
      outputs: node.output.map((column) => column.name),
      sqlOrigin: "FROM",
      scopeId,
    });
    return id;
  }

  const leftId = visit(node.left, scopeId);
  let rightScopeId = scopeId;
  if (node.joinType === "semi") {
    rightScopeId = nextPlanId(state, "scope_subquery");
    state.scopes.push({
      id: rightScopeId,
      kind: "subquery",
      label: `Subquery WHERE #${++state.whereSubqueryScopeCount}`,
      parentId: scopeId,
    });
  }
  const rightId = visit(node.right, rightScopeId);
  const id = nextPlanId(state, "join");
  state.steps.push({
    id,
    kind: "join",
    dependsOn: [leftId, rightId],
    summary: `${node.joinType.toUpperCase()} join`,
    phase: "transform",
    operation: {
      name: "join",
      details: {
        joinType: node.joinType,
        on: `${formatColumnRef(node.leftKey)} = ${formatColumnRef(node.rightKey)}`,
      },
    },
    outputs: node.output.map((column) => column.name),
    sqlOrigin: "FROM",
    scopeId,
  });
  return id;
}

export function buildSetOpStep(
  state: PlanBuildState,
  node: Extract<RelNode, { kind: "set_op" }>,
  scopeId: string,
  visit: VisitExecutionNode,
): string {
  const leftScopeId = nextPlanId(state, "scope_set_left");
  const rightScopeId = nextPlanId(state, "scope_set_right");
  state.scopes.push(
    {
      id: leftScopeId,
      kind: "set_op_branch",
      label: "Set operation left branch",
      parentId: scopeId,
    },
    {
      id: rightScopeId,
      kind: "set_op_branch",
      label: "Set operation right branch",
      parentId: scopeId,
    },
  );
  const leftInput = visit(node.left, leftScopeId);
  const rightInput = visit(node.right, rightScopeId);
  const leftStep = nextPlanId(state, "set_op_branch");
  const rightStep = nextPlanId(state, "set_op_branch");
  state.steps.push(
    {
      id: leftStep,
      kind: "set_op_branch",
      dependsOn: [leftInput],
      summary: "Set operation left branch",
      phase: "transform",
      operation: {
        name: "set_op_branch",
        details: { branch: "left" },
      },
      scopeId: leftScopeId,
    },
    {
      id: rightStep,
      kind: "set_op_branch",
      dependsOn: [rightInput],
      summary: "Set operation right branch",
      phase: "transform",
      operation: {
        name: "set_op_branch",
        details: { branch: "right" },
      },
      scopeId: rightScopeId,
    },
  );
  const id = nextPlanId(state, "projection");
  state.steps.push({
    id,
    kind: "projection",
    dependsOn: [leftStep, rightStep],
    summary: `Apply set operation (${node.op})`,
    phase: "output",
    operation: {
      name: "set_op",
      details: { op: node.op },
    },
    outputs: node.output.map((column) => column.name),
    sqlOrigin: "SET_OP",
    scopeId,
  });
  return id;
}

export function buildWithStep(
  state: PlanBuildState,
  node: Extract<RelNode, { kind: "with" }>,
  scopeId: string,
  visit: VisitExecutionNode,
): string {
  const cteStepIds: string[] = [];
  for (const cte of node.ctes) {
    const cteScopeId = nextPlanId(state, "scope_cte");
    state.scopes.push({
      id: cteScopeId,
      kind: "cte",
      label: `CTE ${cte.name}`,
      parentId: scopeId,
    });
    const cteInput = visit(cte.query, cteScopeId);
    const cteStepId = nextPlanId(state, "cte");
    state.steps.push({
      id: cteStepId,
      kind: "cte",
      dependsOn: [cteInput],
      summary: `CTE ${cte.name}`,
      phase: "transform",
      operation: {
        name: "cte",
        details: { name: cte.name },
      },
      sqlOrigin: "WITH",
      scopeId: cteScopeId,
    });
    cteStepIds.push(cteStepId);
  }
  const bodyStepId = visit(node.body, scopeId);
  const id = nextPlanId(state, "projection");
  state.steps.push({
    id,
    kind: "projection",
    dependsOn: [...cteStepIds, bodyStepId],
    summary: "Finalize WITH query",
    phase: "output",
    operation: {
      name: "with",
      details: { cteCount: node.ctes.length },
    },
    outputs: node.output.map((column) => column.name),
    sqlOrigin: "WITH",
    scopeId,
  });
  return id;
}
