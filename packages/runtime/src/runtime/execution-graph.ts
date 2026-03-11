import type { RelNode } from "@tupl/foundation";

import type { QuerySessionInput } from "./contracts";
import type { PlanBuildState } from "./explain-shaping";
import { nextPlanId, visitExprSubqueries } from "./explain-shaping";
import { resolveSyncLookupJoinCandidate } from "./lookup-join-planning";
import { formatColumnRef } from "./step-routing";
import { tryPlanRemoteFragmentStep } from "./step-families";

/**
 * Execution graph owns recursive rel-to-step graph construction for plan/explain output.
 */
export function buildExecutionGraph<TContext>(
  state: PlanBuildState,
  input: QuerySessionInput<TContext>,
  rel: RelNode,
): void {
  const visit = (node: RelNode, scopeId = "scope_root"): string => {
    const remoteFragmentStepId = tryPlanRemoteFragmentStep(state, input, node, scopeId);
    if (remoteFragmentStepId) {
      return remoteFragmentStepId;
    }

    switch (node.kind) {
      case "scan": {
        const id = nextPlanId(state, "scan");
        state.steps.push({
          id,
          kind: "scan",
          dependsOn: [],
          summary: `Scan ${node.alias ?? node.table} (${node.table})`,
          phase: "fetch",
          operation: {
            name: "scan",
            details: {
              table: node.table,
              alias: node.alias ?? node.table,
            },
          },
          request: {
            select: node.select,
            ...(node.where ? { where: node.where } : {}),
            ...(node.orderBy ? { orderBy: node.orderBy } : {}),
            ...(node.limit != null ? { limit: node.limit } : {}),
            ...(node.offset != null ? { offset: node.offset } : {}),
          },
          outputs: node.output.map((column) => column.name),
          sqlOrigin: "FROM",
          scopeId,
        });
        return id;
      }
      case "filter": {
        const inputId = visit(node.input, scopeId);
        const subqueryDeps = node.expr
          ? visitExprSubqueries(state, node.expr, "WHERE", scopeId, visit)
          : [];
        const id = nextPlanId(state, "filter");
        state.steps.push({
          id,
          kind: "filter",
          dependsOn: [...new Set([inputId, ...subqueryDeps])],
          summary: "Apply WHERE filter",
          phase: "transform",
          operation: {
            name: "filter",
            details: {
              clauseCount: node.where?.length ?? (node.expr ? 1 : 0),
            },
          },
          ...(node.where || node.expr
            ? {
                request: {
                  ...(node.where ? { where: node.where } : {}),
                  ...(node.expr ? { expr: node.expr } : {}),
                },
              }
            : {}),
          outputs: node.output.map((column) => column.name),
          sqlOrigin: "WHERE",
          scopeId,
        });
        return id;
      }
      case "project": {
        const inputId = visit(node.input, scopeId);
        const subqueryDeps = node.columns.flatMap((column) =>
          "expr" in column ? visitExprSubqueries(state, column.expr, "SELECT", scopeId, visit) : [],
        );
        const id = nextPlanId(state, "projection");
        state.steps.push({
          id,
          kind: "projection",
          dependsOn: [...new Set([inputId, ...subqueryDeps])],
          summary: "Project result rows",
          phase: "output",
          operation: {
            name: "project",
            details: {
              columnCount: node.columns.length,
            },
          },
          outputs: node.output.map((column) => column.name),
          sqlOrigin: "SELECT",
          scopeId,
        });
        return id;
      }
      case "join": {
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
      case "aggregate": {
        const inputId = visit(node.input, scopeId);
        const id = nextPlanId(state, "aggregate");
        state.steps.push({
          id,
          kind: "aggregate",
          dependsOn: [inputId],
          summary: "Compute grouped aggregates",
          phase: "transform",
          operation: {
            name: "aggregate",
            details: {
              groupBy: node.groupBy.map(formatColumnRef),
              metrics: node.metrics.map((metric) => ({
                fn: metric.fn,
                as: metric.as,
                ...(metric.column ? { column: formatColumnRef(metric.column) } : {}),
              })),
            },
          },
          outputs: node.output.map((column) => column.name),
          sqlOrigin: "GROUP BY",
          scopeId,
        });
        return id;
      }
      case "window": {
        const inputId = visit(node.input, scopeId);
        const id = nextPlanId(state, "window");
        state.steps.push({
          id,
          kind: "window",
          dependsOn: [inputId],
          summary: "Compute window functions",
          phase: "transform",
          operation: {
            name: "window",
            details: {
              functions: node.functions.map((fn) => ({
                fn: fn.fn,
                as: fn.as,
                partitionBy: fn.partitionBy.map(formatColumnRef),
                orderBy: fn.orderBy.map((term) => ({
                  source: formatColumnRef(term.source),
                  direction: term.direction,
                })),
              })),
            },
          },
          outputs: node.output.map((column) => column.name),
          sqlOrigin: "SELECT",
          scopeId,
        });
        return id;
      }
      case "sort": {
        const inputId = visit(node.input, scopeId);
        const id = nextPlanId(state, "order");
        state.steps.push({
          id,
          kind: "order",
          dependsOn: [inputId],
          summary: "Order result rows",
          phase: "transform",
          operation: {
            name: "order",
            details: {
              orderBy: node.orderBy.map((term) => ({
                source: formatColumnRef(term.source),
                direction: term.direction,
              })),
            },
          },
          outputs: node.output.map((column) => column.name),
          sqlOrigin: "ORDER BY",
          scopeId,
        });
        return id;
      }
      case "limit_offset": {
        const inputId = visit(node.input, scopeId);
        const id = nextPlanId(state, "limit_offset");
        state.steps.push({
          id,
          kind: "limit_offset",
          dependsOn: [inputId],
          summary: "Apply LIMIT/OFFSET",
          phase: "output",
          operation: {
            name: "limit_offset",
            details: {
              ...(node.limit != null ? { limit: node.limit } : {}),
              ...(node.offset != null ? { offset: node.offset } : {}),
            },
          },
          outputs: node.output.map((column) => column.name),
          sqlOrigin: "ORDER BY",
          scopeId,
        });
        return id;
      }
      case "set_op": {
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
      case "with": {
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
      case "sql": {
        const id = nextPlanId(state, "remote_fragment");
        state.steps.push({
          id,
          kind: "remote_fragment",
          dependsOn: [],
          summary: "Execute SQL-shaped relational fragment",
          phase: "fetch",
          operation: {
            name: "provider_fragment",
            details: { fragment: "sql" },
          },
          request: {
            tables: node.tables,
          },
          sqlOrigin: "SELECT",
          scopeId,
        });
        return id;
      }
    }
  };

  visit(rel, "scope_root");
}
