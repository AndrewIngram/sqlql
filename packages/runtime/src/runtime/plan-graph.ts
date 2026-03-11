import { Result } from "better-result";

import { supportsLookupMany } from "@tupl/provider-kit";
import { type RelExpr, type RelNode } from "@tupl/foundation";
import { getNormalizedTableBinding, resolveTableProvider } from "@tupl/schema-model";

import type {
  QueryExecutionPlan,
  QueryExecutionPlanScope,
  QueryExecutionPlanStep,
  QuerySessionInput,
  QueryStepKind,
  QueryStepRoute,
  TuplDiagnostic,
} from "./contracts";
import { isPromiseLike } from "./policy";
import { resolveSyncProviderCapabilityForRel } from "./provider-execution";

/**
 * Plan graph owns execution-plan and step-graph construction for query sessions.
 */
export function buildRelExecutionPlan<TContext>(
  input: QuerySessionInput<TContext>,
  rel: RelNode,
  diagnostics: TuplDiagnostic[] = [],
): QueryExecutionPlan {
  let stepCounter = 0;
  const steps: QueryExecutionPlanStep[] = [];
  const scopes: QueryExecutionPlanScope[] = [
    {
      id: "scope_root",
      kind: "root",
      label: "Root query",
    },
  ];

  const nextId = (prefix: string): string => {
    stepCounter += 1;
    return `${prefix}_${stepCounter}`;
  };

  let whereSubqueryScopeCount = 0;
  let selectSubqueryScopeCount = 0;

  const visitExprSubqueries = (
    expr: RelExpr,
    owner: "WHERE" | "SELECT",
    parentScopeId: string,
  ): string[] => {
    switch (expr.kind) {
      case "literal":
      case "column":
        return [];
      case "function":
        return [
          ...new Set(expr.args.flatMap((arg) => visitExprSubqueries(arg, owner, parentScopeId))),
        ];
      case "subquery": {
        const scopeId = nextId("scope_subquery");
        const label =
          owner === "WHERE"
            ? `Subquery WHERE #${++whereSubqueryScopeCount}`
            : `Subquery SELECT #${++selectSubqueryScopeCount}`;
        scopes.push({
          id: scopeId,
          kind: "subquery",
          label,
          parentId: parentScopeId,
        });
        const rootStepId = visit(expr.rel, scopeId);
        return [rootStepId];
      }
    }
  };

  const visit = (node: RelNode, scopeId = "scope_root"): string => {
    const remoteFragmentStepId = tryPlanRemoteFragmentStep(node, scopeId);
    if (remoteFragmentStepId) {
      return remoteFragmentStepId;
    }

    switch (node.kind) {
      case "scan": {
        const id = nextId("scan");
        steps.push({
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
        const subqueryDeps = node.expr ? visitExprSubqueries(node.expr, "WHERE", scopeId) : [];
        const id = nextId("filter");
        steps.push({
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
          "expr" in column ? visitExprSubqueries(column.expr, "SELECT", scopeId) : [],
        );
        const id = nextId("projection");
        steps.push({
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
          const id = nextId("lookup_join");
          steps.push({
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
          rightScopeId = nextId("scope_subquery");
          scopes.push({
            id: rightScopeId,
            kind: "subquery",
            label: `Subquery WHERE #${++whereSubqueryScopeCount}`,
            parentId: scopeId,
          });
        }
        const rightId = visit(node.right, rightScopeId);
        const id = nextId("join");
        steps.push({
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
        const id = nextId("aggregate");
        steps.push({
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
        const id = nextId("window");
        steps.push({
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
        const id = nextId("order");
        steps.push({
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
        const id = nextId("limit_offset");
        steps.push({
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
        const leftScopeId = `${nextId("scope_set_left")}`;
        const rightScopeId = `${nextId("scope_set_right")}`;
        scopes.push(
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
        const leftStep = nextId("set_op_branch");
        const rightStep = nextId("set_op_branch");
        steps.push(
          {
            id: leftStep,
            kind: "set_op_branch",
            dependsOn: [leftInput],
            summary: "Set operation left branch",
            phase: "transform",
            operation: {
              name: "set_op_branch",
              details: {
                branch: "left",
              },
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
              details: {
                branch: "right",
              },
            },
            scopeId: rightScopeId,
          },
        );
        const id = nextId("projection");
        steps.push({
          id,
          kind: "projection",
          dependsOn: [leftStep, rightStep],
          summary: `Apply set operation (${node.op})`,
          phase: "output",
          operation: {
            name: "set_op",
            details: {
              op: node.op,
            },
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
          const cteScopeId = nextId("scope_cte");
          scopes.push({
            id: cteScopeId,
            kind: "cte",
            label: `CTE ${cte.name}`,
            parentId: scopeId,
          });
          const cteInput = visit(cte.query, cteScopeId);
          const cteStepId = nextId("cte");
          steps.push({
            id: cteStepId,
            kind: "cte",
            dependsOn: [cteInput],
            summary: `CTE ${cte.name}`,
            phase: "transform",
            operation: {
              name: "cte",
              details: {
                name: cte.name,
              },
            },
            sqlOrigin: "WITH",
            scopeId: cteScopeId,
          });
          cteStepIds.push(cteStepId);
        }
        const bodyStepId = visit(node.body, scopeId);
        const id = nextId("projection");
        steps.push({
          id,
          kind: "projection",
          dependsOn: [...cteStepIds, bodyStepId],
          summary: "Finalize WITH query",
          phase: "output",
          operation: {
            name: "with",
            details: {
              cteCount: node.ctes.length,
            },
          },
          outputs: node.output.map((column) => column.name),
          sqlOrigin: "WITH",
          scopeId,
        });
        return id;
      }
      case "sql": {
        const id = nextId("remote_fragment");
        steps.push({
          id,
          kind: "remote_fragment",
          dependsOn: [],
          summary: "Execute SQL-shaped relational fragment",
          phase: "fetch",
          operation: {
            name: "provider_fragment",
            details: {
              fragment: "sql",
            },
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

  const tryPlanRemoteFragmentStep = (node: RelNode, scopeId: string): string | null => {
    if (node.kind === "scan" || node.kind === "sql") {
      return null;
    }

    const resolutionResult = resolveSyncProviderCapabilityForRel(input, node);
    if (Result.isError(resolutionResult)) {
      return null;
    }

    const resolution = resolutionResult.value;
    if (
      !resolution ||
      !resolution.fragment ||
      !resolution.provider ||
      !resolution.report?.supported
    ) {
      return null;
    }

    const id = nextId("remote_fragment");
    steps.push({
      id,
      kind: "remote_fragment",
      dependsOn: [],
      summary: `Execute provider fragment (${resolution.fragment.provider})`,
      phase: "fetch",
      operation: {
        name: "provider_fragment",
        details: {
          provider: resolution.fragment.provider,
        },
      },
      request: {
        fragment: resolution.fragment.kind,
      },
      outputs: node.output.map((column) => column.name),
      sqlOrigin: "SELECT",
      scopeId,
      ...(resolution.diagnostics.length > 0 ? { diagnostics: resolution.diagnostics } : {}),
    });
    return id;
  };

  visit(rel, "scope_root");

  return {
    steps,
    scopes,
    ...(diagnostics.length > 0 ? { diagnostics } : {}),
  };
}

function resolveSyncLookupJoinCandidate<TContext>(
  join: Extract<RelNode, { kind: "join" }>,
  input: QuerySessionInput<TContext>,
): {
  leftProvider: string;
  rightProvider: string;
  leftTable: string;
  rightTable: string;
  leftKey: string;
  rightKey: string;
  joinType: "inner" | "left";
} | null {
  if (join.joinType !== "inner" && join.joinType !== "left") {
    return null;
  }

  const leftScan = findFirstScanForPlan(join.left);
  const rightScan = findFirstScanForPlan(join.right);
  if (!leftScan || !rightScan) {
    return null;
  }
  if (
    (!input.schema.tables[leftScan.table] && !leftScan.entity) ||
    (!input.schema.tables[rightScan.table] && !rightScan.entity)
  ) {
    return null;
  }

  const leftBinding = getNormalizedTableBinding(input.schema, leftScan.table);
  const rightBinding = getNormalizedTableBinding(input.schema, rightScan.table);
  if (leftBinding?.kind === "view" || rightBinding?.kind === "view") {
    return null;
  }

  const leftProvider =
    leftScan.entity?.provider ?? resolveTableProvider(input.schema, leftScan.table);
  const rightProvider =
    rightScan.entity?.provider ?? resolveTableProvider(input.schema, rightScan.table);

  const rightAdapter = input.providers[rightProvider];
  if (!rightAdapter || !supportsLookupMany(rightAdapter)) {
    return null;
  }

  const capability = rightAdapter.canExecute(
    {
      kind: "scan",
      provider: rightProvider,
      table: rightScan.entity?.entity ?? rightScan.table,
      request: {
        table: rightScan.entity?.entity ?? rightScan.table,
        select: rightScan.select,
      },
    },
    input.context,
  );
  if (isPromiseLike(capability)) {
    return null;
  }

  return {
    leftProvider,
    rightProvider,
    leftTable: leftScan.table,
    rightTable: rightScan.table,
    leftKey: join.leftKey.column,
    rightKey: join.rightKey.column,
    joinType: join.joinType,
  };
}

function findFirstScanForPlan(node: RelNode): Extract<RelNode, { kind: "scan" }> | null {
  switch (node.kind) {
    case "scan":
      return node;
    case "filter":
    case "project":
    case "aggregate":
    case "window":
    case "sort":
    case "limit_offset":
      return findFirstScanForPlan(node.input);
    case "join":
    case "set_op":
      return findFirstScanForPlan(node.left) ?? findFirstScanForPlan(node.right);
    case "with":
      return findFirstScanForPlan(node.body);
    case "sql":
      return null;
  }
}

function formatColumnRef(ref: { alias?: string; table?: string; column: string }): string {
  const prefix = ref.alias ?? ref.table;
  return prefix ? `${prefix}.${ref.column}` : ref.column;
}

export function routeForStepKind(kind: QueryStepKind): QueryStepRoute | null {
  switch (kind) {
    case "scan":
      return "scan";
    case "lookup_join":
      return "lookup_join";
    case "remote_fragment":
      return "provider_fragment";
    default:
      return "local";
  }
}
