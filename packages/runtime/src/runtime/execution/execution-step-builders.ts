import type { RelNode } from "@tupl/foundation";

import type { PlanBuildState } from "./explain-shaping";
import { nextPlanId, visitExprSubqueries } from "./explain-shaping";

type VisitExecutionNode = (node: RelNode, scopeId?: string) => string;

/**
 * Execution step builders own leaf and unary execution-plan step construction.
 */
export function buildScanStep(
  state: PlanBuildState,
  node: Extract<RelNode, { kind: "scan" }>,
  scopeId: string,
): string {
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

export function buildFilterStep(
  state: PlanBuildState,
  node: Extract<RelNode, { kind: "filter" }>,
  scopeId: string,
  visit: VisitExecutionNode,
): string {
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

export function buildProjectStep(
  state: PlanBuildState,
  node: Extract<RelNode, { kind: "project" }>,
  scopeId: string,
  visit: VisitExecutionNode,
): string {
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

export function buildAggregateStep(
  state: PlanBuildState,
  node: Extract<RelNode, { kind: "aggregate" }>,
  scopeId: string,
  visit: VisitExecutionNode,
  formatColumnRef: (ref: Extract<RelNode, { kind: "aggregate" }>["groupBy"][number]) => string,
): string {
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

export function buildWindowStep(
  state: PlanBuildState,
  node: Extract<RelNode, { kind: "window" }>,
  scopeId: string,
  visit: VisitExecutionNode,
  formatColumnRef: (
    ref: Extract<RelNode, { kind: "window" }>["functions"][number]["partitionBy"][number],
  ) => string,
): string {
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

export function buildSortStep(
  state: PlanBuildState,
  node: Extract<RelNode, { kind: "sort" }>,
  scopeId: string,
  visit: VisitExecutionNode,
  formatColumnRef: (ref: Extract<RelNode, { kind: "sort" }>["orderBy"][number]["source"]) => string,
): string {
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

export function buildLimitOffsetStep(
  state: PlanBuildState,
  node: Extract<RelNode, { kind: "limit_offset" }>,
  scopeId: string,
  visit: VisitExecutionNode,
): string {
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

export function buildSqlStep(
  state: PlanBuildState,
  node: Extract<RelNode, { kind: "sql" }>,
  scopeId: string,
): string {
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
