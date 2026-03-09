import type { DataEntityHandle } from "./data-entity";
import type { AggregateFunction, ScanFilterClause, ScanOrderBy } from "./primitives";

export type RelConvention = `provider:${string}` | "local";

export interface RelColumnRef {
  table?: string;
  alias?: string;
  column: string;
}

export interface RelLiteralExpr {
  kind: "literal";
  value: string | number | boolean | null;
}

export interface RelColumnExpr {
  kind: "column";
  ref: RelColumnRef;
}

export interface RelFunctionExpr {
  kind: "function";
  name: string;
  args: RelExpr[];
}

export interface RelSubqueryExpr {
  kind: "subquery";
  id: string;
  mode: "scalar" | "exists";
  rel: RelNode;
  outputColumn?: string;
}

export type RelExpr = RelLiteralExpr | RelColumnExpr | RelFunctionExpr | RelSubqueryExpr;

export interface RelProjectColumnMapping {
  kind?: "column";
  source: RelColumnRef;
  output: string;
}

export interface RelProjectExprMapping {
  kind: "expr";
  expr: RelExpr;
  output: string;
}

export type RelProjectMapping = RelProjectColumnMapping | RelProjectExprMapping;

export interface RelOutputColumn {
  name: string;
  type?: string;
  nullable?: boolean;
}

export interface RelNodeBase {
  id: string;
  convention: RelConvention;
  output: RelOutputColumn[];
}

export interface RelScanNode extends RelNodeBase {
  kind: "scan";
  table: string;
  entity?: DataEntityHandle<string>;
  alias?: string;
  select: string[];
  where?: ScanFilterClause[];
  orderBy?: ScanOrderBy[];
  limit?: number;
  offset?: number;
}

export interface RelFilterNode extends RelNodeBase {
  kind: "filter";
  input: RelNode;
  where?: ScanFilterClause[];
  expr?: RelExpr;
}

export interface RelProjectNode extends RelNodeBase {
  kind: "project";
  input: RelNode;
  columns: RelProjectMapping[];
}

export function isRelProjectColumnMapping(
  mapping: RelProjectMapping,
): mapping is RelProjectColumnMapping {
  return mapping.kind !== "expr";
}

export interface RelJoinNode extends RelNodeBase {
  kind: "join";
  joinType: "inner" | "left" | "right" | "full" | "semi";
  left: RelNode;
  right: RelNode;
  leftKey: RelColumnRef;
  rightKey: RelColumnRef;
}

export interface RelRankWindowFunction {
  fn: "dense_rank" | "rank" | "row_number";
  as: string;
  partitionBy: RelColumnRef[];
  orderBy: Array<{
    source: RelColumnRef;
    direction: "asc" | "desc";
  }>;
}

export interface RelAggregateWindowFunction {
  fn: "count" | "sum" | "avg" | "min" | "max";
  as: string;
  partitionBy: RelColumnRef[];
  column?: RelColumnRef;
  distinct?: boolean;
  orderBy: Array<{
    source: RelColumnRef;
    direction: "asc" | "desc";
  }>;
}

export type RelWindowFunction = RelRankWindowFunction | RelAggregateWindowFunction;

export interface RelWindowNode extends RelNodeBase {
  kind: "window";
  input: RelNode;
  functions: RelWindowFunction[];
}

export interface RelAggregateMetric {
  fn: AggregateFunction;
  as: string;
  column?: RelColumnRef;
  distinct?: boolean;
}

export interface RelAggregateNode extends RelNodeBase {
  kind: "aggregate";
  input: RelNode;
  groupBy: RelColumnRef[];
  metrics: RelAggregateMetric[];
}

export interface RelSortNode extends RelNodeBase {
  kind: "sort";
  input: RelNode;
  orderBy: Array<{
    source: RelColumnRef;
    direction: "asc" | "desc";
  }>;
}

export interface RelLimitOffsetNode extends RelNodeBase {
  kind: "limit_offset";
  input: RelNode;
  limit?: number;
  offset?: number;
}

export interface RelSetOpNode extends RelNodeBase {
  kind: "set_op";
  op: "union" | "union_all" | "intersect" | "except";
  left: RelNode;
  right: RelNode;
}

export interface RelWithNode extends RelNodeBase {
  kind: "with";
  ctes: Array<{
    name: string;
    query: RelNode;
  }>;
  body: RelNode;
}

export interface RelSqlNode extends RelNodeBase {
  kind: "sql";
  sql: string;
  tables: string[];
}

export type RelNode =
  | RelScanNode
  | RelFilterNode
  | RelProjectNode
  | RelJoinNode
  | RelAggregateNode
  | RelWindowNode
  | RelSortNode
  | RelLimitOffsetNode
  | RelSetOpNode
  | RelWithNode
  | RelSqlNode;

let relIdCounter = 0;

function nextRelId(prefix: string): string {
  relIdCounter += 1;
  return `${prefix}_${relIdCounter}`;
}

export function createSqlRel(sql: string, tables: string[]): RelSqlNode {
  return {
    id: nextRelId("sql"),
    kind: "sql",
    convention: "local",
    sql,
    tables,
    output: [],
  };
}

export function countRelNodes(node: RelNode): number {
  const countExpr = (expr: RelExpr): number => {
    switch (expr.kind) {
      case "literal":
      case "column":
        return 0;
      case "function":
        return expr.args.reduce((sum, arg) => sum + countExpr(arg), 0);
      case "subquery":
        return countRelNodes(expr.rel);
    }
  };

  switch (node.kind) {
    case "scan":
    case "sql":
      return 1;
    case "filter":
      return 1 + countRelNodes(node.input) + (node.expr ? countExpr(node.expr) : 0);
    case "project":
      return (
        1 +
        countRelNodes(node.input) +
        node.columns.reduce(
          (sum, column) => sum + ("expr" in column ? countExpr(column.expr) : 0),
          0,
        )
      );
    case "aggregate":
      return 1 + countRelNodes(node.input);
    case "window":
      return 1 + countRelNodes(node.input);
    case "sort":
    case "limit_offset":
      return 1 + countRelNodes(node.input);
    case "join":
    case "set_op":
      return 1 + countRelNodes(node.left) + countRelNodes(node.right);
    case "with":
      return (
        1 +
        node.ctes.reduce((sum, cte) => sum + countRelNodes(cte.query), 0) +
        countRelNodes(node.body)
      );
  }
}

export function collectRelTables(node: RelNode): string[] {
  const out = new Set<string>();

  const visitExpr = (expr: RelExpr): void => {
    switch (expr.kind) {
      case "literal":
      case "column":
        return;
      case "function":
        for (const arg of expr.args) {
          visitExpr(arg);
        }
        return;
      case "subquery":
        visit(expr.rel);
        return;
    }
  };

  const visit = (current: RelNode): void => {
    switch (current.kind) {
      case "scan":
        out.add(current.table);
        return;
      case "sql":
        for (const table of current.tables) {
          out.add(table);
        }
        return;
      case "filter":
        if (current.expr) {
          visitExpr(current.expr);
        }
        visit(current.input);
        return;
      case "project":
        for (const column of current.columns) {
          if ("expr" in column) {
            visitExpr(column.expr);
          }
        }
        visit(current.input);
        return;
      case "aggregate":
      case "window":
      case "sort":
      case "limit_offset":
        visit(current.input);
        return;
      case "join":
      case "set_op":
        visit(current.left);
        visit(current.right);
        return;
      case "with":
        for (const cte of current.ctes) {
          visit(cte.query);
        }
        visit(current.body);
        return;
    }
  };

  visit(node);
  return [...out];
}
