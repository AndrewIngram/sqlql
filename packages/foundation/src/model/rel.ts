import type { DataEntityHandle } from "./data-entity";
import type { AggregateFunction, ScanFilterClause, ScanOrderBy } from "./primitives";

/**
 * Relational contracts define the logical IR shared by planner, runtime, and provider normalization.
 * They describe query shape and output semantics, not a backend-specific execution plan.
 */
export type RelConvention = `provider:${string}` | "local";

/** Column refs identify one logical column, optionally qualified by table or alias. */
export interface RelColumnRef {
  table?: string;
  alias?: string;
  column: string;
}

/** Literal expressions embed scalar constants directly into relational expressions. */
export interface RelLiteralExpr {
  kind: "literal";
  value: string | number | boolean | null;
}

/** Column expressions read one logical column from the current relational input row. */
export interface RelColumnExpr {
  kind: "column";
  ref: RelColumnRef;
}

/** Function expressions represent scalar computation over relational expressions. */
export interface RelFunctionExpr {
  kind: "function";
  name: string;
  args: RelExpr[];
}

/**
 * Subquery expressions reference a separately planned relational subtree.
 * `outputColumn` is required only for scalar subqueries that project one value.
 */
export interface RelSubqueryExpr {
  kind: "subquery";
  id: string;
  mode: "scalar" | "exists";
  rel: RelNode;
  outputColumn?: string;
}

/** Relational expressions are the scalar expression vocabulary used inside relational nodes. */
export type RelExpr = RelLiteralExpr | RelColumnExpr | RelFunctionExpr | RelSubqueryExpr;

/** Project-column mappings forward one existing source column into a named output column. */
export interface RelProjectColumnMapping {
  kind?: "column";
  source: RelColumnRef;
  output: string;
}

/** Project-expr mappings create one named output column from a computed expression. */
export interface RelProjectExprMapping {
  kind: "expr";
  expr: RelExpr;
  output: string;
}

/** Project mappings are the full projection vocabulary for relational project nodes. */
export type RelProjectMapping = RelProjectColumnMapping | RelProjectExprMapping;

/** Output columns describe the logical row shape produced by a relational node. */
export interface RelOutputColumn {
  name: string;
  type?: string;
  nullable?: boolean;
}

/**
 * Every relational node carries a stable id, a convention, and its logical output shape.
 * `output` describes post-node columns, not the provider's raw physical row representation.
 */
export interface RelNodeBase {
  id: string;
  convention: RelConvention;
  output: RelOutputColumn[];
}

/** Scan nodes read one table/entity and may carry pushdown-friendly filters, ordering, and pagination. */
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

/** Filter nodes preserve input shape while applying residual filters or computed expressions. */
export interface RelFilterNode extends RelNodeBase {
  kind: "filter";
  input: RelNode;
  where?: ScanFilterClause[];
  expr?: RelExpr;
}

/** Project nodes reshape the row by forwarding or computing a new output column set. */
export interface RelProjectNode extends RelNodeBase {
  kind: "project";
  input: RelNode;
  columns: RelProjectMapping[];
}

/** `isRelProjectColumnMapping` distinguishes forwarded columns from computed projection expressions. */
export function isRelProjectColumnMapping(
  mapping: RelProjectMapping,
): mapping is RelProjectColumnMapping {
  return mapping.kind !== "expr";
}

/** Join nodes combine two relational inputs on one pair of join keys. */
export interface RelJoinNode extends RelNodeBase {
  kind: "join";
  joinType: "inner" | "left" | "right" | "full" | "semi";
  left: RelNode;
  right: RelNode;
  leftKey: RelColumnRef;
  rightKey: RelColumnRef;
}

/** Rank window functions compute rank-style outputs over one partitioned and ordered input. */
export interface RelRankWindowFunction {
  fn: "dense_rank" | "rank" | "row_number";
  as: string;
  partitionBy: RelColumnRef[];
  orderBy: Array<{
    source: RelColumnRef;
    direction: "asc" | "desc";
  }>;
}

/** Aggregate window functions compute aggregate outputs over one partitioned and ordered input. */
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

/** Relational window functions are the supported local/provider-neutral window calculation forms. */
export type RelWindowFunction = RelRankWindowFunction | RelAggregateWindowFunction;

/** Window nodes append window-function outputs without changing the underlying input row count. */
export interface RelWindowNode extends RelNodeBase {
  kind: "window";
  input: RelNode;
  functions: RelWindowFunction[];
}

/** Aggregate metrics define the named aggregate outputs produced by an aggregate node. */
export interface RelAggregateMetric {
  fn: AggregateFunction;
  as: string;
  column?: RelColumnRef;
  distinct?: boolean;
}

/** Aggregate nodes collapse input rows into grouped aggregate outputs. */
export interface RelAggregateNode extends RelNodeBase {
  kind: "aggregate";
  input: RelNode;
  groupBy: RelColumnRef[];
  metrics: RelAggregateMetric[];
}

/** Sort nodes impose a logical output ordering over an input subtree. */
export interface RelSortNode extends RelNodeBase {
  kind: "sort";
  input: RelNode;
  orderBy: Array<{
    source: RelColumnRef;
    direction: "asc" | "desc";
  }>;
}

/** Limit/offset nodes apply pagination to an already ordered or otherwise stable input. */
export interface RelLimitOffsetNode extends RelNodeBase {
  kind: "limit_offset";
  input: RelNode;
  limit?: number;
  offset?: number;
}

/** Set-op nodes combine two aligned inputs using SQL set semantics. */
export interface RelSetOpNode extends RelNodeBase {
  kind: "set_op";
  op: "union" | "union_all" | "intersect" | "except";
  left: RelNode;
  right: RelNode;
}

/** With nodes bind named CTE subqueries for use by the body subtree. */
export interface RelWithNode extends RelNodeBase {
  kind: "with";
  ctes: Array<{
    name: string;
    query: RelNode;
  }>;
  body: RelNode;
}

/**
 * SQL nodes are escape hatches for query shapes not lowered into canonical relational operators.
 * They require provider pushdown and are not executable by the local relational runtime.
 */
export interface RelSqlNode extends RelNodeBase {
  kind: "sql";
  sql: string;
  tables: string[];
}

/** Relational nodes are the full logical IR consumed by runtime and provider normalization. */
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

/** `createSqlRel` wraps an opaque SQL fallback shape as a local-convention relational node. */
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

/**
 * `relContainsSqlNode` detects whether a relational tree or any nested scalar subquery expression
 * still depends on SQL-shaped execution semantics that only a provider can satisfy.
 */
export function relContainsSqlNode(node: RelNode): boolean {
  const exprContainsSqlNode = (expr: RelExpr): boolean => {
    switch (expr.kind) {
      case "literal":
      case "column":
        return false;
      case "function":
        return expr.args.some(exprContainsSqlNode);
      case "subquery":
        return relContainsSqlNode(expr.rel);
    }
  };

  switch (node.kind) {
    case "sql":
      return true;
    case "scan":
      return false;
    case "filter":
      return relContainsSqlNode(node.input) || (node.expr ? exprContainsSqlNode(node.expr) : false);
    case "project":
      return (
        relContainsSqlNode(node.input) ||
        node.columns.some((column) => "expr" in column && exprContainsSqlNode(column.expr))
      );
    case "aggregate":
    case "window":
    case "sort":
    case "limit_offset":
      return relContainsSqlNode(node.input);
    case "join":
    case "set_op":
      return relContainsSqlNode(node.left) || relContainsSqlNode(node.right);
    case "with":
      return (
        node.ctes.some((cte) => relContainsSqlNode(cte.query)) || relContainsSqlNode(node.body)
      );
  }
}

/** `countRelNodes` measures the size of a relational tree for guardrails and diagnostics. */
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

/** `collectRelTables` gathers the set of physical tables referenced anywhere in a relational tree. */
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
