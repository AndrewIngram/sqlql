import type { DataEntityHandle } from "./data-entity";
import type { AggregateFunction, ScanFilterClause, ScanOrderBy } from "./primitives";

/**
 * Relational contracts define the logical IR shared by planner, runtime, and provider normalization.
 * They describe query shape and output semantics, not a backend-specific execution plan.
 */
export type RelConvention = `provider:${string}` | "local" | "logical";

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

/** Values nodes materialize literal rows without requiring a backing table scan. */
export interface RelValuesNode extends RelNodeBase {
  kind: "values";
  rows: Array<Array<string | number | boolean | null>>;
}

/** CTE refs read a named CTE binding using the same logical scan shape as ordinary table access. */
export interface RelCteRefNode extends RelNodeBase {
  kind: "cte_ref";
  name: string;
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

/** Correlate nodes preserve explicit outer-scope dependency until the planner decorrelates them. */
export interface RelCorrelateNode extends RelNodeBase {
  kind: "correlate";
  left: RelNode;
  right: RelNode;
  correlation: {
    outer: RelColumnRef;
    inner: RelColumnRef;
  };
  apply:
    | {
        kind: "semi";
      }
    | {
        kind: "anti";
      }
    | {
        kind: "scalar_filter";
        comparison: string;
        outerCompare: RelColumnRef;
        correlationColumn: string;
        metricColumn: string;
      }
    | {
        kind: "scalar_project";
        correlationColumn: string;
        metricColumn: string;
        outputColumn: string;
      };
}

/** Rank window functions compute rank-style outputs over one partitioned and ordered input. */
export interface RelWindowFrameBound {
  kind: "unbounded_preceding" | "preceding" | "current_row" | "following" | "unbounded_following";
  offset?: number;
}

export interface RelWindowFrame {
  mode: "rows" | "range" | "groups";
  start: RelWindowFrameBound;
  end: RelWindowFrameBound;
}

export interface RelRankWindowFunction {
  fn: "dense_rank" | "rank" | "row_number";
  as: string;
  partitionBy: RelColumnRef[];
  orderBy: Array<{
    source: RelColumnRef;
    direction: "asc" | "desc";
  }>;
  frame?: RelWindowFrame;
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
  frame?: RelWindowFrame;
}

/** Navigation window functions read ordered values from the current partition without changing row count. */
export interface RelNavigationWindowFunction {
  fn: "first_value" | "lag" | "lead";
  as: string;
  partitionBy: RelColumnRef[];
  value: RelExpr;
  orderBy: Array<{
    source: RelColumnRef;
    direction: "asc" | "desc";
  }>;
  offset?: number;
  defaultExpr?: RelExpr;
  frame?: RelWindowFrame;
}

/** Relational window functions are the supported local/provider-neutral window calculation forms. */
export type RelWindowFunction =
  | RelRankWindowFunction
  | RelAggregateWindowFunction
  | RelNavigationWindowFunction;

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

/** Repeat-union nodes evaluate a recursive relation until a fixpoint is reached. */
export interface RelRepeatUnionNode extends RelNodeBase {
  kind: "repeat_union";
  cteName: string;
  mode: "union" | "union_all";
  seed: RelNode;
  iterative: RelNode;
}

/** Relational nodes are the full logical IR consumed by runtime and provider normalization. */
export type RelNode =
  | RelScanNode
  | RelValuesNode
  | RelCteRefNode
  | RelFilterNode
  | RelProjectNode
  | RelJoinNode
  | RelCorrelateNode
  | RelAggregateNode
  | RelWindowNode
  | RelSortNode
  | RelLimitOffsetNode
  | RelSetOpNode
  | RelWithNode
  | RelRepeatUnionNode;

let relIdCounter = 0;

function nextRelId(prefix: string): string {
  relIdCounter += 1;
  return `${prefix}_${relIdCounter}`;
}

/** `createValuesRel` materializes literal rows without any backing table/provider. */
export function createValuesRel(
  rows: Array<Array<string | number | boolean | null>>,
  output: RelOutputColumn[] = [],
): RelValuesNode {
  return {
    id: nextRelId("values"),
    kind: "values",
    convention: "local",
    rows,
    output,
  };
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
    case "values":
    case "cte_ref":
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
    case "correlate":
      return 1 + countRelNodes(node.left) + countRelNodes(node.right);
    case "join":
    case "set_op":
      return 1 + countRelNodes(node.left) + countRelNodes(node.right);
    case "repeat_union":
      return 1 + countRelNodes(node.seed) + countRelNodes(node.iterative);
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
  const visit = (current: RelNode): void => {
    switch (current.kind) {
      case "scan":
        out.add(current.table);
        return;
      case "values":
      case "cte_ref":
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
      case "correlate":
        visit(current.left);
        visit(current.right);
        return;
      case "join":
      case "set_op":
        visit(current.left);
        visit(current.right);
        return;
      case "repeat_union":
        visit(current.seed);
        visit(current.iterative);
        return;
      case "with":
        for (const cte of current.ctes) {
          visit(cte.query);
        }
        visit(current.body);
        return;
    }
  };

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

  visit(node);
  return [...out];
}
