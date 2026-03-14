import type { RelExpr, RelNode, ScanFilterClause, TuplDiagnostic } from "@tupl/foundation";

export type { TuplDiagnostic } from "@tupl/foundation";

export type ProviderRouteFamily = "scan" | "lookup" | "aggregate" | "rel-core" | "rel-advanced";

export type ProviderCapabilityAtom =
  | "scan.project"
  | "scan.filter.basic"
  | "scan.filter.set_membership"
  | "scan.sort"
  | "scan.limit_offset"
  | "lookup.bulk"
  | "aggregate.group_by"
  | "aggregate.having"
  | "join.inner"
  | "join.left"
  | "join.right_full"
  | "set_op.union_all"
  | "set_op.union_distinct"
  | "set_op.intersect"
  | "set_op.except"
  | "cte.non_recursive"
  | "subquery.scalar_uncorrelated"
  | "subquery.exists_uncorrelated"
  | "subquery.in_uncorrelated"
  | "subquery.from"
  | "subquery.correlated"
  | "window.rank_basic"
  | "window.aggregate_default_frame"
  | "window.frame_explicit"
  | "window.navigation"
  | "expr.compare_basic"
  | "expr.like"
  | "expr.in_not_in"
  | "expr.null_distinct"
  | "expr.arithmetic"
  | "expr.case_simple"
  | "expr.case_searched"
  | "expr.string_basic"
  | "expr.numeric_basic"
  | "expr.cast_basic";

export interface QueryFallbackPolicy {
  allowFallback?: boolean;
  warnOnFallback?: boolean;
  rejectOnMissingAtom?: boolean;
  rejectOnEstimatedCost?: boolean;
  maxLocalRows?: number;
  maxLookupFanout?: number;
  maxJoinExpansionRisk?: number;
}

export interface ProviderCapabilityReport {
  supported: boolean;
  reason?: string;
  notes?: string[];
  routeFamily?: ProviderRouteFamily;
  requiredAtoms?: ProviderCapabilityAtom[];
  missingAtoms?: ProviderCapabilityAtom[];
  diagnostics?: TuplDiagnostic[];
  estimatedRows?: number;
  estimatedCost?: number;
  fallbackAllowed?: boolean;
}

export interface ProviderEstimate {
  rows: number;
  cost: number;
}

export function normalizeCapability(
  capability: boolean | ProviderCapabilityReport,
): ProviderCapabilityReport {
  if (typeof capability === "boolean") {
    return capability ? { supported: true } : { supported: false };
  }

  return capability;
}

export function inferRouteFamilyForRel(rel: RelNode) {
  if (rel.kind === "scan") {
    return "scan";
  }
  if (rel.kind === "aggregate") {
    return "aggregate";
  }
  return hasAdvancedRelFeatures(rel) ? "rel-advanced" : "rel-core";
}

export function collectCapabilityAtomsForRel(rel: RelNode): ProviderCapabilityAtom[] {
  const atoms = new Set<ProviderCapabilityAtom>();
  collectCapabilityAtomsForNode(rel, atoms);
  return [...atoms];
}

function collectCapabilityAtomsForNode(node: RelNode, atoms: Set<ProviderCapabilityAtom>): void {
  switch (node.kind) {
    case "scan":
      atoms.add("scan.project");
      for (const clause of node.where ?? []) {
        addFilterAtom(atoms, clause.op);
      }
      if ((node.orderBy ?? []).length > 0) {
        atoms.add("scan.sort");
      }
      if (node.limit != null || node.offset != null) {
        atoms.add("scan.limit_offset");
      }
      return;
    case "filter":
      for (const clause of node.where ?? []) {
        addFilterAtom(atoms, clause.op);
      }
      if (node.expr) {
        collectCapabilityAtomsForExpr(node.expr, atoms);
      }
      collectCapabilityAtomsForNode(node.input, atoms);
      return;
    case "project":
      for (const column of node.columns) {
        if ("expr" in column) {
          collectCapabilityAtomsForExpr(column.expr, atoms);
        }
      }
      collectCapabilityAtomsForNode(node.input, atoms);
      return;
    case "correlate":
      collectCapabilityAtomsForNode(node.left, atoms);
      collectCapabilityAtomsForNode(node.right, atoms);
      return;
    case "join":
      atoms.add(node.joinType === "inner" ? "join.inner" : "join.left");
      if (node.joinType === "right" || node.joinType === "full") {
        atoms.add("join.right_full");
      }
      collectCapabilityAtomsForNode(node.left, atoms);
      collectCapabilityAtomsForNode(node.right, atoms);
      return;
    case "aggregate":
      atoms.add("aggregate.group_by");
      collectCapabilityAtomsForNode(node.input, atoms);
      return;
    case "window":
      if (node.functions.some((fn) => fn.frame != null)) {
        atoms.add("window.frame_explicit");
      }
      if (
        node.functions.some(
          (fn) => fn.fn === "dense_rank" || fn.fn === "rank" || fn.fn === "row_number",
        )
      ) {
        atoms.add("window.rank_basic");
      }
      if (
        node.functions.some(
          (fn) =>
            fn.fn === "count" ||
            fn.fn === "sum" ||
            fn.fn === "avg" ||
            fn.fn === "min" ||
            fn.fn === "max",
        )
      ) {
        atoms.add("window.aggregate_default_frame");
      }
      if (
        node.functions.some((fn) => fn.fn === "lead" || fn.fn === "lag" || fn.fn === "first_value")
      ) {
        atoms.add("window.navigation");
      }
      collectCapabilityAtomsForNode(node.input, atoms);
      return;
    case "sort":
      atoms.add("scan.sort");
      collectCapabilityAtomsForNode(node.input, atoms);
      return;
    case "limit_offset":
      atoms.add("scan.limit_offset");
      collectCapabilityAtomsForNode(node.input, atoms);
      return;
    case "set_op":
      atoms.add(
        node.op === "union_all"
          ? "set_op.union_all"
          : node.op === "union"
            ? "set_op.union_distinct"
            : node.op === "intersect"
              ? "set_op.intersect"
              : "set_op.except",
      );
      collectCapabilityAtomsForNode(node.left, atoms);
      collectCapabilityAtomsForNode(node.right, atoms);
      return;
    case "with":
      atoms.add("cte.non_recursive");
      for (const cte of node.ctes) {
        collectCapabilityAtomsForNode(cte.query, atoms);
      }
      collectCapabilityAtomsForNode(node.body, atoms);
      return;
    case "repeat_union":
      collectCapabilityAtomsForNode(node.seed, atoms);
      collectCapabilityAtomsForNode(node.iterative, atoms);
      return;
  }
}

function collectCapabilityAtomsForExpr(expr: RelExpr, atoms: Set<ProviderCapabilityAtom>): void {
  switch (expr.kind) {
    case "literal":
    case "column":
      return;
    case "subquery":
      atoms.add(
        expr.mode === "exists" ? "subquery.exists_uncorrelated" : "subquery.scalar_uncorrelated",
      );
      collectCapabilityAtomsForNode(expr.rel, atoms);
      return;
    case "function":
      switch (expr.name) {
        case "eq":
        case "neq":
        case "gt":
        case "gte":
        case "lt":
        case "lte":
        case "between":
          atoms.add("expr.compare_basic");
          break;
        case "like":
        case "not_like":
          atoms.add("expr.like");
          break;
        case "in":
        case "not_in":
          atoms.add("expr.in_not_in");
          break;
        case "is_distinct_from":
        case "is_not_distinct_from":
        case "is_null":
        case "is_not_null":
          atoms.add("expr.null_distinct");
          break;
        case "add":
        case "subtract":
        case "multiply":
        case "divide":
        case "mod":
          atoms.add("expr.arithmetic");
          break;
        case "concat":
        case "lower":
        case "upper":
        case "trim":
        case "length":
        case "substr":
        case "coalesce":
        case "nullif":
          atoms.add("expr.string_basic");
          break;
        case "abs":
        case "round":
          atoms.add("expr.numeric_basic");
          break;
        case "cast":
          atoms.add("expr.cast_basic");
          break;
        case "case":
          atoms.add("expr.case_searched");
          break;
      }
      for (const arg of expr.args) {
        collectCapabilityAtomsForExpr(arg, atoms);
      }
      return;
  }
}

function addFilterAtom(atoms: Set<ProviderCapabilityAtom>, op: ScanFilterClause["op"]): void {
  switch (op) {
    case "eq":
    case "neq":
    case "gt":
    case "gte":
    case "lt":
    case "lte":
      atoms.add("scan.filter.basic");
      atoms.add("expr.compare_basic");
      return;
    case "in":
    case "not_in":
      atoms.add("scan.filter.set_membership");
      atoms.add("expr.in_not_in");
      return;
    case "like":
    case "not_like":
      atoms.add("scan.filter.basic");
      atoms.add("expr.like");
      return;
    case "is_distinct_from":
    case "is_not_distinct_from":
      atoms.add("scan.filter.basic");
      atoms.add("expr.null_distinct");
      return;
    case "is_null":
    case "is_not_null":
      atoms.add("scan.filter.basic");
      return;
  }
}

function hasAdvancedRelFeatures(node: RelNode): boolean {
  switch (node.kind) {
    case "window":
    case "correlate":
    case "with":
    case "repeat_union":
    case "set_op":
      return true;
    case "values":
    case "cte_ref":
      return false;
    case "scan":
      return false;
    case "filter":
    case "project":
    case "aggregate":
    case "sort":
    case "limit_offset":
      return hasAdvancedRelFeatures(node.input);
    case "join":
      return hasAdvancedRelFeatures(node.left) || hasAdvancedRelFeatures(node.right);
  }
}
