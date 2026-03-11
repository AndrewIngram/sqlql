import type { RelWindowFunction } from "@tupl/foundation";

/**
 * Expr functions own operator/function name normalization and window-function capability checks.
 */
export function mapBinaryOperatorToRelFunction(operator: string): string | null {
  switch (operator) {
    case "=":
      return "eq";
    case "!=":
    case "<>":
      return "neq";
    case ">":
      return "gt";
    case ">=":
      return "gte";
    case "<":
      return "lt";
    case "<=":
      return "lte";
    case "AND":
      return "and";
    case "OR":
      return "or";
    case "+":
      return "add";
    case "-":
      return "subtract";
    case "*":
      return "multiply";
    case "/":
      return "divide";
    case "%":
      return "mod";
    case "||":
      return "concat";
    case "LIKE":
      return "like";
    case "NOT LIKE":
      return "not_like";
    case "IS DISTINCT FROM":
      return "is_distinct_from";
    case "IS NOT DISTINCT FROM":
      return "is_not_distinct_from";
    default:
      return null;
  }
}

export function readWindowFunctionName(expr: {
  type?: unknown;
  name?: unknown;
}): RelWindowFunction["fn"] | null {
  if (expr.type === "aggr_func" && typeof expr.name === "string") {
    const lowered = expr.name.toLowerCase();
    return lowered === "count" ||
      lowered === "sum" ||
      lowered === "avg" ||
      lowered === "min" ||
      lowered === "max"
      ? lowered
      : null;
  }
  if (expr.type !== "function") {
    return null;
  }

  const raw = expr.name as { name?: Array<{ value?: unknown }> } | undefined;
  const head = raw?.name?.[0]?.value;
  if (typeof head !== "string") {
    return null;
  }
  const lowered = head.toLowerCase();
  return lowered === "dense_rank" || lowered === "rank" || lowered === "row_number"
    ? lowered
    : null;
}

export function supportsRankWindowArgs(args: unknown): boolean {
  if (!args || typeof args !== "object") {
    return true;
  }
  const value = (args as { value?: unknown }).value;
  if (!Array.isArray(value)) {
    return true;
  }
  return value.length === 0;
}
