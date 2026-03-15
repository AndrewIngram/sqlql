import type { RelExpr, ScanFilterClause } from "@tupl/foundation";

import type { LiteralFilter } from "../planner-types";

type ComparableLiteralFilterOp = Exclude<
  ScanFilterClause["op"],
  "in" | "not_in" | "like" | "not_like" | "is_null" | "is_not_null"
>;

/**
 * Literal filter operators own SQL operator normalization, inversion, and rel-expr construction.
 */
export function literalFilterToRelExpr(filter: LiteralFilter): RelExpr {
  const source: RelExpr = {
    kind: "column",
    ref: {
      alias: filter.alias,
      column: filter.clause.column,
    },
  };

  switch (filter.clause.op) {
    case "in":
    case "not_in":
      return {
        kind: "function",
        name: filter.clause.op,
        args: [
          source,
          ...filter.clause.values.map((value) => ({
            kind: "literal" as const,
            value: toRelLiteralValue(value),
          })),
        ],
      };
    case "is_null":
    case "is_not_null":
      return {
        kind: "function",
        name: filter.clause.op,
        args: [source],
      };
    default:
      return {
        kind: "function",
        name: filter.clause.op,
        args: [
          source,
          {
            kind: "literal",
            value: toRelLiteralValue(filter.clause.value),
          },
        ],
      };
  }
}

export function tryNormalizeBinaryOperator(
  raw: unknown,
): Exclude<ScanFilterClause["op"], never> | null {
  switch (raw) {
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
    case "IN":
      return "in";
    case "NOT IN":
      return "not_in";
    case "LIKE":
      return "like";
    case "NOT LIKE":
      return "not_like";
    case "IS DISTINCT FROM":
      return "is_distinct_from";
    case "IS NOT DISTINCT FROM":
      return "is_not_distinct_from";
    case "IS":
      return "is_null";
    case "IS NOT":
      return "is_not_null";
    default:
      return null;
  }
}

export function invertOperator(op: ComparableLiteralFilterOp): ComparableLiteralFilterOp {
  switch (op) {
    case "eq":
      return "eq";
    case "neq":
      return "neq";
    case "is_distinct_from":
      return "is_distinct_from";
    case "is_not_distinct_from":
      return "is_not_distinct_from";
    case "gt":
      return "lt";
    case "gte":
      return "lte";
    case "lt":
      return "gt";
    case "lte":
      return "gte";
  }
}

export function toRelLiteralValue(value: unknown): string | number | boolean | null {
  if (
    value === null ||
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "boolean"
  ) {
    return value;
  }
  throw new Error(`Unsupported literal filter value: ${JSON.stringify(value)}`);
}
