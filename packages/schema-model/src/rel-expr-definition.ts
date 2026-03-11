import type { RelExpr, RelNode } from "@tupl/foundation";

import { resolveColumnDefinition } from "./definition";
import {
  buildInferredColumnDefinition,
  resolveRelRefOutputDefinition,
  withColumnNullability,
} from "./output-definition-utils";
import type { SqlScalarType, TableColumnDefinition } from "./types";

/**
 * Rel expr definitions own inferred output-definition logic for expressions and aggregate metrics.
 */
export function inferAggregateMetricDefinition(
  metric: Extract<RelNode, { kind: "aggregate" }>["metrics"][number],
  inputDefinitions: Record<string, TableColumnDefinition | undefined>,
): TableColumnDefinition | undefined {
  switch (metric.fn) {
    case "count":
      return buildInferredColumnDefinition("integer", false);
    case "avg":
      return buildInferredColumnDefinition("real", true);
    case "sum": {
      const sourceType = metric.column
        ? resolveColumnDefinition(
            resolveRelRefOutputDefinition(inputDefinitions, metric.column) ??
              buildInferredColumnDefinition("real", true),
          ).type
        : "real";
      return buildInferredColumnDefinition(sourceType === "integer" ? "integer" : "real", true);
    }
    case "min":
    case "max": {
      const sourceDefinition = metric.column
        ? resolveRelRefOutputDefinition(inputDefinitions, metric.column)
        : undefined;
      return sourceDefinition ? withColumnNullability(sourceDefinition, true) : undefined;
    }
  }
}

export function inferRelExprDefinition(
  expr: RelExpr,
  inputDefinitions: Record<string, TableColumnDefinition | undefined>,
): TableColumnDefinition | undefined {
  switch (expr.kind) {
    case "literal":
      return inferLiteralDefinition(expr.value);
    case "column":
      return resolveRelRefOutputDefinition(inputDefinitions, expr.ref);
    case "subquery":
      return expr.mode === "exists" ? buildInferredColumnDefinition("boolean", false) : undefined;
    case "function": {
      const args = expr.args.map((arg) => inferRelExprDefinition(arg, inputDefinitions));
      switch (expr.name) {
        case "eq":
        case "neq":
        case "gt":
        case "gte":
        case "lt":
        case "lte":
        case "and":
        case "or":
        case "not":
        case "like":
        case "not_like":
        case "in":
        case "not_in":
        case "is_null":
        case "is_not_null":
        case "is_distinct_from":
        case "is_not_distinct_from":
        case "between":
          return buildInferredColumnDefinition("boolean", true);
        case "add":
        case "subtract":
        case "multiply":
        case "mod":
        case "abs":
        case "round":
          return buildInferredColumnDefinition(resolveNumericExprType(args), true);
        case "divide":
          return buildInferredColumnDefinition("real", true);
        case "concat":
        case "lower":
        case "upper":
        case "trim":
        case "substr":
          return buildInferredColumnDefinition("text", true);
        case "length":
          return buildInferredColumnDefinition("integer", true);
        case "coalesce":
          return args.find((definition) => definition != null);
        case "nullif":
          return args[0] ? withColumnNullability(args[0], true) : undefined;
        case "case":
          return args.find((_, index) => index % 2 === 1);
        case "cast": {
          const target = expr.args[1];
          if (target?.kind !== "literal" || typeof target.value !== "string") {
            return undefined;
          }
          switch (target.value.toLowerCase()) {
            case "integer":
            case "int":
              return buildInferredColumnDefinition("integer", true);
            case "real":
            case "numeric":
            case "float":
              return buildInferredColumnDefinition("real", true);
            case "boolean":
              return buildInferredColumnDefinition("boolean", true);
            case "text":
              return buildInferredColumnDefinition("text", true);
            default:
              return undefined;
          }
        }
        default:
          return undefined;
      }
    }
  }
}

function inferLiteralDefinition(
  value: string | number | boolean | null,
): TableColumnDefinition | undefined {
  if (value == null) {
    return undefined;
  }
  switch (typeof value) {
    case "string":
      return buildInferredColumnDefinition("text", true);
    case "boolean":
      return buildInferredColumnDefinition("boolean", true);
    case "number":
      return buildInferredColumnDefinition(Number.isInteger(value) ? "integer" : "real", true);
    default:
      return undefined;
  }
}

function resolveNumericExprType(
  definitions: Array<TableColumnDefinition | undefined>,
): SqlScalarType {
  return definitions.some(
    (definition) => definition && resolveColumnDefinition(definition).type === "real",
  )
    ? "real"
    : "integer";
}
