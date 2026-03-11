import type { RelColumnRef, RelExpr, RelNode } from "@tupl/foundation";

import { resolveColumnDefinition } from "./definition";
import { createTableDefinitionFromEntity } from "./normalization";
import type {
  SchemaDefinition,
  SchemaValueCoercion,
  SqlScalarType,
  TableColumnDefinition,
} from "./types";

/**
 * Output inference owns column-definition inference for relational output trees.
 */
export function inferRelOutputDefinitions(
  rel: RelNode,
  schema: SchemaDefinition,
  cteDefinitions: Map<string, Record<string, TableColumnDefinition | undefined>> = new Map(),
): Record<string, TableColumnDefinition | undefined> {
  switch (rel.kind) {
    case "scan":
      return inferScanOutputDefinitions(rel, schema, cteDefinitions);
    case "filter":
    case "sort":
    case "limit_offset":
      return inferRelOutputDefinitions(rel.input, schema, cteDefinitions);
    case "project": {
      const inputDefinitions = inferRelOutputDefinitions(rel.input, schema, cteDefinitions);
      return Object.fromEntries(
        rel.columns.map((mapping) => [
          mapping.output,
          mapping.kind !== "expr"
            ? resolveRelRefOutputDefinition(inputDefinitions, mapping.source)
            : inferRelExprDefinition(mapping.expr, inputDefinitions),
        ]),
      );
    }
    case "join": {
      const leftDefinitions = inferRelOutputDefinitions(rel.left, schema, cteDefinitions);
      const rightDefinitions = inferRelOutputDefinitions(rel.right, schema, cteDefinitions);
      return {
        ...applyJoinNullability(
          leftDefinitions,
          rel.joinType === "right" || rel.joinType === "full",
        ),
        ...applyJoinNullability(
          rightDefinitions,
          rel.joinType === "left" || rel.joinType === "full",
        ),
      };
    }
    case "aggregate": {
      const inputDefinitions = inferRelOutputDefinitions(rel.input, schema, cteDefinitions);
      const out: Record<string, TableColumnDefinition | undefined> = {};

      for (let index = 0; index < rel.groupBy.length; index += 1) {
        const groupRef = rel.groupBy[index];
        const output = rel.output[index];
        if (!groupRef || !output) {
          continue;
        }
        out[output.name] = resolveRelRefOutputDefinition(inputDefinitions, groupRef);
      }

      for (let index = 0; index < rel.metrics.length; index += 1) {
        const metric = rel.metrics[index];
        const output = rel.output[rel.groupBy.length + index];
        if (!metric || !output) {
          continue;
        }
        out[output.name] = inferAggregateMetricDefinition(metric, inputDefinitions);
      }

      return out;
    }
    case "window": {
      const out = {
        ...inferRelOutputDefinitions(rel.input, schema, cteDefinitions),
      };
      for (const fn of rel.functions) {
        out[fn.as] = buildInferredColumnDefinition("integer", false);
      }
      return out;
    }
    case "set_op": {
      const leftDefinitions = inferRelOutputDefinitions(rel.left, schema, cteDefinitions);
      const rightDefinitions = inferRelOutputDefinitions(rel.right, schema, cteDefinitions);
      const out: Record<string, TableColumnDefinition | undefined> = {};
      for (let index = 0; index < rel.output.length; index += 1) {
        const output = rel.output[index];
        const leftOutput = rel.left.output[index];
        const rightOutput = rel.right.output[index];
        if (!output) {
          continue;
        }
        out[output.name] =
          (leftOutput && leftDefinitions[leftOutput.name]) ||
          (rightOutput && rightDefinitions[rightOutput.name]);
      }
      return out;
    }
    case "with": {
      const nextCtes = new Map(cteDefinitions);
      for (const cte of rel.ctes) {
        nextCtes.set(cte.name, inferRelOutputDefinitions(cte.query, schema, nextCtes));
      }
      return inferRelOutputDefinitions(rel.body, schema, nextCtes);
    }
    case "sql":
      return {};
  }
}

export function buildRelOutputCoercion(
  definition: TableColumnDefinition,
): SchemaValueCoercion | undefined {
  const resolved = resolveColumnDefinition(definition);
  switch (resolved.type) {
    case "integer":
    case "real":
      return (value) => {
        if (typeof value === "string" || typeof value === "bigint") {
          return Number(value);
        }
        return value;
      };
    case "boolean":
      return (value) => {
        if (typeof value === "string") {
          if (value === "true" || value === "t") {
            return true;
          }
          if (value === "false" || value === "f") {
            return false;
          }
        }
        if (value === 1) {
          return true;
        }
        if (value === 0) {
          return false;
        }
        return value;
      };
    default:
      return undefined;
  }
}

function inferScanOutputDefinitions(
  rel: Extract<RelNode, { kind: "scan" }>,
  schema: SchemaDefinition,
  cteDefinitions: Map<string, Record<string, TableColumnDefinition | undefined>>,
): Record<string, TableColumnDefinition | undefined> {
  const cteDefinition = cteDefinitions.get(rel.table);
  if (cteDefinition) {
    return Object.fromEntries(
      rel.output.map((output, index) => [
        output.name,
        cteDefinition[rel.select[index] ?? output.name],
      ]),
    );
  }

  const table = schema.tables[rel.table];
  if (!table && rel.entity) {
    const entityTable = createTableDefinitionFromEntity(rel.entity);
    return Object.fromEntries(
      rel.output.map((output, index) => {
        const selected = rel.select[index] ?? output.name;
        const logicalColumn = selected.includes(".")
          ? selected.slice(selected.lastIndexOf(".") + 1)
          : selected;
        return [output.name, entityTable.columns[logicalColumn]];
      }),
    );
  }
  if (!table) {
    return {};
  }

  return Object.fromEntries(
    rel.output.map((output, index) => {
      const selected = rel.select[index] ?? output.name;
      const logicalColumn = selected.includes(".")
        ? selected.slice(selected.lastIndexOf(".") + 1)
        : selected;
      return [output.name, table.columns[logicalColumn]];
    }),
  );
}

function inferAggregateMetricDefinition(
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

function inferRelExprDefinition(
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

function resolveRelRefOutputDefinition(
  definitions: Record<string, TableColumnDefinition | undefined>,
  ref: RelColumnRef,
): TableColumnDefinition | undefined {
  const qualified = toRelOutputKey(ref);
  if (qualified && qualified in definitions) {
    return definitions[qualified];
  }
  if (!ref.alias && !ref.table && ref.column in definitions) {
    return definitions[ref.column];
  }

  const matches = Object.entries(definitions)
    .filter(([name]) => name === ref.column || name.endsWith(`.${ref.column}`))
    .map(([, definition]) => definition);
  return matches.length === 1 ? matches[0] : undefined;
}

function applyJoinNullability(
  definitions: Record<string, TableColumnDefinition | undefined>,
  nullable: boolean,
): Record<string, TableColumnDefinition | undefined> {
  if (!nullable) {
    return definitions;
  }

  return Object.fromEntries(
    Object.entries(definitions).map(([name, definition]) => [
      name,
      definition ? withColumnNullability(definition, true) : undefined,
    ]),
  );
}

function withColumnNullability(
  definition: TableColumnDefinition,
  nullable: boolean,
): TableColumnDefinition {
  const resolved = resolveColumnDefinition(definition);
  if (nullable && resolved.nullable) {
    return definition;
  }

  return {
    type: resolved.type,
    nullable,
    ...(resolved.enum ? { enum: resolved.enum } : {}),
    ...(resolved.enumFrom ? { enumFrom: resolved.enumFrom } : {}),
    ...(resolved.enumMap ? { enumMap: resolved.enumMap } : {}),
    ...(resolved.physicalType ? { physicalType: resolved.physicalType } : {}),
    ...(resolved.physicalDialect ? { physicalDialect: resolved.physicalDialect } : {}),
    ...(resolved.foreignKey ? { foreignKey: resolved.foreignKey } : {}),
    ...(resolved.description ? { description: resolved.description } : {}),
  };
}

function buildInferredColumnDefinition(
  type: SqlScalarType,
  nullable: boolean,
): TableColumnDefinition {
  return { type, nullable };
}

function toRelOutputKey(ref: RelColumnRef): string | null {
  const alias = ref.alias ?? ref.table;
  return alias ? `${alias}.${ref.column}` : null;
}
