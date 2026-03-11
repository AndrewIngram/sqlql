import type { RelColumnRef } from "@tupl/foundation";

import { resolveColumnDefinition } from "./definition";
import type { SchemaValueCoercion, SqlScalarType, TableColumnDefinition } from "./types";

/**
 * Output definition utilities own shared column-definition helpers for inferred relational outputs.
 */
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

export function resolveRelRefOutputDefinition(
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

export function applyJoinNullability(
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

export function withColumnNullability(
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

export function buildInferredColumnDefinition(
  type: SqlScalarType,
  nullable: boolean,
): TableColumnDefinition {
  return { type, nullable };
}

function toRelOutputKey(ref: RelColumnRef): string | null {
  const alias = ref.alias ?? ref.table;
  return alias ? `${alias}.${ref.column}` : null;
}
