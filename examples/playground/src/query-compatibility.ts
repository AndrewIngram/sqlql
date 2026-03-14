import { relContainsSqlNode } from "@tupl/foundation";
import { defaultSqlAstParser, lowerSqlToRel } from "@tupl/planner";
import type { SchemaDefinition } from "@tupl/schema";

import type {
  CatalogQueryEntry,
  QueryCompatibility,
  QueryCompatibilityMap,
  SchemaParseResult,
} from "./types";

const INVALID_SCHEMA_REASON = "Fix schema TypeScript first.";

function normalizeSql(value: string): string {
  return value.trim().replace(/;+$/u, "").trim();
}

function asReason(error: unknown): string {
  const message = error instanceof Error ? error.message : "Unsupported query for this schema.";
  return message.replace(/\s+/gu, " ").trim();
}

export function checkQueryCompatibility(schema: SchemaDefinition, sql: string): QueryCompatibility {
  const normalizedSql = normalizeSql(sql);
  if (normalizedSql.length === 0) {
    return {
      compatible: false,
      reason: "SQL query cannot be empty.",
    };
  }

  try {
    const parsed = defaultSqlAstParser.astify(normalizedSql);
    if (Array.isArray(parsed)) {
      return {
        compatible: false,
        reason: "Only a single SQL statement is supported.",
      };
    }

    const astType = (parsed as { type?: unknown }).type;
    if (astType !== "select") {
      return {
        compatible: false,
        reason: "Only SELECT statements are currently supported.",
      };
    }

    const lowered = lowerSqlToRel(normalizedSql, schema);
    if (relContainsSqlNode(lowered.rel)) {
      return {
        compatible: false,
        reason:
          "This query shape is not executable in the current provider runtime yet (for example CTE/window, UNION, or subquery-heavy forms).",
      };
    }

    return { compatible: true };
  } catch (error) {
    return {
      compatible: false,
      reason: asReason(error),
    };
  }
}

export function buildQueryCompatibilityMap(
  schemaParse: SchemaParseResult,
  queryCatalog: CatalogQueryEntry[],
): QueryCompatibilityMap {
  const entries: Array<[string, QueryCompatibility]> = [];

  if (!schemaParse.ok || !schemaParse.schema) {
    for (const entry of queryCatalog) {
      entries.push([
        entry.id,
        {
          compatible: false,
          reason: INVALID_SCHEMA_REASON,
        },
      ]);
    }
    return Object.fromEntries(entries);
  }

  for (const entry of queryCatalog) {
    entries.push([entry.id, checkQueryCompatibility(schemaParse.schema, entry.sql)]);
  }

  return Object.fromEntries(entries);
}
