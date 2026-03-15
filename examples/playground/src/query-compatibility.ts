import { Result } from "better-result";

import { lowerSqlToRelResult } from "@tupl/planner";
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

  const lowered = lowerSqlToRelResult(normalizedSql, schema);
  if (Result.isError(lowered)) {
    return {
      compatible: false,
      reason: asReason(lowered.error),
    };
  }

  return { compatible: true };
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
