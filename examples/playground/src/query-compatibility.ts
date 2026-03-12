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

function collectCteNames(ast: unknown, names: Set<string>): void {
  if (!ast || typeof ast !== "object") {
    return;
  }

  const withClauses = (ast as { with?: unknown }).with;
  if (!Array.isArray(withClauses)) {
    return;
  }

  for (const clause of withClauses) {
    const stmt = clause as { name?: { value?: unknown }; stmt?: { ast?: unknown } };
    const name = stmt.name?.value;
    if (typeof name === "string") {
      names.add(name);
    }
    collectCteNames(stmt.stmt?.ast, names);
  }
}

function collectReferencedTables(ast: unknown, tables: Set<string>): void {
  if (!ast || typeof ast !== "object") {
    return;
  }

  const fromEntries = (ast as { from?: unknown }).from;
  if (Array.isArray(fromEntries)) {
    for (const entry of fromEntries) {
      const fromEntry = entry as { table?: unknown; expr?: { ast?: unknown } };
      if (typeof fromEntry.table === "string") {
        tables.add(fromEntry.table);
      }
      collectReferencedTables(fromEntry.expr?.ast, tables);
    }
  }

  const nextEntry = (ast as { _next?: unknown })._next;
  if (nextEntry && typeof nextEntry === "object") {
    collectReferencedTables(nextEntry, tables);
  }

  const setOp = (ast as { set_op?: unknown }).set_op;
  if (setOp && typeof setOp === "object") {
    collectReferencedTables(setOp, tables);
  }

  const withClauses = (ast as { with?: unknown }).with;
  if (Array.isArray(withClauses)) {
    for (const clause of withClauses) {
      const stmt = clause as { stmt?: { ast?: unknown } };
      collectReferencedTables(stmt.stmt?.ast, tables);
    }
  }
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

    const cteNames = new Set<string>();
    collectCteNames(parsed, cteNames);

    const referencedTables = new Set<string>();
    collectReferencedTables(parsed, referencedTables);

    const schemaTables = new Set(Object.keys(schema.tables));
    for (const tableName of referencedTables) {
      if (cteNames.has(tableName)) {
        continue;
      }

      if (!schemaTables.has(tableName)) {
        return {
          compatible: false,
          reason: `Table not found in schema: ${tableName}`,
        };
      }
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
