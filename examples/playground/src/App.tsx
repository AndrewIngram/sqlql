import { startTransition, useCallback, useEffect, useMemo, useRef, useState } from "react";
import type React from "react";
import Editor, { type OnMount } from "@monaco-editor/react";
import { ChevronDown, ChevronRight, Database, SearchCode, Table2, Trash2, X } from "lucide-react";
import type * as Monaco from "monaco-editor";
import { parseAsStringLiteral, useQueryState } from "nuqs";
import type {
  QueryExecutionPlanScope,
  QueryExecutionPlanStep,
  QuerySession,
  QueryStepEvent,
  QueryStepState,
} from "@tupl/core";
import { toSqlDDL, type QueryRow, type SchemaDefinition, type SqlScalarType } from "@tupl/schema";

import { DataGrid } from "@/data-grid";
import {
  addEmptyRow,
  coerceCellInput,
  deleteRow,
  formatCellValue,
  mergeTableRows,
  updateRowCell,
} from "@/data-editing";
import {
  CONTEXT_MODULE_ID,
  buildQueryCatalog,
  DB_PROVIDER_MODULE_ID,
  DEFAULT_CONTEXT_CODE,
  DEFAULT_DB_PROVIDER_CODE,
  DEFAULT_REDIS_PROVIDER_CODE,
  DEFAULT_QUERY_ID,
  DEFAULT_SCENARIO_ID,
  DEFAULT_FACADE_SCHEMA_CODE,
  GENERATED_DB_MODULE_ID,
  FACADE_SCHEMA,
  QUERY_PRESETS,
  REDIS_PROVIDER_MODULE_ID,
  SCENARIO_PRESETS,
  serializeJson,
} from "@/examples";
import { PlanGraph } from "@/PlanGraph";
import { buildQueryCompatibilityMap } from "@/query-compatibility";
import { truncateReason } from "@/query-preview";
import {
  canSelectCatalogQuery,
  CUSTOM_QUERY_ID,
  selectionAfterManualSqlEdit,
  selectionAfterSchemaChange,
} from "@/query-selection-state";
import { SchemaRelationsGraph } from "@/SchemaRelationsGraph";
import { compilePlaygroundInput, createSession, runSessionToCompletion } from "@/session-runtime";
import { SqlPreviewLine } from "@/SqlPreviewLine";
import { registerSqlCompletionProvider } from "@/sql-completion";
import { configureSchemaTypescriptProject } from "@/schema-monaco";
import {
  PLAYGROUND_CONTEXT_FILE_URI,
  PLAYGROUND_DB_PROVIDER_FILE_URI,
  PLAYGROUND_GENERATED_DB_FILE_URI,
  PLAYGROUND_REDIS_PROVIDER_FILE_URI,
  PLAYGROUND_SCHEMA_FILE_URI,
} from "@/playground-workspace";
import { REDIS_INPUT_TABLE_DEFINITION, REDIS_INPUT_TABLE_NAME } from "@/redis-provider";
import { parseDownstreamRowsText, parseFacadeSchemaCode } from "@/validation";
import { DOWNSTREAM_ROWS_SCHEMA, DOWNSTREAM_TABLE_NAMES } from "@/downstream-model";
import {
  type ExecutedProviderOperation,
  isColumnNullable,
  readColumnEnumValues,
  readColumnType,
  type PlaygroundContext,
  type SchemaParseResult,
} from "@/types";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { ScrollArea } from "@/components/ui/scroll-area";
import {
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectLabel,
  SelectSeparator,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { cn } from "@/lib/utils";

const SCHEMA_MODEL_PATH = PLAYGROUND_SCHEMA_FILE_URI;
const SCHEMA_CONTEXT_MODEL_PATH = PLAYGROUND_CONTEXT_FILE_URI;
const SCHEMA_PROVIDER_MODEL_PATH = PLAYGROUND_DB_PROVIDER_FILE_URI;
const SCHEMA_REDIS_PROVIDER_MODEL_PATH = PLAYGROUND_REDIS_PROVIDER_FILE_URI;
const SCHEMA_GENERATED_MODEL_PATH = PLAYGROUND_GENERATED_DB_FILE_URI;
const SCHEMA_DDL_MODEL_PATH = "inmemory://tupl/schema.ddl.sql";
const DOWNSTREAM_DDL_MODEL_PATH = "inmemory://tupl/downstream-schema.ddl.sql";
const SQL_MODEL_PATH = "inmemory://tupl/query.sql";
const CUSTOM_SCENARIO_ID = "__custom_scenario__";
const EXPANDED_QUERY_EDITOR_PADDING_Y_PX = 16;
const EXPANDED_QUERY_EDITOR_DEFAULT_HEIGHT_PX = 120;
const SCHEMA_SPLIT_MIN_PERCENT = 30;
const SCHEMA_SPLIT_MAX_PERCENT = 70;
const QUERY_SPLIT_MIN_PERCENT = 35;
const QUERY_SPLIT_MAX_PERCENT = 80;
const MONACO_INDENT_OPTIONS = {
  detectIndentation: false,
  insertSpaces: true,
  tabSize: 2,
} as const;

type TopTab = "data" | "schema" | "query";
type SchemaTab = "diagram" | "ddl";
type QueryTab = "result" | "explain";
type PostgresWorkspaceMode = "data" | "structure";
type PostgresStructureTab = "table" | "diagram" | "ddl";
type SchemaSourceTab = "schema" | "context" | "provider" | "redis_provider" | "generated";
type DataSourceSection = "postgres" | "redis";

interface EditableStructureColumn {
  name: string;
  type: SqlScalarType;
  physicalType: string;
  enumValues: string[];
  nullable: boolean;
  foreignTable: string;
  foreignColumn: string;
}

function executedOperationSqlModelPath(index: number): string {
  return `inmemory://tupl/executed-operation-${index}.sql`;
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&");
}

function positionFromIndex(sql: string, rawIndex: number): { line: number; column: number } {
  const index = Math.max(0, Math.min(rawIndex, sql.length));
  let line = 1;
  let column = 1;

  for (let i = 0; i < index; i += 1) {
    if (sql[i] === "\n") {
      line += 1;
      column = 1;
    } else {
      column += 1;
    }
  }

  return { line, column };
}

function rangeFromIndex(sql: string, startIndex: number, endIndexExclusive: number) {
  const start = positionFromIndex(sql, startIndex);
  const end = positionFromIndex(sql, Math.max(startIndex + 1, endIndexExclusive));
  return {
    startLineNumber: start.line,
    startColumn: start.column,
    endLineNumber: end.line,
    endColumn: end.column,
  };
}

function findTokenRangeAtPosition(sql: string, position: number) {
  const tokenChar = /[A-Za-z0-9_.$"]/u;
  const clamped = Math.max(0, Math.min(position, Math.max(0, sql.length - 1)));
  let start = clamped;
  let end = clamped + 1;

  while (start > 0 && tokenChar.test(sql[start - 1] ?? "")) {
    start -= 1;
  }
  while (end < sql.length && tokenChar.test(sql[end] ?? "")) {
    end += 1;
  }

  if (start === end) {
    end = Math.min(sql.length, start + 1);
  }
  return rangeFromIndex(sql, start, end);
}

function findIdentifierRange(sql: string, identifier: string) {
  if (!identifier) {
    return undefined;
  }

  const pattern = new RegExp(`\\b${escapeRegExp(identifier)}\\b`, "iu");
  const match = pattern.exec(sql);
  if (!match || match.index == null) {
    return undefined;
  }

  return rangeFromIndex(sql, match.index, match.index + match[0].length);
}

function findQualifiedIdentifierRange(sql: string, qualifier: string, identifier: string) {
  if (!qualifier || !identifier) {
    return undefined;
  }

  const pattern = new RegExp(
    `\\b${escapeRegExp(qualifier)}\\b\\s*\\.\\s*\\b${escapeRegExp(identifier)}\\b`,
    "iu",
  );
  const match = pattern.exec(sql);
  if (!match || match.index == null) {
    return undefined;
  }

  return rangeFromIndex(sql, match.index, match.index + match[0].length);
}

function findClauseBounds(
  sql: string,
  clause: "where" | "order_by",
): {
  start: number;
  end: number;
} | null {
  const source = sql;
  const startMatch =
    clause === "where" ? /\bwhere\b/iu.exec(source) : /\border\s+by\b/iu.exec(source);
  if (!startMatch || startMatch.index == null) {
    return null;
  }

  const start = startMatch.index;
  const remainder = source.slice(start + startMatch[0].length);
  const endPatterns =
    clause === "where"
      ? [
          /\bgroup\s+by\b/iu,
          /\border\s+by\b/iu,
          /\blimit\b/iu,
          /\boffset\b/iu,
          /\bhaving\b/iu,
          /\bunion\b/iu,
          /\bintersect\b/iu,
          /\bexcept\b/iu,
          /;/u,
        ]
      : [/\blimit\b/iu, /\boffset\b/iu, /\bunion\b/iu, /\bintersect\b/iu, /\bexcept\b/iu, /;/u];
  let minRelativeEnd = remainder.length;

  for (const pattern of endPatterns) {
    const match = pattern.exec(remainder);
    if (match?.index == null) {
      continue;
    }
    minRelativeEnd = Math.min(minRelativeEnd, match.index);
  }

  return {
    start,
    end: start + startMatch[0].length + minRelativeEnd,
  };
}

function findQualifiedIdentifierRangeInSlice(
  sql: string,
  qualifier: string,
  identifier: string,
  start: number,
  end: number,
) {
  const slice = sql.slice(start, end);
  const pattern = new RegExp(
    `\\b${escapeRegExp(qualifier)}\\b\\s*\\.\\s*\\b${escapeRegExp(identifier)}\\b`,
    "iu",
  );
  const match = pattern.exec(slice);
  if (!match || match.index == null) {
    return undefined;
  }
  const absoluteStart = start + match.index;
  return rangeFromIndex(sql, absoluteStart, absoluteStart + match[0].length);
}

function findIdentifierRangeInSlice(sql: string, identifier: string, start: number, end: number) {
  const slice = sql.slice(start, end);
  const pattern = new RegExp(`\\b${escapeRegExp(identifier)}\\b`, "iu");
  const match = pattern.exec(slice);
  if (!match || match.index == null) {
    return undefined;
  }
  const absoluteStart = start + match.index;
  return rangeFromIndex(sql, absoluteStart, absoluteStart + match[0].length);
}

function readAliasesForSql(sqlText: string): Map<string, string> {
  const aliases = new Map<string, string>();
  const aliasRegex = /\b(?:from|join)\s+([a-z_][\w]*)\s*(?:as\s+)?([a-z_][\w]*)?/gi;

  let match = aliasRegex.exec(sqlText);
  while (match) {
    const tableName = match[1];
    if (tableName) {
      const alias = (match[2] ?? tableName).toLowerCase();
      aliases.set(alias, tableName);
    }
    match = aliasRegex.exec(sqlText);
  }

  return aliases;
}

function findStringLiteralRange(
  sql: string,
  value: string,
): ReturnType<typeof rangeFromIndex> | undefined {
  const escapedValue = value.replaceAll("'", "''");
  const pattern = new RegExp(`'${escapeRegExp(escapedValue)}'`, "gu");
  const match = pattern.exec(sql);
  if (!match || match.index == null) {
    return undefined;
  }
  return rangeFromIndex(sql, match.index, match.index + match[0].length);
}

function findEnumLiteralRange(
  sql: string,
  tableName: string,
  columnName: string,
  enumValue: string,
): ReturnType<typeof rangeFromIndex> | undefined {
  const aliases = readAliasesForSql(sql);
  const qualifiers = new Set<string>([tableName.toLowerCase()]);
  for (const [alias, resolvedTable] of aliases.entries()) {
    if (resolvedTable.toLowerCase() === tableName.toLowerCase()) {
      qualifiers.add(alias);
    }
  }

  const escapedValue = enumValue.replaceAll("'", "''");
  const literalPattern = new RegExp(`'${escapeRegExp(escapedValue)}'`, "gu");
  let literalMatch = literalPattern.exec(sql);
  while (literalMatch) {
    if (literalMatch.index == null) {
      literalMatch = literalPattern.exec(sql);
      continue;
    }

    const contextStart = Math.max(0, literalMatch.index - 180);
    const contextText = sql.slice(contextStart, literalMatch.index);
    const simpleColumnPattern = new RegExp(`\\b${escapeRegExp(columnName)}\\b`, "iu");
    const qualifiedPatterns = [...qualifiers].map(
      (qualifier) =>
        new RegExp(
          `\\b${escapeRegExp(qualifier)}\\b\\s*\\.\\s*\\b${escapeRegExp(columnName)}\\b`,
          "iu",
        ),
    );
    const hasColumnContext =
      simpleColumnPattern.test(contextText) ||
      qualifiedPatterns.some((pattern) => pattern.test(contextText));

    if (hasColumnContext) {
      return rangeFromIndex(sql, literalMatch.index, literalMatch.index + literalMatch[0].length);
    }

    literalMatch = literalPattern.exec(sql);
  }

  return findStringLiteralRange(sql, enumValue);
}

function inferSqlErrorRange(sql: string, message: string) {
  const unsupportedFilterMatch =
    /^Filtering on\s+([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\s+is not supported by schema policy\.?$/u.exec(
      message,
    );
  if (unsupportedFilterMatch?.[1] && unsupportedFilterMatch?.[2]) {
    const tableName = unsupportedFilterMatch[1];
    const columnName = unsupportedFilterMatch[2];
    const aliases = readAliasesForSql(sql);
    const whereBounds = findClauseBounds(sql, "where");
    const qualifiers = new Set<string>([tableName]);
    for (const [alias, resolvedTable] of aliases.entries()) {
      if (resolvedTable.toLowerCase() === tableName.toLowerCase()) {
        qualifiers.add(alias);
      }
    }

    if (whereBounds) {
      for (const qualifier of qualifiers) {
        const range = findQualifiedIdentifierRangeInSlice(
          sql,
          qualifier,
          columnName,
          whereBounds.start,
          whereBounds.end,
        );
        if (range) {
          return range;
        }
      }
      const range = findIdentifierRangeInSlice(sql, columnName, whereBounds.start, whereBounds.end);
      if (range) {
        return range;
      }
    }

    for (const qualifier of qualifiers) {
      const range = findQualifiedIdentifierRange(sql, qualifier, columnName);
      if (range) {
        return range;
      }
    }
    return findIdentifierRange(sql, columnName);
  }

  const unsupportedSortMatch =
    /^Sorting by\s+([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\s+is not supported by schema policy\.?$/u.exec(
      message,
    );
  if (unsupportedSortMatch?.[1] && unsupportedSortMatch?.[2]) {
    const tableName = unsupportedSortMatch[1];
    const columnName = unsupportedSortMatch[2];
    const aliases = readAliasesForSql(sql);
    const orderByBounds = findClauseBounds(sql, "order_by");
    const qualifiers = new Set<string>([tableName]);
    for (const [alias, resolvedTable] of aliases.entries()) {
      if (resolvedTable.toLowerCase() === tableName.toLowerCase()) {
        qualifiers.add(alias);
      }
    }

    if (orderByBounds) {
      for (const qualifier of qualifiers) {
        const range = findQualifiedIdentifierRangeInSlice(
          sql,
          qualifier,
          columnName,
          orderByBounds.start,
          orderByBounds.end,
        );
        if (range) {
          return range;
        }
      }
      const range = findIdentifierRangeInSlice(
        sql,
        columnName,
        orderByBounds.start,
        orderByBounds.end,
      );
      if (range) {
        return range;
      }
    }

    for (const qualifier of qualifiers) {
      const range = findQualifiedIdentifierRange(sql, qualifier, columnName);
      if (range) {
        return range;
      }
    }
    return findIdentifierRange(sql, columnName);
  }

  const invalidEnumMatch =
    /^Invalid enum value for\s+([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*):\s*(.+?)\.\s+Allowed values:/u.exec(
      message,
    );
  if (invalidEnumMatch?.[1] && invalidEnumMatch?.[2] && invalidEnumMatch?.[3]) {
    try {
      const parsedValue = JSON.parse(invalidEnumMatch[3]);
      if (typeof parsedValue === "string") {
        const literalRange = findEnumLiteralRange(
          sql,
          invalidEnumMatch[1],
          invalidEnumMatch[2],
          parsedValue,
        );
        if (literalRange) {
          return literalRange;
        }
      }
    } catch {
      // fall through to generic handling
    }

    return (
      findQualifiedIdentifierRange(sql, invalidEnumMatch[1], invalidEnumMatch[2]) ??
      findIdentifierRange(sql, invalidEnumMatch[2])
    );
  }

  const parserPositionMatch = /\(at position (\d+)\)\s*$/u.exec(message);
  if (parserPositionMatch?.[1]) {
    return findTokenRangeAtPosition(sql, Number.parseInt(parserPositionMatch[1], 10));
  }

  const unknownQualifiedColumnMatch =
    /^Unknown column:\s*([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)/iu.exec(message);
  if (unknownQualifiedColumnMatch?.[1] && unknownQualifiedColumnMatch?.[2]) {
    return findQualifiedIdentifierRange(
      sql,
      unknownQualifiedColumnMatch[1],
      unknownQualifiedColumnMatch[2],
    );
  }

  const unknownColumnMatch = /^Unknown column:\s*([A-Za-z_][A-Za-z0-9_]*)/iu.exec(message);
  if (unknownColumnMatch?.[1]) {
    return findIdentifierRange(sql, unknownColumnMatch[1]);
  }

  const unknownAliasMatch = /^Unknown table alias:\s*([A-Za-z_][A-Za-z0-9_]*)/iu.exec(message);
  if (unknownAliasMatch?.[1]) {
    return findIdentifierRange(sql, unknownAliasMatch[1]);
  }

  const unknownTableMatch =
    /^Unknown table:\s*([A-Za-z_][A-Za-z0-9_]*)/iu.exec(message) ??
    /^No table methods registered for table:\s*([A-Za-z_][A-Za-z0-9_]*)/iu.exec(message) ??
    /^Table not found in schema:\s*([A-Za-z_][A-Za-z0-9_]*)/iu.exec(message);
  if (unknownTableMatch?.[1]) {
    return findIdentifierRange(sql, unknownTableMatch[1]);
  }

  const ambiguousColumnMatch =
    /^Ambiguous unqualified column reference:\s*([A-Za-z_][A-Za-z0-9_]*)/iu.exec(message);
  if (ambiguousColumnMatch?.[1]) {
    return findIdentifierRange(sql, ambiguousColumnMatch[1]);
  }

  return undefined;
}

function findTableLineNumber(schemaText: string, tableName: string): number | null {
  const regex = new RegExp(
    `^\\s*(?:"${escapeRegExp(tableName)}"|${escapeRegExp(tableName)})\\s*:`,
    "mu",
  );
  const match = regex.exec(schemaText);
  const quoted = schemaText.indexOf(`"${tableName}"`);
  const unquoted = schemaText.indexOf(`${tableName}:`);
  const index = match?.index ?? (quoted >= 0 ? quoted : unquoted);

  if (index < 0) {
    return null;
  }

  return schemaText.slice(0, index).split("\n").length;
}

function extractRowsForEditing(
  schema: SchemaDefinition | undefined,
  rowsText: string,
  parsedRows: Record<string, QueryRow[]> | undefined,
): Record<string, QueryRow[]> {
  if (!schema) {
    return {};
  }

  if (parsedRows) {
    return parsedRows;
  }

  const fallback = Object.fromEntries(
    Object.keys(schema.tables).map((tableName) => [tableName, [] as QueryRow[]]),
  );

  try {
    const parsed = JSON.parse(rowsText) as unknown;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      return fallback;
    }

    for (const tableName of Object.keys(schema.tables)) {
      const tableRows = (parsed as Record<string, unknown>)[tableName];
      if (!Array.isArray(tableRows)) {
        continue;
      }

      const onlyObjects = tableRows.filter(
        (entry) => entry != null && typeof entry === "object" && !Array.isArray(entry),
      );

      fallback[tableName] = onlyObjects as QueryRow[];
    }

    return fallback;
  } catch {
    return fallback;
  }
}

function tableIssueLines(
  issues: Array<{ path: string; message: string }>,
  tableName: string,
): string[] {
  const prefix = `${tableName}`;
  return issues
    .filter((issue) => issue.path === "$" || issue.path.startsWith(prefix))
    .map((issue) => `${issue.path}: ${issue.message}`);
}

function uniqueNonNullValues(rows: QueryRow[], columnName: string): string[] {
  const values = new Set<string>();
  for (const row of rows) {
    const value = row[columnName];
    if (value == null) {
      continue;
    }
    values.add(String(value));
  }
  return [...values].sort((a, b) => a.localeCompare(b));
}

function buildEditableStructureRows(
  schema: SchemaDefinition,
): Record<string, EditableStructureColumn[]> {
  const rowsByTable: Record<string, EditableStructureColumn[]> = {};
  for (const [tableName, tableDefinition] of Object.entries(schema.tables)) {
    const foreignKeysByColumn = new Map<string, { table: string; column: string }>();
    for (const foreignKey of tableDefinition.constraints?.foreignKeys ?? []) {
      if (foreignKey.columns.length !== 1 || foreignKey.references.columns.length !== 1) {
        continue;
      }
      const columnName = foreignKey.columns[0];
      const referencedColumn = foreignKey.references.columns[0];
      if (!columnName || !referencedColumn) {
        continue;
      }
      foreignKeysByColumn.set(columnName, {
        table: foreignKey.references.table,
        column: referencedColumn,
      });
    }

    rowsByTable[tableName] = Object.entries(tableDefinition.columns).map(
      ([columnName, columnDefinition]) => ({
        name: columnName,
        type: readColumnType(columnDefinition),
        physicalType:
          typeof columnDefinition === "string"
            ? mapScalarTypeToPostgresType(columnDefinition)
            : (columnDefinition.physicalType ?? mapScalarTypeToPostgresType(columnDefinition.type)),
        enumValues: typeof columnDefinition === "string" ? [] : [...(columnDefinition.enum ?? [])],
        nullable: isColumnNullable(columnDefinition),
        foreignTable:
          typeof columnDefinition !== "string" && columnDefinition.foreignKey
            ? columnDefinition.foreignKey.table
            : (foreignKeysByColumn.get(columnName)?.table ?? ""),
        foreignColumn:
          typeof columnDefinition !== "string" && columnDefinition.foreignKey
            ? columnDefinition.foreignKey.column
            : (foreignKeysByColumn.get(columnName)?.column ?? ""),
      }),
    );
  }
  return rowsByTable;
}

function quoteIdentifier(identifier: string): string {
  return `"${identifier.replaceAll('"', '""')}"`;
}

function mapScalarTypeToPostgresType(type: SqlScalarType): string {
  switch (type) {
    case "text":
      return "TEXT";
    case "integer":
      return "INTEGER";
    case "real":
      return "DOUBLE PRECISION";
    case "blob":
      return "BYTEA";
    case "boolean":
      return "BOOLEAN";
    case "date":
      return "DATE";
    case "datetime":
    case "timestamp":
      return "TIMESTAMP";
    case "json":
      return "JSONB";
  }
}

function buildPostgresSchemaFromRows(
  rowsByTable: Record<string, EditableStructureColumn[]>,
): SchemaDefinition {
  const tables: SchemaDefinition["tables"] = {};
  for (const tableName of DOWNSTREAM_TABLE_NAMES) {
    const rows = rowsByTable[tableName] ?? [];
    tables[tableName] = {
      provider: "dbProvider",
      columns: Object.fromEntries(
        rows.map((row) => [
          row.name,
          {
            type: row.type,
            nullable: row.nullable,
            ...(row.physicalType.trim().length > 0
              ? { physicalType: row.physicalType.trim() }
              : {}),
            ...(row.enumValues.length > 0 ? { enum: row.enumValues } : {}),
            ...(row.foreignTable.trim().length > 0 && row.foreignColumn.trim().length > 0
              ? {
                  foreignKey: {
                    table: row.foreignTable,
                    column: row.foreignColumn,
                  },
                }
              : {}),
          },
        ]),
      ),
    };
  }

  return {
    tables,
  };
}

function buildPostgresDdlFromRows(rowsByTable: Record<string, EditableStructureColumn[]>): string {
  const enumTypeStatements: string[] = [];
  const seenEnumTypes = new Set<string>();

  for (const rows of Object.values(rowsByTable)) {
    for (const row of rows) {
      const physicalType = row.physicalType.trim();
      if (physicalType.length === 0 || row.enumValues.length === 0) {
        continue;
      }
      if (!/^[A-Za-z_][A-Za-z0-9_]*$/u.test(physicalType)) {
        continue;
      }
      const upper = physicalType.toUpperCase();
      if (
        ["TEXT", "INTEGER", "BOOLEAN", "TIMESTAMP", "TIMESTAMPTZ", "NUMERIC", "JSONB"].includes(
          upper,
        )
      ) {
        continue;
      }
      if (seenEnumTypes.has(physicalType)) {
        continue;
      }
      seenEnumTypes.add(physicalType);
      const valuesSql = row.enumValues
        .map((value) => `'${value.replaceAll("'", "''")}'`)
        .join(", ");
      enumTypeStatements.push(
        `DO $$ BEGIN CREATE TYPE ${quoteIdentifier(physicalType)} AS ENUM (${valuesSql}); EXCEPTION WHEN duplicate_object THEN NULL; END $$;`,
      );
    }
  }

  const tableStatements = DOWNSTREAM_TABLE_NAMES.map((tableName) => {
    const rows = rowsByTable[tableName] ?? [];
    if (rows.length === 0) {
      return `CREATE TABLE IF NOT EXISTS ${quoteIdentifier(tableName)} (\n);\n`;
    }

    const columnLines = rows.map(
      (row) =>
        `  ${quoteIdentifier(row.name)} ${
          row.physicalType.trim().length > 0
            ? row.physicalType.trim()
            : mapScalarTypeToPostgresType(row.type)
        }${row.nullable ? "" : " NOT NULL"}`,
    );

    const hasIdColumn = rows.some((row) => row.name === "id");
    if (hasIdColumn) {
      columnLines.push(`  PRIMARY KEY (${quoteIdentifier("id")})`);
    }

    for (const row of rows) {
      const referencedTable = row.foreignTable.trim();
      const referencedColumn = row.foreignColumn.trim();
      if (referencedTable.length === 0 || referencedColumn.length === 0) {
        continue;
      }

      columnLines.push(
        `  FOREIGN KEY (${quoteIdentifier(row.name)}) REFERENCES ${quoteIdentifier(referencedTable)} (${quoteIdentifier(referencedColumn)})`,
      );
    }

    return `CREATE TABLE IF NOT EXISTS ${quoteIdentifier(tableName)} (\n${columnLines.join(",\n")}\n);\n`;
  }).join("\n");

  return [...enumTypeStatements, tableStatements].join("\n\n");
}

function buildGeneratedDbModuleCode(schema: SchemaDefinition): string {
  const tableNames = Object.keys(schema.tables);
  const tableVarByName = new Map<string, string>();

  const toCamelCase = (value: string): string => {
    const normalized = value
      .replace(/[^A-Za-z0-9]+/gu, " ")
      .trim()
      .split(/\s+/u)
      .map((part, index) => {
        const lower = part.toLowerCase();
        return index === 0 ? lower : `${lower.slice(0, 1).toUpperCase()}${lower.slice(1)}`;
      })
      .join("");
    return normalized.length > 0 ? normalized : "table";
  };

  for (const tableName of tableNames) {
    tableVarByName.set(tableName, `${toCamelCase(tableName)}Table`);
  }

  const tableDefinitions = tableNames
    .map((tableName) => {
      const table = schema.tables[tableName];
      const tableVar = tableVarByName.get(tableName) ?? `${toCamelCase(tableName)}Table`;
      const columns = Object.entries(table?.columns ?? {})
        .map(([columnName, columnDefinition]) => {
          const scalarType = readColumnType(columnDefinition);
          const baseBuilder =
            scalarType === "timestamp"
              ? `timestamp(${JSON.stringify(columnName)}, { mode: "string" })`
              : `${scalarType}(${JSON.stringify(columnName)})`;

          const modifiers: string[] = [];
          if (typeof columnDefinition !== "string" && columnDefinition.primaryKey) {
            modifiers.push("primaryKey()");
          }
          if (!isColumnNullable(columnDefinition)) {
            modifiers.push("notNull()");
          }
          if (typeof columnDefinition !== "string" && columnDefinition.foreignKey) {
            const refVar = tableVarByName.get(columnDefinition.foreignKey.table);
            if (refVar) {
              modifiers.push(
                `references(() => ${refVar}[${JSON.stringify(columnDefinition.foreignKey.column)}])`,
              );
            }
          }

          const chain = modifiers.length > 0 ? `.${modifiers.join(".")}` : "";
          return `  ${JSON.stringify(columnName)}: ${baseBuilder}${chain},`;
        })
        .join("\n");

      return `const ${tableVar} = pgTable(${JSON.stringify(tableName)}, {\n${columns}\n});`;
    })
    .join("\n\n");

  const tableEntries = tableNames
    .map((tableName) => {
      const tableVar = tableVarByName.get(tableName) ?? `${toCamelCase(tableName)}Table`;
      return `  ${JSON.stringify(tableName)}: ${tableVar},`;
    })
    .join("\n");

  return `
// Generated from the downstream Postgres model used by the playground.
// This file is read-only in the editor.
import { boolean, integer, pgTable, text, timestamp } from "drizzle-orm/pg-core";

${tableDefinitions}

export const tables = {
${tableEntries}
} as const;
`.trim();
}

function toDateTimeLocalValue(value: unknown): string {
  if (typeof value !== "string") {
    return "";
  }

  const trimmed = value.trim();
  if (trimmed.length === 0) {
    return "";
  }

  const dateOnly = /^(\d{4}-\d{2}-\d{2})$/u.exec(trimmed);
  if (dateOnly?.[1]) {
    return `${dateOnly[1]}T00:00`;
  }

  const dateTime = /^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2})(?::\d{2}(?:\.\d+)?)?(?:Z)?$/u.exec(
    trimmed,
  );
  if (dateTime?.[1] && dateTime?.[2]) {
    return `${dateTime[1]}T${dateTime[2]}`;
  }

  return "";
}

function JsonBlock({ value }: { value: unknown }): React.JSX.Element {
  return (
    <ScrollArea className="h-40 rounded-md border bg-slate-50 p-2">
      <pre className="font-mono text-xs text-slate-700">{JSON.stringify(value, null, 2)}</pre>
    </ScrollArea>
  );
}

function ExecutedProviderOperationsPanel({
  operations,
  onMonacoMount,
  className,
}: {
  operations: ExecutedProviderOperation[];
  onMonacoMount: OnMount;
  className?: string;
}): React.JSX.Element {
  return (
    <div
      className={cn(
        "flex h-full min-h-0 flex-col overflow-hidden rounded-md border bg-white",
        className,
      )}
    >
      <div className="border-b bg-slate-50 px-3 py-2">
        <div className="text-sm font-semibold text-slate-900">
          Executed provider operations ({operations.length})
        </div>
      </div>
      {operations.length === 0 ? (
        <div className="px-3 py-4 text-xs text-slate-500">
          No provider operations executed for this run.
        </div>
      ) : (
        <ScrollArea className="min-h-0 flex-1 px-3 py-2">
          <div className="space-y-3">
            {operations.map((entry, index) => {
              const isSql = entry.kind === "sql_query";
              const editorHeight = isSql
                ? Math.max(72, Math.min(220, (entry.sql.split("\n").length + 1) * 20))
                : 120;
              return (
                <div key={`executed-query-${index}`} className="overflow-hidden rounded-md border">
                  <div className="border-b bg-slate-50 px-2 py-1">
                    <div className="flex items-center gap-2 text-[11px] text-slate-600">
                      <span>Operation {index + 1}</span>
                      <Badge
                        variant="secondary"
                        className="h-5 rounded-sm px-1.5 text-[10px] font-medium"
                      >
                        {entry.provider}
                      </Badge>
                      <Badge
                        variant="outline"
                        className="h-5 rounded-sm px-1.5 text-[10px] font-medium"
                      >
                        {isSql ? "SQL query" : "Redis lookup"}
                      </Badge>
                    </div>
                  </div>
                  {isSql ? (
                    <Editor
                      path={executedOperationSqlModelPath(index)}
                      language="sql"
                      value={entry.sql}
                      onMount={onMonacoMount}
                      options={{
                        minimap: { enabled: false },
                        fontSize: 12,
                        readOnly: true,
                        scrollBeyondLastLine: false,
                        lineNumbers: "off",
                        wordWrap: "on",
                        ...MONACO_INDENT_OPTIONS,
                      }}
                      height={`${editorHeight}px`}
                    />
                  ) : (
                    <div className="px-2 py-2">
                      <JsonBlock value={entry.lookup} />
                    </div>
                  )}
                  <div className="border-t bg-slate-50 px-2 py-1 font-mono text-[11px] text-slate-600">
                    variables: {JSON.stringify(entry.variables)}
                  </div>
                </div>
              );
            })}
          </div>
        </ScrollArea>
      )}
    </div>
  );
}

function renderRows(
  rows: Array<Record<string, unknown>>,
  options?: { heightClassName?: string; frameClassName?: string; expandNestedObjects?: boolean },
): React.JSX.Element {
  if (rows.length === 0) {
    return <div className="text-sm text-slate-500">No rows.</div>;
  }

  const normalizedRows = options?.expandNestedObjects
    ? rows.map((row) => {
        const out: Record<string, unknown> = {};
        for (const [key, value] of Object.entries(row)) {
          if (value && typeof value === "object" && !Array.isArray(value)) {
            const nestedEntries = Object.entries(value as Record<string, unknown>);
            if (nestedEntries.length === 0) {
              out[key] = null;
              continue;
            }
            for (const [nestedKey, nestedValue] of nestedEntries) {
              out[`${key}.${nestedKey}`] = nestedValue;
            }
            continue;
          }

          out[key] = value;
        }
        return out;
      })
    : rows;

  const columns = [...new Set(normalizedRows.flatMap((row) => Object.keys(row)))];

  return (
    <ScrollArea
      className={cn(
        options?.heightClassName ?? "h-[460px]",
        options?.frameClassName ?? "rounded-md border bg-white",
      )}
    >
      <Table>
        <TableHeader>
          <TableRow>
            {columns.map((column) => (
              <TableHead key={column} className="sticky top-0 bg-slate-100/95">
                {column}
              </TableHead>
            ))}
          </TableRow>
        </TableHeader>
        <TableBody>
          {normalizedRows.map((row, rowIndex) => (
            <TableRow key={`row-${rowIndex}`}>
              {columns.map((column) => (
                <TableCell key={`${rowIndex}:${column}`} className="font-mono text-xs">
                  {JSON.stringify(row[column] ?? null)}
                </TableCell>
              ))}
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </ScrollArea>
  );
}

function StepSection({
  title,
  defaultOpen = true,
  children,
}: {
  title: string;
  defaultOpen?: boolean;
  children: React.ReactNode;
}): React.JSX.Element {
  const [open, setOpen] = useState(defaultOpen);

  return (
    <Collapsible open={open} onOpenChange={setOpen} className="rounded-md border bg-slate-50 p-3">
      <CollapsibleTrigger asChild>
        <Button
          variant="ghost"
          size="sm"
          className="h-auto w-full justify-between px-0 py-0 text-sm font-semibold"
        >
          {title}
          {open ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4" />}
        </Button>
      </CollapsibleTrigger>
      <CollapsibleContent className="mt-3 space-y-2">{children}</CollapsibleContent>
    </Collapsible>
  );
}

export function App(): React.JSX.Element {
  const defaultScenario =
    SCENARIO_PRESETS.find((scenario) => scenario.id === DEFAULT_SCENARIO_ID) ?? SCENARIO_PRESETS[0];
  const defaultQuery =
    QUERY_PRESETS.find((query) => query.id === DEFAULT_QUERY_ID) ?? QUERY_PRESETS[0];

  const [, setActiveScenarioId] = useState(defaultScenario?.id ?? CUSTOM_SCENARIO_ID);
  const [activeTopTab, setActiveTopTab] = useQueryState(
    "view",
    parseAsStringLiteral(["data", "schema", "query"] as const).withDefault("data"),
  );
  const [activeSchemaTab, setActiveSchemaTab] = useState<SchemaTab>("diagram");
  const [activeQueryTab, setActiveQueryTab] = useState<QueryTab>("result");
  const [postgresWorkspaceMode, setPostgresWorkspaceMode] = useState<PostgresWorkspaceMode>("data");
  const [postgresStructureTab, setPostgresStructureTab] = useState<PostgresStructureTab>("table");
  const [activeSchemaSourceTab, setActiveSchemaSourceTab] = useState<SchemaSourceTab>("schema");

  const [schemaCodeText, setSchemaCodeText] = useState(DEFAULT_FACADE_SCHEMA_CODE);
  const [providerCodeText, setProviderCodeText] = useState(DEFAULT_DB_PROVIDER_CODE);
  const [redisProviderCodeText, setRedisProviderCodeText] = useState(DEFAULT_REDIS_PROVIDER_CODE);
  const contextCodeText = DEFAULT_CONTEXT_CODE;
  const [rowsJsonText, setRowsJsonText] = useState(
    defaultScenario ? serializeJson(defaultScenario.rows) : "{}\n",
  );
  const [sqlText, setSqlText] = useState(defaultQuery?.sql ?? "SELECT 1");
  const [orgId, setOrgId] = useState(defaultScenario?.context.orgId ?? "");
  const [userId, setUserId] = useState(defaultScenario?.context.userId ?? "");

  const [selectedSchemaTable, setSelectedSchemaTable] = useState<string | null>(
    Object.keys(FACADE_SCHEMA.tables)[0] ?? null,
  );
  const [selectedDataTable, setSelectedDataTable] = useState<string | null>(
    DOWNSTREAM_TABLE_NAMES[0] ?? null,
  );
  const [selectedDataRowIndex, setSelectedDataRowIndex] = useState<number | null>(0);
  const [downstreamTableFilter, setDownstreamTableFilter] = useState("");
  const [openDataSourceSection, setOpenDataSourceSection] = useState<DataSourceSection>("postgres");
  const [rowEditorDrafts, setRowEditorDrafts] = useState<Record<string, string>>({});
  const [rowEditorErrors, setRowEditorErrors] = useState<Record<string, string>>({});
  const [downstreamStructureRowsByTable, setDownstreamStructureRowsByTable] = useState<
    Record<string, EditableStructureColumn[]>
  >(() => buildEditableStructureRows(DOWNSTREAM_ROWS_SCHEMA));

  const [runtimeError, setRuntimeError] = useState<string | null>(null);
  const [executedOperations, setExecutedOperations] = useState<ExecutedProviderOperation[]>([]);
  const [planSteps, setPlanSteps] = useState<QueryExecutionPlanStep[]>([]);
  const [planScopes, setPlanScopes] = useState<QueryExecutionPlanScope[]>([]);
  const [events, setEvents] = useState<QueryStepEvent[]>([]);
  const [resultRows, setResultRows] = useState<Array<Record<string, unknown>> | null>(null);
  const [selectedStepId, setSelectedStepId] = useState<string | null>(null);
  const [selectedCatalogQueryId, setSelectedCatalogQueryId] = useState(
    defaultScenario?.defaultQueryId ?? defaultQuery?.id ?? CUSTOM_QUERY_ID,
  );
  const [isContextPopoverOpen, setIsContextPopoverOpen] = useState(false);
  const [isQueryEditorExpanded, setIsQueryEditorExpanded] = useState(false);
  const [expandedQueryEditorHeightPx, setExpandedQueryEditorHeightPx] = useState(
    EXPANDED_QUERY_EDITOR_DEFAULT_HEIGHT_PX,
  );
  const [schemaSplitPercent, setSchemaSplitPercent] = useState(50);
  const [isSchemaSplitDragging, setIsSchemaSplitDragging] = useState(false);
  const [querySplitPercent, setQuerySplitPercent] = useState(62);
  const [isQuerySplitDragging, setIsQuerySplitDragging] = useState(false);
  const [sessionTick, setSessionTick] = useState(0);
  const [schemaParse, setSchemaParse] = useState<SchemaParseResult>(() => ({
    ok: true,
    schema: FACADE_SCHEMA,
    issues: [],
  }));

  const monacoRef = useRef<typeof Monaco | null>(null);
  const schemaEditorRef = useRef<Monaco.editor.IStandaloneCodeEditor | null>(null);
  const sqlEditorRef = useRef<Monaco.editor.IStandaloneCodeEditor | null>(null);
  const sqlProviderDisposableRef = useRef<Monaco.IDisposable | null>(null);
  const schemaDecorationIdsRef = useRef<string[]>([]);
  const schemaTypesRegisteredRef = useRef(false);
  const schemaParseRequestIdRef = useRef(0);
  const sqlMarkerRequestIdRef = useRef(0);
  const queryEditorShellRef = useRef<HTMLDivElement | null>(null);
  const contextPopoverRef = useRef<HTMLDivElement | null>(null);
  const schemaWorkspaceRef = useRef<HTMLDivElement | null>(null);
  const queryWorkspaceRef = useRef<HTMLDivElement | null>(null);

  const sessionRef = useRef<QuerySession | null>(null);
  const executionRequestIdRef = useRef(0);
  const schemaForCompletionRef = useRef<SchemaDefinition | null>(FACADE_SCHEMA);
  const clampSchemaSplitPercent = useCallback((value: number): number => {
    return Math.min(SCHEMA_SPLIT_MAX_PERCENT, Math.max(SCHEMA_SPLIT_MIN_PERCENT, value));
  }, []);
  const clampQuerySplitPercent = useCallback((value: number): number => {
    return Math.min(QUERY_SPLIT_MAX_PERCENT, Math.max(QUERY_SPLIT_MIN_PERCENT, value));
  }, []);
  const schemaSplitGridTemplate = useMemo(() => {
    const right = 100 - schemaSplitPercent;
    return `calc(${schemaSplitPercent}% - 5px) 10px calc(${right}% - 5px)`;
  }, [schemaSplitPercent]);
  const querySplitGridTemplate = useMemo(() => {
    const bottom = 100 - querySplitPercent;
    return `calc(${querySplitPercent}% - 0.5px) 1px calc(${bottom}% - 0.5px)`;
  }, [querySplitPercent]);
  const recalculateExpandedQueryEditorHeight = useCallback(() => {
    if (!isQueryEditorExpanded) {
      return;
    }

    const editor = sqlEditorRef.current;
    if (!editor) {
      return;
    }

    try {
      const viewportMaxHeight = Math.max(
        1,
        Math.floor(window.innerHeight * 0.5) - EXPANDED_QUERY_EDITOR_PADDING_Y_PX,
      );
      const contentHeight = Math.max(1, Math.ceil(editor.getContentHeight()));
      const nextHeight = Math.min(contentHeight, viewportMaxHeight);
      setExpandedQueryEditorHeightPx(nextHeight);
    } catch {
      setExpandedQueryEditorHeightPx(EXPANDED_QUERY_EDITOR_DEFAULT_HEIGHT_PX);
    }
  }, [isQueryEditorExpanded]);
  const startSchemaSplitDrag = useCallback((event: React.PointerEvent<HTMLButtonElement>) => {
    event.preventDefault();
    setIsSchemaSplitDragging(true);
  }, []);
  const startQuerySplitDrag = useCallback((event: React.PointerEvent<HTMLButtonElement>) => {
    event.preventDefault();
    setIsQuerySplitDragging(true);
  }, []);

  const queryCatalog = useMemo(() => buildQueryCatalog(QUERY_PRESETS), []);
  const generatedDbCodeText = useMemo(
    () => buildGeneratedDbModuleCode(buildPostgresSchemaFromRows(downstreamStructureRowsByTable)),
    [downstreamStructureRowsByTable],
  );
  const schemaProgramModules = useMemo(
    () => ({
      [CONTEXT_MODULE_ID]: contextCodeText,
      [DB_PROVIDER_MODULE_ID]: providerCodeText,
      [GENERATED_DB_MODULE_ID]: generatedDbCodeText,
      [REDIS_PROVIDER_MODULE_ID]: redisProviderCodeText,
    }),
    [contextCodeText, generatedDbCodeText, providerCodeText, redisProviderCodeText],
  );
  const schemaEditorPath = useMemo(() => {
    switch (activeSchemaSourceTab) {
      case "context":
        return SCHEMA_CONTEXT_MODEL_PATH;
      case "provider":
        return SCHEMA_PROVIDER_MODEL_PATH;
      case "redis_provider":
        return SCHEMA_REDIS_PROVIDER_MODEL_PATH;
      case "generated":
        return SCHEMA_GENERATED_MODEL_PATH;
      case "schema":
      default:
        return SCHEMA_MODEL_PATH;
    }
  }, [activeSchemaSourceTab]);
  const schemaEditorReadOnly =
    activeSchemaSourceTab === "context" || activeSchemaSourceTab === "generated";
  const syncSchemaSourceModels = useCallback((): void => {
    const monaco = monacoRef.current;
    if (!monaco) {
      return;
    }

    const ensureModel = (path: string, value: string): void => {
      const uri = monaco.Uri.parse(path);
      const existing = monaco.editor.getModel(uri);
      if (!existing) {
        monaco.editor.createModel(value, "typescript", uri);
        return;
      }
      if (existing.getValue() !== value) {
        existing.setValue(value);
      }
    };

    ensureModel(SCHEMA_MODEL_PATH, schemaCodeText);
    ensureModel(SCHEMA_CONTEXT_MODEL_PATH, contextCodeText);
    ensureModel(SCHEMA_PROVIDER_MODEL_PATH, providerCodeText);
    ensureModel(SCHEMA_REDIS_PROVIDER_MODEL_PATH, redisProviderCodeText);
    ensureModel(SCHEMA_GENERATED_MODEL_PATH, generatedDbCodeText);
  }, [
    contextCodeText,
    generatedDbCodeText,
    providerCodeText,
    redisProviderCodeText,
    schemaCodeText,
  ]);
  const queryCompatibilityById = useMemo(
    () => buildQueryCompatibilityMap(schemaParse, queryCatalog),
    [queryCatalog, schemaParse],
  );
  const rowsParse = useMemo(() => parseDownstreamRowsText(rowsJsonText), [rowsJsonText]);

  useEffect(() => {
    const requestId = schemaParseRequestIdRef.current + 1;
    schemaParseRequestIdRef.current = requestId;

    void parseFacadeSchemaCode(schemaCodeText, {
      modules: schemaProgramModules,
    })
      .then((nextParse) => {
        if (schemaParseRequestIdRef.current !== requestId) {
          return;
        }
        setSchemaParse(nextParse);
        schemaForCompletionRef.current = nextParse.ok ? (nextParse.schema ?? null) : null;
      })
      .catch((error: unknown) => {
        if (schemaParseRequestIdRef.current !== requestId) {
          return;
        }
        setSchemaParse({
          ok: false,
          issues: [
            {
              path: "schema.ts",
              message: error instanceof Error ? error.message : "Failed to parse schema module.",
            },
          ],
        });
        schemaForCompletionRef.current = null;
      });
  }, [schemaCodeText, schemaProgramModules]);

  useEffect(() => {
    syncSchemaSourceModels();
  }, [syncSchemaSourceModels]);
  useEffect(() => {
    syncSchemaSourceModels();
  }, [activeSchemaSourceTab, syncSchemaSourceModels]);

  const facadeDdlText = useMemo(() => {
    if (!schemaParse.ok || !schemaParse.schema) {
      return "";
    }

    try {
      return toSqlDDL(schemaParse.schema, { ifNotExists: true });
    } catch {
      return "";
    }
  }, [schemaParse]);

  const downstreamStructureSchema = useMemo(
    () => buildPostgresSchemaFromRows(downstreamStructureRowsByTable),
    [downstreamStructureRowsByTable],
  );

  const downstreamDdlText = useMemo(() => {
    return buildPostgresDdlFromRows(downstreamStructureRowsByTable);
  }, [downstreamStructureRowsByTable]);

  const downstreamTableNames = useMemo(
    () => [...DOWNSTREAM_TABLE_NAMES, REDIS_INPUT_TABLE_NAME],
    [],
  );

  const editableRowsByTable = useMemo(
    () =>
      extractRowsForEditing(
        downstreamStructureSchema,
        rowsJsonText,
        rowsParse.ok ? rowsParse.rows : undefined,
      ),
    [downstreamStructureSchema, rowsJsonText, rowsParse],
  );

  const currentDataTable =
    selectedDataTable && downstreamTableNames.includes(selectedDataTable)
      ? selectedDataTable
      : (downstreamTableNames[0] ?? null);

  const currentDataTableDefinition =
    currentDataTable === REDIS_INPUT_TABLE_NAME
      ? REDIS_INPUT_TABLE_DEFINITION
      : currentDataTable
        ? downstreamStructureSchema.tables[currentDataTable]
        : undefined;

  const currentDataRows = currentDataTable ? (editableRowsByTable[currentDataTable] ?? []) : [];
  const currentStructureRows =
    currentDataTable && currentDataTable !== REDIS_INPUT_TABLE_NAME
      ? (downstreamStructureRowsByTable[currentDataTable] ?? [])
      : [];
  const selectedTableSupportsStructure = currentDataTable !== REDIS_INPUT_TABLE_NAME;

  const currentTableIssues =
    !rowsParse.ok && currentDataTable ? tableIssueLines(rowsParse.issues, currentDataTable) : [];
  const filteredPostgresTableNames = useMemo(() => {
    const query = downstreamTableFilter.trim().toLowerCase();
    if (query.length === 0) {
      return DOWNSTREAM_TABLE_NAMES;
    }
    return DOWNSTREAM_TABLE_NAMES.filter((tableName) => tableName.toLowerCase().includes(query));
  }, [downstreamTableFilter]);
  const filteredRedisTableNames = useMemo(() => {
    const query = downstreamTableFilter.trim().toLowerCase();
    if (query.length === 0) {
      return [REDIS_INPUT_TABLE_NAME];
    }
    return REDIS_INPUT_TABLE_NAME.toLowerCase().includes(query) ? [REDIS_INPUT_TABLE_NAME] : [];
  }, [downstreamTableFilter]);
  const hasAnyFilteredTables =
    filteredPostgresTableNames.length > 0 || filteredRedisTableNames.length > 0;
  const selectedDataRow =
    selectedDataRowIndex != null &&
    selectedDataRowIndex >= 0 &&
    selectedDataRowIndex < currentDataRows.length
      ? currentDataRows[selectedDataRowIndex]
      : null;
  const usersByOrg = useMemo(() => {
    const map = new Map<string, string[]>();
    const users = editableRowsByTable.users ?? [];

    for (const row of users) {
      const orgValue = row.org_id;
      const userValue = row.id;
      if (orgValue == null || userValue == null) {
        continue;
      }

      const org = String(orgValue);
      const userIdValue = String(userValue);
      if (org.length === 0 || userIdValue.length === 0) {
        continue;
      }

      const current = map.get(org) ?? [];
      if (!current.includes(userIdValue)) {
        current.push(userIdValue);
      }
      map.set(org, current);
    }

    for (const [org, users] of map.entries()) {
      map.set(
        org,
        [...users].sort((left, right) => left.localeCompare(right)),
      );
    }

    return map;
  }, [editableRowsByTable]);

  const orgByUser = useMemo(() => {
    const map = new Map<string, string>();
    const users = editableRowsByTable.users ?? [];

    for (const row of users) {
      const orgValue = row.org_id;
      const userValue = row.id;
      if (orgValue == null || userValue == null) {
        continue;
      }

      const org = String(orgValue);
      const userIdValue = String(userValue);
      if (org.length === 0 || userIdValue.length === 0) {
        continue;
      }

      map.set(userIdValue, org);
    }

    return map;
  }, [editableRowsByTable]);

  const orgIdOptions = useMemo(() => {
    const orgIds = uniqueNonNullValues(editableRowsByTable.orgs ?? [], "id");
    const withUsers = orgIds.filter((candidate) => (usersByOrg.get(candidate)?.length ?? 0) > 0);
    const fromUsers = [...usersByOrg.keys()];
    return [...new Set([...withUsers, ...fromUsers])].sort((left, right) =>
      left.localeCompare(right),
    );
  }, [editableRowsByTable, usersByOrg]);

  const userIdOptions = useMemo(() => {
    if (orgId.length > 0) {
      return usersByOrg.get(orgId) ?? [];
    }
    const firstOrg = orgIdOptions[0];
    return firstOrg ? (usersByOrg.get(firstOrg) ?? []) : [];
  }, [orgId, orgIdOptions, usersByOrg]);

  const currentStepId = events.length > 0 ? (events[events.length - 1]?.id ?? null) : null;

  const statesById = useMemo(() => {
    const map: Record<string, QueryStepState | undefined> = {};
    const session = sessionRef.current;
    if (!session) {
      return map;
    }

    for (const step of planSteps) {
      map[step.id] = session.getStepState(step.id);
    }

    return map;
  }, [planSteps, sessionTick]);

  const selectedStep = selectedStepId
    ? (planSteps.find((step) => step.id === selectedStepId) ?? null)
    : null;
  const selectedStepState = selectedStep ? statesById[selectedStep.id] : undefined;

  const configureSchemaTypescript = useCallback((): void => {
    const monaco = monacoRef.current;
    if (!monaco || schemaTypesRegisteredRef.current) {
      return;
    }

    configureSchemaTypescriptProject(monaco);
    schemaTypesRegisteredRef.current = true;
  }, []);

  const applySqlMarkers = useCallback((): void => {
    const monaco = monacoRef.current;
    if (!monaco) {
      return;
    }

    const model =
      sqlEditorRef.current?.getModel() ?? monaco.editor.getModel(monaco.Uri.parse(SQL_MODEL_PATH));
    if (!model) {
      return;
    }

    if (!schemaParse.ok || !schemaParse.schema) {
      monaco.editor.setModelMarkers(model, "tupl", [
        {
          severity: monaco.MarkerSeverity.Error,
          message: "Fix schema TypeScript before validating SQL.",
          startLineNumber: 1,
          startColumn: 1,
          endLineNumber: 1,
          endColumn: 1,
        },
      ]);
      return;
    }

    const buildErrorMarkers = (messages: string[]) =>
      messages.map((message) => {
        const range = inferSqlErrorRange(sqlText, message);
        if (range) {
          return {
            severity: monaco.MarkerSeverity.Error,
            message,
            ...range,
          };
        }

        return {
          severity: monaco.MarkerSeverity.Error,
          message,
          startLineNumber: 1,
          startColumn: 1,
          endLineNumber: 1,
          endColumn: Math.max(2, model.getLineMaxColumn(1)),
        };
      });

    const requestId = sqlMarkerRequestIdRef.current + 1;
    sqlMarkerRequestIdRef.current = requestId;

    void compilePlaygroundInput(schemaCodeText, rowsJsonText, sqlText, {
      modules: schemaProgramModules,
    })
      .then((compileResult) => {
        if (sqlMarkerRequestIdRef.current !== requestId) {
          return;
        }

        if (!compileResult.ok) {
          const messages =
            compileResult.issues.length > 0 ? compileResult.issues : ["Invalid SQL."];
          monaco.editor.setModelMarkers(model, "tupl", buildErrorMarkers(messages));
          return;
        }

        if (runtimeError) {
          monaco.editor.setModelMarkers(model, "tupl", buildErrorMarkers([runtimeError]));
          return;
        }

        monaco.editor.setModelMarkers(model, "tupl", []);
      })
      .catch((error: unknown) => {
        if (sqlMarkerRequestIdRef.current !== requestId) {
          return;
        }
        const message = error instanceof Error ? error.message : "Invalid SQL.";
        monaco.editor.setModelMarkers(model, "tupl", buildErrorMarkers([message]));
      });
  }, [rowsJsonText, runtimeError, schemaCodeText, schemaParse, schemaProgramModules, sqlText]);

  useEffect(() => {
    if (!schemaParse.ok || !schemaParse.schema) {
      setSelectedSchemaTable(null);
      return;
    }

    const tableNames = Object.keys(schemaParse.schema.tables);
    const firstTable = tableNames[0] ?? null;

    setSelectedSchemaTable((current) =>
      current && tableNames.includes(current) ? current : firstTable,
    );
  }, [schemaParse]);

  useEffect(() => {
    setSelectedDataTable((current) =>
      current && downstreamTableNames.includes(current)
        ? current
        : (downstreamTableNames[0] ?? null),
    );
  }, [downstreamTableNames]);

  useEffect(() => {
    if (!selectedTableSupportsStructure && postgresWorkspaceMode === "structure") {
      setPostgresWorkspaceMode("data");
    }
  }, [postgresWorkspaceMode, selectedTableSupportsStructure]);

  useEffect(() => {
    if (currentDataTable === REDIS_INPUT_TABLE_NAME) {
      setOpenDataSourceSection("redis");
      return;
    }
    setOpenDataSourceSection("postgres");
  }, [currentDataTable]);

  useEffect(() => {
    setSelectedDataRowIndex((current) => {
      if (currentDataRows.length === 0) {
        return null;
      }
      if (current == null || current < 0 || current >= currentDataRows.length) {
        return 0;
      }
      return current;
    });
  }, [currentDataRows.length, currentDataTable]);

  useEffect(() => {
    if (!selectedDataRow || !currentDataTableDefinition) {
      setRowEditorDrafts({});
      setRowEditorErrors({});
      return;
    }

    const nextDrafts: Record<string, string> = {};
    for (const [columnName] of Object.entries(currentDataTableDefinition.columns)) {
      nextDrafts[columnName] = formatCellValue(selectedDataRow[columnName]);
    }
    setRowEditorDrafts(nextDrafts);
    setRowEditorErrors({});
  }, [selectedDataRow, currentDataTableDefinition]);

  useEffect(() => {
    setSelectedCatalogQueryId((current) =>
      selectionAfterSchemaChange(current, queryCompatibilityById),
    );
  }, [queryCompatibilityById]);

  useEffect(() => {
    const editor = schemaEditorRef.current;
    const monaco = monacoRef.current;
    if (!editor || !monaco) {
      return;
    }

    if (activeSchemaSourceTab !== "schema") {
      schemaDecorationIdsRef.current = editor.deltaDecorations(schemaDecorationIdsRef.current, []);
      return;
    }

    if (!selectedSchemaTable) {
      schemaDecorationIdsRef.current = editor.deltaDecorations(schemaDecorationIdsRef.current, []);
      return;
    }

    const line = findTableLineNumber(schemaCodeText, selectedSchemaTable);
    if (!line) {
      schemaDecorationIdsRef.current = editor.deltaDecorations(schemaDecorationIdsRef.current, []);
      return;
    }

    editor.revealLineInCenter(line);
    schemaDecorationIdsRef.current = editor.deltaDecorations(schemaDecorationIdsRef.current, [
      {
        range: new monaco.Range(line, 1, line, 1),
        options: {
          isWholeLine: true,
          className: "schema-table-highlight",
        },
      },
    ]);
  }, [activeSchemaSourceTab, schemaCodeText, selectedSchemaTable]);

  useEffect(() => {
    applySqlMarkers();
  }, [applySqlMarkers]);

  const markScenarioCustom = (): void => {
    setActiveScenarioId((current) =>
      current === CUSTOM_SCENARIO_ID ? current : CUSTOM_SCENARIO_ID,
    );
  };

  const handleSchemaEditorChange = (value: string | undefined): void => {
    const nextValue = value ?? "";
    const modelPath = schemaEditorRef.current?.getModel()?.uri.toString();
    const sourcePath = modelPath ?? schemaEditorPath;

    if (sourcePath === SCHEMA_GENERATED_MODEL_PATH) {
      return;
    }

    if (sourcePath === SCHEMA_CONTEXT_MODEL_PATH) {
      return;
    }

    if (sourcePath === SCHEMA_PROVIDER_MODEL_PATH) {
      if (nextValue !== providerCodeText) {
        markScenarioCustom();
        setProviderCodeText(nextValue);
      }
      return;
    }

    if (sourcePath === SCHEMA_REDIS_PROVIDER_MODEL_PATH) {
      if (nextValue !== redisProviderCodeText) {
        markScenarioCustom();
        setRedisProviderCodeText(nextValue);
      }
      return;
    }

    if (sourcePath === SCHEMA_MODEL_PATH && nextValue !== schemaCodeText) {
      markScenarioCustom();
      setSchemaCodeText(nextValue);
    }
  };

  useEffect(() => {
    const requestId = executionRequestIdRef.current + 1;
    executionRequestIdRef.current = requestId;

    setRuntimeError(null);
    setExecutedOperations([]);
    setEvents([]);

    void compilePlaygroundInput(schemaCodeText, rowsJsonText, sqlText, {
      modules: schemaProgramModules,
    })
      .then((bundle) => {
        if (executionRequestIdRef.current !== requestId) {
          return undefined;
        }

        if (!bundle.ok) {
          const issueMessage = bundle.issues[0] ?? "Invalid SQL query.";
          setRuntimeError(issueMessage);
          sessionRef.current = null;
          setPlanSteps([]);
          setPlanScopes([]);
          setExecutedOperations([]);
          setSessionTick((tick) => tick + 1);
          return undefined;
        }

        const context: PlaygroundContext = {
          orgId,
          userId,
        };

        return createSession(bundle, context);
      })
      .then((bundle) => {
        if (!bundle) {
          return;
        }
        if (executionRequestIdRef.current !== requestId) {
          return;
        }

        sessionRef.current = bundle.session;
        const freshPlan = bundle.session.getPlan();
        setPlanSteps(freshPlan.steps);
        setPlanScopes(freshPlan.scopes ?? []);
        setSessionTick((tick) => tick + 1);

        return runSessionToCompletion(bundle.session, []);
      })
      .then((snapshot) => {
        if (!snapshot || executionRequestIdRef.current !== requestId) {
          return;
        }

        setEvents(snapshot.events);
        setResultRows(snapshot.result);
        setExecutedOperations(snapshot.executedOperations);
        setSessionTick((tick) => tick + 1);
      })
      .catch((error: unknown) => {
        if (executionRequestIdRef.current !== requestId) {
          return;
        }

        setRuntimeError(error instanceof Error ? error.message : "Failed to execute query.");
        setExecutedOperations([]);
      });
  }, [orgId, rowsJsonText, schemaCodeText, schemaProgramModules, sqlText, userId]);

  const handleMonacoMount: OnMount = (editor, monaco) => {
    monacoRef.current = monaco;
    configureSchemaTypescript();
    syncSchemaSourceModels();

    if (!sqlProviderDisposableRef.current) {
      sqlProviderDisposableRef.current = registerSqlCompletionProvider(
        monaco,
        () => schemaForCompletionRef.current,
      );
    }

    const uri = editor.getModel()?.uri.toString();
    if (uri === SQL_MODEL_PATH) {
      sqlEditorRef.current = editor;
      window.requestAnimationFrame(() => {
        recalculateExpandedQueryEditorHeight();
        applySqlMarkers();
      });
    }

    if (
      uri === SCHEMA_MODEL_PATH ||
      uri === SCHEMA_CONTEXT_MODEL_PATH ||
      uri === SCHEMA_PROVIDER_MODEL_PATH ||
      uri === SCHEMA_REDIS_PROVIDER_MODEL_PATH ||
      uri === SCHEMA_GENERATED_MODEL_PATH
    ) {
      schemaEditorRef.current = editor;
    }
  };

  useEffect(() => {
    if (!isQueryEditorExpanded) {
      return;
    }

    const frameId = window.requestAnimationFrame(() => {
      recalculateExpandedQueryEditorHeight();
    });
    return () => {
      window.cancelAnimationFrame(frameId);
    };
  }, [isQueryEditorExpanded, sqlText, recalculateExpandedQueryEditorHeight]);

  useEffect(() => {
    if (!isQueryEditorExpanded) {
      return;
    }

    const onResize = (): void => {
      recalculateExpandedQueryEditorHeight();
    };
    window.addEventListener("resize", onResize);
    return () => {
      window.removeEventListener("resize", onResize);
    };
  }, [isQueryEditorExpanded, recalculateExpandedQueryEditorHeight]);

  useEffect(() => {
    if (!isSchemaSplitDragging) {
      return;
    }

    const onPointerMove = (event: PointerEvent): void => {
      const workspace = schemaWorkspaceRef.current;
      if (!workspace) {
        return;
      }
      if (window.innerWidth < 1024) {
        return;
      }

      const rect = workspace.getBoundingClientRect();
      if (rect.width <= 0) {
        return;
      }

      const rawPercent = ((event.clientX - rect.left) / rect.width) * 100;
      setSchemaSplitPercent(clampSchemaSplitPercent(rawPercent));
    };

    const onPointerUp = (): void => {
      setIsSchemaSplitDragging(false);
    };

    const previousUserSelect = document.body.style.userSelect;
    const previousCursor = document.body.style.cursor;
    document.body.style.userSelect = "none";
    document.body.style.cursor = "col-resize";

    window.addEventListener("pointermove", onPointerMove);
    window.addEventListener("pointerup", onPointerUp);

    return () => {
      document.body.style.userSelect = previousUserSelect;
      document.body.style.cursor = previousCursor;
      window.removeEventListener("pointermove", onPointerMove);
      window.removeEventListener("pointerup", onPointerUp);
    };
  }, [clampSchemaSplitPercent, isSchemaSplitDragging]);

  useEffect(() => {
    if (!isQuerySplitDragging) {
      return;
    }

    const onPointerMove = (event: PointerEvent): void => {
      const workspace = queryWorkspaceRef.current;
      if (!workspace) {
        return;
      }

      const rect = workspace.getBoundingClientRect();
      if (rect.height <= 0) {
        return;
      }

      const rawPercent = ((event.clientY - rect.top) / rect.height) * 100;
      setQuerySplitPercent(clampQuerySplitPercent(rawPercent));
    };

    const onPointerUp = (): void => {
      setIsQuerySplitDragging(false);
    };

    const previousUserSelect = document.body.style.userSelect;
    const previousCursor = document.body.style.cursor;
    document.body.style.userSelect = "none";
    document.body.style.cursor = "row-resize";

    window.addEventListener("pointermove", onPointerMove);
    window.addEventListener("pointerup", onPointerUp);

    return () => {
      document.body.style.userSelect = previousUserSelect;
      document.body.style.cursor = previousCursor;
      window.removeEventListener("pointermove", onPointerMove);
      window.removeEventListener("pointerup", onPointerUp);
    };
  }, [clampQuerySplitPercent, isQuerySplitDragging]);

  useEffect(() => {
    if (activeTopTab !== "schema") {
      return;
    }

    const frameId = window.requestAnimationFrame(() => {
      schemaEditorRef.current?.layout();
    });

    return () => {
      window.cancelAnimationFrame(frameId);
    };
  }, [activeTopTab, schemaSplitPercent]);

  useEffect(() => {
    const frameId = window.requestAnimationFrame(() => {
      schemaEditorRef.current?.layout();
      sqlEditorRef.current?.layout();
    });

    return () => {
      window.cancelAnimationFrame(frameId);
    };
  }, [
    activeTopTab,
    activeSchemaTab,
    activeQueryTab,
    isQueryEditorExpanded,
    querySplitPercent,
    schemaSplitPercent,
  ]);

  useEffect(() => {
    if (!isQueryEditorExpanded) {
      return;
    }

    const onPointerDown = (event: PointerEvent): void => {
      const shell = queryEditorShellRef.current;
      if (!shell) {
        return;
      }

      if (shell.contains(event.target as Node)) {
        return;
      }

      setIsQueryEditorExpanded(false);
    };

    const onKeyDown = (event: KeyboardEvent): void => {
      if (event.key === "Escape") {
        setIsQueryEditorExpanded(false);
      }
    };

    window.addEventListener("pointerdown", onPointerDown);
    window.addEventListener("keydown", onKeyDown);

    return () => {
      window.removeEventListener("pointerdown", onPointerDown);
      window.removeEventListener("keydown", onKeyDown);
    };
  }, [isQueryEditorExpanded]);

  useEffect(() => {
    if (!isContextPopoverOpen) {
      return;
    }

    const onPointerDown = (event: PointerEvent): void => {
      const shell = contextPopoverRef.current;
      if (!shell) {
        return;
      }

      if (shell.contains(event.target as Node)) {
        return;
      }

      setIsContextPopoverOpen(false);
    };

    const onKeyDown = (event: KeyboardEvent): void => {
      if (event.key === "Escape") {
        setIsContextPopoverOpen(false);
      }
    };

    window.addEventListener("pointerdown", onPointerDown);
    window.addEventListener("keydown", onKeyDown);

    return () => {
      window.removeEventListener("pointerdown", onPointerDown);
      window.removeEventListener("keydown", onKeyDown);
    };
  }, [isContextPopoverOpen]);

  useEffect(() => {
    if (orgIdOptions.length === 0) {
      if (orgId !== "") {
        setOrgId("");
      }
      if (userId !== "") {
        setUserId("");
      }
      return;
    }

    const normalizedOrgId = orgIdOptions.includes(orgId) ? orgId : (orgIdOptions[0] ?? "");
    const usersForOrg = normalizedOrgId.length > 0 ? (usersByOrg.get(normalizedOrgId) ?? []) : [];

    let normalizedUserId = userId;
    if (!usersForOrg.includes(normalizedUserId)) {
      normalizedUserId = usersForOrg[0] ?? "";
    }

    if (normalizedOrgId !== orgId) {
      setOrgId(normalizedOrgId);
    }
    if (normalizedUserId !== userId) {
      setUserId(normalizedUserId);
    }
  }, [orgId, orgIdOptions, userId, usersByOrg]);

  useEffect(() => {
    if (activeTopTab !== "schema") {
      setIsSchemaSplitDragging(false);
    }
    if (activeTopTab !== "query") {
      setIsQueryEditorExpanded(false);
      setIsContextPopoverOpen(false);
    }
  }, [activeTopTab]);

  useEffect(() => {
    return () => {
      sqlProviderDisposableRef.current?.dispose();
      sqlProviderDisposableRef.current = null;
    };
  }, []);

  const handleSelectSchemaTable = (tableName: string): void => {
    setSelectedSchemaTable(tableName);
  };

  const handleSelectStep = (stepId: string): void => {
    setSelectedStepId(stepId);
  };

  const handleCloseStepOverlay = (): void => {
    setSelectedStepId(null);
  };

  const handleSelectDataTable = (tableName: string): void => {
    setSelectedDataTable(tableName);
    setOpenDataSourceSection(tableName === REDIS_INPUT_TABLE_NAME ? "redis" : "postgres");
  };

  const handleSetTableRows = (tableName: string, tableRows: QueryRow[]): void => {
    const merged = mergeTableRows(editableRowsByTable, tableName, tableRows);
    markScenarioCustom();
    setRowsJsonText(serializeJson(merged));
  };

  const handleAddDataRow = (): void => {
    if (!currentDataTable || !currentDataTableDefinition) {
      return;
    }

    const nextRows = addEmptyRow(currentDataRows, currentDataTableDefinition);
    handleSetTableRows(currentDataTable, nextRows);
    setSelectedDataRowIndex(nextRows.length - 1);
    setPostgresWorkspaceMode("data");
  };

  const handleDeleteSelectedDataRow = (): void => {
    if (!currentDataTable || selectedDataRowIndex == null) {
      return;
    }

    handleSetTableRows(currentDataTable, deleteRow(currentDataRows, selectedDataRowIndex));
    setSelectedDataRowIndex((current) => {
      if (current == null) {
        return null;
      }
      if (current <= 0) {
        return 0;
      }
      return current - 1;
    });
  };

  const handleRowEditorFieldChange = (columnName: string, rawValue: string): void => {
    if (
      !currentDataTable ||
      !currentDataTableDefinition ||
      selectedDataRowIndex == null ||
      selectedDataRowIndex < 0 ||
      selectedDataRowIndex >= currentDataRows.length
    ) {
      return;
    }

    const columnDefinition = currentDataTableDefinition.columns[columnName];
    if (!columnDefinition) {
      return;
    }

    setRowEditorDrafts((previous) => ({ ...previous, [columnName]: rawValue }));

    const coercion = coerceCellInput(columnDefinition, rawValue);
    if (!coercion.ok) {
      setRowEditorErrors((previous) => ({ ...previous, [columnName]: coercion.error }));
      return;
    }

    setRowEditorErrors((previous) => {
      if (!(columnName in previous)) {
        return previous;
      }
      const next = { ...previous };
      delete next[columnName];
      return next;
    });

    handleSetTableRows(
      currentDataTable,
      updateRowCell(currentDataRows, selectedDataRowIndex, columnName, coercion.value),
    );
  };

  const handleUpdateStructureColumn = (
    tableName: string,
    rowIndex: number,
    update: Partial<EditableStructureColumn>,
  ): void => {
    setDownstreamStructureRowsByTable((previous) => {
      const rows = previous[tableName] ?? [];
      if (rowIndex < 0 || rowIndex >= rows.length) {
        return previous;
      }

      const nextRows = rows.map((row, index) => (index === rowIndex ? { ...row, ...update } : row));
      return {
        ...previous,
        [tableName]: nextRows,
      };
    });
  };

  const handleAddStructureColumn = (tableName: string): void => {
    setDownstreamStructureRowsByTable((previous) => {
      const rows = previous[tableName] ?? [];
      let suffix = 1;
      let candidate = "new_column";
      const existing = new Set(rows.map((row) => row.name));
      while (existing.has(candidate)) {
        suffix += 1;
        candidate = `new_column_${suffix}`;
      }

      return {
        ...previous,
        [tableName]: [
          ...rows,
          {
            name: candidate,
            type: "text",
            physicalType: "TEXT",
            enumValues: [],
            nullable: true,
            foreignTable: "",
            foreignColumn: "",
          },
        ],
      };
    });
  };

  const handleDeleteStructureColumn = (tableName: string, rowIndex: number): void => {
    setDownstreamStructureRowsByTable((previous) => {
      const rows = previous[tableName] ?? [];
      if (rows.length <= 1 || rowIndex < 0 || rowIndex >= rows.length) {
        return previous;
      }

      return {
        ...previous,
        [tableName]: rows.filter((_, index) => index !== rowIndex),
      };
    });
  };

  const setSqlTextDeferred = useCallback((nextValue: string): void => {
    startTransition(() => {
      setSqlText(nextValue);
    });
  }, []);

  const handleSqlTextChange = (nextValue: string): void => {
    if (nextValue === sqlText) {
      return;
    }

    setSelectedCatalogQueryId(selectionAfterManualSqlEdit());
    setSqlTextDeferred(nextValue);
  };

  const handleCatalogQuerySelect = (queryId: string): void => {
    if (queryId === CUSTOM_QUERY_ID) {
      setSelectedCatalogQueryId(CUSTOM_QUERY_ID);
      return;
    }

    if (!canSelectCatalogQuery(queryId, queryCompatibilityById)) {
      return;
    }

    const queryEntry = queryCatalog.find((entry) => entry.id === queryId);
    if (!queryEntry) {
      return;
    }

    setSelectedCatalogQueryId(queryEntry.id);
    setSqlTextDeferred(queryEntry.sql);
    setIsQueryEditorExpanded(false);
  };

  return (
    <div className="h-screen w-screen overflow-hidden">
      <Tabs
        value={activeTopTab}
        onValueChange={(value) => void setActiveTopTab(value as TopTab)}
        className="h-full"
      >
        <div className="flex h-full min-h-0 flex-col">
          <header className="shrink-0 border-b bg-background shadow-sm">
            <div className="grid gap-2 px-2 py-2 lg:grid-cols-[auto_minmax(0,1fr)_auto] lg:items-center">
              <TabsList className="gap-1">
                <TabsTrigger
                  value="data"
                  title="PostgreSQL workspace"
                  aria-label="PostgreSQL workspace"
                  className="px-2.5"
                >
                  <Database className="h-4 w-4" />
                  <span className="sr-only">PostgreSQL workspace</span>
                </TabsTrigger>
                <TabsTrigger
                  value="schema"
                  title="tupl schema"
                  aria-label="tupl schema"
                  className="px-2.5"
                >
                  <Table2 className="h-4 w-4" />
                  <span className="sr-only">tupl schema</span>
                </TabsTrigger>
                <TabsTrigger value="query" title="Query" aria-label="Query" className="px-2.5">
                  <SearchCode className="h-4 w-4" />
                  <span className="sr-only">Query</span>
                </TabsTrigger>
              </TabsList>

              <div className="min-w-0">
                {activeTopTab === "query" ? (
                  <div className="grid gap-2 lg:grid-cols-[minmax(180px,260px)_minmax(0,1fr)_auto] lg:items-center">
                    <Select value={selectedCatalogQueryId} onValueChange={handleCatalogQuerySelect}>
                      <SelectTrigger>
                        <SelectValue placeholder="Preset query" />
                      </SelectTrigger>
                      <SelectContent className="min-w-[520px]">
                        <SelectItem value={CUSTOM_QUERY_ID}>Custom</SelectItem>
                        <SelectSeparator />
                        <SelectGroup>
                          <SelectLabel>Query presets</SelectLabel>
                          {queryCatalog.map((entry) => {
                            const compatibility = queryCompatibilityById[entry.id];
                            const compatible = compatibility?.compatible === true;
                            const reason = compatibility?.reason ?? "Unsupported for this schema.";

                            return (
                              <SelectItem
                                key={entry.id}
                                value={entry.id}
                                disabled={!compatible}
                                title={!compatible ? reason : undefined}
                              >
                                <div className="flex min-w-0 flex-col">
                                  <span>{entry.label}</span>
                                  {!compatible ? (
                                    <span className="text-xs text-muted-foreground">
                                      {truncateReason(reason)}
                                    </span>
                                  ) : null}
                                </div>
                              </SelectItem>
                            );
                          })}
                        </SelectGroup>
                      </SelectContent>
                    </Select>

                    <div ref={queryEditorShellRef} className="relative min-w-0">
                      <SqlPreviewLine
                        monaco={monacoRef.current}
                        sql={sqlText}
                        onActivate={() => setIsQueryEditorExpanded(true)}
                      />
                      {isQueryEditorExpanded ? (
                        <div className="query-editor-overlay absolute inset-x-0 top-0 overflow-visible rounded-md border bg-white px-3 py-2 shadow-2xl">
                          <Editor
                            path={SQL_MODEL_PATH}
                            language="sql"
                            value={sqlText}
                            onMount={handleMonacoMount}
                            onChange={(value) => handleSqlTextChange(value ?? "")}
                            options={{
                              minimap: { enabled: false },
                              fontSize: 13,
                              scrollBeyondLastLine: false,
                              fixedOverflowWidgets: true,
                              suggestOnTriggerCharacters: true,
                              quickSuggestions: {
                                other: true,
                                comments: false,
                                strings: true,
                              },
                              quickSuggestionsDelay: 0,
                              ...MONACO_INDENT_OPTIONS,
                            }}
                            height={`${expandedQueryEditorHeightPx}px`}
                          />
                        </div>
                      ) : null}
                    </div>

                    <div ref={contextPopoverRef} className="relative">
                      <Button
                        type="button"
                        variant="outline"
                        size="sm"
                        onClick={() => setIsContextPopoverOpen((open) => !open)}
                      >
                        Context
                      </Button>
                      {isContextPopoverOpen ? (
                        <div className="absolute right-0 top-10 z-40 w-72 rounded-md border bg-white p-3 shadow-xl">
                          <div className="mb-2 text-sm font-medium text-slate-900">
                            Query context
                          </div>
                          <div className="space-y-3">
                            <div className="space-y-1">
                              <label className="text-xs text-slate-600">orgId</label>
                              <select
                                className="h-8 w-full rounded-md border border-slate-200 bg-white px-2 text-xs"
                                value={orgId}
                                disabled={orgIdOptions.length === 0}
                                onChange={(event) => {
                                  const nextOrgId = event.target.value;
                                  const nextUserOptions = usersByOrg.get(nextOrgId) ?? [];
                                  markScenarioCustom();
                                  setOrgId(nextOrgId);
                                  setUserId(
                                    nextUserOptions.includes(userId)
                                      ? userId
                                      : (nextUserOptions[0] ?? ""),
                                  );
                                }}
                              >
                                {orgIdOptions.map((option) => (
                                  <option key={option} value={option}>
                                    {option}
                                  </option>
                                ))}
                              </select>
                            </div>
                            <div className="space-y-1">
                              <label className="text-xs text-slate-600">userId</label>
                              <select
                                className="h-8 w-full rounded-md border border-slate-200 bg-white px-2 text-xs"
                                value={userId}
                                disabled={userIdOptions.length === 0}
                                onChange={(event) => {
                                  const nextUserId = event.target.value;
                                  const nextOrgId = orgByUser.get(nextUserId);
                                  markScenarioCustom();
                                  setUserId(nextUserId);
                                  if (nextOrgId && nextOrgId !== orgId) {
                                    setOrgId(nextOrgId);
                                  }
                                }}
                              >
                                {userIdOptions.map((option) => (
                                  <option key={option} value={option}>
                                    {option}
                                  </option>
                                ))}
                              </select>
                            </div>
                          </div>
                        </div>
                      ) : null}
                    </div>
                  </div>
                ) : (
                  <div className="truncate text-xs text-slate-500">
                    Edit downstream data, facade schema, then run queries.
                  </div>
                )}
              </div>

              <div className="flex items-center gap-2 justify-self-start lg:justify-self-end">
                {activeTopTab === "schema" ? (
                  <Tabs
                    value={activeSchemaTab}
                    onValueChange={(value) => setActiveSchemaTab(value as SchemaTab)}
                  >
                    <TabsList className="gap-1">
                      <TabsTrigger value="diagram">Diagram</TabsTrigger>
                      <TabsTrigger value="ddl">DDL</TabsTrigger>
                    </TabsList>
                  </Tabs>
                ) : null}

                {activeTopTab === "query" ? (
                  <Tabs
                    value={activeQueryTab}
                    onValueChange={(value) => setActiveQueryTab(value as QueryTab)}
                  >
                    <TabsList className="gap-1">
                      <TabsTrigger value="result">Result</TabsTrigger>
                      <TabsTrigger value="explain">Explain</TabsTrigger>
                    </TabsList>
                  </Tabs>
                ) : null}
              </div>
            </div>
          </header>

          <div className="min-h-0 flex-1 overflow-hidden bg-background p-2">
            <TabsContent value="data" forceMount className="mt-0 h-full min-h-0">
              {downstreamTableNames.length > 0 ? (
                <div className="grid h-full min-h-0 gap-2 lg:grid-cols-[240px_minmax(0,1fr)]">
                  <div className="flex min-h-0 flex-col rounded-md border bg-slate-50 p-2">
                    <input
                      value={downstreamTableFilter}
                      onChange={(event) => setDownstreamTableFilter(event.target.value)}
                      placeholder="Search tables..."
                      className="h-8 rounded-md border border-slate-200 bg-white px-2 text-xs"
                    />
                    <ScrollArea className="mt-2 min-h-0 flex-1">
                      <div className="space-y-2 pr-1">
                        {!hasAnyFilteredTables ? (
                          <div className="rounded-md border border-dashed px-3 py-2 text-xs text-slate-500">
                            No tables match the filter.
                          </div>
                        ) : null}

                        <Collapsible
                          open={openDataSourceSection === "postgres"}
                          onOpenChange={(open) => {
                            if (!open) {
                              return;
                            }
                            setOpenDataSourceSection("postgres");
                          }}
                          className="overflow-hidden rounded-md border bg-white"
                        >
                          <CollapsibleTrigger className="flex w-full items-center justify-between px-3 py-2 text-left">
                            <span className="text-xs font-semibold uppercase tracking-wide text-slate-600">
                              PostgreSQL ({filteredPostgresTableNames.length})
                            </span>
                            {openDataSourceSection === "postgres" ? (
                              <ChevronDown className="h-4 w-4 text-slate-500" />
                            ) : (
                              <ChevronRight className="h-4 w-4 text-slate-500" />
                            )}
                          </CollapsibleTrigger>
                          <CollapsibleContent className="border-t bg-slate-50/60 px-2 py-2">
                            {filteredPostgresTableNames.length > 0 ? (
                              <div className="space-y-1">
                                {filteredPostgresTableNames.map((tableName) => (
                                  <button
                                    type="button"
                                    key={tableName}
                                    className={cn(
                                      "w-full rounded-md px-3 py-2 text-left text-sm",
                                      currentDataTable === tableName
                                        ? "bg-background text-foreground shadow"
                                        : "hover:bg-slate-100",
                                    )}
                                    onClick={() => handleSelectDataTable(tableName)}
                                  >
                                    {tableName}
                                  </button>
                                ))}
                              </div>
                            ) : (
                              <div className="px-2 py-1 text-xs text-slate-500">
                                No PostgreSQL tables match the filter.
                              </div>
                            )}
                          </CollapsibleContent>
                        </Collapsible>

                        <Collapsible
                          open={openDataSourceSection === "redis"}
                          onOpenChange={(open) => {
                            if (!open) {
                              return;
                            }
                            setOpenDataSourceSection("redis");
                          }}
                          className="overflow-hidden rounded-md border bg-white"
                        >
                          <CollapsibleTrigger className="flex w-full items-center justify-between px-3 py-2 text-left">
                            <span className="text-xs font-semibold uppercase tracking-wide text-slate-600">
                              Redis ({filteredRedisTableNames.length})
                            </span>
                            {openDataSourceSection === "redis" ? (
                              <ChevronDown className="h-4 w-4 text-slate-500" />
                            ) : (
                              <ChevronRight className="h-4 w-4 text-slate-500" />
                            )}
                          </CollapsibleTrigger>
                          <CollapsibleContent className="border-t bg-slate-50/60 px-2 py-2">
                            {filteredRedisTableNames.length > 0 ? (
                              <div className="space-y-1">
                                {filteredRedisTableNames.map((tableName) => (
                                  <button
                                    type="button"
                                    key={tableName}
                                    className={cn(
                                      "w-full rounded-md px-3 py-2 text-left text-sm",
                                      currentDataTable === tableName
                                        ? "bg-background text-foreground shadow"
                                        : "hover:bg-slate-100",
                                    )}
                                    onClick={() => handleSelectDataTable(tableName)}
                                  >
                                    {tableName}
                                  </button>
                                ))}
                              </div>
                            ) : (
                              <div className="px-2 py-1 text-xs text-slate-500">
                                No Redis tables match the filter.
                              </div>
                            )}
                          </CollapsibleContent>
                        </Collapsible>
                      </div>
                    </ScrollArea>
                  </div>

                  <div className="min-h-0 overflow-hidden rounded-md border bg-white">
                    {currentDataTable && currentDataTableDefinition ? (
                      <div className="flex h-full min-h-0 flex-col">
                        <div className="min-h-0 flex-1 overflow-hidden">
                          {postgresWorkspaceMode === "data" || !selectedTableSupportsStructure ? (
                            <div className="grid h-full min-h-0 grid-cols-[minmax(0,1fr)_340px]">
                              <div className="min-h-0 overflow-hidden border-r">
                                <DataGrid
                                  table={currentDataTableDefinition}
                                  rows={currentDataRows}
                                  onRowsChange={(nextRows) =>
                                    handleSetTableRows(currentDataTable, nextRows)
                                  }
                                  selectedRowIndex={selectedDataRowIndex}
                                  onSelectRow={setSelectedDataRowIndex}
                                  editable={false}
                                  scrollAreaClassName="h-full rounded-none border-0 bg-white"
                                />
                              </div>
                              <div className="min-h-0 overflow-hidden border-l bg-slate-50">
                                {selectedDataRow ? (
                                  <ScrollArea className="h-full p-3">
                                    <div className="space-y-3 pr-2">
                                      <div className="flex items-start justify-between gap-2">
                                        <div className="space-y-1">
                                          <div className="text-sm font-semibold text-slate-900">
                                            {currentDataTable} row{" "}
                                            {selectedDataRowIndex != null
                                              ? selectedDataRowIndex + 1
                                              : "?"}
                                          </div>
                                          <p className="text-xs text-slate-500">
                                            Edit all fields in one place.
                                          </p>
                                        </div>
                                        <Button
                                          type="button"
                                          variant="outline"
                                          size="sm"
                                          onClick={handleDeleteSelectedDataRow}
                                        >
                                          Delete
                                        </Button>
                                      </div>
                                      {Object.entries(currentDataTableDefinition.columns).map(
                                        ([columnName, columnDefinition]) => {
                                          const type = readColumnType(columnDefinition);
                                          const enumValues =
                                            readColumnEnumValues(columnDefinition) ?? [];
                                          const foreignKey =
                                            typeof columnDefinition === "string"
                                              ? undefined
                                              : columnDefinition.foreignKey;
                                          const foreignKeyChoices = foreignKey
                                            ? uniqueNonNullValues(
                                                editableRowsByTable[foreignKey.table] ?? [],
                                                foreignKey.column,
                                              )
                                            : [];
                                          const inputValue =
                                            rowEditorDrafts[columnName] ??
                                            formatCellValue(selectedDataRow[columnName]);
                                          const error = rowEditorErrors[columnName];
                                          const timestampValue = toDateTimeLocalValue(inputValue);
                                          const timestampFallback =
                                            type === "timestamp" &&
                                            inputValue.length > 0 &&
                                            timestampValue.length === 0;
                                          return (
                                            <div
                                              key={columnName}
                                              className="space-y-1 rounded-md border bg-white p-2"
                                            >
                                              <div className="flex items-center justify-between gap-2">
                                                <label className="truncate text-xs font-medium text-slate-700">
                                                  {columnName}
                                                </label>
                                                <span className="text-[11px] text-slate-500">
                                                  {type}
                                                  {isColumnNullable(columnDefinition)
                                                    ? " | nullable"
                                                    : ""}
                                                </span>
                                              </div>
                                              {foreignKey ? (
                                                <select
                                                  className="h-8 w-full rounded-md border border-slate-200 bg-white px-2 text-xs"
                                                  value={
                                                    inputValue.length === 0
                                                      ? "__null__"
                                                      : inputValue
                                                  }
                                                  onChange={(event) =>
                                                    handleRowEditorFieldChange(
                                                      columnName,
                                                      event.target.value === "__null__"
                                                        ? ""
                                                        : event.target.value,
                                                    )
                                                  }
                                                >
                                                  {isColumnNullable(columnDefinition) ? (
                                                    <option value="__null__"></option>
                                                  ) : null}
                                                  {inputValue.length > 0 &&
                                                  !foreignKeyChoices.includes(inputValue) ? (
                                                    <option value={inputValue}>{inputValue}</option>
                                                  ) : null}
                                                  {foreignKeyChoices.map((value) => (
                                                    <option key={value} value={value}>
                                                      {value}
                                                    </option>
                                                  ))}
                                                </select>
                                              ) : enumValues.length > 0 ? (
                                                <select
                                                  className="h-8 w-full rounded-md border border-slate-200 bg-white px-2 text-xs"
                                                  value={
                                                    inputValue.length === 0
                                                      ? "__null__"
                                                      : inputValue
                                                  }
                                                  onChange={(event) =>
                                                    handleRowEditorFieldChange(
                                                      columnName,
                                                      event.target.value === "__null__"
                                                        ? ""
                                                        : event.target.value,
                                                    )
                                                  }
                                                >
                                                  {isColumnNullable(columnDefinition) ? (
                                                    <option value="__null__"></option>
                                                  ) : null}
                                                  {enumValues.map((value) => (
                                                    <option key={value} value={value}>
                                                      {value}
                                                    </option>
                                                  ))}
                                                </select>
                                              ) : type === "boolean" ? (
                                                <select
                                                  className="h-8 w-full rounded-md border border-slate-200 bg-white px-2 text-xs"
                                                  value={
                                                    inputValue.length === 0
                                                      ? "__null__"
                                                      : inputValue === "true"
                                                        ? "true"
                                                        : "false"
                                                  }
                                                  onChange={(event) =>
                                                    handleRowEditorFieldChange(
                                                      columnName,
                                                      event.target.value === "__null__"
                                                        ? ""
                                                        : event.target.value,
                                                    )
                                                  }
                                                >
                                                  {isColumnNullable(columnDefinition) ? (
                                                    <option value="__null__"></option>
                                                  ) : null}
                                                  <option value="true">true</option>
                                                  <option value="false">false</option>
                                                </select>
                                              ) : type === "integer" ? (
                                                <input
                                                  type="number"
                                                  step={1}
                                                  inputMode="numeric"
                                                  className="h-8 w-full rounded-md border border-slate-200 px-2 font-mono text-xs"
                                                  value={inputValue}
                                                  onChange={(event) =>
                                                    handleRowEditorFieldChange(
                                                      columnName,
                                                      event.target.value,
                                                    )
                                                  }
                                                />
                                              ) : type === "timestamp" && !timestampFallback ? (
                                                <input
                                                  type="datetime-local"
                                                  className="h-8 w-full rounded-md border border-slate-200 px-2 font-mono text-xs"
                                                  value={timestampValue}
                                                  onChange={(event) =>
                                                    handleRowEditorFieldChange(
                                                      columnName,
                                                      event.target.value,
                                                    )
                                                  }
                                                />
                                              ) : (
                                                <input
                                                  className="h-8 w-full rounded-md border border-slate-200 px-2 font-mono text-xs"
                                                  value={inputValue}
                                                  onChange={(event) =>
                                                    handleRowEditorFieldChange(
                                                      columnName,
                                                      event.target.value,
                                                    )
                                                  }
                                                />
                                              )}
                                              {error ? (
                                                <div className="text-[11px] text-rose-600">
                                                  {error}
                                                </div>
                                              ) : null}
                                            </div>
                                          );
                                        },
                                      )}
                                    </div>
                                  </ScrollArea>
                                ) : (
                                  <div className="flex h-full items-center justify-center p-4 text-sm text-slate-500">
                                    Select a row to edit details.
                                  </div>
                                )}
                              </div>
                            </div>
                          ) : (
                            <div className="flex h-full min-h-0 flex-col">
                              <div className="border-b p-2">
                                <Tabs
                                  value={postgresStructureTab}
                                  onValueChange={(value) =>
                                    setPostgresStructureTab(value as PostgresStructureTab)
                                  }
                                >
                                  <TabsList className="gap-1">
                                    <TabsTrigger value="table">Table</TabsTrigger>
                                    <TabsTrigger value="diagram">Diagram</TabsTrigger>
                                    <TabsTrigger value="ddl">DDL</TabsTrigger>
                                  </TabsList>
                                </Tabs>
                              </div>
                              <div className="min-h-0 flex-1 overflow-hidden p-2">
                                {postgresStructureTab === "table" ? (
                                  <div className="flex h-full min-h-0 flex-col overflow-hidden rounded-md border bg-white">
                                    <ScrollArea className="min-h-0 flex-1">
                                      <Table>
                                        <TableHeader>
                                          <TableRow>
                                            <TableHead>Column</TableHead>
                                            <TableHead>Logical</TableHead>
                                            <TableHead>Physical Type</TableHead>
                                            <TableHead>Enum Values</TableHead>
                                            <TableHead>Nullable</TableHead>
                                            <TableHead>References</TableHead>
                                            <TableHead className="w-12 text-right"></TableHead>
                                          </TableRow>
                                        </TableHeader>
                                        <TableBody>
                                          {currentStructureRows.map((column, rowIndex) => (
                                            <TableRow key={`${column.name}:${rowIndex}`}>
                                              <TableCell>
                                                <input
                                                  className="h-8 w-full rounded-md border border-slate-200 px-2 font-mono text-xs"
                                                  value={column.name}
                                                  onChange={(event) =>
                                                    handleUpdateStructureColumn(
                                                      currentDataTable,
                                                      rowIndex,
                                                      { name: event.target.value },
                                                    )
                                                  }
                                                />
                                              </TableCell>
                                              <TableCell>
                                                <select
                                                  className="h-8 w-full rounded-md border border-slate-200 bg-white px-2 text-xs"
                                                  value={column.type}
                                                  onChange={(event) =>
                                                    handleUpdateStructureColumn(
                                                      currentDataTable,
                                                      rowIndex,
                                                      { type: event.target.value as SqlScalarType },
                                                    )
                                                  }
                                                >
                                                  <option value="text">text</option>
                                                  <option value="integer">integer</option>
                                                  <option value="real">real</option>
                                                  <option value="blob">blob</option>
                                                  <option value="boolean">boolean</option>
                                                  <option value="timestamp">timestamp</option>
                                                  <option value="date">date</option>
                                                  <option value="datetime">datetime</option>
                                                  <option value="json">json</option>
                                                </select>
                                              </TableCell>
                                              <TableCell>
                                                <input
                                                  className="h-8 w-full rounded-md border border-slate-200 px-2 font-mono text-xs"
                                                  value={column.physicalType}
                                                  onChange={(event) =>
                                                    handleUpdateStructureColumn(
                                                      currentDataTable,
                                                      rowIndex,
                                                      { physicalType: event.target.value },
                                                    )
                                                  }
                                                />
                                              </TableCell>
                                              <TableCell>
                                                <input
                                                  className="h-8 w-full rounded-md border border-slate-200 px-2 font-mono text-xs"
                                                  value={column.enumValues.join(", ")}
                                                  placeholder="draft, paid, shipped"
                                                  onChange={(event) =>
                                                    handleUpdateStructureColumn(
                                                      currentDataTable,
                                                      rowIndex,
                                                      {
                                                        enumValues: event.target.value
                                                          .split(",")
                                                          .map((value) => value.trim())
                                                          .filter((value) => value.length > 0),
                                                      },
                                                    )
                                                  }
                                                />
                                              </TableCell>
                                              <TableCell>
                                                <label className="inline-flex items-center gap-2 text-xs text-slate-700">
                                                  <input
                                                    type="checkbox"
                                                    checked={column.nullable}
                                                    onChange={(event) =>
                                                      handleUpdateStructureColumn(
                                                        currentDataTable,
                                                        rowIndex,
                                                        { nullable: event.target.checked },
                                                      )
                                                    }
                                                  />
                                                  nullable
                                                </label>
                                              </TableCell>
                                              <TableCell>
                                                <div className="grid grid-cols-[minmax(0,1fr)_minmax(0,1fr)] gap-2">
                                                  <select
                                                    className="h-8 w-full rounded-md border border-slate-200 bg-white px-2 text-xs"
                                                    value={column.foreignTable}
                                                    onChange={(event) => {
                                                      const nextForeignTable = event.target.value;
                                                      const referencedColumns =
                                                        nextForeignTable.length > 0
                                                          ? (
                                                              downstreamStructureRowsByTable[
                                                                nextForeignTable
                                                              ] ?? []
                                                            ).map((entry) => entry.name)
                                                          : [];
                                                      const nextForeignColumn =
                                                        nextForeignTable.length > 0 &&
                                                        referencedColumns.includes(
                                                          column.foreignColumn,
                                                        )
                                                          ? column.foreignColumn
                                                          : (referencedColumns[0] ?? "");

                                                      handleUpdateStructureColumn(
                                                        currentDataTable,
                                                        rowIndex,
                                                        {
                                                          foreignTable: nextForeignTable,
                                                          foreignColumn: nextForeignColumn,
                                                        },
                                                      );
                                                    }}
                                                  >
                                                    <option value="">none</option>
                                                    {DOWNSTREAM_TABLE_NAMES.map((tableName) => (
                                                      <option key={tableName} value={tableName}>
                                                        {tableName}
                                                      </option>
                                                    ))}
                                                  </select>
                                                  <select
                                                    className="h-8 w-full rounded-md border border-slate-200 bg-white px-2 text-xs"
                                                    value={column.foreignColumn}
                                                    disabled={column.foreignTable.length === 0}
                                                    onChange={(event) =>
                                                      handleUpdateStructureColumn(
                                                        currentDataTable,
                                                        rowIndex,
                                                        {
                                                          foreignColumn: event.target.value,
                                                        },
                                                      )
                                                    }
                                                  >
                                                    <option value="">
                                                      {column.foreignTable.length === 0
                                                        ? "column"
                                                        : "select column"}
                                                    </option>
                                                    {(column.foreignTable.length > 0
                                                      ? (
                                                          downstreamStructureRowsByTable[
                                                            column.foreignTable
                                                          ] ?? []
                                                        ).map((entry) => entry.name)
                                                      : []
                                                    ).map((columnName) => (
                                                      <option key={columnName} value={columnName}>
                                                        {columnName}
                                                      </option>
                                                    ))}
                                                  </select>
                                                </div>
                                              </TableCell>
                                              <TableCell className="text-right">
                                                <Button
                                                  type="button"
                                                  variant="ghost"
                                                  size="icon"
                                                  className="h-8 w-8"
                                                  disabled={currentStructureRows.length <= 1}
                                                  onClick={() =>
                                                    handleDeleteStructureColumn(
                                                      currentDataTable,
                                                      rowIndex,
                                                    )
                                                  }
                                                >
                                                  <Trash2 className="h-4 w-4" />
                                                </Button>
                                              </TableCell>
                                            </TableRow>
                                          ))}
                                        </TableBody>
                                      </Table>
                                    </ScrollArea>
                                    <div className="border-t bg-slate-50 px-3 py-2">
                                      <Button
                                        type="button"
                                        size="sm"
                                        variant="outline"
                                        onClick={() => handleAddStructureColumn(currentDataTable)}
                                      >
                                        Add column
                                      </Button>
                                    </div>
                                  </div>
                                ) : postgresStructureTab === "diagram" ? (
                                  <SchemaRelationsGraph
                                    schema={downstreamStructureSchema}
                                    selectedTableName={currentDataTable}
                                    onSelectTable={(tableName) => setSelectedDataTable(tableName)}
                                    isVisible={
                                      activeTopTab === "data" &&
                                      postgresWorkspaceMode === "structure" &&
                                      postgresStructureTab === "diagram"
                                    }
                                    heightClassName="h-full"
                                  />
                                ) : (
                                  <div className="h-full overflow-hidden rounded-md border">
                                    <Editor
                                      path={DOWNSTREAM_DDL_MODEL_PATH}
                                      language="sql"
                                      value={
                                        downstreamDdlText ||
                                        "Unable to generate downstream schema DDL."
                                      }
                                      options={{
                                        minimap: { enabled: false },
                                        fontSize: 13,
                                        scrollBeyondLastLine: false,
                                        readOnly: true,
                                        wordWrap: "off",
                                        lineNumbers: "on",
                                        ...MONACO_INDENT_OPTIONS,
                                      }}
                                      height="100%"
                                    />
                                  </div>
                                )}
                              </div>
                            </div>
                          )}
                        </div>
                        {currentTableIssues.length > 0 ? (
                          <Alert variant="warning" className="m-2 mt-0">
                            <AlertTitle>Data issues</AlertTitle>
                            <AlertDescription className="whitespace-pre-wrap font-mono text-xs">
                              {currentTableIssues.join("\n")}
                            </AlertDescription>
                          </Alert>
                        ) : null}
                        <div className="flex items-center justify-between border-t bg-slate-50 px-3 py-2">
                          <Tabs
                            value={postgresWorkspaceMode}
                            onValueChange={(value) =>
                              setPostgresWorkspaceMode(value as PostgresWorkspaceMode)
                            }
                          >
                            <TabsList className="gap-1">
                              <TabsTrigger value="data">Data</TabsTrigger>
                              <TabsTrigger
                                value="structure"
                                disabled={!selectedTableSupportsStructure}
                              >
                                Structure
                              </TabsTrigger>
                            </TabsList>
                          </Tabs>
                          <div className="text-xs text-slate-500">
                            {currentDataRows.length} row{currentDataRows.length === 1 ? "" : "s"}
                          </div>
                          <Button
                            type="button"
                            size="sm"
                            disabled={
                              postgresWorkspaceMode !== "data" && selectedTableSupportsStructure
                            }
                            onClick={handleAddDataRow}
                          >
                            + Add row
                          </Button>
                        </div>
                      </div>
                    ) : (
                      <div className="flex h-full items-center justify-center text-sm text-slate-500">
                        Select a table.
                      </div>
                    )}
                  </div>
                </div>
              ) : (
                <div className="flex h-full items-center justify-center text-sm text-slate-500">
                  No downstream tables available.
                </div>
              )}
            </TabsContent>

            <TabsContent value="schema" forceMount className="mt-0 h-full min-h-0">
              <div className="hidden h-full min-h-0 flex-col gap-2 lg:flex">
                <div
                  ref={schemaWorkspaceRef}
                  className="min-h-0 flex-1 overflow-visible rounded-md border bg-white lg:grid"
                  style={{ gridTemplateColumns: schemaSplitGridTemplate }}
                >
                  <div className="flex h-full min-h-0 flex-col overflow-visible">
                    <div className="shrink-0 border-b bg-slate-50 px-2 py-1">
                      <Tabs
                        value={activeSchemaSourceTab}
                        onValueChange={(value) =>
                          setActiveSchemaSourceTab(value as SchemaSourceTab)
                        }
                      >
                        <TabsList className="gap-1">
                          <TabsTrigger value="schema">schema.ts</TabsTrigger>
                          <TabsTrigger value="context">context.ts</TabsTrigger>
                          <TabsTrigger value="provider">db-provider.ts</TabsTrigger>
                          <TabsTrigger value="redis_provider">redis-provider.ts</TabsTrigger>
                          <TabsTrigger value="generated">generated-db.ts</TabsTrigger>
                        </TabsList>
                      </Tabs>
                    </div>
                    <div className="min-h-0 flex-1 overflow-visible">
                      <Editor
                        path={schemaEditorPath}
                        language="typescript"
                        onMount={handleMonacoMount}
                        onChange={handleSchemaEditorChange}
                        options={{
                          minimap: { enabled: false },
                          fontSize: 13,
                          scrollBeyondLastLine: false,
                          readOnly: schemaEditorReadOnly,
                          ...MONACO_INDENT_OPTIONS,
                        }}
                        height="100%"
                      />
                    </div>
                  </div>

                  <div className="flex min-h-0 items-stretch justify-center">
                    <button
                      type="button"
                      aria-label="Resize schema panels"
                      className="group h-full w-2 cursor-col-resize bg-transparent"
                      onPointerDown={startSchemaSplitDrag}
                    >
                      <span
                        className={cn(
                          "mx-auto block h-full w-px bg-slate-400 transition-colors group-hover:bg-slate-500",
                          isSchemaSplitDragging ? "bg-slate-500" : null,
                        )}
                      />
                    </button>
                  </div>

                  <div className="min-h-0 overflow-hidden">
                    {schemaParse.ok && schemaParse.schema ? (
                      activeSchemaTab === "diagram" ? (
                        <SchemaRelationsGraph
                          schema={schemaParse.schema}
                          selectedTableName={selectedSchemaTable}
                          onSelectTable={handleSelectSchemaTable}
                          onClearSelection={() => setSelectedSchemaTable(null)}
                          isVisible={activeTopTab === "schema" && activeSchemaTab === "diagram"}
                          heightClassName="h-full"
                          embedded
                        />
                      ) : (
                        <Editor
                          path={SCHEMA_DDL_MODEL_PATH}
                          language="sql"
                          value={facadeDdlText || "Fix schema to generate DDL."}
                          options={{
                            minimap: { enabled: false },
                            fontSize: 13,
                            scrollBeyondLastLine: false,
                            readOnly: true,
                            wordWrap: "off",
                            lineNumbers: "on",
                            ...MONACO_INDENT_OPTIONS,
                          }}
                          height="100%"
                        />
                      )
                    ) : (
                      <div className="flex h-full items-center justify-center text-sm text-slate-500">
                        Fix schema TypeScript to render relations.
                      </div>
                    )}
                  </div>
                </div>
                {!schemaParse.ok ? (
                  <Alert variant="warning">
                    <AlertTitle>Schema issues</AlertTitle>
                    <AlertDescription className="whitespace-pre-wrap font-mono text-xs">
                      {schemaParse.issues
                        .map((issue) => `${issue.path}: ${issue.message}`)
                        .join("\n")}
                    </AlertDescription>
                  </Alert>
                ) : null}
              </div>

              <div className="flex h-full min-h-0 flex-col gap-2 lg:hidden">
                <div className="min-h-0 flex-1 overflow-visible rounded-md border">
                  <div className="flex h-full min-h-0 flex-col overflow-visible">
                    <div className="shrink-0 border-b bg-slate-50 px-2 py-1">
                      <Tabs
                        value={activeSchemaSourceTab}
                        onValueChange={(value) =>
                          setActiveSchemaSourceTab(value as SchemaSourceTab)
                        }
                      >
                        <TabsList className="gap-1">
                          <TabsTrigger value="schema">schema.ts</TabsTrigger>
                          <TabsTrigger value="context">context.ts</TabsTrigger>
                          <TabsTrigger value="provider">db-provider.ts</TabsTrigger>
                          <TabsTrigger value="redis_provider">redis-provider.ts</TabsTrigger>
                          <TabsTrigger value="generated">generated-db.ts</TabsTrigger>
                        </TabsList>
                      </Tabs>
                    </div>
                    <div className="min-h-0 flex-1 overflow-visible">
                      <Editor
                        path={schemaEditorPath}
                        language="typescript"
                        onMount={handleMonacoMount}
                        onChange={handleSchemaEditorChange}
                        options={{
                          minimap: { enabled: false },
                          fontSize: 13,
                          scrollBeyondLastLine: false,
                          readOnly: schemaEditorReadOnly,
                          ...MONACO_INDENT_OPTIONS,
                        }}
                        height="100%"
                      />
                    </div>
                  </div>
                </div>
                {!schemaParse.ok ? (
                  <Alert variant="warning">
                    <AlertTitle>Schema issues</AlertTitle>
                    <AlertDescription className="whitespace-pre-wrap font-mono text-xs">
                      {schemaParse.issues
                        .map((issue) => `${issue.path}: ${issue.message}`)
                        .join("\n")}
                    </AlertDescription>
                  </Alert>
                ) : null}
                <div className="min-h-0 flex-1 overflow-hidden">
                  {schemaParse.ok && schemaParse.schema ? (
                    activeSchemaTab === "diagram" ? (
                      <SchemaRelationsGraph
                        schema={schemaParse.schema}
                        selectedTableName={selectedSchemaTable}
                        onSelectTable={handleSelectSchemaTable}
                        onClearSelection={() => setSelectedSchemaTable(null)}
                        isVisible={activeTopTab === "schema" && activeSchemaTab === "diagram"}
                        heightClassName="h-full"
                      />
                    ) : (
                      <div className="h-full overflow-hidden rounded-md border">
                        <Editor
                          path={SCHEMA_DDL_MODEL_PATH}
                          language="sql"
                          value={facadeDdlText || "Fix schema to generate DDL."}
                          options={{
                            minimap: { enabled: false },
                            fontSize: 13,
                            scrollBeyondLastLine: false,
                            readOnly: true,
                            wordWrap: "off",
                            lineNumbers: "on",
                            ...MONACO_INDENT_OPTIONS,
                          }}
                          height="100%"
                        />
                      </div>
                    )
                  ) : (
                    <div className="flex h-full items-center justify-center rounded-md border border-dashed text-sm text-slate-500">
                      Fix schema TypeScript to render relations.
                    </div>
                  )}
                </div>
              </div>
            </TabsContent>

            <TabsContent value="query" forceMount className="mt-0 h-full min-h-0">
              <div className="flex h-full min-h-0 flex-col gap-0">
                <div
                  ref={queryWorkspaceRef}
                  className="grid min-h-0 flex-1 overflow-hidden rounded-md border bg-white"
                  style={{ gridTemplateRows: querySplitGridTemplate }}
                >
                  <div className="min-h-0 overflow-hidden">
                    {activeQueryTab === "result" ? (
                      <div className="h-full min-h-0">
                        {runtimeError ? (
                          <Alert variant="destructive">
                            <AlertTitle>Query rejected</AlertTitle>
                            <AlertDescription className="whitespace-pre-wrap font-mono text-xs">
                              {runtimeError}
                            </AlertDescription>
                          </Alert>
                        ) : resultRows ? (
                          renderRows(resultRows, {
                            heightClassName: "h-full",
                            frameClassName: "h-full rounded-none border-0 bg-transparent",
                          })
                        ) : (
                          <div className="flex h-full items-center justify-center text-sm text-slate-500">
                            No results yet.
                          </div>
                        )}
                      </div>
                    ) : (
                      <div className="relative h-full min-h-0">
                        <PlanGraph
                          steps={planSteps}
                          scopes={planScopes}
                          statesById={statesById}
                          currentStepId={currentStepId}
                          selectedStepId={selectedStepId}
                          isVisible={activeTopTab === "query" && activeQueryTab === "explain"}
                          onSelectStep={handleSelectStep}
                          onClearSelection={handleCloseStepOverlay}
                          heightClassName="h-full"
                          containerClassName="rounded-none border-0 bg-transparent"
                        />
                        {selectedStep ? (
                          <div className="pointer-events-none absolute inset-y-4 right-4 z-20 w-[430px] max-w-[48%]">
                            <div className="pointer-events-auto flex h-full flex-col rounded-xl border border-sky-200 bg-white/95 shadow-2xl backdrop-blur-sm">
                              <div className="flex items-start justify-between gap-2 border-b p-3">
                                <div className="min-w-0 space-y-2">
                                  <div className="flex flex-wrap items-center gap-2">
                                    <Badge variant="secondary" className="font-mono text-[11px]">
                                      {selectedStep.id}
                                    </Badge>
                                    <Badge variant="outline">{selectedStep.kind}</Badge>
                                    <Badge variant="outline">{selectedStep.phase}</Badge>
                                    {selectedStep.sqlOrigin ? (
                                      <Badge variant="outline">{selectedStep.sqlOrigin}</Badge>
                                    ) : null}
                                  </div>
                                  <p className="text-sm text-slate-700">{selectedStep.summary}</p>
                                </div>
                                <Button
                                  type="button"
                                  variant="ghost"
                                  size="icon"
                                  className="h-7 w-7 shrink-0"
                                  onClick={handleCloseStepOverlay}
                                >
                                  <X className="h-4 w-4" />
                                </Button>
                              </div>
                              <div className="min-h-0 flex-1 p-3 pt-2">
                                <ScrollArea className="h-full pr-2">
                                  <div className="space-y-3">
                                    <div className="rounded-md border bg-slate-50 p-3">
                                      <p className="text-xs text-slate-500">
                                        Depends on: {selectedStep.dependsOn.join(", ") || "none"}
                                      </p>
                                    </div>

                                    <StepSection title="Logical operation" defaultOpen>
                                      <p className="text-xs text-slate-500">
                                        The planner-level intent for this step and the columns it
                                        aims to produce.
                                      </p>
                                      <JsonBlock value={selectedStep.operation} />
                                      {selectedStep.outputs && selectedStep.outputs.length > 0 ? (
                                        <div className="text-xs text-slate-600">
                                          Outputs: {selectedStep.outputs.join(", ")}
                                        </div>
                                      ) : null}
                                    </StepSection>

                                    <StepSection title="Request" defaultOpen>
                                      <p className="text-xs text-slate-500">
                                        The normalized input shape passed into this step at
                                        execution time.
                                      </p>
                                      <JsonBlock value={selectedStep.request ?? {}} />
                                    </StepSection>

                                    <StepSection title="Routing / Pushdown" defaultOpen>
                                      <p className="text-xs text-slate-500">
                                        How work is split between table methods and local engine
                                        processing.
                                      </p>
                                      <div className="text-xs text-slate-600">
                                        Route used:{" "}
                                        <span className="font-medium text-slate-900">
                                          {selectedStepState?.routeUsed ?? "pending"}
                                        </span>
                                      </div>
                                      <JsonBlock value={selectedStep.pushdown ?? {}} />
                                      {selectedStepState?.notes &&
                                      selectedStepState.notes.length > 0 ? (
                                        <ul className="list-disc space-y-1 pl-5 text-xs text-slate-600">
                                          {selectedStepState.notes.map((note: string) => (
                                            <li key={note}>{note}</li>
                                          ))}
                                        </ul>
                                      ) : null}
                                    </StepSection>

                                    <StepSection title="Runtime" defaultOpen>
                                      <p className="text-xs text-slate-500">
                                        Execution status and timing/row-count metrics for this step
                                        instance.
                                      </p>
                                      <div className="grid gap-1 text-xs text-slate-600">
                                        <div>Status: {selectedStepState?.status ?? "ready"}</div>
                                        <div>
                                          Execution index:{" "}
                                          {selectedStepState?.executionIndex != null
                                            ? selectedStepState.executionIndex
                                            : "pending"}
                                        </div>
                                        {selectedStepState?.durationMs != null ? (
                                          <div>Duration: {selectedStepState.durationMs}ms</div>
                                        ) : null}
                                        {selectedStepState?.inputRowCount != null ? (
                                          <div>Input rows: {selectedStepState.inputRowCount}</div>
                                        ) : null}
                                        {selectedStepState?.outputRowCount != null ? (
                                          <div>Output rows: {selectedStepState.outputRowCount}</div>
                                        ) : selectedStepState?.rowCount != null ? (
                                          <div>Output rows: {selectedStepState.rowCount}</div>
                                        ) : null}
                                      </div>
                                      {selectedStepState?.error ? (
                                        <Alert variant="destructive">
                                          <AlertTitle>Step error</AlertTitle>
                                          <AlertDescription>
                                            {selectedStepState.error}
                                          </AlertDescription>
                                        </Alert>
                                      ) : null}
                                    </StepSection>

                                    {selectedStepState?.rows ? (
                                      <StepSection title="Data preview" defaultOpen={false}>
                                        <p className="text-xs text-slate-500">
                                          Sample output rows emitted by this step after execution.
                                        </p>
                                        {renderRows(selectedStepState.rows, {
                                          heightClassName: "h-[260px]",
                                          expandNestedObjects: true,
                                        })}
                                      </StepSection>
                                    ) : null}
                                  </div>
                                </ScrollArea>
                              </div>
                            </div>
                          </div>
                        ) : null}
                      </div>
                    )}
                  </div>

                  <div className="relative min-h-0 overflow-visible">
                    <button
                      type="button"
                      aria-label="Resize query panels"
                      className="group absolute inset-x-0 top-1/2 z-10 flex h-[10px] -translate-y-1/2 cursor-row-resize items-center bg-transparent"
                      onPointerDown={startQuerySplitDrag}
                    >
                      <span
                        className={cn(
                          "mx-auto block h-px w-full bg-slate-400 transition-colors group-hover:bg-slate-500",
                          isQuerySplitDragging ? "bg-slate-500" : null,
                        )}
                      />
                    </button>
                  </div>

                  <div className="min-h-0 overflow-hidden">
                    <ExecutedProviderOperationsPanel
                      operations={executedOperations}
                      onMonacoMount={handleMonacoMount}
                      className="rounded-none border-0 bg-transparent"
                    />
                  </div>
                </div>
              </div>
            </TabsContent>
          </div>
        </div>
      </Tabs>
    </div>
  );
}
