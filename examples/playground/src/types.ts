import type { SchemaDefinition, SqlScalarType, TableColumnDefinition } from "sqlql";

export interface PlaygroundContext {
  orgId: string;
  userId: string;
}

export interface ExecutedSqlProviderOperation {
  id: string;
  timestamp: number;
  provider: string;
  kind: "sql_query";
  sql: string;
  variables: unknown[];
}

export interface ExecutedKvLookupProviderOperation {
  id: string;
  timestamp: number;
  provider: string;
  kind: "kv_lookup";
  lookup: {
    entity: string;
    key?: unknown;
    keys?: unknown[];
    table?: string;
    op?: string;
  };
  variables: unknown;
}

export type ExecutedProviderOperation =
  | ExecutedSqlProviderOperation
  | ExecutedKvLookupProviderOperation;

export type DownstreamRows = Record<string, Array<Record<string, unknown>>>;

export interface PlaygroundQueryPreset {
  id: string;
  label: string;
  sql: string;
  description?: string;
}

export interface PlaygroundScenarioPreset {
  id: string;
  label: string;
  description: string;
  context: PlaygroundContext;
  rows: DownstreamRows;
  defaultQueryId: string;
}

export type CatalogQueryId = string;

export interface CatalogQueryEntry {
  id: CatalogQueryId;
  label: string;
  sql: string;
  description?: string;
}

export interface QueryCompatibility {
  compatible: boolean;
  reason?: string;
}

export type QueryCompatibilityMap = Record<CatalogQueryId, QueryCompatibility>;

export interface SchemaValidationIssue {
  path: string;
  message: string;
}

export interface SchemaParseResult {
  ok: boolean;
  schema?: SchemaDefinition;
  issues: SchemaValidationIssue[];
}

export interface RowsParseResult {
  ok: boolean;
  rows?: Record<string, Array<Record<string, unknown>>>;
  issues: SchemaValidationIssue[];
}

export function readColumnType(column: TableColumnDefinition): SqlScalarType {
  return typeof column === "string" ? column : column.type;
}

export function isColumnNullable(column: TableColumnDefinition): boolean {
  return typeof column === "string" ? true : (column.nullable ?? true);
}

export function readColumnEnumValues(column: TableColumnDefinition): readonly string[] | undefined {
  if (typeof column === "string" || column.type !== "text") {
    return undefined;
  }
  return column.enum;
}
