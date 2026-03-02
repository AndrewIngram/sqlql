import type { SchemaDefinition, SqlScalarType, TableColumnDefinition } from "sqlql";

export interface ExampleQuery {
  label: string;
  sql: string;
}

export type CatalogQueryId = string;

export interface CatalogQueryEntry {
  id: CatalogQueryId;
  packId: string;
  packLabel: string;
  queryLabel: string;
  sql: string;
}

export interface QueryCompatibility {
  compatible: boolean;
  reason?: string;
}

export type QueryCompatibilityMap = Record<CatalogQueryId, QueryCompatibility>;

export interface ExamplePack {
  id: string;
  label: string;
  description: string;
  schema: SchemaDefinition;
  rows: Record<string, Array<Record<string, unknown>>>;
  queries: ExampleQuery[];
}

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
