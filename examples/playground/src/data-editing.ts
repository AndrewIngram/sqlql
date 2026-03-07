import type { QueryRow, TableColumnDefinition, TableDefinition } from "../../../src/index";

import { isColumnNullable, readColumnEnumValues, readColumnType } from "./types";

export interface CoerceInputSuccess {
  ok: true;
  value: unknown;
}

export interface CoerceInputFailure {
  ok: false;
  error: string;
}

export type CoerceInputResult = CoerceInputSuccess | CoerceInputFailure;

export function defaultValueForColumn(column: TableColumnDefinition): unknown {
  const type = readColumnType(column);
  if (isColumnNullable(column)) {
    return null;
  }

  switch (type) {
    case "text":
      return readColumnEnumValues(column)?.[0] ?? "";
    case "date":
    case "datetime":
    case "timestamp":
      return "";
    case "integer":
    case "real":
      return 0;
    case "boolean":
      return false;
    case "json":
      return {};
    case "blob":
      return new Uint8Array();
  }
}

export function coerceCellInput(column: TableColumnDefinition, raw: string): CoerceInputResult {
  const trimmed = raw.trim();
  if (trimmed.length === 0) {
    if (isColumnNullable(column)) {
      return { ok: true, value: null };
    }

    return { ok: false, error: "Value is required." };
  }

  const type = readColumnType(column);
  switch (type) {
    case "text": {
      const enumValues = readColumnEnumValues(column);
      if (enumValues && enumValues.length > 0 && !enumValues.includes(raw)) {
        return {
          ok: false,
          error: `Expected one of: ${enumValues.join(", ")}.`,
        };
      }
      return { ok: true, value: raw };
    }

    case "date":
    case "datetime":
    case "timestamp":
      return { ok: true, value: raw };

    case "integer": {
      const parsed = Number(trimmed);
      if (!Number.isFinite(parsed) || !Number.isInteger(parsed)) {
        return { ok: false, error: "Expected an integer value." };
      }

      return { ok: true, value: parsed };
    }

    case "real": {
      const parsed = Number(trimmed);
      if (!Number.isFinite(parsed)) {
        return { ok: false, error: "Expected a numeric value." };
      }
      return { ok: true, value: parsed };
    }

    case "boolean": {
      const normalized = trimmed.toLowerCase();
      if (normalized === "true" || normalized === "1") {
        return { ok: true, value: true };
      }

      if (normalized === "false" || normalized === "0") {
        return { ok: true, value: false };
      }

      return { ok: false, error: "Expected true/false." };
    }

    case "json":
      try {
        return { ok: true, value: JSON.parse(raw) };
      } catch {
        return { ok: false, error: "Expected valid JSON." };
      }

    case "blob":
      return { ok: false, error: "Blob editing is not supported in the playground yet." };
  }
}

export function formatCellValue(value: unknown): string {
  if (value == null) {
    return "";
  }

  if (typeof value === "string") {
    return value;
  }

  if (typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }

  return JSON.stringify(value);
}

export function buildEmptyRow(table: TableDefinition): QueryRow {
  const row: QueryRow = {};
  for (const [columnName, columnDefinition] of Object.entries(table.columns)) {
    row[columnName] = defaultValueForColumn(columnDefinition);
  }

  return row;
}

export function updateRowCell(
  rows: QueryRow[],
  rowIndex: number,
  columnName: string,
  value: unknown,
): QueryRow[] {
  return rows.map((row, index) => {
    if (index !== rowIndex) {
      return row;
    }

    return {
      ...row,
      [columnName]: value,
    };
  });
}

export function addEmptyRow(rows: QueryRow[], table: TableDefinition): QueryRow[] {
  return [...rows, buildEmptyRow(table)];
}

export function deleteRow(rows: QueryRow[], rowIndex: number): QueryRow[] {
  return rows.filter((_, index) => index !== rowIndex);
}

export function mergeTableRows(
  allRows: Record<string, QueryRow[]>,
  tableName: string,
  tableRows: QueryRow[],
): Record<string, QueryRow[]> {
  return {
    ...allRows,
    [tableName]: tableRows,
  };
}
