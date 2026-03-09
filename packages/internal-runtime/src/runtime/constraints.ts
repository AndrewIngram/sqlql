import {
  getTable,
  resolveColumnDefinition,
  resolveTablePrimaryKeyConstraint,
  resolveTableUniqueConstraints,
  type CheckConstraint,
  type QueryRow,
  type SchemaDefinition,
  type TableColumnDefinition,
  type UniqueConstraint,
} from "@tupl-internal/schema";

export type ConstraintValidationMode = "off" | "warn" | "error";

export interface ConstraintValidationOptions {
  mode?: ConstraintValidationMode;
  onViolation?: (violation: ConstraintViolation) => void;
}

export type ConstraintViolationType = "not_null" | "primary_key" | "unique" | "check";

export interface ConstraintViolation {
  table: string;
  type: ConstraintViolationType;
  columns: string[];
  message: string;
  constraintName?: string;
  rowIndexes?: number[];
  key?: unknown[];
}

export interface ValidateTableConstraintsInput {
  schema: SchemaDefinition;
  tableName: string;
  rows: QueryRow[];
  options?: ConstraintValidationOptions;
}

export function validateTableConstraintRows(input: ValidateTableConstraintsInput): void {
  const mode = input.options?.mode ?? "off";
  if (mode === "off") {
    return;
  }

  const violations = collectConstraintViolations(input.schema, input.tableName, input.rows);
  if (violations.length === 0) {
    return;
  }

  if (mode === "warn") {
    for (const violation of violations) {
      if (input.options?.onViolation) {
        input.options.onViolation(violation);
      } else {
        console.warn(violation.message);
      }
    }
    return;
  }

  const first = violations[0];
  if (!first) {
    return;
  }

  throw new Error(first.message);
}

function collectConstraintViolations(
  schema: SchemaDefinition,
  tableName: string,
  rows: QueryRow[],
): ConstraintViolation[] {
  const table = getTable(schema, tableName);
  const violations: ConstraintViolation[] = [];

  for (const [columnName, columnDefinition] of Object.entries(table.columns)) {
    if (!isNonNullable(columnDefinition) || !rowsHaveColumn(rows, columnName)) {
      continue;
    }

    rows.forEach((row, rowIndex) => {
      if (row[columnName] != null) {
        return;
      }

      violations.push({
        table: tableName,
        type: "not_null",
        columns: [columnName],
        message: `Constraint violation on table ${tableName}: column "${columnName}" is NOT NULL but row ${rowIndex} had null.`,
        rowIndexes: [rowIndex],
      });
    });
  }

  const primaryKey = resolveTablePrimaryKeyConstraint(table);
  if (primaryKey && rowsHaveColumns(rows, primaryKey.columns)) {
    const duplicate = findDuplicateKey(rows, primaryKey.columns);
    if (duplicate) {
      violations.push({
        table: tableName,
        type: "primary_key",
        columns: primaryKey.columns,
        key: duplicate.key,
        rowIndexes: duplicate.rowIndexes,
        message: `Constraint violation on table ${tableName}: duplicate primary key (${renderKey(duplicate.key)}) for columns ${primaryKey.columns.join(", ")}.`,
        ...(primaryKey.name ? { constraintName: primaryKey.name } : {}),
      });
    }
  }

  for (const uniqueConstraint of resolveTableUniqueConstraints(table)) {
    if (!rowsHaveColumns(rows, uniqueConstraint.columns)) {
      continue;
    }

    const duplicate = findDuplicateKey(rows, uniqueConstraint.columns);
    if (!duplicate) {
      continue;
    }

    violations.push({
      table: tableName,
      type: "unique",
      columns: uniqueConstraint.columns,
      key: duplicate.key,
      rowIndexes: duplicate.rowIndexes,
      message: renderUniqueViolationMessage(tableName, uniqueConstraint, duplicate.key),
      ...(uniqueConstraint.name ? { constraintName: uniqueConstraint.name } : {}),
    });
  }

  for (const [columnName, columnDefinition] of Object.entries(table.columns)) {
    const resolved = resolveColumnDefinition(columnDefinition);
    if (!resolved.enum || !rowsHaveColumn(rows, columnName)) {
      continue;
    }

    rows.forEach((row, rowIndex) => {
      const value = row[columnName];
      if (value == null) {
        return;
      }

      if (typeof value !== "string" || !resolved.enum?.includes(value)) {
        violations.push({
          table: tableName,
          type: "check",
          columns: [columnName],
          constraintName: `${tableName}_${columnName}_enum_check`,
          rowIndexes: [rowIndex],
          message: `Constraint violation on table ${tableName}: enum check failed for column "${columnName}" with value ${JSON.stringify(value)}.`,
        });
      }
    });
  }

  for (const check of table.constraints?.checks ?? []) {
    if (check.kind !== "in") {
      continue;
    }
    if (!rowsHaveColumn(rows, check.column)) {
      continue;
    }

    const allowed = new Set(
      check.values.filter((value): value is string | number | boolean => value != null),
    );
    rows.forEach((row, rowIndex) => {
      const value = row[check.column];
      if (value == null) {
        return;
      }

      if (!allowed.has(value as string | number | boolean)) {
        violations.push({
          table: tableName,
          type: "check",
          columns: [check.column],
          ...(check.name ? { constraintName: check.name } : {}),
          rowIndexes: [rowIndex],
          message: renderCheckViolationMessage(tableName, check, value),
        });
      }
    });
  }

  return violations;
}

function isNonNullable(columnDefinition: TableColumnDefinition): boolean {
  const resolved = resolveColumnDefinition(columnDefinition);
  return resolved.nullable === false;
}

function rowsHaveColumn(rows: QueryRow[], column: string): boolean {
  return rows.length > 0 && rows.every((row) => Object.prototype.hasOwnProperty.call(row, column));
}

function rowsHaveColumns(rows: QueryRow[], columns: string[]): boolean {
  return columns.every((column) => rowsHaveColumn(rows, column));
}

function findDuplicateKey(
  rows: QueryRow[],
  columns: string[],
): { key: unknown[]; rowIndexes: [number, number] } | undefined {
  const seen = new Map<string, { key: unknown[]; rowIndex: number }>();

  for (let index = 0; index < rows.length; index += 1) {
    const row = rows[index];
    if (!row) {
      continue;
    }

    const key = columns.map((column) => row[column] ?? null);
    const signature = JSON.stringify(key);
    const existing = seen.get(signature);
    if (existing) {
      return {
        key,
        rowIndexes: [existing.rowIndex, index],
      };
    }

    seen.set(signature, { key, rowIndex: index });
  }

  return undefined;
}

function renderKey(key: unknown[]): string {
  return key.map((value) => JSON.stringify(value)).join(", ");
}

function renderUniqueViolationMessage(
  tableName: string,
  constraint: UniqueConstraint,
  key: unknown[],
): string {
  const nameSuffix = constraint.name ? ` (${constraint.name})` : "";
  return `Constraint violation on table ${tableName}${nameSuffix}: duplicate unique key (${renderKey(key)}) for columns ${constraint.columns.join(", ")}.`;
}

function renderCheckViolationMessage(
  tableName: string,
  check: CheckConstraint,
  value: unknown,
): string {
  if (check.kind === "in") {
    const nameSuffix = check.name ? ` (${check.name})` : "";
    return `Constraint violation on table ${tableName}${nameSuffix}: value ${JSON.stringify(value)} for column ${check.column} is outside CHECK IN set ${JSON.stringify(check.values)}.`;
  }

  return `Constraint violation on table ${tableName}: check constraint failed.`;
}
