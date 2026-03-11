import {
  resolveColumnDefinition,
  resolveTableForeignKeys,
  resolveTablePrimaryKeyConstraint,
  resolveTableUniqueConstraints,
} from "./definition";
import { validateSchemaConstraints } from "./constraints";
import type { ResolvedColumnDefinition as ResolvedColumnDefinitionType } from "./definition";
import type { SchemaDefinition, SqlScalarType, TableDefinition } from "./index";

export interface SqlDdlOptions {
  ifNotExists?: boolean;
}

interface CheckConstraintForDDL {
  name?: string;
  column: string;
  values: readonly (string | number | boolean | null)[];
}

/**
 * DDL generation owns the SQL rendering of a fully validated logical schema.
 */
export function toSqlDDL(schema: SchemaDefinition, options: SqlDdlOptions = {}): string {
  validateSchemaConstraints(schema);

  const createPrefix = options.ifNotExists ? "CREATE TABLE IF NOT EXISTS" : "CREATE TABLE";
  const statements: string[] = [];

  for (const [tableName, table] of Object.entries(schema.tables)) {
    if (table.provider === "__view__") {
      continue;
    }

    const columnEntries = Object.entries(table.columns);
    if (columnEntries.length === 0) {
      throw new Error(`Cannot generate DDL for table ${tableName} with no columns.`);
    }

    const definitionLines = columnEntries.map(([columnName, columnDefinition]) => {
      const resolved = resolveColumnDefinition(columnDefinition);
      const nullability = resolved.nullable ? "" : " NOT NULL";
      const metadataComment = renderColumnMetadataComment(resolved);
      return `  ${escapeIdentifier(columnName)} ${toSqlType(resolved.type)}${nullability}${metadataComment}`;
    });

    const primaryKey = resolveTablePrimaryKeyConstraint(table);
    if (primaryKey) {
      definitionLines.push(
        `  ${renderConstraintPrefix(primaryKey.name)}PRIMARY KEY (${renderColumnList(primaryKey.columns)})`,
      );
    }

    for (const uniqueConstraint of resolveTableUniqueConstraints(table)) {
      definitionLines.push(
        `  ${renderConstraintPrefix(uniqueConstraint.name)}UNIQUE (${renderColumnList(uniqueConstraint.columns)})`,
      );
    }

    for (const foreignKey of resolveTableForeignKeys(table)) {
      const onDelete = foreignKey.onDelete ? ` ON DELETE ${foreignKey.onDelete}` : "";
      const onUpdate = foreignKey.onUpdate ? ` ON UPDATE ${foreignKey.onUpdate}` : "";
      definitionLines.push(
        `  ${renderConstraintPrefix(foreignKey.name)}FOREIGN KEY (${renderColumnList(foreignKey.columns)}) REFERENCES ${escapeIdentifier(foreignKey.references.table)} (${renderColumnList(foreignKey.references.columns)})${onDelete}${onUpdate}`,
      );
    }

    for (const checkConstraint of buildCheckConstraints(tableName, table)) {
      definitionLines.push(
        `  ${renderConstraintPrefix(checkConstraint.name)}CHECK (${renderCheckExpression(checkConstraint)})`,
      );
    }

    statements.push(
      `${createPrefix} ${escapeIdentifier(tableName)} (\n${definitionLines.join(",\n")}\n);`,
    );
  }

  return statements.join("\n\n");
}

function buildCheckConstraints(tableName: string, table: TableDefinition): CheckConstraintForDDL[] {
  const checks: CheckConstraintForDDL[] = [];

  for (const [columnName, columnDefinition] of Object.entries(table.columns)) {
    const resolved = resolveColumnDefinition(columnDefinition);
    if (resolved.enum && resolved.enum.length > 0) {
      checks.push({
        name: `${tableName}_${columnName}_enum_check`,
        column: columnName,
        values: [...resolved.enum],
      });
    }
  }

  for (const check of table.constraints?.checks ?? []) {
    if (check.kind === "in") {
      checks.push({
        ...(check.name ? { name: check.name } : {}),
        column: check.column,
        values: [...check.values],
      });
    }
  }

  return checks;
}

function renderCheckExpression(check: CheckConstraintForDDL): string {
  const values = [...check.values];
  const hasNull = values.some((value) => value == null);
  const nonNullValues = values.filter((value) => value != null);

  if (nonNullValues.length === 0) {
    return `${escapeIdentifier(check.column)} IS NULL`;
  }

  const inList = nonNullValues.map((value) => renderSqlLiteral(value)).join(", ");
  const inExpr = `${escapeIdentifier(check.column)} IN (${inList})`;
  if (!hasNull) {
    return inExpr;
  }

  return `(${inExpr} OR ${escapeIdentifier(check.column)} IS NULL)`;
}

function renderSqlLiteral(value: string | number | boolean): string {
  if (typeof value === "string") {
    return `'${value.replaceAll("'", "''")}'`;
  }

  if (typeof value === "boolean") {
    return value ? "1" : "0";
  }

  return String(value);
}

function toSqlType(type: SqlScalarType): string {
  switch (type) {
    case "text":
      return "TEXT";
    case "integer":
      return "INTEGER";
    case "real":
      return "REAL";
    case "blob":
      return "BLOB";
    case "boolean":
      return "INTEGER";
    case "timestamp":
    case "date":
    case "datetime":
    case "json":
      return "TEXT";
  }
}

function renderColumnMetadataComment(column: ResolvedColumnDefinitionType): string {
  const attributes: string[] = [];

  if (column.type === "timestamp") {
    attributes.push("format:iso8601");
  }
  if (column.type === "date") {
    attributes.push("format:date");
  }
  if (column.type === "datetime") {
    attributes.push("format:datetime");
  }
  if (column.type === "json") {
    attributes.push("format:json");
  }

  if (column.description) {
    attributes.push(`description:${JSON.stringify(column.description)}`);
  }

  if (attributes.length === 0) {
    return "";
  }

  return ` /* tupl: ${attributes.join(" ")} */`;
}

function escapeIdentifier(name: string): string {
  return `"${name.replaceAll('"', '""')}"`;
}

function renderColumnList(columns: string[]): string {
  return columns.map(escapeIdentifier).join(", ");
}

function renderConstraintPrefix(name: string | undefined): string {
  return name ? `CONSTRAINT ${escapeIdentifier(name)} ` : "";
}
