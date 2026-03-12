import {
  getTable,
  resolveColumnDefinition,
  resolveTableColumnDefinition,
  resolveTableForeignKeys,
  resolveTablePrimaryKeyConstraint,
  resolveTableUniqueConstraints,
  type ResolvedColumnDefinition,
} from "./definition";
import type { SchemaDefinition } from "./types";

class SchemaConstraintValidationError extends Error {
  readonly issues: readonly string[];

  constructor(issues: readonly string[]) {
    super(formatSchemaConstraintIssues(issues));
    this.name = "SchemaConstraintValidationError";
    this.issues = [...issues];
  }
}

/**
 * Schema constraint validation owns logical schema invariants for tables, columns, and constraints.
 */
export function validateSchemaConstraints(schema: SchemaDefinition): void {
  const issues = collectSchemaConstraintIssues(schema);
  if (issues.length > 0) {
    throw new SchemaConstraintValidationError(issues);
  }
}

function collectSchemaConstraintIssues(schema: SchemaDefinition) {
  const issues: string[] = [];

  for (const [tableName, table] of Object.entries(schema.tables)) {
    for (const [columnName, columnDefinition] of Object.entries(table.columns)) {
      const resolved = resolveColumnDefinition(columnDefinition);
      validateColumnDefinition(tableName, columnName, resolved, issues);
    }

    const columnPrimaryKeyColumns = readColumnPrimaryKeyColumns(table);
    if (columnPrimaryKeyColumns.length > 1) {
      issues.push(
        `Invalid primary key on ${tableName}: multiple column-level primaryKey declarations found (${columnPrimaryKeyColumns.join(", ")}). Use table.constraints.primaryKey for composite keys.`,
      );
    }

    const tablePrimaryKey = table.constraints?.primaryKey;
    if (tablePrimaryKey && columnPrimaryKeyColumns.length === 1) {
      const columnPrimaryKeyColumn = columnPrimaryKeyColumns[0];
      const tablePrimaryKeyIsSameSingleColumn =
        tablePrimaryKey.columns.length === 1 &&
        tablePrimaryKey.columns[0] === columnPrimaryKeyColumn;
      if (!tablePrimaryKeyIsSameSingleColumn) {
        issues.push(
          `Invalid primary key on ${tableName}: column-level primaryKey on "${columnPrimaryKeyColumn}" conflicts with table.constraints.primaryKey. Use one declaration style.`,
        );
      }
    }

    const resolvedPrimaryKey = resolveTablePrimaryKeyConstraint(table);
    if (resolvedPrimaryKey) {
      validateConstraintColumns(
        schema,
        tableName,
        "primary key",
        resolvedPrimaryKey.columns,
        issues,
      );
      validateNoDuplicateColumns(tableName, "primary key", resolvedPrimaryKey.columns, issues);
    }

    resolveTableUniqueConstraints(table).forEach((uniqueConstraint, index) => {
      const label = uniqueConstraint.name ?? `unique constraint #${index + 1}`;
      validateConstraintColumns(schema, tableName, label, uniqueConstraint.columns, issues);
      validateNoDuplicateColumns(tableName, label, uniqueConstraint.columns, issues);
    });

    const foreignKeys = resolveTableForeignKeys(table);
    foreignKeys.forEach((foreignKey, index) => {
      const label = foreignKey.name ?? `foreign key #${index + 1}`;
      validateConstraintColumns(schema, tableName, label, foreignKey.columns, issues);
      validateNoDuplicateColumns(tableName, label, foreignKey.columns, issues);

      const referencedTable = schema.tables[foreignKey.references.table];
      if (!referencedTable) {
        issues.push(
          `Invalid ${label} on ${tableName}: referenced table "${foreignKey.references.table}" does not exist.`,
        );
      }

      if (foreignKey.columns.length !== foreignKey.references.columns.length) {
        issues.push(
          `Invalid ${label} on ${tableName}: local columns (${foreignKey.columns.length}) and referenced columns (${foreignKey.references.columns.length}) must have the same length.`,
        );
      }

      if (foreignKey.references.columns.length === 0) {
        issues.push(`Invalid ${label} on ${tableName}: referenced columns cannot be empty.`);
      }

      if (referencedTable) {
        for (const referencedColumn of foreignKey.references.columns) {
          if (!(referencedColumn in referencedTable.columns)) {
            issues.push(
              `Invalid ${label} on ${tableName}: referenced column "${referencedColumn}" does not exist on table "${foreignKey.references.table}".`,
            );
          }
        }
      }

      validateNoDuplicateColumns(
        `${tableName} -> ${foreignKey.references.table}`,
        `${label} referenced columns`,
        foreignKey.references.columns,
        issues,
      );
    });

    table.constraints?.checks?.forEach((checkConstraint, index) => {
      const label = checkConstraint.name ?? `check constraint #${index + 1}`;
      if (checkConstraint.kind === "in") {
        const hasValidColumn = validateConstraintColumns(
          schema,
          tableName,
          label,
          [checkConstraint.column],
          issues,
        );
        if (checkConstraint.values.length === 0) {
          issues.push(`Invalid ${label} on ${tableName}: values cannot be empty.`);
        }

        if (!hasValidColumn) {
          return;
        }

        const columnType = resolveTableColumnDefinition(
          schema,
          tableName,
          checkConstraint.column,
        ).type;
        const valueTypes = new Set(
          checkConstraint.values
            .filter((value): value is string | number | boolean => value != null)
            .map((value) => typeof value),
        );
        for (const valueType of valueTypes) {
          if (!columnTypeAllowsValueType(columnType, valueType)) {
            issues.push(
              `Invalid ${label} on ${tableName}: value type ${valueType} does not match column type ${columnType}.`,
            );
          }
        }
      }
    });
  }

  return issues;
}

function readColumnPrimaryKeyColumns(table: SchemaDefinition["tables"][string]): string[] {
  const primaryKeyColumns: string[] = [];

  for (const [columnName, columnDefinition] of Object.entries(table.columns)) {
    if (typeof columnDefinition === "string" || columnDefinition.primaryKey !== true) {
      continue;
    }
    primaryKeyColumns.push(columnName);
  }

  return primaryKeyColumns;
}

function validateColumnDefinition(
  tableName: string,
  columnName: string,
  definition: ResolvedColumnDefinition,
  issues: string[],
) {
  if (definition.primaryKey && definition.unique) {
    issues.push(
      `Invalid column ${tableName}.${columnName}: primaryKey and unique cannot both be true.`,
    );
  }

  if (definition.primaryKey && definition.nullable) {
    issues.push(
      `Invalid column ${tableName}.${columnName}: primaryKey columns must be nullable: false.`,
    );
  }

  if (definition.enum && definition.type !== "text") {
    issues.push(
      `Invalid column ${tableName}.${columnName}: enum is only supported on text columns.`,
    );
  }

  if (definition.enumFrom && definition.type !== "text") {
    issues.push(
      `Invalid column ${tableName}.${columnName}: enumFrom is only supported on text columns.`,
    );
  }

  if (definition.enumFrom && definition.enumFrom.trim().length === 0) {
    issues.push(`Invalid column ${tableName}.${columnName}: enumFrom cannot be empty.`);
  }

  if (definition.enum) {
    if (definition.enum.length === 0) {
      issues.push(`Invalid column ${tableName}.${columnName}: enum cannot be empty.`);
    }

    const unique = new Set(definition.enum);
    if (unique.size !== definition.enum.length) {
      issues.push(`Invalid column ${tableName}.${columnName}: enum contains duplicate values.`);
    }
  }

  if (definition.enumMap) {
    if (!definition.enumFrom) {
      issues.push(`Invalid column ${tableName}.${columnName}: enumMap requires enumFrom.`);
    }

    for (const [sourceValue, mappedValue] of Object.entries(definition.enumMap)) {
      if (sourceValue.length === 0) {
        issues.push(
          `Invalid column ${tableName}.${columnName}: enumMap contains an empty source key.`,
        );
      }
      if (mappedValue.length === 0) {
        issues.push(
          `Invalid column ${tableName}.${columnName}: enumMap contains an empty mapped value.`,
        );
      }
      if (definition.enum && !definition.enum.includes(mappedValue)) {
        issues.push(
          `Invalid column ${tableName}.${columnName}: enumMap value "${mappedValue}" is not listed in enum.`,
        );
      }
    }
  }

  if (definition.foreignKey) {
    if (definition.foreignKey.table.trim().length === 0) {
      issues.push(`Invalid column ${tableName}.${columnName}: foreignKey.table cannot be empty.`);
    }
    if (definition.foreignKey.column.trim().length === 0) {
      issues.push(`Invalid column ${tableName}.${columnName}: foreignKey.column cannot be empty.`);
    }
  }
}

function validateConstraintColumns(
  schema: SchemaDefinition,
  tableName: string,
  label: string,
  columns: string[],
  issues: string[],
) {
  let isValid = true;
  if (columns.length === 0) {
    issues.push(`Invalid ${label} on ${tableName}: columns cannot be empty.`);
    return false;
  }

  const table = getTable(schema, tableName);
  for (const column of columns) {
    if (!(column in table.columns)) {
      isValid = false;
      issues.push(
        `Invalid ${label} on ${tableName}: column "${column}" does not exist on table "${tableName}".`,
      );
    }
  }

  return isValid;
}

function validateNoDuplicateColumns(
  tableName: string,
  label: string,
  columns: string[],
  issues: string[],
) {
  const seen = new Set<string>();
  const duplicates = new Set<string>();
  for (const column of columns) {
    if (seen.has(column)) {
      if (!duplicates.has(column)) {
        issues.push(
          `Invalid ${label} on ${tableName}: duplicate column "${column}" in constraint definition.`,
        );
        duplicates.add(column);
      }
      continue;
    }
    seen.add(column);
  }
}

function columnTypeAllowsValueType(
  columnType: ResolvedColumnDefinition["type"],
  valueType: string,
) {
  switch (columnType) {
    case "text":
    case "timestamp":
    case "date":
    case "datetime":
    case "json":
      return valueType === "string";
    case "integer":
    case "real":
      return valueType === "number";
    case "boolean":
      return valueType === "boolean";
    case "blob":
      return false;
  }
}

function formatSchemaConstraintIssues(issues: readonly string[]) {
  const label = issues.length === 1 ? "issue" : "issues";
  return `Schema constraint validation failed with ${issues.length} ${label}:\n${issues
    .map((issue) => `- ${issue}`)
    .join("\n")}`;
}
