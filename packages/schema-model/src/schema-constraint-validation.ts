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

/**
 * Schema constraint validation owns logical schema invariants for tables, columns, and constraints.
 */
export function validateSchemaConstraints(schema: SchemaDefinition): void {
  for (const [tableName, table] of Object.entries(schema.tables)) {
    for (const [columnName, columnDefinition] of Object.entries(table.columns)) {
      const resolved = resolveColumnDefinition(columnDefinition);
      validateColumnDefinition(tableName, columnName, resolved);
    }

    const columnPrimaryKeyColumns = readColumnPrimaryKeyColumns(table);
    if (columnPrimaryKeyColumns.length > 1) {
      throw new Error(
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
        throw new Error(
          `Invalid primary key on ${tableName}: column-level primaryKey on "${columnPrimaryKeyColumn}" conflicts with table.constraints.primaryKey. Use one declaration style.`,
        );
      }
    }

    const resolvedPrimaryKey = resolveTablePrimaryKeyConstraint(table);
    if (resolvedPrimaryKey) {
      validateConstraintColumns(schema, tableName, "primary key", resolvedPrimaryKey.columns);
      validateNoDuplicateColumns(tableName, "primary key", resolvedPrimaryKey.columns);
    }

    resolveTableUniqueConstraints(table).forEach((uniqueConstraint, index) => {
      const label = uniqueConstraint.name ?? `unique constraint #${index + 1}`;
      validateConstraintColumns(schema, tableName, label, uniqueConstraint.columns);
      validateNoDuplicateColumns(tableName, label, uniqueConstraint.columns);
    });

    const foreignKeys = resolveTableForeignKeys(table);
    foreignKeys.forEach((foreignKey, index) => {
      const label = foreignKey.name ?? `foreign key #${index + 1}`;
      validateConstraintColumns(schema, tableName, label, foreignKey.columns);
      validateNoDuplicateColumns(tableName, label, foreignKey.columns);

      const referencedTable = schema.tables[foreignKey.references.table];
      if (!referencedTable) {
        throw new Error(
          `Invalid ${label} on ${tableName}: referenced table "${foreignKey.references.table}" does not exist.`,
        );
      }

      if (foreignKey.columns.length !== foreignKey.references.columns.length) {
        throw new Error(
          `Invalid ${label} on ${tableName}: local columns (${foreignKey.columns.length}) and referenced columns (${foreignKey.references.columns.length}) must have the same length.`,
        );
      }

      if (foreignKey.references.columns.length === 0) {
        throw new Error(`Invalid ${label} on ${tableName}: referenced columns cannot be empty.`);
      }

      for (const referencedColumn of foreignKey.references.columns) {
        if (!(referencedColumn in referencedTable.columns)) {
          throw new Error(
            `Invalid ${label} on ${tableName}: referenced column "${referencedColumn}" does not exist on table "${foreignKey.references.table}".`,
          );
        }
      }

      validateNoDuplicateColumns(
        `${tableName} -> ${foreignKey.references.table}`,
        `${label} referenced columns`,
        foreignKey.references.columns,
      );
    });

    table.constraints?.checks?.forEach((checkConstraint, index) => {
      const label = checkConstraint.name ?? `check constraint #${index + 1}`;
      if (checkConstraint.kind === "in") {
        validateConstraintColumns(schema, tableName, label, [checkConstraint.column]);
        if (checkConstraint.values.length === 0) {
          throw new Error(`Invalid ${label} on ${tableName}: values cannot be empty.`);
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
          if (
            ((columnType === "text" ||
              columnType === "timestamp" ||
              columnType === "date" ||
              columnType === "datetime" ||
              columnType === "json") &&
              valueType !== "string") ||
            ((columnType === "integer" || columnType === "real") && valueType !== "number") ||
            (columnType === "boolean" && valueType !== "boolean") ||
            columnType === "blob"
          ) {
            throw new Error(
              `Invalid ${label} on ${tableName}: value type ${valueType} does not match column type ${columnType}.`,
            );
          }
        }
      }
    });
  }
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
): void {
  if (definition.primaryKey && definition.unique) {
    throw new Error(
      `Invalid column ${tableName}.${columnName}: primaryKey and unique cannot both be true.`,
    );
  }

  if (definition.primaryKey && definition.nullable) {
    throw new Error(
      `Invalid column ${tableName}.${columnName}: primaryKey columns must be nullable: false.`,
    );
  }

  if (definition.enum && definition.type !== "text") {
    throw new Error(
      `Invalid column ${tableName}.${columnName}: enum is only supported on text columns.`,
    );
  }

  if (definition.enumFrom && definition.type !== "text") {
    throw new Error(
      `Invalid column ${tableName}.${columnName}: enumFrom is only supported on text columns.`,
    );
  }

  if (definition.enumFrom && definition.enumFrom.trim().length === 0) {
    throw new Error(`Invalid column ${tableName}.${columnName}: enumFrom cannot be empty.`);
  }

  if (definition.enum) {
    if (definition.enum.length === 0) {
      throw new Error(`Invalid column ${tableName}.${columnName}: enum cannot be empty.`);
    }

    const unique = new Set(definition.enum);
    if (unique.size !== definition.enum.length) {
      throw new Error(`Invalid column ${tableName}.${columnName}: enum contains duplicate values.`);
    }
  }

  if (definition.enumMap) {
    if (!definition.enumFrom) {
      throw new Error(`Invalid column ${tableName}.${columnName}: enumMap requires enumFrom.`);
    }

    for (const [sourceValue, mappedValue] of Object.entries(definition.enumMap)) {
      if (sourceValue.length === 0) {
        throw new Error(
          `Invalid column ${tableName}.${columnName}: enumMap contains an empty source key.`,
        );
      }
      if (mappedValue.length === 0) {
        throw new Error(
          `Invalid column ${tableName}.${columnName}: enumMap contains an empty mapped value.`,
        );
      }
      if (definition.enum && !definition.enum.includes(mappedValue)) {
        throw new Error(
          `Invalid column ${tableName}.${columnName}: enumMap value "${mappedValue}" is not listed in enum.`,
        );
      }
    }
  }

  if (definition.foreignKey) {
    if (definition.foreignKey.table.trim().length === 0) {
      throw new Error(
        `Invalid column ${tableName}.${columnName}: foreignKey.table cannot be empty.`,
      );
    }
    if (definition.foreignKey.column.trim().length === 0) {
      throw new Error(
        `Invalid column ${tableName}.${columnName}: foreignKey.column cannot be empty.`,
      );
    }
  }
}

function validateConstraintColumns(
  schema: SchemaDefinition,
  tableName: string,
  label: string,
  columns: string[],
): void {
  if (columns.length === 0) {
    throw new Error(`Invalid ${label} on ${tableName}: columns cannot be empty.`);
  }

  const table = getTable(schema, tableName);
  for (const column of columns) {
    if (!(column in table.columns)) {
      throw new Error(
        `Invalid ${label} on ${tableName}: column "${column}" does not exist on table "${tableName}".`,
      );
    }
  }
}

function validateNoDuplicateColumns(tableName: string, label: string, columns: string[]): void {
  const seen = new Set<string>();
  for (const column of columns) {
    if (seen.has(column)) {
      throw new Error(
        `Invalid ${label} on ${tableName}: duplicate column "${column}" in constraint definition.`,
      );
    }
    seen.add(column);
  }
}
