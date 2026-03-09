import { Result } from "better-result";

import type { DataEntityHandle } from "../model/data-entity";
import { TuplProviderBindingError } from "../model/errors";
import type { RelNode } from "../model/rel";
import type { ProvidersMap } from "../provider";
import {
  resolveColumnDefinition,
  resolveTableColumnDefinition,
  resolveTableForeignKeys,
  resolveTablePrimaryKeyConstraint,
  resolveTableUniqueConstraints,
  type ResolvedColumnDefinition,
  type SchemaDefinition,
} from "./definition";
import { getNormalizedTableBinding } from "./normalize";

export function finalizeSchemaDefinition<TSchema extends SchemaDefinition>(
  schema: TSchema,
): TSchema {
  validateNormalizedTableBindings(schema);
  validateTableProviders(schema);
  validateSchemaConstraints(schema);
  return schema;
}

function validateTableProviders(schema: SchemaDefinition): void {
  for (const [tableName, table] of Object.entries(schema.tables)) {
    const normalized = getNormalizedTableBinding(schema, tableName);
    if (normalized?.kind === "view") {
      continue;
    }

    if (table.provider == null) {
      continue;
    }

    if (typeof table.provider !== "string" || table.provider.trim().length === 0) {
      throw new Error(
        `Table ${tableName} must define a non-empty provider binding (table.provider).`,
      );
    }
  }
}

function validateNormalizedTableBindings(schema: SchemaDefinition): void {
  for (const [tableName, table] of Object.entries(schema.tables)) {
    const binding = getNormalizedTableBinding(schema, tableName);
    if (!binding) {
      throw new Error(
        `Table ${tableName} must be declared via createSchemaBuilder().table(name, provider.entities.someTable, config).`,
      );
    }

    if (binding.kind === "view") {
      continue;
    }

    if (typeof binding.entity !== "string" || binding.entity.trim().length === 0) {
      throw new Error(`Table ${tableName} is missing an entity-backed physical binding.`);
    }

    if (typeof binding.provider !== "string" || binding.provider.trim().length === 0) {
      throw new Error(`Table ${tableName} is missing a provider-backed physical binding.`);
    }

    if (table.provider !== binding.provider) {
      throw new Error(
        `Table ${tableName} must define provider ${binding.provider} to match its entity-backed physical binding.`,
      );
    }
  }
}

function validateSchemaConstraints(schema: SchemaDefinition): void {
  for (const [tableName, table] of Object.entries(schema.tables)) {
    for (const [columnName, columnDefinition] of Object.entries(table.columns)) {
      const resolved = resolveColumnDefinition(columnDefinition);
      validateColumnDefinition(tableName, columnName, resolved);
    }

    const columnPrimaryKeyColumns = Object.entries(table.columns)
      .filter(([, definition]) => typeof definition !== "string" && definition.primaryKey === true)
      .map(([columnName]) => columnName);
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

  const table = schema.tables[tableName];
  if (!table) {
    throw new Error(`Unknown table in schema constraints: ${tableName}`);
  }

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

export function validateRelAgainstSchema(node: RelNode, schema: SchemaDefinition): void {
  const validateScanColumn = (
    tableName: string,
    column: string,
    entity?: DataEntityHandle<string>,
  ): void => {
    if (entity?.columns) {
      const logicalColumn = column.includes(".")
        ? column.slice(column.lastIndexOf(".") + 1)
        : column;
      if (!(logicalColumn in entity.columns)) {
        throw new Error(`Unknown column in relational plan: ${tableName}.${logicalColumn}`);
      }
      return;
    }

    const table = schema.tables[tableName];
    if (!table) {
      return;
    }
    const logicalColumn = column.includes(".") ? column.slice(column.lastIndexOf(".") + 1) : column;
    if (!(logicalColumn in table.columns)) {
      throw new Error(`Unknown column in relational plan: ${tableName}.${logicalColumn}`);
    }
  };

  const visit = (current: RelNode, cteNames: Set<string>): void => {
    switch (current.kind) {
      case "scan":
        if (!cteNames.has(current.table) && !schema.tables[current.table] && !current.entity) {
          throw new Error(`Unknown table in relational plan: ${current.table}`);
        }
        if (!cteNames.has(current.table) && (schema.tables[current.table] || current.entity)) {
          for (const column of current.select) {
            validateScanColumn(current.table, column, current.entity);
          }
          for (const clause of current.where ?? []) {
            validateScanColumn(current.table, clause.column, current.entity);
          }
          for (const term of current.orderBy ?? []) {
            validateScanColumn(current.table, term.column, current.entity);
          }
        }
        return;
      case "sql":
        for (const table of current.tables) {
          if (!cteNames.has(table) && !schema.tables[table]) {
            throw new Error(`Unknown table in relational plan: ${table}`);
          }
        }
        return;
      case "filter":
      case "project":
      case "aggregate":
      case "window":
      case "sort":
      case "limit_offset":
        visit(current.input, cteNames);
        return;
      case "join":
      case "set_op":
        visit(current.left, cteNames);
        visit(current.right, cteNames);
        return;
      case "with": {
        const nextCteNames = new Set(cteNames);
        for (const cte of current.ctes) {
          nextCteNames.add(cte.name);
        }
        for (const cte of current.ctes) {
          visit(cte.query, nextCteNames);
        }
        visit(current.body, nextCteNames);
      }
    }
  };

  visit(node, new Set<string>());
}

export function resolveTableProvider(schema: SchemaDefinition, table: string): string {
  const result = resolveTableProviderResult(schema, table);
  if (Result.isError(result)) {
    throw result.error;
  }

  return result.value;
}

export function resolveTableProviderResult(schema: SchemaDefinition, table: string) {
  const normalized = getNormalizedTableBinding(schema, table);
  if (normalized?.kind === "physical" && normalized.provider) {
    return Result.ok(normalized.provider);
  }

  if (normalized?.kind === "view") {
    return Result.err(
      new TuplProviderBindingError({
        table,
        message: `View table ${table} does not have a direct provider binding.`,
      }),
    );
  }

  const tableDefinition = schema.tables[table];
  if (!tableDefinition) {
    return Result.err(
      new TuplProviderBindingError({
        table,
        message: `Unknown table: ${table}`,
      }),
    );
  }

  if (!tableDefinition.provider || tableDefinition.provider.length === 0) {
    return Result.err(
      new TuplProviderBindingError({
        table,
        message: `Table ${table} is missing required provider mapping.`,
      }),
    );
  }

  return Result.ok(tableDefinition.provider);
}

export function validateProviderBindings<TContext>(
  schema: SchemaDefinition,
  providers: ProvidersMap<TContext>,
): void {
  const result = validateProviderBindingsResult(schema, providers);
  if (Result.isError(result)) {
    throw result.error;
  }
}

export function validateProviderBindingsResult<TContext>(
  schema: SchemaDefinition,
  providers: ProvidersMap<TContext>,
) {
  for (const tableName of Object.keys(schema.tables)) {
    const normalized = getNormalizedTableBinding(schema, tableName);
    if (normalized?.kind === "view") {
      continue;
    }

    const providerNameResult =
      normalized?.kind === "physical" && normalized.provider
        ? Result.ok(normalized.provider)
        : resolveTableProviderResult(schema, tableName);
    if (Result.isError(providerNameResult)) {
      return providerNameResult;
    }

    const providerName = providerNameResult.value;
    if (!providers[providerName]) {
      return Result.err(
        new TuplProviderBindingError({
          table: tableName,
          provider: providerName,
          message: `Table ${tableName} is bound to provider ${providerName}, but no such provider is registered.`,
        }),
      );
    }
  }

  return Result.ok(undefined);
}
