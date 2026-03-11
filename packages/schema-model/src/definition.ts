import type { PhysicalDialect, SchemaDefinition, SqlScalarType } from "./index";
import type {
  ColumnDefinition,
  ColumnForeignKeyReference,
  ForeignKeyConstraint,
  PrimaryKeyConstraint,
  TableColumnDefinition,
  TableDefinition,
  UniqueConstraint,
} from "./index";

/**
 * Schema definition helpers normalize and resolve table and column metadata without touching
 * builder state or runtime execution concerns.
 */
export interface ResolvedColumnDefinition {
  type: SqlScalarType;
  nullable: boolean;
  primaryKey: boolean;
  unique: boolean;
  enum?: readonly string[];
  enumFrom?: string;
  enumMap?: Record<string, string>;
  physicalType?: string;
  physicalDialect?: PhysicalDialect;
  foreignKey?: ColumnForeignKeyReference;
  description?: string;
}

function readColumnPrimaryKeyColumns(table: TableDefinition): string[] {
  const primaryKeyColumns: string[] = [];

  for (const [columnName, columnDefinition] of Object.entries(table.columns)) {
    if (typeof columnDefinition === "string" || columnDefinition.primaryKey !== true) {
      continue;
    }
    primaryKeyColumns.push(columnName);
  }

  return primaryKeyColumns;
}

function readColumnUniqueConstraints(table: TableDefinition): UniqueConstraint[] {
  const uniqueConstraints: UniqueConstraint[] = [];

  for (const [columnName, columnDefinition] of Object.entries(table.columns)) {
    if (typeof columnDefinition === "string" || columnDefinition.unique !== true) {
      continue;
    }
    uniqueConstraints.push({
      columns: [columnName],
    });
  }

  return uniqueConstraints;
}

function dedupeUniqueConstraints(uniqueConstraints: UniqueConstraint[]): UniqueConstraint[] {
  const out: UniqueConstraint[] = [];
  const seen = new Set<string>();

  for (const uniqueConstraint of uniqueConstraints) {
    const signature = JSON.stringify({
      columns: uniqueConstraint.columns,
    });
    if (seen.has(signature)) {
      continue;
    }
    seen.add(signature);
    out.push(uniqueConstraint);
  }

  return out;
}

function dedupeForeignKeys(foreignKeys: ForeignKeyConstraint[]): ForeignKeyConstraint[] {
  const out: ForeignKeyConstraint[] = [];
  const seen = new Set<string>();

  for (const foreignKey of foreignKeys) {
    const signature = JSON.stringify({
      columns: foreignKey.columns,
      references: foreignKey.references,
      name: foreignKey.name ?? null,
      onDelete: foreignKey.onDelete ?? null,
      onUpdate: foreignKey.onUpdate ?? null,
    });
    if (seen.has(signature)) {
      continue;
    }
    seen.add(signature);
    out.push(foreignKey);
  }

  return out;
}

function normalizeEnumFromDefinition(
  enumFrom: ColumnDefinition["enumFrom"] | undefined,
): string | undefined {
  if (!enumFrom) {
    return undefined;
  }

  if (typeof enumFrom === "string") {
    return enumFrom;
  }

  return enumFrom.ref;
}

export function getTable(schema: SchemaDefinition, tableName: string): TableDefinition {
  const table = schema.tables[tableName];
  if (!table) {
    throw new Error(`Unknown table: ${tableName}`);
  }
  return table;
}

export function resolveTablePrimaryKeyConstraint(
  table: TableDefinition,
): PrimaryKeyConstraint | undefined {
  if (table.constraints?.primaryKey) {
    return table.constraints.primaryKey;
  }

  const primaryKeyColumns = readColumnPrimaryKeyColumns(table);
  if (primaryKeyColumns.length !== 1) {
    return undefined;
  }
  const primaryKeyColumn = primaryKeyColumns[0];
  if (!primaryKeyColumn) {
    return undefined;
  }

  return {
    columns: [primaryKeyColumn],
  };
}

export function resolveTableUniqueConstraints(table: TableDefinition): UniqueConstraint[] {
  return dedupeUniqueConstraints([
    ...readColumnUniqueConstraints(table),
    ...(table.constraints?.unique ?? []),
  ]);
}

export function resolveTableForeignKeys(table: TableDefinition): ForeignKeyConstraint[] {
  const fromColumns: ForeignKeyConstraint[] = [];
  for (const [columnName, columnDefinition] of Object.entries(table.columns)) {
    if (typeof columnDefinition === "string" || !columnDefinition.foreignKey) {
      continue;
    }

    const foreignKey = columnDefinition.foreignKey;
    fromColumns.push({
      columns: [columnName],
      references: {
        table: foreignKey.table,
        columns: [foreignKey.column],
      },
      ...(foreignKey.name ? { name: foreignKey.name } : {}),
      ...(foreignKey.onDelete ? { onDelete: foreignKey.onDelete } : {}),
      ...(foreignKey.onUpdate ? { onUpdate: foreignKey.onUpdate } : {}),
    });
  }

  return dedupeForeignKeys([...fromColumns, ...(table.constraints?.foreignKeys ?? [])]);
}

export function resolveColumnDefinition(
  definition: TableColumnDefinition,
): ResolvedColumnDefinition {
  if (typeof definition === "string") {
    return {
      type: definition,
      nullable: true,
      primaryKey: false,
      unique: false,
    };
  }

  const normalizedEnumFrom = normalizeEnumFromDefinition(definition.enumFrom);

  return {
    type: definition.type,
    nullable: definition.nullable ?? true,
    primaryKey: definition.primaryKey === true,
    unique: definition.unique === true,
    ...(definition.enum ? { enum: definition.enum } : {}),
    ...(normalizedEnumFrom ? { enumFrom: normalizedEnumFrom } : {}),
    ...(definition.enumMap ? { enumMap: definition.enumMap } : {}),
    ...(definition.physicalType ? { physicalType: definition.physicalType } : {}),
    ...(definition.physicalDialect ? { physicalDialect: definition.physicalDialect } : {}),
    ...(definition.foreignKey ? { foreignKey: definition.foreignKey } : {}),
    ...(definition.description ? { description: definition.description } : {}),
  };
}

export function resolveTableColumnDefinition(
  schema: SchemaDefinition,
  tableName: string,
  columnName: string,
): ResolvedColumnDefinition {
  const table = getTable(schema, tableName);
  const column = table.columns[columnName];
  if (!column) {
    throw new Error(`Unknown column ${tableName}.${columnName}`);
  }

  return resolveColumnDefinition(column);
}

export function resolveColumnType(definition: TableColumnDefinition): SqlScalarType {
  return resolveColumnDefinition(definition).type;
}
