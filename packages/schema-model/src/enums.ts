import { resolveColumnDefinition } from "./definition";
import { copyNormalizedSchemaBindings, finalizeSchemaDefinition } from "./normalization";
import type { ColumnDefinition, SchemaDefinition, TableColumns, TableDefinition } from "./types";

/**
 * Enum resolution owns `enumFrom` expansion and validation against upstream schema definitions.
 */
export interface EnumLinkReference {
  table: string;
  column: string;
}

export interface ResolveSchemaLinkedEnumsOptions {
  resolveEnumValues?: (
    ref: EnumLinkReference,
    schema: SchemaDefinition,
  ) => readonly string[] | undefined;
  onUnresolved?: "throw" | "ignore";
  strictUnmapped?: boolean;
}

export function resolveSchemaLinkedEnums(
  schema: SchemaDefinition,
  options: ResolveSchemaLinkedEnumsOptions = {},
): SchemaDefinition {
  const resolveEnumValues = options.resolveEnumValues ?? defaultResolveLinkedEnumValues;
  const onUnresolved = options.onUnresolved ?? "throw";
  const strictUnmapped = options.strictUnmapped ?? true;

  let changed = false;
  const tables: Record<string, TableDefinition> = {};

  for (const [tableName, table] of Object.entries(schema.tables)) {
    const columns: TableColumns = {};

    for (const [columnName, columnDefinition] of Object.entries(table.columns)) {
      if (typeof columnDefinition === "string") {
        columns[columnName] = columnDefinition;
        continue;
      }

      const resolved = resolveColumnDefinition(columnDefinition);
      if (!resolved.enumFrom) {
        columns[columnName] = columnDefinition;
        continue;
      }

      const ref = parseEnumLinkReference(resolved.enumFrom, tableName, columnName);
      const upstreamEnum = resolveEnumValues(ref, schema);
      if (!upstreamEnum || upstreamEnum.length === 0) {
        if (onUnresolved === "throw") {
          throw new Error(
            `Unable to resolve enumFrom for ${tableName}.${columnName} from ${ref.table}.${ref.column}.`,
          );
        }
        columns[columnName] = columnDefinition;
        continue;
      }

      const mappedValues: string[] = [];
      for (const upstreamValue of upstreamEnum) {
        if (resolved.enumMap) {
          const mapped = resolved.enumMap[upstreamValue];
          if (!mapped) {
            if (strictUnmapped) {
              throw new Error(
                `Unmapped enumFrom value "${upstreamValue}" for ${tableName}.${columnName}.`,
              );
            }
            continue;
          }
          mappedValues.push(mapped);
          continue;
        }
        mappedValues.push(upstreamValue);
      }

      const inferredEnum = [...new Set(mappedValues)];
      if (inferredEnum.length === 0 && strictUnmapped) {
        throw new Error(
          `enumFrom resolution for ${tableName}.${columnName} produced no facade values.`,
        );
      }

      if (resolved.enum) {
        for (const enumValue of inferredEnum) {
          if (!resolved.enum.includes(enumValue)) {
            throw new Error(
              `enumFrom mapping produced value "${enumValue}" not listed in enum for ${tableName}.${columnName}.`,
            );
          }
        }
      }

      const materializedEnum = resolved.enum ?? inferredEnum;
      columns[columnName] = {
        ...columnDefinition,
        enum: materializedEnum,
      } satisfies ColumnDefinition;
      changed = true;
    }

    tables[tableName] = {
      ...table,
      columns,
    };
  }

  if (!changed) {
    return schema;
  }

  const resolvedSchema: SchemaDefinition = { tables };
  copyNormalizedSchemaBindings(schema, resolvedSchema);
  return finalizeSchemaDefinition(resolvedSchema);
}

function parseEnumLinkReference(
  enumFrom: string,
  tableName: string,
  columnName: string,
): EnumLinkReference {
  const idx = enumFrom.lastIndexOf(".");
  if (idx < 0) {
    return {
      table: tableName,
      column: enumFrom,
    };
  }

  const table = enumFrom.slice(0, idx).trim();
  const column = enumFrom.slice(idx + 1).trim();
  if (!table || !column) {
    throw new Error(`Invalid enumFrom reference on ${tableName}.${columnName}: "${enumFrom}".`);
  }
  return { table, column };
}

function defaultResolveLinkedEnumValues(
  ref: EnumLinkReference,
  schema: SchemaDefinition,
): readonly string[] | undefined {
  const table = schema.tables[ref.table];
  if (!table) {
    return undefined;
  }

  const columnDefinition = table.columns[ref.column];
  if (!columnDefinition || typeof columnDefinition === "string") {
    return undefined;
  }

  const resolved = resolveColumnDefinition(columnDefinition);
  return resolved.enum;
}
