import { Result, type Result as BetterResult } from "better-result";
import type { TuplResult, TuplSchemaNormalizationError } from "@tupl/foundation";

import { resolveColumnDefinition } from "./definition";
import { copyNormalizedSchemaBindings, finalizeSchemaDefinition } from "./normalization";
import { createSchemaNormalizationError } from "./schema-errors";
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
  onUnresolved?: "error" | "ignore";
  strictUnmapped?: boolean;
}

export function resolveSchemaLinkedEnums<TSchema extends SchemaDefinition>(
  schema: TSchema,
  options: ResolveSchemaLinkedEnumsOptions = {},
): TuplResult<TSchema> {
  const resolveEnumValues = options.resolveEnumValues ?? defaultResolveLinkedEnumValues;
  const onUnresolved = options.onUnresolved ?? "error";
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

      const refResult = parseEnumLinkReference(resolved.enumFrom, tableName, columnName);
      if (Result.isError(refResult)) {
        return refResult;
      }
      const ref = refResult.value;

      const upstreamEnum = resolveEnumValues(ref, schema);
      if (!upstreamEnum || upstreamEnum.length === 0) {
        if (onUnresolved === "error") {
          return Result.err(
            createSchemaNormalizationError({
              operation: "resolve schema linked enums",
              message: `Unable to resolve enumFrom for ${tableName}.${columnName} from ${ref.table}.${ref.column}.`,
              table: tableName,
              column: columnName,
            }),
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
              return Result.err(
                createSchemaNormalizationError({
                  operation: "resolve schema linked enums",
                  message: `Unmapped enumFrom value "${upstreamValue}" for ${tableName}.${columnName}.`,
                  table: tableName,
                  column: columnName,
                }),
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
        return Result.err(
          createSchemaNormalizationError({
            operation: "resolve schema linked enums",
            message: `enumFrom resolution for ${tableName}.${columnName} produced no facade values.`,
            table: tableName,
            column: columnName,
          }),
        );
      }

      if (resolved.enum) {
        for (const enumValue of inferredEnum) {
          if (!resolved.enum.includes(enumValue)) {
            return Result.err(
              createSchemaNormalizationError({
                operation: "resolve schema linked enums",
                message: `enumFrom mapping produced value "${enumValue}" not listed in enum for ${tableName}.${columnName}.`,
                table: tableName,
                column: columnName,
              }),
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
    return Result.ok(schema);
  }

  const resolvedSchema: SchemaDefinition = { tables };
  copyNormalizedSchemaBindings(schema, resolvedSchema);
  return finalizeSchemaDefinition(resolvedSchema as TSchema);
}

function parseEnumLinkReference(
  enumFrom: string,
  tableName: string,
  columnName: string,
): BetterResult<EnumLinkReference, TuplSchemaNormalizationError> {
  const idx = enumFrom.lastIndexOf(".");
  if (idx < 0) {
    return Result.ok({
      table: tableName,
      column: enumFrom,
    });
  }

  const table = enumFrom.slice(0, idx).trim();
  const column = enumFrom.slice(idx + 1).trim();
  if (!table || !column) {
    return Result.err(
      createSchemaNormalizationError({
        operation: "parse enum link reference",
        message: `Invalid enumFrom reference on ${tableName}.${columnName}: "${enumFrom}".`,
        table: tableName,
        column: columnName,
      }),
    );
  }
  return Result.ok({ table, column });
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
