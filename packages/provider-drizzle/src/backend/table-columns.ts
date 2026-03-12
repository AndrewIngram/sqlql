import type { AnyColumn } from "drizzle-orm";
import type { DataEntityHandle } from "@tupl/provider-kit";

import type {
  DrizzleColumnMap,
  DrizzleProviderTableConfig,
  InferredDrizzleSqlScalarType,
} from "../types";

export function resolveColumns<TContext>(
  tableConfig: DrizzleProviderTableConfig<TContext>,
  tableName: string,
): DrizzleColumnMap<string> {
  if (tableConfig.columns) {
    return tableConfig.columns;
  }

  const derived = deriveColumnsFromTable(tableConfig.table);
  if (Object.keys(derived).length === 0) {
    throw new Error(
      `Unable to derive columns for table "${tableName}". Provide an explicit columns map.`,
    );
  }

  return derived;
}

export function deriveColumnsFromTable(table: object): DrizzleColumnMap<string> {
  const out: DrizzleColumnMap<string> = {};

  for (const [propertyKey, raw] of Object.entries(table as Record<string, unknown>)) {
    if (!looksLikeDrizzleColumn(raw)) {
      continue;
    }

    const column = raw as AnyColumn;
    out[propertyKey] = column;

    const dbName = readColumnName(column);
    if (dbName) {
      out[dbName] = column;
    }
  }

  return out;
}

export function deriveEntityColumnsFromTable(table: object): DataEntityHandle<string>["columns"] {
  const out: NonNullable<DataEntityHandle<string>["columns"]> = {};

  for (const [propertyKey, raw] of Object.entries(table as Record<string, unknown>)) {
    if (!looksLikeDrizzleColumn(raw)) {
      continue;
    }

    const column = raw as AnyColumn;
    const metadata: NonNullable<DataEntityHandle<string>["columns"]>[string] = {
      source: readColumnName(column) ?? propertyKey,
    };
    const inferredType = inferTuplTypeFromDrizzleColumn(column);
    if (inferredType) {
      metadata.type = inferredType;
    }
    if (column.notNull) {
      metadata.nullable = false;
    }
    if (column.primary) {
      metadata.primaryKey = true;
    } else if (column.isUnique) {
      metadata.unique = true;
    }
    if (Array.isArray(column.enumValues) && column.enumValues.length > 0) {
      metadata.enum = column.enumValues;
    }
    if (typeof column.dataType === "string") {
      metadata.physicalType = column.dataType;
    }
    out[propertyKey] = metadata;
  }

  return out;
}

export function inferTuplTypeFromDrizzleColumn(
  column: AnyColumn,
): InferredDrizzleSqlScalarType | undefined {
  const dataTypeValue = (column as { dataType?: unknown }).dataType;
  const dataType = typeof dataTypeValue === "string" ? dataTypeValue.toLowerCase() : "";
  const sqlType = typeof column.getSQLType === "function" ? column.getSQLType().toLowerCase() : "";
  const normalizedSqlType = sqlType.replace(/\s+/g, " ");

  if (dataType === "boolean" || sqlType === "boolean") {
    return "boolean";
  }
  if (dataType === "json" || normalizedSqlType.includes("json")) {
    return "json";
  }
  if (
    dataType === "arraybuffer" ||
    normalizedSqlType.includes("blob") ||
    normalizedSqlType.includes("bytea")
  ) {
    return "blob";
  }
  if (normalizedSqlType.includes("datetime")) {
    return "datetime";
  }
  if (normalizedSqlType === "date") {
    return "date";
  }
  if (dataType === "date" || normalizedSqlType.includes("timestamp")) {
    return "timestamp";
  }
  if (
    normalizedSqlType.includes("real") ||
    normalizedSqlType.includes("double") ||
    normalizedSqlType.includes("float")
  ) {
    return "real";
  }
  if (
    dataType === "number" ||
    normalizedSqlType.includes("int") ||
    normalizedSqlType.includes("numeric") ||
    normalizedSqlType.includes("decimal")
  ) {
    return "integer";
  }
  if (dataType === "string" || sqlType.length > 0) {
    return "text";
  }
  return undefined;
}

export function looksLikeDrizzleColumn(value: unknown): value is AnyColumn {
  return (
    !!value && typeof value === "object" && typeof (value as { name?: unknown }).name === "string"
  );
}

export function readColumnName(column: AnyColumn): string | null {
  const maybeName = (column as unknown as { name?: unknown }).name;
  return typeof maybeName === "string" ? maybeName : null;
}
