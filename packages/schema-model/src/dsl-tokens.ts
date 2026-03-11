import type { RelExpr } from "@tupl/foundation";

import type {
  SchemaCalculatedColumnDefinition,
  SchemaColRefToken,
  SchemaColumnLensDefinition,
  SchemaDataEntityHandle,
  SchemaDslTableToken,
  SchemaTypedColumnDefinition,
} from "./types";

/**
 * DSL tokens own the token constructors and type guards used by the schema builder DSL.
 */
export function createSchemaDslTableToken<
  TColumns extends string,
>(): SchemaDslTableToken<TColumns> {
  return {
    kind: "dsl_table_token",
    __id: Symbol("schema_dsl_table"),
  } as SchemaDslTableToken<TColumns>;
}

export function isSchemaDslTableToken(value: unknown): value is SchemaDslTableToken<string> {
  return (
    !!value &&
    typeof value === "object" &&
    (value as { kind?: unknown }).kind === "dsl_table_token" &&
    typeof (value as { __id?: unknown }).__id === "symbol"
  );
}

export function isSchemaDataEntityHandle(value: unknown): value is SchemaDataEntityHandle<string> {
  return (
    !!value &&
    typeof value === "object" &&
    (value as { kind?: unknown }).kind === "data_entity" &&
    typeof (value as { entity?: unknown }).entity === "string" &&
    typeof (value as { provider?: unknown }).provider === "string"
  );
}

export function isRelExpr(value: unknown): value is RelExpr {
  if (!value || typeof value !== "object") {
    return false;
  }

  const kind = (value as { kind?: unknown }).kind;
  if (kind === "literal") {
    return true;
  }
  if (kind === "function") {
    return Array.isArray((value as { args?: unknown }).args);
  }
  if (kind === "column") {
    return (
      !!(value as { ref?: unknown }).ref && typeof (value as { ref?: unknown }).ref === "object"
    );
  }
  return kind === "subquery";
}

export function isDslTableDefinition(value: unknown): boolean {
  return (
    !!value &&
    typeof value === "object" &&
    (value as { kind?: unknown }).kind === "dsl_table" &&
    isSchemaDslTableToken((value as { tableToken?: unknown }).tableToken)
  );
}

export function isDslViewDefinition(value: unknown): boolean {
  return (
    !!value &&
    typeof value === "object" &&
    (value as { kind?: unknown }).kind === "dsl_view" &&
    isSchemaDslTableToken((value as { tableToken?: unknown }).tableToken)
  );
}

export function isSchemaTypedColumnDefinition(
  value: unknown,
): value is SchemaTypedColumnDefinition<string> {
  return (
    !!value &&
    typeof value === "object" &&
    (value as { kind?: unknown }).kind === "dsl_typed_column" &&
    typeof (value as { sourceColumn?: unknown }).sourceColumn === "string"
  );
}

export function isSchemaCalculatedColumnDefinition(
  value: unknown,
): value is SchemaCalculatedColumnDefinition {
  return (
    !!value &&
    typeof value === "object" &&
    (value as { kind?: unknown }).kind === "dsl_calculated_column" &&
    isRelExpr((value as { expr?: unknown }).expr)
  );
}

export function isSchemaColRefToken(value: unknown): value is SchemaColRefToken {
  if (!value || typeof value !== "object" || (value as { kind?: unknown }).kind !== "dsl_col_ref") {
    return false;
  }

  const token = value as { ref?: unknown; table?: unknown; entity?: unknown; column?: unknown };
  const hasStringRef = typeof token.ref === "string";
  const hasTableColumnRef = isSchemaDslTableToken(token.table) && typeof token.column === "string";
  const hasEntityColumnRef =
    isSchemaDataEntityHandle(token.entity) && typeof token.column === "string";
  return hasStringRef || hasTableColumnRef || hasEntityColumnRef;
}

export function isColumnLensDefinition(value: unknown): value is SchemaColumnLensDefinition {
  if (!value || typeof value !== "object") {
    return false;
  }

  const source = (value as { source?: unknown }).source;
  return typeof source === "string" || isSchemaColRefToken(source);
}

export function toSchemaDslTableToken<TColumns extends string>(
  table: unknown,
): SchemaDslTableToken<TColumns> {
  if (isSchemaDslTableToken(table)) {
    return table as SchemaDslTableToken<TColumns>;
  }
  return (table as { tableToken: SchemaDslTableToken<TColumns> }).tableToken;
}
