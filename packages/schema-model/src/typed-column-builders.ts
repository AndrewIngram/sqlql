import type {
  DataEntityColumnMetadata,
  DataEntityReadMetadataMap,
  RelExpr,
} from "@tupl/foundation";

import {
  isDslTableDefinition,
  isDslViewDefinition,
  isRelExpr,
  isSchemaDataEntityHandle,
  isSchemaDslTableToken,
  toSchemaDslTableToken,
} from "./dsl-tokens";
import type {
  ColumnDefinition,
  ColumnForeignKeyReference,
  SchemaCalculatedColumnDefinition,
  SchemaColRefToken,
  SchemaColumnLensDefinition,
  SchemaTypedColumnDefinition,
  SchemaTypedColumnBuilder,
  SchemaValueCoercion,
  SqlScalarType,
} from "./types";

export interface SchemaTypedColumnBuilderOptions {
  nullable?: boolean;
  primaryKey?: boolean;
  unique?: boolean;
  enum?: readonly string[];
  enumFrom?: SchemaColRefToken | string;
  enumMap?: Record<string, string>;
  physicalType?: string;
  physicalDialect?: string;
  foreignKey?: ColumnForeignKeyReference;
  description?: string;
  coerce?: SchemaValueCoercion;
}

/**
 * Typed column builders own source-lens and calculated-column construction for the schema DSL.
 */
export function buildTypedColumnBuilder<
  TSourceColumns extends string,
  TColumnMetadata extends Partial<Record<TSourceColumns, DataEntityColumnMetadata<any>>> =
    DataEntityReadMetadataMap<TSourceColumns, Record<TSourceColumns, unknown>>,
>(): SchemaTypedColumnBuilder<TSourceColumns, TColumnMetadata> {
  const buildSourceLensDefinition = (
    source: string | SchemaColRefToken,
    type: SqlScalarType,
    options: SchemaTypedColumnBuilderOptions = {},
  ): SchemaColumnLensDefinition => ({
    source,
    type,
    ...(options.nullable != null ? { nullable: options.nullable } : {}),
    ...(options.primaryKey != null ? { primaryKey: options.primaryKey } : {}),
    ...(options.unique != null ? { unique: options.unique } : {}),
    ...(options.enum ? { enum: options.enum } : {}),
    ...(options.enumFrom ? { enumFrom: options.enumFrom } : {}),
    ...(options.enumMap ? { enumMap: options.enumMap } : {}),
    ...(options.physicalType ? { physicalType: options.physicalType } : {}),
    ...(options.physicalDialect ? { physicalDialect: options.physicalDialect as never } : {}),
    ...(options.foreignKey ? { foreignKey: options.foreignKey } : {}),
    ...(options.description ? { description: options.description } : {}),
    ...(options.coerce ? { coerce: options.coerce } : {}),
  });

  const buildTypedColumnDefinition = <TSourceColumn extends string>(
    sourceColumn: TSourceColumn,
    type: SqlScalarType,
    options: SchemaTypedColumnBuilderOptions = {},
  ): SchemaTypedColumnDefinition<TSourceColumn> => {
    const definition = { type } as ColumnDefinition;
    if (options.nullable != null) {
      definition.nullable = options.nullable;
    }
    if (options.primaryKey === true) {
      definition.primaryKey = true;
    } else if (options.unique === true) {
      definition.unique = true;
    }
    if (options.enum) {
      definition.enum = options.enum;
    }
    if (options.enumFrom) {
      definition.enumFrom = options.enumFrom;
    }
    if (options.enumMap) {
      definition.enumMap = options.enumMap;
    }
    if (options.physicalType) {
      definition.physicalType = options.physicalType;
    }
    if (options.physicalDialect) {
      definition.physicalDialect = options.physicalDialect as never;
    }
    if (options.foreignKey) {
      definition.foreignKey = options.foreignKey;
    }
    if (options.description) {
      definition.description = options.description;
    }
    return {
      kind: "dsl_typed_column",
      sourceColumn,
      definition,
      ...(options.coerce ? { coerce: options.coerce } : {}),
    };
  };

  const buildCalculatedColumnDefinition = (
    expr: RelExpr,
    type: SqlScalarType,
    options: Omit<
      SchemaTypedColumnBuilderOptions,
      "primaryKey" | "unique" | "enum" | "enumFrom" | "enumMap"
    > = {},
  ): SchemaCalculatedColumnDefinition => {
    const definition = {
      type,
      ...(options.nullable != null ? { nullable: options.nullable } : {}),
      ...(options.physicalType ? { physicalType: options.physicalType } : {}),
      ...(options.physicalDialect ? { physicalDialect: options.physicalDialect as never } : {}),
      ...(options.foreignKey ? { foreignKey: options.foreignKey } : {}),
      ...(options.description ? { description: options.description } : {}),
    } satisfies ColumnDefinition;

    return {
      kind: "dsl_calculated_column",
      expr,
      definition,
      ...(options.coerce ? { coerce: options.coerce } : {}),
    };
  };

  const build = (type: SqlScalarType) =>
    ((arg1: unknown, arg2?: unknown, arg3?: unknown) => {
      if (isRelExpr(arg1)) {
        return buildCalculatedColumnDefinition(
          arg1,
          type,
          (arg2 as
            | Omit<
                SchemaTypedColumnBuilderOptions,
                "primaryKey" | "unique" | "enum" | "enumFrom" | "enumMap"
              >
            | undefined) ?? {},
        );
      }

      if (
        (isSchemaDslTableToken(arg1) || isDslTableDefinition(arg1) || isDslViewDefinition(arg1)) &&
        typeof arg2 === "string"
      ) {
        return buildSourceLensDefinition(
          {
            kind: "dsl_col_ref",
            table: toSchemaDslTableToken(arg1),
            column: arg2,
          },
          type,
          (arg3 as SchemaTypedColumnBuilderOptions | undefined) ?? {},
        );
      }

      if (isSchemaDataEntityHandle(arg1) && typeof arg2 === "string") {
        return buildSourceLensDefinition(
          {
            kind: "dsl_col_ref",
            entity: arg1,
            column: arg2,
          },
          type,
          (arg3 as SchemaTypedColumnBuilderOptions | undefined) ?? {},
        );
      }

      return buildTypedColumnDefinition(
        arg1 as TSourceColumns,
        type,
        (arg2 as SchemaTypedColumnBuilderOptions | undefined) ?? {},
      );
    }) as never;

  return {
    id: ((arg1: unknown, arg2?: unknown, arg3?: unknown) => {
      const options = (
        isRelExpr(arg1)
          ? arg2
          : isSchemaDslTableToken(arg1) ||
              isDslTableDefinition(arg1) ||
              isDslViewDefinition(arg1) ||
              isSchemaDataEntityHandle(arg1)
            ? arg3
            : arg2
      ) as SchemaTypedColumnBuilderOptions | undefined;
      return (build("text") as (...args: unknown[]) => unknown)(arg1, arg2, {
        ...options,
        nullable: false,
        primaryKey: true,
      });
    }) as never,
    string: build("text"),
    integer: build("integer"),
    real: build("real"),
    blob: build("blob"),
    boolean: build("boolean"),
    timestamp: build("timestamp"),
    date: build("date"),
    datetime: build("datetime"),
    json: build("json"),
  };
}
