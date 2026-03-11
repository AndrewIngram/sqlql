import type {
  DataEntityColumnMetadata,
  DataEntityReadMetadataMap,
  RelExpr,
} from "@tupl/foundation";

import type {
  SchemaColRefToken,
  SchemaDataEntityHandle,
  SchemaValueCoercion,
  SqlScalarType,
} from "./schema-contracts";
import type {
  SchemaCalculatedColumnDefinition,
  SchemaColumnLensDefinition,
  SchemaDslRelationRef,
  SchemaTypedColumnDefinition,
} from "./table-definition-contracts";

/**
 * Typed column builder contracts own compile-time compatibility checks and typed column builder signatures.
 */
type CompatibleSourceScalarType<TTarget extends SqlScalarType> = TTarget extends "real"
  ? "real" | "integer"
  : TTarget;

type IsAny<T> = 0 extends 1 & T ? true : false;
type IsExactly<T, U> = [T] extends [U] ? ([U] extends [T] ? true : false) : false;
type StripNullish<T> = Exclude<T, null | undefined>;

type InferSourceScalarTypeFromRead<TRead> =
  IsAny<TRead> extends true
    ? never
    : [StripNullish<TRead>] extends [never]
      ? never
      : StripNullish<TRead> extends number
        ? "integer" | "real"
        : StripNullish<TRead> extends boolean
          ? "boolean"
          : StripNullish<TRead> extends Uint8Array
            ? "blob"
            : StripNullish<TRead> extends Date
              ? "timestamp"
              : StripNullish<TRead> extends string
                ? "text"
                : StripNullish<TRead> extends object
                  ? "json"
                  : never;

type ExplicitSourceScalarType<TMetadata> = Extract<
  TMetadata extends { type?: infer TType } ? TType : never,
  SqlScalarType
>;

type KnownSourceScalarType<TMetadata> = [ExplicitSourceScalarType<TMetadata>] extends [never]
  ? InferSourceScalarTypeFromRead<
      TMetadata extends { readonly __read__?: infer TRead } ? TRead : never
    >
  : IsExactly<ExplicitSourceScalarType<TMetadata>, SqlScalarType> extends true
    ? InferSourceScalarTypeFromRead<
        TMetadata extends { readonly __read__?: infer TRead } ? TRead : never
      >
    : ExplicitSourceScalarType<TMetadata>;

type IsCompileTimeCompatibleSourceType<
  TSource extends SqlScalarType,
  TTarget extends SqlScalarType,
> = [Extract<TSource, CompatibleSourceScalarType<TTarget>>] extends [never] ? false : true;

type CompatibleColumnName<
  TColumns extends string,
  TColumnMetadata extends Partial<Record<TColumns, DataEntityColumnMetadata<any>>>,
  TTarget extends SqlScalarType,
> = Extract<
  {
    [K in TColumns]: [KnownSourceScalarType<TColumnMetadata[K]>] extends [never]
      ? K
      : IsCompileTimeCompatibleSourceType<
            KnownSourceScalarType<TColumnMetadata[K]>,
            TTarget
          > extends true
        ? K
        : never;
  }[TColumns],
  string
>;

interface SchemaTypedColumnBuilderOptions {
  nullable?: boolean;
  primaryKey?: boolean;
  unique?: boolean;
  enum?: readonly string[];
  enumFrom?: SchemaColRefToken | string;
  enumMap?: Record<string, string>;
  physicalType?: string;
  physicalDialect?: import("./schema-contracts").PhysicalDialect;
  foreignKey?: import("./schema-contracts").ColumnForeignKeyReference;
  description?: string;
  coerce?: SchemaValueCoercion;
}

type SchemaTypedColumnBuilderMethod<
  TSourceColumns extends string,
  TColumnMetadata extends Partial<Record<TSourceColumns, DataEntityColumnMetadata<any>>>,
  TType extends SqlScalarType,
  TOptions extends SchemaTypedColumnBuilderOptions,
> = {
  <TSourceColumn extends CompatibleColumnName<TSourceColumns, TColumnMetadata, TType>>(
    sourceColumn: TSourceColumn,
    options?: TOptions,
  ): SchemaTypedColumnDefinition<TSourceColumn>;
  <TSourceColumn extends TSourceColumns>(
    sourceColumn: TSourceColumn,
    options: TOptions & { coerce: SchemaValueCoercion },
  ): SchemaTypedColumnDefinition<TSourceColumn>;
  <TRelColumns extends string, TColumn extends TRelColumns>(
    table: SchemaDataEntityHandle<TRelColumns> | SchemaDslRelationRef<TRelColumns>,
    column: TColumn,
    options?: TOptions,
  ): SchemaColumnLensDefinition;
  (
    expr: RelExpr,
    options?: Omit<TOptions, "primaryKey" | "unique" | "enum" | "enumFrom" | "enumMap">,
  ): SchemaCalculatedColumnDefinition;
};

export interface SchemaTypedColumnBuilder<
  TSourceColumns extends string,
  TColumnMetadata extends Partial<Record<TSourceColumns, DataEntityColumnMetadata<any>>> =
    DataEntityReadMetadataMap<TSourceColumns, Record<TSourceColumns, unknown>>,
> {
  id: SchemaTypedColumnBuilderMethod<
    TSourceColumns,
    TColumnMetadata,
    "text",
    Omit<
      SchemaTypedColumnBuilderOptions,
      "primaryKey" | "nullable" | "enum" | "enumFrom" | "enumMap"
    >
  >;
  string: SchemaTypedColumnBuilderMethod<
    TSourceColumns,
    TColumnMetadata,
    "text",
    SchemaTypedColumnBuilderOptions
  >;
  integer: SchemaTypedColumnBuilderMethod<
    TSourceColumns,
    TColumnMetadata,
    "integer",
    SchemaTypedColumnBuilderOptions
  >;
  real: SchemaTypedColumnBuilderMethod<
    TSourceColumns,
    TColumnMetadata,
    "real",
    SchemaTypedColumnBuilderOptions
  >;
  blob: SchemaTypedColumnBuilderMethod<
    TSourceColumns,
    TColumnMetadata,
    "blob",
    SchemaTypedColumnBuilderOptions
  >;
  boolean: SchemaTypedColumnBuilderMethod<
    TSourceColumns,
    TColumnMetadata,
    "boolean",
    SchemaTypedColumnBuilderOptions
  >;
  timestamp: SchemaTypedColumnBuilderMethod<
    TSourceColumns,
    TColumnMetadata,
    "timestamp",
    SchemaTypedColumnBuilderOptions
  >;
  date: SchemaTypedColumnBuilderMethod<
    TSourceColumns,
    TColumnMetadata,
    "date",
    SchemaTypedColumnBuilderOptions
  >;
  datetime: SchemaTypedColumnBuilderMethod<
    TSourceColumns,
    TColumnMetadata,
    "datetime",
    SchemaTypedColumnBuilderOptions
  >;
  json: SchemaTypedColumnBuilderMethod<
    TSourceColumns,
    TColumnMetadata,
    "json",
    SchemaTypedColumnBuilderOptions
  >;
}
