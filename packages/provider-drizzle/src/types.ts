import type { AnyColumn, InferSelectModel, Table, SQL } from "drizzle-orm";
import type {
  DataEntityColumnMetadata,
  DataEntityHandle,
  DataEntityReadMetadataMap,
  DataEntityShape,
  InferDataEntityShapeMetadata,
  ProviderRuntimeBinding,
} from "@tupl/provider-kit";
import type { SqlScalarType } from "@tupl/foundation";

export type DrizzleColumnMap<TColumn extends string = string> = Record<TColumn, AnyColumn>;

export interface DrizzleQueryExecutor {
  select: (...args: unknown[]) => unknown;
}

export interface DrizzleProviderTableConfig<
  TContext,
  TTable extends object = object,
  TColumn extends string = string,
> {
  table: TTable;
  /**
   * Optional explicit column map. If omitted, columns are derived from the
   * Drizzle table object and exposed by both property key and DB column name.
   */
  columns?: DrizzleColumnMap<TColumn>;
  shape?: DataEntityShape<TColumn>;
  scope?:
    | ((context: TContext) => SQL | SQL[] | undefined | Promise<SQL | SQL[] | undefined>)
    | undefined;
}

export interface CreateDrizzleProviderOptions<
  TContext,
  TTables extends Record<string, DrizzleProviderTableConfig<TContext>> = Record<
    string,
    DrizzleProviderTableConfig<TContext>
  >,
> {
  name?: string;
  dialect?: "postgres" | "sqlite";
  db: ProviderRuntimeBinding<TContext, DrizzleQueryExecutor>;
  tables: TTables;
}

export type InferDrizzleEntityRow<TConfig> =
  TConfig extends DrizzleProviderTableConfig<any, infer TTable, any>
    ? TTable extends Table
      ? InferSelectModel<TTable>
      : Record<string, unknown>
    : Record<string, unknown>;

export type InferDrizzleTableColumns<TConfig> =
  TConfig extends DrizzleProviderTableConfig<any, any, infer TColumn>
    ? [TColumn] extends [string]
      ? Extract<keyof InferDrizzleEntityRow<TConfig>, string> extends never
        ? TColumn
        : Extract<keyof InferDrizzleEntityRow<TConfig>, string>
      : Extract<keyof InferDrizzleEntityRow<TConfig>, string>
    : string;

export type InferDrizzleColumnRead<TColumn> = TColumn extends {
  _: {
    data: infer TData;
    notNull: infer TNotNull;
  };
}
  ? TNotNull extends true
    ? TData
    : TData | null
  : unknown;

export type InferDrizzleScalarTypeFromColumnMetadata<
  TColumnType extends string,
  TDataType extends string,
> = TColumnType extends `${string}Timestamp${string}`
  ? "timestamp"
  : TColumnType extends `${string}DateTime${string}`
    ? "datetime"
    : TColumnType extends `${string}Date${string}`
      ? "date"
      : TDataType extends "boolean"
        ? "boolean"
        : TDataType extends "json"
          ? "json"
          : TDataType extends "arraybuffer"
            ? "blob"
            : TColumnType extends
                  | `${string}Real${string}`
                  | `${string}Double${string}`
                  | `${string}Float${string}`
              ? "real"
              : TColumnType extends
                    | `${string}Int${string}`
                    | `${string}Serial${string}`
                    | `${string}Numeric${string}`
                    | `${string}Decimal${string}`
                ? "integer"
                : TDataType extends "number"
                  ? "integer"
                  : TDataType extends "date"
                    ? "timestamp"
                    : TDataType extends "string"
                      ? "text"
                      : never;

export type InferDrizzleColumnTuplType<TColumn> = TColumn extends {
  _: {
    columnType: infer TColumnType extends string;
    dataType: infer TDataType extends string;
  };
}
  ? InferDrizzleScalarTypeFromColumnMetadata<TColumnType, TDataType>
  : never;

export type InferDrizzleEntityColumnMetadataFromColumns<TColumns extends Record<string, unknown>> =
  {
    [K in Extract<keyof TColumns, string>]: DataEntityColumnMetadata<
      InferDrizzleColumnRead<TColumns[K]>
    > & {
      source: K;
    } & ([InferDrizzleColumnTuplType<TColumns[K]>] extends [never]
        ? {}
        : {
            type: InferDrizzleColumnTuplType<TColumns[K]>;
          });
  };

export type InferDrizzleEntityColumnMetadata<TConfig> = TConfig extends { shape: infer TShape }
  ? InferDataEntityShapeMetadata<
      InferDrizzleTableColumns<TConfig>,
      Extract<TShape, DataEntityShape<InferDrizzleTableColumns<TConfig>>>
    >
  : TConfig extends DrizzleProviderTableConfig<any, infer TTable, any>
    ? TTable extends Table
      ? InferDrizzleEntityColumnMetadataFromColumns<TTable["_"]["columns"]>
      : DataEntityReadMetadataMap<InferDrizzleTableColumns<TConfig>, InferDrizzleEntityRow<TConfig>>
    : DataEntityReadMetadataMap<InferDrizzleTableColumns<TConfig>, InferDrizzleEntityRow<TConfig>>;

export type DrizzleProviderEntities<
  TTables extends Record<string, DrizzleProviderTableConfig<any>>,
> = {
  [K in keyof TTables]: DataEntityHandle<
    InferDrizzleTableColumns<TTables[K]>,
    InferDrizzleEntityRow<TTables[K]>,
    InferDrizzleEntityColumnMetadata<TTables[K]>
  >;
};

export interface RunDrizzleScanOptions<TTable extends string, TColumn extends string> {
  db: DrizzleQueryExecutor;
  tableName: TTable;
  table: object;
  columns: DrizzleColumnMap<TColumn>;
  request: import("@tupl/provider-kit").TableScanRequest<TTable, TColumn>;
  scope?: SQL | SQL[];
}

export interface DrizzleExecutableBuilder {
  execute: () => Promise<import("@tupl/provider-kit").QueryRow[]>;
  orderBy?: (...clauses: SQL[]) => unknown;
  limit?: (value: number) => unknown;
  offset?: (value: number) => unknown;
  where?: (condition: SQL) => unknown;
}

export type InferredDrizzleSqlScalarType = SqlScalarType;
