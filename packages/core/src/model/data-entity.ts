import type { PhysicalDialect, SqlScalarType } from "../schema/definition";

declare const DATA_ENTITY_COLUMNS_BRAND: unique symbol;
declare const DATA_ENTITY_ROW_BRAND: unique symbol;
export const DATA_ENTITY_ADAPTER_BRAND = Symbol("tupl.data_entity.adapter");

export interface DataEntityColumnMetadata<TRead = unknown> {
  source: string;
  type?: SqlScalarType;
  nullable?: boolean;
  primaryKey?: boolean;
  unique?: boolean;
  enum?: readonly string[];
  physicalType?: string;
  physicalDialect?: PhysicalDialect;
  readonly __read__?: TRead;
}

export type DataEntityShapeColumn = SqlScalarType | Omit<DataEntityColumnMetadata, "source">;
export type DataEntityShape<TColumns extends string = string> = Record<
  TColumns,
  DataEntityShapeColumn
>;

export type DataEntityColumnMetadataRecord<TColumns extends string = string> = Partial<
  Record<TColumns, DataEntityColumnMetadata<any>>
>;

export type DataEntityReadMetadataMap<
  TColumns extends string = string,
  TRow extends Partial<Record<TColumns, unknown>> = Record<TColumns, unknown>,
> = {
  [K in TColumns]: DataEntityColumnMetadata<K extends keyof TRow ? TRow[K] : unknown>;
};

export type InferDataEntityShapeMetadata<
  TColumns extends string,
  TShape extends DataEntityShape<TColumns>,
> = {
  [K in TColumns]: TShape[K] extends SqlScalarType
    ? DataEntityColumnMetadata<unknown> & {
        source: K;
        type: TShape[K];
      }
    : DataEntityColumnMetadata<unknown> & {
        source: K;
      } & Extract<TShape[K], Omit<DataEntityColumnMetadata, "source">>;
};

export type DataEntityColumnMap<
  TColumns extends string = string,
  TRow extends Partial<Record<TColumns, unknown>> = Record<TColumns, unknown>,
  TColumnMetadata extends DataEntityColumnMetadataRecord<TColumns> = DataEntityReadMetadataMap<
    TColumns,
    TRow
  >,
> = {
  [K in TColumns]: K extends keyof TColumnMetadata
    ? TColumnMetadata[K] & {
        readonly __read__?: K extends keyof TRow ? TRow[K] : unknown;
      }
    : DataEntityColumnMetadata<K extends keyof TRow ? TRow[K] : unknown>;
};

export interface DataEntityHandle<
  TColumns extends string = string,
  TRow extends Partial<Record<TColumns, unknown>> = Record<TColumns, unknown>,
  TColumnMetadata extends DataEntityColumnMetadataRecord<TColumns> = DataEntityReadMetadataMap<
    TColumns,
    TRow
  >,
> {
  kind: "data_entity";
  entity: string;
  provider: string;
  columns?: DataEntityColumnMap<TColumns, TRow, TColumnMetadata>;
  readonly __columns__?: TColumns;
  readonly [DATA_ENTITY_ROW_BRAND]?: TRow;
  readonly [DATA_ENTITY_COLUMNS_BRAND]?: TColumns;
  readonly [DATA_ENTITY_ADAPTER_BRAND]?: unknown;
}
