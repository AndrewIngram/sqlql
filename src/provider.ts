import {
  getNormalizedTableBinding,
} from "./schema";
import type {
  PhysicalDialect,
  QueryRow,
  ScanFilterClause,
  SchemaDefinition,
  SqlScalarType,
  TableAggregateRequest,
  TableScanRequest,
} from "./schema";
import type { RelNode } from "./rel";

export type MaybePromise<T> = T | Promise<T>;

export interface ProviderCapabilityReport {
  supported: boolean;
  reason?: string;
  notes?: string[];
}

export interface ProviderEstimate {
  rows: number;
  cost: number;
}

export interface ProviderCompiledPlan {
  provider: string;
  kind: string;
  payload: unknown;
}

declare const DATA_ENTITY_COLUMNS_BRAND: unique symbol;
declare const DATA_ENTITY_ROW_BRAND: unique symbol;
const DATA_ENTITY_ADAPTER_BRAND = Symbol("sqlql.data_entity.adapter");

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
export type DataEntityShape<TColumns extends string = string> = Record<TColumns, DataEntityShapeColumn>;

type DataEntityColumnMetadataRecord<TColumns extends string = string> = Partial<
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
  /**
   * Source-neutral entity identifier. This can represent a SQL table, an ES index,
   * a Redis keyspace abstraction, a Mongo collection, etc.
   */
  entity: string;
  /**
   * Logical provider name used for runtime routing.
   */
  provider: string;
  columns?: DataEntityColumnMap<TColumns, TRow, TColumnMetadata>;
  readonly __columns__?: TColumns;
  readonly [DATA_ENTITY_ROW_BRAND]?: TRow;
  readonly [DATA_ENTITY_COLUMNS_BRAND]?: TColumns;
  readonly [DATA_ENTITY_ADAPTER_BRAND]?: ProviderAdapter<any>;
}

export function createDataEntityHandle<
  TColumns extends string = string,
  TRow extends Partial<Record<TColumns, unknown>> = Record<TColumns, unknown>,
  TColumnMetadata extends DataEntityColumnMetadataRecord<TColumns> = DataEntityReadMetadataMap<
    TColumns,
    TRow
  >,
>(input: {
  entity: string;
  provider: string;
  columns?: DataEntityColumnMap<TColumns, TRow, TColumnMetadata>;
  adapter?: ProviderAdapter<any>;
}): DataEntityHandle<TColumns, TRow, TColumnMetadata> {
  const handle = {
    kind: "data_entity",
    entity: input.entity,
    provider: input.provider,
    ...(input.columns ? { columns: input.columns } : {}),
  } as DataEntityHandle<TColumns, TRow, TColumnMetadata>;

  if (input.adapter) {
    bindDataEntityHandleToAdapter(handle, input.adapter);
  }

  return handle;
}

export function bindDataEntityHandleToAdapter(
  handle: DataEntityHandle<string>,
  adapter: ProviderAdapter<any>,
): DataEntityHandle<string> {
  Object.defineProperty(handle, DATA_ENTITY_ADAPTER_BRAND, {
    value: adapter,
    enumerable: false,
    configurable: true,
    writable: false,
  });
  return handle;
}

export function getDataEntityAdapter(
  handle: DataEntityHandle<string>,
): ProviderAdapter<any> | undefined {
  return handle[DATA_ENTITY_ADAPTER_BRAND];
}

export function bindAdapterEntities<TContext, TAdapter extends ProviderAdapter<TContext>>(
  adapter: TAdapter,
): TAdapter {
  const entities = adapter.entities;
  if (!entities) {
    return adapter;
  }

  for (const [entityName, handle] of Object.entries(entities)) {
    if (!handle.provider || handle.provider.length === 0) {
      handle.provider = adapter.name;
    }
    if (!handle.entity || handle.entity.length === 0) {
      handle.entity = entityName;
    }
    bindDataEntityHandleToAdapter(handle, adapter);
  }

  return adapter;
}

export function isDataEntityHandle(value: unknown): value is DataEntityHandle<string> {
  return (
    !!value &&
    typeof value === "object" &&
    (value as { kind?: unknown }).kind === "data_entity" &&
    typeof (value as { entity?: unknown }).entity === "string" &&
    typeof (value as { provider?: unknown }).provider === "string"
  );
}

export function getDataEntityColumnMetadata(
  entity: DataEntityHandle<string>,
  column: string,
): DataEntityColumnMetadata | undefined {
  return entity.columns?.[column];
}

export function normalizeDataEntityShape<
  TColumns extends string,
  TShape extends DataEntityShape<TColumns>,
>(
  shape: TShape,
): DataEntityColumnMap<TColumns, Record<TColumns, unknown>, InferDataEntityShapeMetadata<TColumns, TShape>> {
  return Object.fromEntries(
    Object.entries(shape).map(([column, definition]) => [
      column,
      typeof definition === "string"
        ? {
            source: column,
            type: definition,
          }
        : {
            source: column,
            ...(definition as Omit<DataEntityColumnMetadata, "source">),
          },
    ]),
  ) as DataEntityColumnMap<
    TColumns,
    Record<TColumns, unknown>,
    InferDataEntityShapeMetadata<TColumns, TShape>
  >;
}

export interface ProviderLookupManyRequest {
  table: string;
  key: string;
  keys: unknown[];
  select: string[];
  where?: ScanFilterClause[];
}

export interface RelProviderFragment {
  kind: "rel";
  provider: string;
  rel: RelNode;
}

export interface ScanProviderFragment {
  kind: "scan";
  provider: string;
  table: string;
  request: TableScanRequest;
}

export interface AggregateProviderFragment {
  kind: "aggregate";
  provider: string;
  table: string;
  request: TableAggregateRequest;
}

export type ProviderFragment =
  | RelProviderFragment
  | ScanProviderFragment
  | AggregateProviderFragment;

export interface ProviderAdapter<TContext = unknown> {
  name: string;
  canExecute(fragment: ProviderFragment, context: TContext): MaybePromise<boolean | ProviderCapabilityReport>;
  compile(fragment: ProviderFragment, context: TContext): MaybePromise<ProviderCompiledPlan>;
  execute(plan: ProviderCompiledPlan, context: TContext): Promise<QueryRow[]>;
  lookupMany?(request: ProviderLookupManyRequest, context: TContext): Promise<QueryRow[]>;
  estimate?(fragment: ProviderFragment, context: TContext): MaybePromise<ProviderEstimate>;
  /**
   * Optional source-neutral physical entity handles owned by this adapter.
   */
  entities?: Record<string, DataEntityHandle<string>>;
}

export type ProvidersMap<TContext = unknown> = Record<string, ProviderAdapter<TContext>>;
export type DataSourceAdapter<TContext = unknown> = ProviderAdapter<TContext>;

export function normalizeCapability(
  capability: boolean | ProviderCapabilityReport,
): ProviderCapabilityReport {
  if (typeof capability === "boolean") {
    return capability ? { supported: true } : { supported: false };
  }

  return capability;
}

export function resolveTableProvider(schema: SchemaDefinition, table: string): string {
  const normalized = getNormalizedTableBinding(schema, table);
  if (normalized?.kind === "physical" && normalized.provider) {
    return normalized.provider;
  }

  if (normalized?.kind === "view") {
    throw new Error(`View table ${table} does not have a direct provider binding.`);
  }

  const tableDefinition = schema.tables[table];
  if (!tableDefinition) {
    throw new Error(`Unknown table: ${table}`);
  }

  if (!tableDefinition.provider || tableDefinition.provider.length === 0) {
    throw new Error(`Table ${table} is missing required provider mapping.`);
  }

  return tableDefinition.provider;
}

export function validateProviderBindings<TContext>(
  schema: SchemaDefinition,
  providers: ProvidersMap<TContext>,
): void {
  for (const tableName of Object.keys(schema.tables)) {
    const normalized = getNormalizedTableBinding(schema, tableName);
    if (normalized?.kind === "view") {
      continue;
    }

    const providerName = normalized?.kind === "physical"
      ? normalized.provider ?? resolveTableProvider(schema, tableName)
      : resolveTableProvider(schema, tableName);
    if (!providers[providerName]) {
      throw new Error(
        `Table ${tableName} is bound to provider ${providerName}, but no such provider is registered.`,
      );
    }
  }
}
