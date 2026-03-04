import {
  getNormalizedTableBinding,
} from "./schema";
import type {
  QueryRow,
  ScanFilterClause,
  SchemaDefinition,
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

export interface DataEntityHandle<TColumns extends string = string> {
  kind: "data_entity";
  /**
   * Source-neutral entity identifier. This can represent a SQL table, an ES index,
   * a Redis keyspace abstraction, a Mongo collection, etc.
   */
  entity: string;
  /**
   * Logical provider registration key expected in defineProviders(...).
   */
  provider: string;
  readonly __columns__?: TColumns;
  readonly [DATA_ENTITY_COLUMNS_BRAND]?: TColumns;
}

export function createDataEntityHandle<TColumns extends string = string>(input: {
  entity: string;
  provider: string;
}): DataEntityHandle<TColumns> {
  return {
    kind: "data_entity",
    entity: input.entity,
    provider: input.provider,
  } as DataEntityHandle<TColumns>;
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

export function defineProviders<TContext, TProviders extends ProvidersMap<TContext>>(
  providers: TProviders,
): TProviders {
  for (const [providerName, adapter] of Object.entries(providers)) {
    const entities = adapter.entities;
    if (!entities) {
      continue;
    }

    for (const [entityName, handle] of Object.entries(entities)) {
      if (!handle.provider || handle.provider.length === 0) {
        handle.provider = providerName;
      }
      if (!handle.entity || handle.entity.length === 0) {
        handle.entity = entityName;
      }
    }

    adapter.entities = entities;
  }

  return providers;
}

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
