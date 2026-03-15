import type {
  DataEntityColumnMap,
  DataEntityColumnMetadata,
  DataEntityColumnMetadataRecord,
  DataEntityHandle,
  DataEntityReadMetadataMap,
  DataEntityShape,
  InferDataEntityShapeMetadata,
} from "@tupl/foundation";
import { DATA_ENTITY_PROVIDER_BRAND } from "@tupl/foundation";

import type { ProviderAdapter } from "./contracts";

export type {
  DataEntityColumnMap,
  DataEntityColumnMetadata,
  DataEntityColumnMetadataRecord,
  DataEntityHandle,
  DataEntityReadMetadataMap,
  DataEntityShape,
  InferDataEntityShapeMetadata,
} from "@tupl/foundation";

/**
 * Entity handles own the provider-bound description of a physical data source.
 * Callers should use these helpers instead of depending on the provider brand details.
 */
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
  providerInstance?: ProviderAdapter<any>;
}): DataEntityHandle<TColumns, TRow, TColumnMetadata> {
  const handle = {
    kind: "data_entity",
    entity: input.entity,
    provider: input.provider,
    ...(input.columns ? { columns: input.columns } : {}),
  } as DataEntityHandle<TColumns, TRow, TColumnMetadata>;

  if (input.providerInstance) {
    bindDataEntityHandleToProvider(handle, input.providerInstance);
  }

  return handle;
}

export function bindDataEntityHandleToProvider(
  handle: DataEntityHandle<string>,
  provider: ProviderAdapter<any>,
): DataEntityHandle<string> {
  Object.defineProperty(handle, DATA_ENTITY_PROVIDER_BRAND, {
    value: provider,
    enumerable: false,
    configurable: true,
    writable: false,
  });
  return handle;
}

export function getDataEntityProvider(
  handle: DataEntityHandle<string>,
): ProviderAdapter<any> | undefined {
  return handle[DATA_ENTITY_PROVIDER_BRAND] as ProviderAdapter<any> | undefined;
}

export function bindProviderEntities<TContext, TAdapter extends ProviderAdapter<TContext>>(
  provider: TAdapter,
): TAdapter {
  const entities = provider.entities;
  if (!entities) {
    return provider;
  }

  for (const [entityName, value] of Object.entries(entities)) {
    const handle = value as DataEntityHandle<string>;
    if (!handle.provider || handle.provider.length === 0) {
      handle.provider = provider.name;
    }
    if (!handle.entity || handle.entity.length === 0) {
      handle.entity = entityName;
    }
    bindDataEntityHandleToProvider(handle, provider);
  }

  return provider;
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
): DataEntityColumnMap<
  TColumns,
  Record<TColumns, unknown>,
  InferDataEntityShapeMetadata<TColumns, TShape>
> {
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
