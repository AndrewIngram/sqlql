import {
  createDataEntityHandle,
  normalizeDataEntityShape,
  type DataEntityColumnMap,
} from "../entity-handles";
import type { ProviderAdapter } from "../contracts";
import type {
  RelationalProviderAdapterOptions,
  RelationalProviderEntityConfig,
  RelationalProviderHandles,
} from "./relational-adapter-types";

export function buildRelationalEntityHandles<
  TContext,
  TEntities extends Record<string, RelationalProviderEntityConfig>,
  TStrategy extends string,
  THandles extends RelationalProviderHandles<TEntities> = RelationalProviderHandles<TEntities>,
>(
  adapter: ProviderAdapter<TContext>,
  options: RelationalProviderAdapterOptions<TContext, TEntities, TStrategy>,
): THandles {
  const handles = {} as RelationalProviderHandles<TEntities>;

  for (const entity of Object.keys(options.entities) as Array<Extract<keyof TEntities, string>>) {
    const config = options.entities[entity] as TEntities[typeof entity];
    const columns =
      config?.shape != null
        ? normalizeDataEntityShape(config.shape)
        : options.resolveEntityColumns?.({
            config,
            entity,
            name: options.name,
          });

    handles[entity] = hasColumns(columns)
      ? createDataEntityHandle({
          entity,
          provider: options.name,
          providerInstance: adapter,
          columns,
        })
      : createDataEntityHandle({
          entity,
          provider: options.name,
          providerInstance: adapter,
        });
  }

  return handles as THandles;
}

function hasColumns(
  columns: DataEntityColumnMap<string> | undefined,
): columns is DataEntityColumnMap<string> {
  return columns != null && Object.keys(columns).length > 0;
}
