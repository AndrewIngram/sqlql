import {
  AdapterResult,
  bindAdapterEntities,
  createDataEntityHandle,
  normalizeDataEntityShape,
  type DataEntityHandle,
  type DataEntityShape,
  type InferDataEntityShapeMetadata,
  type MaybePromise,
  type ProviderAdapter,
  type ProviderCapabilityAtom,
  type ProviderCapabilityReport,
  type ProviderCompiledPlan,
  type ProviderRuntimeBinding,
} from "@tupl/core";
import type { QueryRow } from "@tupl/core/schema";
import {
  buildLookupOnlyUnsupportedReport,
  filterLookupRows,
  projectLookupRow,
  validateLookupRequest,
} from "@tupl/core/provider-shapes";

export interface RedisPipelineResult {
  hgetall: [Error | null, Record<string, string>];
}

export interface RedisPipelineLike {
  hgetall(key: string): RedisPipelineLike;
  exec(): Promise<Array<RedisPipelineResult["hgetall"] | null>>;
}

export interface RedisLike {
  pipeline(): RedisPipelineLike;
}

export interface IoredisLookupRecord {
  entity: string;
  key: string;
  keys: unknown[];
  redisKeys: string[];
}

export interface IoredisProviderOperation {
  kind: "redis_lookup";
  provider: string;
  lookup: IoredisLookupRecord;
  variables: unknown;
}

export interface IoredisEntityConfig<
  TContext,
  TColumns extends string = string,
  TRow extends Partial<Record<TColumns, unknown>> = Record<TColumns, unknown>,
  TShape extends DataEntityShape<TColumns> | undefined = undefined,
> {
  entity: string;
  lookupKey: TColumns;
  columns: readonly TColumns[];
  shape?: TShape;
  buildRedisKey(input: { key: unknown; context: TContext }): string;
  decodeRow(input: {
    redisKey: string;
    hash: Record<string, string>;
    context: TContext;
  }): TRow | null;
}

type IoredisEntityMap<TContext> = Record<
  string,
  IoredisEntityConfig<
    TContext,
    string,
    Record<string, unknown>,
    DataEntityShape<string> | undefined
  >
>;

type InferIoredisProviderContext<TEntities extends IoredisEntityMap<any>> = {
  [K in keyof TEntities]: TEntities[K] extends IoredisEntityConfig<infer TContext, string>
    ? TContext
    : never;
}[keyof TEntities];

type InferEntityColumns<TConfig> =
  TConfig extends IoredisEntityConfig<any, infer TColumns, any, any> ? TColumns : string;

type InferEntityRow<TConfig> =
  TConfig extends IoredisEntityConfig<any, any, infer TRow, any> ? TRow : Record<string, unknown>;

type InferEntityShape<TConfig> =
  TConfig extends IoredisEntityConfig<any, infer TColumns, any, infer TShape>
    ? TShape extends DataEntityShape<TColumns>
      ? InferDataEntityShapeMetadata<TColumns, TShape>
      : Record<string, never>
    : Record<string, never>;

export interface CreateIoredisProviderOptions<
  TContext,
  TEntities extends IoredisEntityMap<TContext> = IoredisEntityMap<TContext>,
> {
  name?: string;
  redis: ProviderRuntimeBinding<TContext, RedisLike>;
  entities: TEntities;
  recordOperation?: (operation: IoredisProviderOperation) => void;
}

const LOOKUP_ATOMS: readonly ProviderCapabilityAtom[] = ["lookup.bulk"];

function isRuntimeBindingResolver<TContext, TValue>(
  binding: ProviderRuntimeBinding<TContext, TValue>,
): binding is (context: TContext) => MaybePromise<TValue> {
  return typeof binding === "function";
}

function isPromiseLike<T>(value: unknown): value is PromiseLike<T> {
  return (
    (typeof value === "object" || typeof value === "function") &&
    value !== null &&
    typeof (value as { then?: unknown }).then === "function"
  );
}

function assertRedis(redis: RedisLike | null | undefined): RedisLike {
  if (!redis || typeof redis.pipeline !== "function") {
    throw new Error(
      "Ioredis provider runtime binding did not resolve to a valid Redis client. Check your context and redis callback.",
    );
  }
  return redis;
}

function resolveRedisMaybeSync<TContext>(
  binding: ProviderRuntimeBinding<TContext, RedisLike>,
  context: TContext,
): MaybePromise<RedisLike> {
  if (!isRuntimeBindingResolver(binding)) {
    return assertRedis(binding);
  }

  const redis = binding(context);
  return isPromiseLike(redis) ? redis.then(assertRedis) : assertRedis(redis);
}

async function resolveRedis<TContext>(
  binding: ProviderRuntimeBinding<TContext, RedisLike>,
  context: TContext,
): Promise<RedisLike> {
  return await Promise.resolve(resolveRedisMaybeSync(binding, context));
}

function getEntityConfigOrThrow<TContext>(
  entitiesByName: Map<string, IoredisEntityConfig<TContext, string>>,
  entity: string,
): IoredisEntityConfig<TContext, string> {
  const mapping = entitiesByName.get(entity);
  if (!mapping) {
    throw new Error(`Unknown Redis entity ${entity}.`);
  }
  return mapping;
}

function inferEntityHandle<
  TConfig extends IoredisEntityConfig<
    any,
    string,
    Record<string, unknown>,
    DataEntityShape<string> | undefined
  >,
>(
  config: TConfig,
  provider: string,
  adapter: ProviderAdapter<any>,
): DataEntityHandle<
  InferEntityColumns<TConfig>,
  InferEntityRow<TConfig>,
  InferEntityShape<TConfig>
> {
  return createDataEntityHandle({
    entity: config.entity,
    provider,
    ...(config.shape
      ? {
          columns: normalizeDataEntityShape(
            config.shape as DataEntityShape<InferEntityColumns<TConfig>>,
          ),
        }
      : {}),
    adapter,
  }) as unknown as DataEntityHandle<
    InferEntityColumns<TConfig>,
    InferEntityRow<TConfig>,
    InferEntityShape<TConfig>
  >;
}

export function createIoredisProvider<
  TContext,
  const TEntities extends IoredisEntityMap<TContext> = IoredisEntityMap<TContext>,
>(
  options: CreateIoredisProviderOptions<TContext, TEntities>,
): ProviderAdapter<TContext> & {
  entities: {
    [K in keyof TEntities]: DataEntityHandle<
      InferEntityColumns<TEntities[K]>,
      InferEntityRow<TEntities[K]>,
      InferEntityShape<TEntities[K]>
    >;
  };
};

export function createIoredisProvider<
  const TEntities extends IoredisEntityMap<any>,
  TContext = InferIoredisProviderContext<TEntities>,
>(
  options: CreateIoredisProviderOptions<TContext, TEntities>,
): ProviderAdapter<TContext> & {
  entities: {
    [K in keyof TEntities]: DataEntityHandle<
      InferEntityColumns<TEntities[K]>,
      InferEntityRow<TEntities[K]>,
      InferEntityShape<TEntities[K]>
    >;
  };
} {
  const providerName = options.name ?? "redis";
  const handles = {} as {
    [K in keyof TEntities]: DataEntityHandle<
      InferEntityColumns<TEntities[K]>,
      InferEntityRow<TEntities[K]>,
      InferEntityShape<TEntities[K]>
    >;
  };
  const entitiesByName = new Map<string, IoredisEntityConfig<TContext, string>>();

  const adapter = {
    name: providerName,
    routeFamilies: ["lookup"],
    capabilityAtoms: [...LOOKUP_ATOMS],
    entities: handles,
    canExecute(fragment): boolean | ProviderCapabilityReport {
      switch (fragment.kind) {
        case "scan":
          return buildLookupOnlyUnsupportedReport(
            fragment,
            "Ioredis provider is lookup-only in v1 and does not support scan pushdown.",
          );
        case "aggregate":
          return buildLookupOnlyUnsupportedReport(
            fragment,
            "Ioredis provider is lookup-only in v1 and does not support aggregate pushdown.",
          );
        case "rel":
          return buildLookupOnlyUnsupportedReport(
            fragment,
            "Ioredis provider is lookup-only in v1 and does not support relational pushdown.",
          );
      }
    },
    async compile(fragment) {
      return AdapterResult.err(
        new Error(
          `Ioredis provider does not compile ${fragment.kind} fragments in v1. Use lookupMany-backed plans instead.`,
        ),
      );
    },
    async execute(plan: ProviderCompiledPlan) {
      return AdapterResult.err(
        new Error(
          `Ioredis provider does not execute compiled ${plan.kind} plans in v1. Use lookupMany-backed plans instead.`,
        ),
      );
    },
    async lookupMany(request, context) {
      const entity = getEntityConfigOrThrow(entitiesByName, request.table);
      const validation = validateLookupRequest(request, entity);
      if (AdapterResult.isError(validation)) {
        return validation;
      }

      const redis = await resolveRedis(options.redis, context);
      const redisKeys = request.keys.map((key) => entity.buildRedisKey({ key, context }));
      const pipeline = redis.pipeline();
      for (const redisKey of redisKeys) {
        pipeline.hgetall(redisKey);
      }

      const responses = await pipeline.exec();
      const rows: QueryRow[] = [];

      for (const [index, response] of responses.entries()) {
        if (!response) {
          continue;
        }
        const [error, hash] = response;
        if (error) {
          return AdapterResult.err(error);
        }

        if (!hash || Object.keys(hash).length === 0) {
          continue;
        }

        const redisKey = redisKeys[index];
        if (!redisKey) {
          continue;
        }

        const decoded = entity.decodeRow({
          redisKey,
          hash,
          context,
        });
        if (!decoded) {
          continue;
        }
        rows.push(decoded as QueryRow);
      }

      const filtered = filterLookupRows(rows, request.where);

      options.recordOperation?.({
        kind: "redis_lookup",
        provider: providerName,
        lookup: {
          entity: request.table,
          key: request.key,
          keys: request.keys,
          redisKeys,
        },
        variables: {
          request,
        },
      });

      return AdapterResult.ok(filtered.map((row) => projectLookupRow(row, request.select)));
    },
  } satisfies ProviderAdapter<TContext> & {
    entities: {
      [K in keyof TEntities]: DataEntityHandle<
        InferEntityColumns<TEntities[K]>,
        InferEntityRow<TEntities[K]>,
        InferEntityShape<TEntities[K]>
      >;
    };
  };

  for (const [entityKey, config] of Object.entries(options.entities) as Array<
    [keyof TEntities, TEntities[keyof TEntities]]
  >) {
    handles[entityKey] = inferEntityHandle(config, providerName, adapter);
    entitiesByName.set(config.entity, config as IoredisEntityConfig<TContext, string>);
  }

  return bindAdapterEntities(adapter);
}
