import {
  TuplExecutionError,
  TuplProviderBindingError,
  stringifyUnknownValue,
} from "@tupl/foundation";
import {
  AdapterResult,
  bindProviderEntities,
  collectCapabilityAtomsForRel,
  createDataEntityHandle,
  extractSimpleRelScanRequest,
  inferRouteFamilyForRel,
  normalizeDataEntityShape,
  type DataEntityHandle,
  type DataEntityShape,
  type InferDataEntityShapeMetadata,
  type ProviderCapabilityAtom,
  type ProviderCapabilityReport,
  type ProviderCompiledPlan,
  type ProviderAdapter,
  type ProviderRuntimeBinding,
  type QueryRow,
  type ScanFilterClause,
  type TableScanRequest,
} from "@tupl/provider-kit";
import type { RelNode } from "@tupl/foundation";
import {
  filterLookupRows,
  projectLookupRow,
  type ProviderLookupManyRequest,
  type LookupManyCapableProviderAdapter,
  validateLookupRequest,
  validateSimpleRelScanRequest,
} from "@tupl/provider-kit/shapes";

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

interface IoredisCompiledRelPayload {
  fetchColumns: string[];
  key: string;
  keys: unknown[];
  request: TableScanRequest;
  strategy: "key_lookup_scan";
}

const REDIS_CAPABILITY_ATOMS: readonly ProviderCapabilityAtom[] = [
  "lookup.bulk",
  "scan.project",
  "scan.filter.basic",
  "scan.filter.set_membership",
  "scan.limit_offset",
  "scan.sort",
] as const;

function isRuntimeBindingResolver<TContext, TValue>(
  binding: ProviderRuntimeBinding<TContext, TValue>,
): binding is (context: TContext) => TValue | PromiseLike<TValue> {
  return typeof binding === "function";
}

function isValidRedis(redis: RedisLike | null | undefined): redis is RedisLike {
  return Boolean(redis && typeof redis.pipeline === "function");
}

function buildUnsupportedCapabilityReport(rel: RelNode, reason: string): ProviderCapabilityReport {
  const requiredAtoms = collectCapabilityAtomsForRel(rel);
  return {
    supported: false,
    reason,
    routeFamily: inferRouteFamilyForRel(rel),
    requiredAtoms,
    missingAtoms: requiredAtoms.filter((atom) => !REDIS_CAPABILITY_ATOMS.includes(atom)),
  };
}

function resolveRedisResult<TContext>(
  binding: ProviderRuntimeBinding<TContext, RedisLike>,
  context: TContext,
  provider: string,
) {
  return AdapterResult.tryPromise({
    try: async () => {
      const redis = isRuntimeBindingResolver(binding) ? await binding(context) : binding;
      if (!isValidRedis(redis)) {
        throw new TuplProviderBindingError({
          provider,
          message:
            "Ioredis provider runtime binding did not resolve to a valid Redis client. Check your context and redis callback.",
        });
      }
      return redis;
    },
    catch: (cause) =>
      cause instanceof TuplProviderBindingError
        ? cause
        : new TuplProviderBindingError({
            provider,
            cause,
            message:
              "Ioredis provider runtime binding did not resolve to a valid Redis client. Check your context and redis callback.",
          }),
  });
}

function getEntityConfigResult<TContext>(
  entitiesByName: Map<string, IoredisEntityConfig<TContext, string>>,
  entity: string,
  provider: string,
) {
  const mapping = entitiesByName.get(entity);
  if (!mapping) {
    return AdapterResult.err(
      new TuplProviderBindingError({
        provider,
        table: entity,
        message: `Unknown Redis entity ${entity}.`,
      }),
    );
  }

  return AdapterResult.ok(mapping);
}

function inferExactLookupKeys(
  where: readonly ScanFilterClause[] | undefined,
  lookupKey: string,
): unknown[] | null {
  let candidateKeys: Set<unknown> | null = null;

  for (const clause of where ?? []) {
    if (clause.column !== lookupKey) {
      continue;
    }

    let clauseKeys: Set<unknown> | null = null;
    switch (clause.op) {
      case "eq":
        clauseKeys = clause.value == null ? new Set() : new Set([clause.value]);
        break;
      case "in":
        clauseKeys = new Set(clause.values.filter((value) => value != null));
        break;
      default:
        return null;
    }
    if (!clauseKeys) {
      return null;
    }

    if (candidateKeys === null) {
      candidateKeys = clauseKeys;
      continue;
    }

    const intersected = new Set<unknown>();
    for (const value of candidateKeys) {
      if (clauseKeys.has(value)) {
        intersected.add(value);
      }
    }
    candidateKeys = intersected;
  }

  return candidateKeys ? [...candidateKeys] : null;
}

function collectFetchColumns(request: TableScanRequest, lookupKey: string): string[] {
  const columns = new Set<string>(request.select);
  columns.add(lookupKey);

  for (const clause of request.where ?? []) {
    columns.add(clause.column);
  }
  for (const term of request.orderBy ?? []) {
    columns.add(term.column);
  }

  return [...columns];
}

function buildRelExecutionPayload<TContext>(
  rel: RelNode,
  entitiesByName: Map<string, IoredisEntityConfig<TContext, string>>,
  provider: string,
) {
  const request = extractSimpleRelScanRequest(rel);
  if (!request) {
    return AdapterResult.err(
      new TuplExecutionError({
        operation: "compile redis fragment",
        message: "Ioredis provider only supports simple single-entity scan pipelines.",
      }),
    );
  }

  return AdapterResult.gen(function* () {
    const entity = yield* getEntityConfigResult(entitiesByName, request.table, provider);
    const supportedColumns = new Set(entity.columns);
    const requestValidation = validateSimpleRelScanRequest(request, {
      supportsSelectColumn(column) {
        return supportedColumns.has(column);
      },
      supportsFilterClause(clause) {
        return supportedColumns.has(clause.column);
      },
      supportsSortTerm(term) {
        return supportedColumns.has(term.column);
      },
    });
    if (AdapterResult.isError(requestValidation)) {
      return yield* AdapterResult.err(
        new TuplExecutionError({
          operation: "compile redis fragment",
          message: requestValidation.error.message,
        }),
      );
    }

    const keys = inferExactLookupKeys(request.where, entity.lookupKey);
    if (keys === null) {
      return yield* AdapterResult.err(
        new TuplExecutionError({
          operation: "compile redis fragment",
          message:
            "Ioredis provider requires an equality or IN predicate on the entity lookup key.",
        }),
      );
    }

    const fetchColumns = collectFetchColumns(request, entity.lookupKey);
    yield* validateLookupRequest(
      {
        table: request.table,
        key: entity.lookupKey,
        keys,
        select: fetchColumns,
      },
      entity,
    );

    return AdapterResult.ok({
      strategy: "key_lookup_scan",
      request,
      key: entity.lookupKey,
      keys,
      fetchColumns,
    } satisfies IoredisCompiledRelPayload);
  });
}

function compareNullableValues(left: unknown, right: unknown): number {
  if (left === right) {
    return 0;
  }
  if (left == null) {
    return -1;
  }
  if (right == null) {
    return 1;
  }
  return stringifyUnknownValue(left).localeCompare(stringifyUnknownValue(right));
}

function applyScanRequest(rows: QueryRow[], request: TableScanRequest): QueryRow[] {
  let filtered = filterLookupRows(rows, request.where);

  if ((request.orderBy?.length ?? 0) > 0) {
    filtered = [...filtered].sort((left, right) => {
      for (const term of request.orderBy ?? []) {
        const comparison = compareNullableValues(left[term.column], right[term.column]);
        if (comparison !== 0) {
          return term.direction === "asc" ? comparison : -comparison;
        }
      }
      return 0;
    });
  }

  if (request.offset != null) {
    filtered = filtered.slice(request.offset);
  }
  if (request.limit != null) {
    filtered = filtered.slice(0, request.limit);
  }

  return filtered.map((row) => projectLookupRow(row, request.select));
}

async function fetchLookupRowsResult<TContext>(
  input: {
    context: TContext;
    provider: string;
    recordOperation?: (operation: IoredisProviderOperation) => void;
    redis: ProviderRuntimeBinding<TContext, RedisLike>;
    request: {
      key: string;
      keys: unknown[];
      select: string[];
      table: string;
      where?: ScanFilterClause[];
    };
  },
  entitiesByName: Map<string, IoredisEntityConfig<TContext, string>>,
) {
  return AdapterResult.gen(async function* () {
    const entity = yield* getEntityConfigResult(
      entitiesByName,
      input.request.table,
      input.provider,
    );
    yield* validateLookupRequest(input.request, entity);
    const redis = yield* AdapterResult.await(
      resolveRedisResult(input.redis, input.context, input.provider),
    );

    const redisKeys = input.request.keys.map((key) =>
      entity.buildRedisKey({ key, context: input.context }),
    );
    const pipeline = redis.pipeline();
    for (const redisKey of redisKeys) {
      pipeline.hgetall(redisKey);
    }

    const responses = yield* AdapterResult.await(
      AdapterResult.tryPromise({
        try: () => pipeline.exec(),
        catch: (cause) =>
          new TuplExecutionError({
            operation: "execute redis lookup",
            cause,
            message: "Redis pipeline execution failed.",
          }),
      }),
    );

    const rows: QueryRow[] = [];
    for (const [index, response] of responses.entries()) {
      if (!response) {
        continue;
      }

      const [error, hash] = response;
      if (error) {
        return yield* AdapterResult.err(error);
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
        context: input.context,
      });
      if (!decoded) {
        continue;
      }

      rows.push(decoded as QueryRow);
    }

    input.recordOperation?.({
      kind: "redis_lookup",
      provider: input.provider,
      lookup: {
        entity: input.request.table,
        key: input.request.key,
        keys: input.request.keys,
        redisKeys,
      },
      variables: {
        request: input.request,
      },
    });

    return AdapterResult.ok(rows);
  });
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
    providerInstance: adapter,
    ...(config.shape
      ? {
          columns: normalizeDataEntityShape(
            config.shape as DataEntityShape<InferEntityColumns<TConfig>>,
          ),
        }
      : {}),
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
  lookupMany: LookupManyCapableProviderAdapter<TContext>["lookupMany"];
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
  lookupMany: LookupManyCapableProviderAdapter<TContext>["lookupMany"];
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
    capabilityAtoms: [...REDIS_CAPABILITY_ATOMS],
    entities: handles,
    canExecute(rel) {
      const payload = buildRelExecutionPayload(rel, entitiesByName, providerName);
      return AdapterResult.isError(payload)
        ? buildUnsupportedCapabilityReport(rel, payload.error.message)
        : true;
    },
    async compile(rel) {
      const payload = buildRelExecutionPayload(rel, entitiesByName, providerName);
      if (AdapterResult.isError(payload)) {
        return payload;
      }

      return AdapterResult.ok({
        provider: providerName,
        kind: "rel",
        payload: payload.value,
      } satisfies ProviderCompiledPlan);
    },
    async describeCompiledPlan(plan) {
      const payload = plan.payload as IoredisCompiledRelPayload;
      const summary = `${providerName} keyed hash lookup`;
      return {
        kind: "redis_lookup_scan",
        summary,
        operations: [
          {
            kind: "redis_lookup",
            target: payload.request.table,
            summary: `${payload.keys.length} keyed hash fetch${payload.keys.length === 1 ? "" : "es"}`,
            raw: {
              strategy: payload.strategy,
              key: payload.key,
              keys: payload.keys,
              request: payload.request,
            },
          },
        ],
        raw: {
          strategy: payload.strategy,
          table: payload.request.table,
          key: payload.key,
          keys: payload.keys,
          fetchColumns: payload.fetchColumns,
          request: payload.request,
        },
      };
    },
    async execute(plan, context) {
      const payload = plan.payload as IoredisCompiledRelPayload;
      const fetchedRows = await fetchLookupRowsResult(
        {
          context,
          provider: providerName,
          redis: options.redis,
          ...(options.recordOperation ? { recordOperation: options.recordOperation } : {}),
          request: {
            table: payload.request.table,
            key: payload.key,
            keys: payload.keys,
            select: payload.fetchColumns,
          },
        },
        entitiesByName,
      );
      if (AdapterResult.isError(fetchedRows)) {
        return fetchedRows;
      }

      return AdapterResult.ok(applyScanRequest(fetchedRows.value, payload.request));
    },
    async lookupMany(request: ProviderLookupManyRequest, context: TContext) {
      const fetchedRows = await fetchLookupRowsResult(
        {
          context,
          provider: providerName,
          redis: options.redis,
          ...(options.recordOperation ? { recordOperation: options.recordOperation } : {}),
          request,
        },
        entitiesByName,
      );
      if (AdapterResult.isError(fetchedRows)) {
        return fetchedRows;
      }

      return AdapterResult.ok(
        filterLookupRows(fetchedRows.value, request.where).map((row) =>
          projectLookupRow(row, request.select),
        ),
      );
    },
  } satisfies ProviderAdapter<TContext> &
    LookupManyCapableProviderAdapter<TContext> & {
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

  return bindProviderEntities(adapter);
}
