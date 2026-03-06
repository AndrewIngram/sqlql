import {
  bindAdapterEntities,
  createDataEntityHandle,
  type DataEntityHandle,
  type ProviderAdapter,
  type ProviderCapabilityReport,
  type ProviderCompiledPlan,
  type QueryRow,
  type ScanFilterClause,
  type ScanOrderBy,
  type TableDefinition,
  type TableScanRequest,
} from "sqlql";

export const KV_PROVIDER_NAME = "kvProvider";
export const KV_INPUT_TABLE_NAME = "kv_product_views";

export interface KvInputRow {
  key: string;
  value: unknown;
}

export interface KvProviderOperation {
  kind: "kv_lookup";
  provider: string;
  lookup: {
    entity: string;
    op: "scan" | "lookupMany";
    key?: string;
    keys?: unknown[];
  };
  variables: unknown;
}

export const KV_INPUT_TABLE_DEFINITION: TableDefinition = {
  provider: KV_PROVIDER_NAME,
  columns: {
    key: { type: "text", nullable: false },
    value: { type: "integer", nullable: false },
  },
};

export interface KvEntityMappingConfig<TContext, TColumns extends string = string> {
  entity: string;
  columns: readonly TColumns[];
  mapRow: (input: { key: string; value: unknown; context: TContext }) => QueryRow | null;
}

type KvEntityMappingMap<TContext> = Record<string, KvEntityMappingConfig<TContext, string>>;

export interface CreateKvProviderOptions<
  TContext,
  TEntities extends KvEntityMappingMap<TContext> = KvEntityMappingMap<TContext>,
> {
  name?: string;
  rows: KvInputRow[];
  entities: TEntities;
  recordOperation?: (operation: KvProviderOperation) => void;
}

export interface KvProviderFactoryRuntime {
  rows: KvInputRow[];
  recordOperation?: (operation: KvProviderOperation) => void;
}

interface KvCompiledScanPlan {
  table: string;
  request: TableScanRequest;
}

function matchesClause(row: QueryRow, clause: ScanFilterClause): boolean {
  const value = row[clause.column];

  switch (clause.op) {
    case "eq":
      return value === clause.value;
    case "neq":
      return value !== clause.value;
    case "gt":
      return typeof value === "number" && value > Number(clause.value);
    case "gte":
      return typeof value === "number" && value >= Number(clause.value);
    case "lt":
      return typeof value === "number" && value < Number(clause.value);
    case "lte":
      return typeof value === "number" && value <= Number(clause.value);
    case "in":
      return clause.values.includes(value);
    case "is_null":
      return value == null;
    case "is_not_null":
      return value != null;
  }
}

function compareNullableValues(left: unknown, right: unknown): number {
  if (left == null && right == null) {
    return 0;
  }
  if (left == null) {
    return 1;
  }
  if (right == null) {
    return -1;
  }
  if (typeof left === "number" && typeof right === "number") {
    return left - right;
  }
  return String(left).localeCompare(String(right));
}

function applyOrderBy(rows: QueryRow[], orderBy: ScanOrderBy[] | undefined): QueryRow[] {
  if (!orderBy || orderBy.length === 0) {
    return rows;
  }

  return [...rows].sort((left, right) => {
    for (const term of orderBy) {
      const comparison = compareNullableValues(left[term.column], right[term.column]);
      if (comparison !== 0) {
        return term.direction === "asc" ? comparison : -comparison;
      }
    }
    return 0;
  });
}

function applyScanRequest(rows: QueryRow[], request: TableScanRequest): QueryRow[] {
  let out = [...rows];

  for (const clause of request.where ?? []) {
    out = out.filter((row) => matchesClause(row, clause));
  }

  out = applyOrderBy(out, request.orderBy);

  if (request.offset != null && request.offset > 0) {
    out = out.slice(request.offset);
  }

  if (request.limit != null) {
    out = out.slice(0, request.limit);
  }

  return out.map((row) => {
    const selected: QueryRow = {};
    for (const column of request.select) {
      selected[column] = row[column] ?? null;
    }
    return selected;
  });
}

function filterToKnownColumns(row: QueryRow, columns: readonly string[]): QueryRow {
  const out: QueryRow = {};
  for (const column of columns) {
    out[column] = row[column] ?? null;
  }
  return out;
}

function materializeEntityRows<TContext>(
  rows: KvInputRow[],
  mapping: KvEntityMappingConfig<TContext, string>,
  context: TContext,
): QueryRow[] {
  const out: QueryRow[] = [];
  for (const row of rows) {
    const mapped = mapping.mapRow({ key: row.key, value: row.value, context });
    if (!mapped) {
      continue;
    }
    out.push(filterToKnownColumns(mapped, mapping.columns));
  }
  return out;
}

function inferEntityHandleColumns<TConfig extends KvEntityMappingConfig<any, string>>(
  mapping: TConfig,
  provider: string,
  adapter: ProviderAdapter<any>,
): DataEntityHandle<TConfig["columns"][number]> {
  return createDataEntityHandle<TConfig["columns"][number]>({
    provider,
    entity: mapping.entity,
    adapter,
  });
}

export function createKvProvider<
  TContext,
  TEntities extends KvEntityMappingMap<TContext> = KvEntityMappingMap<TContext>,
>(
  options: CreateKvProviderOptions<TContext, TEntities>,
): ProviderAdapter<TContext> & {
  entities: { [K in keyof TEntities]: DataEntityHandle<TEntities[K]["columns"][number]> };
} {
  const providerName = options.name ?? KV_PROVIDER_NAME;
  const handles = {} as { [K in keyof TEntities]: DataEntityHandle<TEntities[K]["columns"][number]> };
  const adapter = {
    name: providerName,
    entities: handles,
    canExecute(fragment): boolean | ProviderCapabilityReport {
      switch (fragment.kind) {
        case "scan":
          return entityByName.has(fragment.table);
        case "rel":
          return {
            supported: false,
            reason: "KV provider only supports scan fragments in this playground demo.",
          };
        default:
          return false;
      }
    },
    async compile(fragment): Promise<ProviderCompiledPlan> {
      if (fragment.kind !== "scan") {
        throw new Error(`Unsupported fragment kind for KV provider: ${fragment.kind}`);
      }

      const mapping = getEntityMappingOrThrow(fragment.table);
      const allowedColumns = new Set(mapping.columns);
      for (const column of fragment.request.select) {
        if (!allowedColumns.has(column)) {
          throw new Error(`Unsupported KV column in select for ${fragment.table}: ${column}`);
        }
      }

      return {
        provider: providerName,
        kind: fragment.kind,
        payload: {
          table: fragment.table,
          request: fragment.request,
        } satisfies KvCompiledScanPlan,
      };
    },
    async execute(plan, context): Promise<QueryRow[]> {
      if (plan.kind !== "scan") {
        throw new Error(`Unsupported KV compiled plan kind: ${plan.kind}`);
      }

      const compiled = plan.payload as KvCompiledScanPlan;
      const mapping = getEntityMappingOrThrow(compiled.table);
      const rows = materializeEntityRows(options.rows, mapping, context);

      options.recordOperation?.({
        kind: "kv_lookup",
        provider: providerName,
        lookup: {
          entity: compiled.table,
          op: "scan",
        },
        variables: {
          request: compiled.request,
        },
      });

      return applyScanRequest(rows, compiled.request);
    },
    async lookupMany(request, context): Promise<QueryRow[]> {
      const mapping = getEntityMappingOrThrow(request.table);
      const allowedColumns = new Set(mapping.columns);

      if (!allowedColumns.has(request.key)) {
        throw new Error(`Unsupported KV lookup key column for ${request.table}: ${request.key}`);
      }
      for (const column of request.select) {
        if (!allowedColumns.has(column)) {
          throw new Error(`Unsupported KV lookup select column for ${request.table}: ${column}`);
        }
      }

      const rows = materializeEntityRows(options.rows, mapping, context);
      const inFiltered = rows.filter((row) => request.keys.includes(row[request.key]));
      const withResidual = (request.where ?? []).reduce(
        (current, clause) => current.filter((row) => matchesClause(row, clause)),
        inFiltered,
      );

      const selected = withResidual.map((row) => {
        const out: QueryRow = {};
        for (const column of request.select) {
          out[column] = row[column] ?? null;
        }
        return out;
      });

      options.recordOperation?.({
        kind: "kv_lookup",
        provider: providerName,
        lookup: {
          entity: request.table,
          op: "lookupMany",
          key: request.key,
          keys: request.keys,
        },
        variables: {
          request,
        },
      });

      return selected;
    },
  } satisfies ProviderAdapter<TContext> & {
    entities: { [K in keyof TEntities]: DataEntityHandle<TEntities[K]["columns"][number]> };
  };

  const entityByName = new Map<string, KvEntityMappingConfig<TContext, string>>();
  const entityByKey = options.entities;
  for (const [entityKey, mapping] of Object.entries(entityByKey) as Array<
    [keyof TEntities, TEntities[keyof TEntities]]
  >) {
    handles[entityKey] = inferEntityHandleColumns(mapping, providerName, adapter);
    entityByName.set(mapping.entity, mapping);
  }

  function getEntityMappingOrThrow(entity: string): KvEntityMappingConfig<TContext, string> {
    const mapping = entityByName.get(entity);
    if (!mapping) {
      throw new Error(`Unknown KV entity ${entity}.`);
    }
    return mapping;
  }

  return bindAdapterEntities(adapter);
}
