import {
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

import type { PlaygroundContext } from "./types";

export const KV_PROVIDER_NAME = "kvProvider";
export const KV_DATA_TABLE_NAME = "kv_product_views";
export const KV_ENTITY_NAME = "product_view_counts";

export interface KvDataRow {
  key: string;
  value: number;
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

export const KV_DATA_TABLE_DEFINITION: TableDefinition = {
  provider: KV_PROVIDER_NAME,
  columns: {
    key: { type: "text", nullable: false },
    value: { type: "integer", nullable: false },
  },
};

interface CreateInMemoryKvProviderOptions<TContext extends PlaygroundContext> {
  name?: string;
  rows: KvDataRow[];
  recordOperation?: (operation: KvProviderOperation) => void;
  resolveUserId?: (context: TContext) => string;
}

type KvEntityColumn = "product_id" | "view_count";

type MaterializedKvRow = {
  product_id: string;
  view_count: number;
};

function parseKvKey(raw: string): { userId: string; productId: string } | null {
  const separator = raw.indexOf(":");
  if (separator <= 0 || separator >= raw.length - 1) {
    return null;
  }

  const userId = raw.slice(0, separator).trim();
  const productId = raw.slice(separator + 1).trim();
  if (userId.length === 0 || productId.length === 0) {
    return null;
  }

  return {
    userId,
    productId,
  };
}

function materializeRows<TContext extends PlaygroundContext>(
  rows: KvDataRow[],
  context: TContext,
  resolveUserId: (context: TContext) => string,
): MaterializedKvRow[] {
  const userId = resolveUserId(context);
  const out: MaterializedKvRow[] = [];

  for (const row of rows) {
    const parsed = parseKvKey(row.key);
    if (!parsed || parsed.userId !== userId) {
      continue;
    }

    out.push({
      product_id: parsed.productId,
      view_count: row.value,
    });
  }

  return out;
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

interface KvCompiledScanPlan {
  table: string;
  request: TableScanRequest;
}

export function createInMemoryKvProvider<TContext extends PlaygroundContext>(
  options: CreateInMemoryKvProviderOptions<TContext>,
): ProviderAdapter<TContext> & {
  entities: {
    product_view_counts: DataEntityHandle<KvEntityColumn>;
  };
} {
  const providerName = options.name ?? KV_PROVIDER_NAME;
  const entityHandle = createDataEntityHandle<KvEntityColumn>({
    provider: providerName,
    entity: KV_ENTITY_NAME,
  });
  const resolveUserId = options.resolveUserId ?? ((context: TContext) => context.userId);
  const knownColumns: readonly KvEntityColumn[] = ["product_id", "view_count"];

  return {
    entities: {
      product_view_counts: entityHandle,
    },
    canExecute(fragment): boolean | ProviderCapabilityReport {
      switch (fragment.kind) {
        case "scan":
          return fragment.table === KV_ENTITY_NAME;
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
      if (fragment.table !== KV_ENTITY_NAME) {
        throw new Error(`Unknown KV entity ${fragment.table}.`);
      }

      for (const column of fragment.request.select) {
        if (!knownColumns.includes(column as KvEntityColumn)) {
          throw new Error(`Unsupported KV column in select: ${column}`);
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
      const materialized = materializeRows(options.rows, context, resolveUserId);
      const rows = materialized.map((row) => ({
        product_id: row.product_id,
        view_count: row.view_count,
      }));

      options.recordOperation?.({
        kind: "kv_lookup",
        provider: providerName,
        lookup: {
          entity: compiled.table,
          op: "scan",
        },
        variables: {
          request: compiled.request,
          userId: resolveUserId(context),
        },
      });

      return applyScanRequest(rows, compiled.request);
    },
    async lookupMany(request, context): Promise<QueryRow[]> {
      if (request.table !== KV_ENTITY_NAME) {
        throw new Error(`Unknown KV entity ${request.table}.`);
      }

      if (!knownColumns.includes(request.key as KvEntityColumn)) {
        throw new Error(`Unsupported KV lookup key column: ${request.key}`);
      }

      for (const column of request.select) {
        if (!knownColumns.includes(column as KvEntityColumn)) {
          throw new Error(`Unsupported KV lookup select column: ${column}`);
        }
      }
      const keyColumn = request.key as KvEntityColumn;
      const selectColumns = request.select as KvEntityColumn[];

      const materialized = materializeRows(options.rows, context, resolveUserId).map((row) => ({
        product_id: row.product_id,
        view_count: row.view_count,
      }));

      const inFiltered = materialized.filter((row) => request.keys.includes(row[keyColumn]));
      const withResidual = (request.where ?? []).reduce(
        (rows, clause) => rows.filter((row) => matchesClause(row, clause)),
        inFiltered,
      );

      const selected = withResidual.map((row) => {
        const out: QueryRow = {};
        for (const column of selectColumns) {
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
          userId: resolveUserId(context),
        },
      });

      return selected;
    },
  };
}
