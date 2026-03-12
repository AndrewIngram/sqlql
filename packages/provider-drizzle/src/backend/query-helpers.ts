import {
  and,
  asc,
  desc,
  eq,
  gt,
  gte,
  inArray,
  isNotNull,
  isNull,
  lt,
  lte,
  ne,
  sql,
  type AnyColumn,
  type SQL,
} from "drizzle-orm";
import type { QueryRow, ScanFilterClause, ScanOrderBy } from "@tupl/provider-kit";

import type { DrizzleColumnMap, DrizzleQueryExecutor, RunDrizzleScanOptions } from "../types";

export function impossibleCondition(): SQL {
  return sql`0 = 1`;
}

export async function runDrizzleScan<TTable extends string, TColumn extends string>(
  options: RunDrizzleScanOptions<TTable, TColumn>,
): Promise<QueryRow[]> {
  const selection = buildSelection(options.request.select, options.columns, options.tableName);
  const filterConditions = (options.request.where ?? []).map((clause) =>
    toSqlCondition(clause, options.columns, options.tableName),
  );
  const scopeConditions = normalizeScope(options.scope);
  const whereConditions = [...scopeConditions, ...filterConditions];

  const selectable = options.db.select(selection) as {
    from: (table: never) => {
      where: (condition: SQL) => unknown;
      orderBy: (...clauses: SQL[]) => unknown;
      limit: (value: number) => unknown;
      offset: (value: number) => unknown;
      execute: () => Promise<QueryRow[]>;
    };
  };

  let builder = selectable.from(options.table as never) as {
    where: (condition: SQL) => unknown;
    orderBy: (...clauses: SQL[]) => unknown;
    limit: (value: number) => unknown;
    offset: (value: number) => unknown;
    execute: () => Promise<QueryRow[]>;
  };

  const where = and(...whereConditions);
  if (where) {
    builder = builder.where(where) as typeof builder;
  }

  const orderBy = buildOrderBy(options.request.orderBy, options.columns, options.tableName);
  if (orderBy.length > 0) {
    builder = builder.orderBy(...orderBy) as typeof builder;
  }

  if (options.request.limit != null) {
    builder = builder.limit(options.request.limit) as typeof builder;
  }

  if (options.request.offset != null) {
    builder = builder.offset(options.request.offset) as typeof builder;
  }

  return builder.execute();
}

export async function executeDrizzleQueryBuilder(
  builder: unknown,
  db: DrizzleQueryExecutor,
): Promise<QueryRow[]> {
  if (builder && typeof builder === "object") {
    const execute = (builder as { execute?: unknown }).execute;
    if (typeof execute === "function") {
      return (await execute.call(builder)) as QueryRow[];
    }
    const then = (builder as { then?: unknown }).then;
    if (typeof then === "function") {
      return await (builder as Promise<QueryRow[]>);
    }
  }

  const dbExecute = (db as { execute?: unknown }).execute;
  if (typeof dbExecute === "function") {
    const getSql = (builder as { getSQL?: unknown } | null)?.getSQL;
    if (typeof getSql !== "function") {
      const keys =
        builder && typeof builder === "object"
          ? Object.keys(builder as Record<string, unknown>).join(", ")
          : String(builder);
      throw new Error(
        `Drizzle fallback execute() expected getSQL() on query object. Received keys: ${keys}`,
      );
    }
    return await (dbExecute as (query: unknown) => Promise<QueryRow[]>)(builder);
  }

  throw new Error(
    "Drizzle query builder is not executable via execute(), promise semantics, or db.execute().",
  );
}

export function normalizeScope(scope: SQL | SQL[] | undefined): SQL[] {
  if (!scope) {
    return [];
  }
  return Array.isArray(scope) ? scope : [scope];
}

export function buildSelection<TColumn extends string>(
  selectedColumns: TColumn[],
  columns: DrizzleColumnMap<TColumn>,
  tableName: string,
): Record<TColumn, AnyColumn> {
  const out = {} as Record<TColumn, AnyColumn>;
  for (const column of selectedColumns) {
    const source = columns[column];
    if (!source) {
      throw new Error(`Unsupported column "${column}" for table "${tableName}".`);
    }
    out[column] = source;
  }
  return out;
}

export function buildOrderBy<TColumn extends string>(
  orderBy: ScanOrderBy<TColumn>[] | undefined,
  columns: DrizzleColumnMap<TColumn>,
  tableName: string,
): SQL[] {
  const out: SQL[] = [];
  for (const term of orderBy ?? []) {
    const source = columns[term.column];
    if (!source) {
      throw new Error(`Unsupported ORDER BY column "${term.column}" for table "${tableName}".`);
    }

    out.push(term.direction === "asc" ? asc(source) : desc(source));
  }
  return out;
}

export function toSqlCondition<TColumn extends string>(
  clause: ScanFilterClause<TColumn>,
  columns: DrizzleColumnMap<TColumn>,
  tableName: string,
): SQL {
  const source = columns[clause.column as TColumn];
  if (!source) {
    throw new Error(`Unsupported filter column "${clause.column}" for table "${tableName}".`);
  }

  return toSqlConditionFromSource(clause, source);
}

export function toSqlConditionFromSource(clause: ScanFilterClause, source: AnyColumn | SQL): SQL {
  switch (clause.op) {
    case "eq":
      return eq(source as never, clause.value as never);
    case "neq":
      return ne(source as never, clause.value as never);
    case "gt":
      return gt(source as never, clause.value as never);
    case "gte":
      return gte(source as never, clause.value as never);
    case "lt":
      return lt(source as never, clause.value as never);
    case "lte":
      return lte(source as never, clause.value as never);
    case "in": {
      const values = clause.values.filter((value) => value != null);
      if (values.length === 0) {
        return impossibleCondition();
      }
      return inArray(source as never, values as never[]);
    }
    case "not_in": {
      const values = clause.values.filter((value) => value != null);
      if (values.length === 0) {
        return sql`true`;
      }
      return sql`${source} not in ${values as never[]}`;
    }
    case "like":
      return sql`${source} like ${clause.value as never}`;
    case "not_like":
      return sql`${source} not like ${clause.value as never}`;
    case "is_distinct_from":
      return sql`${source} is distinct from ${clause.value as never}`;
    case "is_not_distinct_from":
      return sql`${source} is not distinct from ${clause.value as never}`;
    case "is_null":
      return isNull(source as never);
    case "is_not_null":
      return isNotNull(source as never);
  }
}
