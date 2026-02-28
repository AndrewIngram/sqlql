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
  type SQL,
} from "drizzle-orm";
import type { BetterSQLite3Database } from "drizzle-orm/better-sqlite3";
import type { AnySQLiteColumn } from "drizzle-orm/sqlite-core";
import type {
  QueryRow,
  ScanFilterClause,
  ScanOrderBy,
  TableLookupRequest,
  TableMethods,
  TableScanRequest,
} from "sqlql";

export type DrizzleColumnMap<TColumn extends string> = Record<TColumn, AnySQLiteColumn>;

export interface CreateDrizzleTableMethodsOptions<
  TContext,
  TTable extends string,
  TColumn extends string,
> {
  db: BetterSQLite3Database;
  tableName: TTable;
  table: object;
  columns: DrizzleColumnMap<TColumn>;
  scope:
    | ((context: TContext) => SQL | SQL[] | undefined | Promise<SQL | SQL[] | undefined>)
    | undefined;
  includeLookup?: boolean;
}

export function createDrizzleTableMethods<TContext, TTable extends string, TColumn extends string>(
  options: CreateDrizzleTableMethodsOptions<TContext, TTable, TColumn>,
): TableMethods<TContext, TTable, TColumn> {
  const includeLookup = options.includeLookup ?? true;

  const scan = async (
    request: TableScanRequest<TTable, TColumn>,
    context: TContext,
  ): Promise<QueryRow[]> => {
    const scope = options.scope ? await options.scope(context) : undefined;
    const payload = {
      db: options.db,
      tableName: options.tableName,
      table: options.table,
      columns: options.columns,
      request,
    } as const;

    if (scope) {
      return runDrizzleScan({
        ...payload,
        scope,
      });
    }

    return runDrizzleScan(payload);
  };

  const methods: TableMethods<TContext, TTable, TColumn> = {
    scan,
  };

  if (includeLookup) {
    methods.lookup = async (
      request: TableLookupRequest<TTable, TColumn>,
      context: TContext,
    ): Promise<QueryRow[]> => {
      const where: ScanFilterClause<TColumn>[] = [
        ...(request.where ?? []),
        {
          column: request.key,
          op: "in",
          values: request.values,
        },
      ];

      const scanRequest: TableScanRequest<TTable, TColumn> = {
        table: request.table,
        select: request.select,
        where,
        ...(request.alias ? { alias: request.alias } : {}),
      };

      return scan(scanRequest, context);
    };
  }

  return methods;
}

export interface RunDrizzleScanOptions<TTable extends string, TColumn extends string> {
  db: BetterSQLite3Database;
  tableName: TTable;
  table: object;
  columns: DrizzleColumnMap<TColumn>;
  request: TableScanRequest<TTable, TColumn>;
  scope?: SQL | SQL[];
}

export function runDrizzleScan<TTable extends string, TColumn extends string>(
  options: RunDrizzleScanOptions<TTable, TColumn>,
): QueryRow[] {
  const selection = buildSelection(options.request.select, options.columns, options.tableName);
  const filterConditions = (options.request.where ?? []).map((clause) =>
    toSqlCondition(clause, options.columns, options.tableName),
  );
  const scopeConditions = normalizeScope(options.scope);
  const whereConditions = [...scopeConditions, ...filterConditions];

  let builder = options.db.select(selection).from(options.table as never) as {
    where: (condition: SQL) => unknown;
    orderBy: (...clauses: SQL[]) => unknown;
    limit: (value: number) => unknown;
    offset: (value: number) => unknown;
    all: () => QueryRow[];
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

  return builder.all();
}

export function impossibleCondition(): SQL {
  return sql`0 = 1`;
}

function normalizeScope(scope: SQL | SQL[] | undefined): SQL[] {
  if (!scope) {
    return [];
  }
  return Array.isArray(scope) ? scope : [scope];
}

function buildSelection<TColumn extends string>(
  selectedColumns: TColumn[],
  columns: DrizzleColumnMap<TColumn>,
  tableName: string,
): Record<TColumn, AnySQLiteColumn> {
  const out = {} as Record<TColumn, AnySQLiteColumn>;
  for (const column of selectedColumns) {
    const source = columns[column];
    if (!source) {
      throw new Error(`Unsupported column "${column}" for table "${tableName}".`);
    }
    out[column] = source;
  }
  return out;
}

function buildOrderBy<TColumn extends string>(
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

function toSqlCondition<TColumn extends string>(
  clause: ScanFilterClause<TColumn>,
  columns: DrizzleColumnMap<TColumn>,
  tableName: string,
): SQL {
  const source = columns[clause.column];
  if (!source) {
    throw new Error(`Unsupported filter column "${clause.column}" for table "${tableName}".`);
  }

  switch (clause.op) {
    case "eq":
      return eq(source, clause.value as never);
    case "neq":
      return ne(source, clause.value as never);
    case "gt":
      return gt(source, clause.value as never);
    case "gte":
      return gte(source, clause.value as never);
    case "lt":
      return lt(source, clause.value as never);
    case "lte":
      return lte(source, clause.value as never);
    case "in": {
      const values = clause.values.filter((value) => value != null);
      if (values.length === 0) {
        return impossibleCondition();
      }
      return inArray(source, values as never[]);
    }
    case "is_null":
      return isNull(source);
    case "is_not_null":
      return isNotNull(source);
  }
}
