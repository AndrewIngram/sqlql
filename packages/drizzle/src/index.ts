import { Result } from "better-result";
import {
  type AnyColumn,
  type InferSelectModel,
  type Table,
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
import {
  bindAdapterEntities,
  collectCapabilityAtomsForFragment,
  createDataEntityHandle,
  inferRouteFamilyForFragment,
  isRelProjectColumnMapping,
  normalizeDataEntityShape,
  type DataEntityColumnMetadata,
  type DataEntityShape,
  type DataEntityHandle,
  type DataEntityReadMetadataMap,
  type InferDataEntityShapeMetadata,
  type MaybePromise,
  type ProviderAdapter,
  type ProviderCapabilityAtom,
  type ProviderCapabilityReport,
  type ProviderCompiledPlan,
  type ProviderFragment,
  type ProviderLookupManyRequest,
  type ProviderRuntimeBinding,
  type QueryRow,
  type RelExpr,
  type RelNode,
  type ScanFilterClause,
  type ScanOrderBy,
  type SqlScalarType,
  type TableScanRequest,
} from "sqlql";

export type DrizzleColumnMap<TColumn extends string = string> = Record<TColumn, AnyColumn>;

export interface DrizzleQueryExecutor {
  select: (...args: unknown[]) => unknown;
}

export interface DrizzleProviderTableConfig<
  TContext,
  TTable extends object = object,
  TColumn extends string = string,
> {
  table: TTable;
  /**
   * Optional explicit column map. If omitted, columns are derived from the
   * Drizzle table object and exposed by both property key and DB column name.
   */
  columns?: DrizzleColumnMap<TColumn>;
  shape?: DataEntityShape<TColumn>;
  scope?:
    | ((context: TContext) => SQL | SQL[] | undefined | Promise<SQL | SQL[] | undefined>)
    | undefined;
}

export interface CreateDrizzleProviderOptions<
  TContext,
  TTables extends Record<string, DrizzleProviderTableConfig<TContext>> = Record<
    string,
    DrizzleProviderTableConfig<TContext>
  >,
> {
  name?: string;
  dialect?: "postgres" | "sqlite";
  db: ProviderRuntimeBinding<TContext, DrizzleQueryExecutor>;
  tables: TTables;
}

interface DrizzleRelCompiledPlan {
  strategy: DrizzleRelCompileStrategy;
  rel: RelNode;
}

type DrizzleRelCompileStrategy = "basic" | "set_op" | "with";

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

function assertDrizzleDb(db: DrizzleQueryExecutor | null | undefined): DrizzleQueryExecutor {
  if (!db || typeof db.select !== "function") {
    throw new Error(
      "Drizzle provider runtime binding did not resolve to a valid database instance. Check your context and db callback.",
    );
  }
  return db;
}

function resolveDrizzleDbMaybeSync<TContext>(
  options: CreateDrizzleProviderOptions<TContext>,
  context: TContext,
): MaybePromise<DrizzleQueryExecutor> {
  if (!isRuntimeBindingResolver(options.db)) {
    return assertDrizzleDb(options.db);
  }

  const db = options.db(context);
  return isPromiseLike(db) ? db.then(assertDrizzleDb) : assertDrizzleDb(db);
}

async function resolveDrizzleDb<TContext>(
  options: CreateDrizzleProviderOptions<TContext>,
  context: TContext,
): Promise<DrizzleQueryExecutor> {
  return await Promise.resolve(resolveDrizzleDbMaybeSync(options, context));
}

function requireColumnProjectMapping(
  mapping: Extract<RelNode, { kind: "project" }>["columns"][number],
): { source: { alias?: string; table?: string; column: string }; output: string } {
  if (!isRelProjectColumnMapping(mapping)) {
    throw new UnsupportedSingleQueryPlanError(
      "Computed projections are not supported in Drizzle single-query pushdown.",
    );
  }
  return mapping;
}

type InferDrizzleEntityRow<TConfig> = TConfig extends DrizzleProviderTableConfig<any, infer TTable, any>
  ? TTable extends Table
    ? InferSelectModel<TTable>
    : Record<string, unknown>
  : Record<string, unknown>;

type InferDrizzleTableColumns<TConfig> = TConfig extends DrizzleProviderTableConfig<any, any, infer TColumn>
  ? [TColumn] extends [string]
    ? Extract<keyof InferDrizzleEntityRow<TConfig>, string> extends never
      ? TColumn
      : Extract<keyof InferDrizzleEntityRow<TConfig>, string>
    : Extract<keyof InferDrizzleEntityRow<TConfig>, string>
  : string;

type InferDrizzleColumnRead<TColumn> = TColumn extends {
  _: {
    data: infer TData;
    notNull: infer TNotNull;
  };
}
  ? TNotNull extends true
    ? TData
    : TData | null
  : unknown;

type InferDrizzleScalarTypeFromColumnMetadata<
  TColumnType extends string,
  TDataType extends string,
> = TColumnType extends `${string}Timestamp${string}`
  ? "timestamp"
  : TColumnType extends `${string}DateTime${string}`
    ? "datetime"
    : TColumnType extends `${string}Date${string}`
      ? "date"
      : TDataType extends "boolean"
        ? "boolean"
        : TDataType extends "json"
          ? "json"
          : TDataType extends "arraybuffer"
            ? "blob"
            : TColumnType extends `${string}Real${string}` | `${string}Double${string}` | `${string}Float${string}`
              ? "real"
              : TColumnType extends
                    | `${string}Int${string}`
                    | `${string}Serial${string}`
                    | `${string}Numeric${string}`
                    | `${string}Decimal${string}`
                ? "integer"
                : TDataType extends "number"
                  ? "integer"
                  : TDataType extends "date"
                    ? "timestamp"
                    : TDataType extends "string"
                      ? "text"
                      : never;

type InferDrizzleColumnSqlqlType<TColumn> = TColumn extends {
  _: {
    columnType: infer TColumnType extends string;
    dataType: infer TDataType extends string;
  };
}
  ? InferDrizzleScalarTypeFromColumnMetadata<TColumnType, TDataType>
  : never;

type InferDrizzleEntityColumnMetadataFromColumns<TColumns extends Record<string, unknown>> = {
  [K in Extract<keyof TColumns, string>]: DataEntityColumnMetadata<InferDrizzleColumnRead<TColumns[K]>> & {
    source: K;
  } & ([InferDrizzleColumnSqlqlType<TColumns[K]>] extends [never]
      ? {}
      : {
          type: InferDrizzleColumnSqlqlType<TColumns[K]>;
        });
};

type InferDrizzleEntityColumnMetadata<TConfig> = TConfig extends { shape: infer TShape }
  ? InferDataEntityShapeMetadata<
      InferDrizzleTableColumns<TConfig>,
      Extract<TShape, DataEntityShape<InferDrizzleTableColumns<TConfig>>>
    >
  : TConfig extends DrizzleProviderTableConfig<any, infer TTable, any>
    ? TTable extends Table
      ? InferDrizzleEntityColumnMetadataFromColumns<TTable["_"]["columns"]>
      : DataEntityReadMetadataMap<InferDrizzleTableColumns<TConfig>, InferDrizzleEntityRow<TConfig>>
    : DataEntityReadMetadataMap<InferDrizzleTableColumns<TConfig>, InferDrizzleEntityRow<TConfig>>;

export function createDrizzleProvider<
  TContext,
  TTables extends Record<string, DrizzleProviderTableConfig<TContext>> = Record<
    string,
    DrizzleProviderTableConfig<TContext>
  >,
>(
  options: CreateDrizzleProviderOptions<TContext, TTables>,
): ProviderAdapter<TContext> & {
  entities: {
    [K in keyof TTables]: DataEntityHandle<
      InferDrizzleTableColumns<TTables[K]>,
      InferDrizzleEntityRow<TTables[K]>,
      InferDrizzleEntityColumnMetadata<TTables[K]>
    >;
  };
} {
  const declaredAtoms: readonly ProviderCapabilityAtom[] = [
    "scan.project",
    "scan.filter.basic",
    "scan.filter.set_membership",
    "scan.sort",
    "scan.limit_offset",
    "lookup.bulk",
    "aggregate.group_by",
    "join.inner",
    "join.left",
    "join.right_full",
    "set_op.union_all",
    "set_op.union_distinct",
    "set_op.intersect",
    "set_op.except",
    "cte.non_recursive",
    "window.rank_basic",
  ];
  const providerName = options.name ?? "drizzle";
  const tableConfigs = options.tables as Record<string, DrizzleProviderTableConfig<TContext>>;
  const dialect = options.dialect ?? inferDrizzleDialect(options.db, tableConfigs);
  void dialect;

  const handles = {} as {
    [K in keyof TTables]: DataEntityHandle<
      InferDrizzleTableColumns<TTables[K]>,
      InferDrizzleEntityRow<TTables[K]>,
      InferDrizzleEntityColumnMetadata<TTables[K]>
    >;
  };
  const adapter = {
    name: providerName,
    entities: handles,
    routeFamilies: ["scan", "lookup", "aggregate", "rel-core", "rel-advanced"] as const,
    capabilityAtoms: [...declaredAtoms],
    canExecute(fragment, context): MaybePromise<boolean | ProviderCapabilityReport> {
      switch (fragment.kind) {
        case "scan":
          return !!tableConfigs[fragment.table];
        case "rel": {
          const requiredAtoms = collectCapabilityAtomsForFragment(fragment);
          const missingAtoms = requiredAtoms.filter((atom) => !declaredAtoms.includes(atom));
          const routeFamily = inferRouteFamilyForFragment(fragment);
          const evaluateWithDb = (db: DrizzleQueryExecutor): boolean | ProviderCapabilityReport => {
            const strategy = resolveDrizzleRelCompileStrategy(fragment.rel, tableConfigs);
            if (strategy && !isStrategyAvailableOnDrizzleDb(strategy, db)) {
              return {
                supported: false,
                routeFamily,
                requiredAtoms,
                missingAtoms,
                reason: `Drizzle database instance does not support required APIs for "${strategy}" rel pushdown.`,
              };
            }
            return strategy
              ? true
              : {
                  supported: false,
                  routeFamily,
                  requiredAtoms,
                  missingAtoms,
                  reason: hasSqlNode(fragment.rel)
                    ? "rel fragment must not contain sql nodes."
                    : "Rel fragment is not supported for single-query drizzle pushdown.",
                };
          };

          if (!isRuntimeBindingResolver(options.db)) {
            return evaluateWithDb(options.db);
          }

          const db = resolveDrizzleDbMaybeSync(options, context);
          return isPromiseLike(db) ? db.then(evaluateWithDb) : evaluateWithDb(db);
        }
        default:
          return false;
      }
    },
    async compile(fragment, context) {
      switch (fragment.kind) {
        case "scan":
          return Result.ok({
            provider: providerName,
            kind: fragment.kind,
            payload: fragment,
          });
        case "rel": {
          const strategy = resolveDrizzleRelCompileStrategy(fragment.rel, tableConfigs);
          if (!strategy) {
            return Result.err(new Error("Unsupported relational fragment for drizzle provider."));
          }
          const db = await resolveDrizzleDb(options, context);
          if (!isStrategyAvailableOnDrizzleDb(strategy, db)) {
            return Result.err(
              new Error(
                `Drizzle database instance does not support required APIs for "${strategy}" rel pushdown.`,
              ),
            );
          }

          return Result.ok({
            provider: providerName,
            kind: fragment.kind,
            payload: {
              strategy,
              rel: fragment.rel,
            } satisfies DrizzleRelCompiledPlan,
          });
        }
        default:
          return Result.err(
            new Error(`Unsupported drizzle fragment kind: ${(fragment as { kind?: unknown }).kind}`),
          );
      }
    },
    async execute(plan, context) {
      return Result.tryPromise({
        try: () => executeDrizzlePlan(plan, options, context),
        catch: (error) => (error instanceof Error ? error : new Error(String(error))),
      });
    },
    async lookupMany(request, context) {
      return Result.tryPromise({
        try: () => lookupManyWithDrizzle(options, request, context),
        catch: (error) => (error instanceof Error ? error : new Error(String(error))),
      });
    },
  } satisfies ProviderAdapter<TContext> & {
    entities: {
      [K in keyof TTables]: DataEntityHandle<
        InferDrizzleTableColumns<TTables[K]>,
        InferDrizzleEntityRow<TTables[K]>,
        InferDrizzleEntityColumnMetadata<TTables[K]>
      >;
    };
  };
  for (const tableName of Object.keys(options.tables) as Array<Extract<keyof TTables, string>>) {
    const tableConfig = options.tables[tableName];
    if (!tableConfig) {
      throw new Error(`Missing drizzle table config: ${tableName}`);
    }
    const entityColumns = tableConfig.shape
      ? normalizeDataEntityShape(tableConfig.shape as DataEntityShape<InferDrizzleTableColumns<TTables[typeof tableName]>>)
      : deriveEntityColumnsFromTable(tableConfig.table);
    handles[tableName] = createDataEntityHandle<
      InferDrizzleTableColumns<TTables[typeof tableName]>,
      InferDrizzleEntityRow<TTables[typeof tableName]>,
      InferDrizzleEntityColumnMetadata<TTables[typeof tableName]>
    >({
      entity: tableName,
      provider: providerName,
      adapter,
      ...(entityColumns && Object.keys(entityColumns).length > 0
        ? { columns: entityColumns as never }
        : {}),
    });
  }

  return bindAdapterEntities(adapter);
}

function inferDrizzleDialect<TContext>(
  db: ProviderRuntimeBinding<TContext, DrizzleQueryExecutor>,
  tableConfigs: Record<string, DrizzleProviderTableConfig<TContext>>,
): "postgres" | "sqlite" {
  const tableDialects = new Set<"postgres" | "sqlite">();
  for (const tableConfig of Object.values(tableConfigs)) {
    const detected = normalizeDialectName(readTableDialectName(tableConfig.table));
    if (detected) {
      tableDialects.add(detected);
    }
  }

  if (tableDialects.size > 1) {
    throw new Error(
      `Unable to infer drizzle dialect: provider tables declare mixed dialects (${[
        ...tableDialects,
      ].join(", ")}).`,
    );
  }

  const fromTables = [...tableDialects][0];
  if (fromTables) {
    return fromTables;
  }

  if (isRuntimeBindingResolver(db)) {
    throw new Error(
      "Unable to infer drizzle dialect from a context-resolved db binding. Set options.dialect explicitly or use tables with declared dialects.",
    );
  }

  const fromDb = normalizeDialectName(readDbDialectHint(db));
  if (fromDb) {
    return fromDb;
  }

  return "sqlite";
}

function readTableDialectName(table: object): string | undefined {
  const candidate = (table as { _?: { config?: { dialect?: unknown } } })._?.config?.dialect;
  return typeof candidate === "string" ? candidate : undefined;
}

function readDbDialectHint(db: DrizzleQueryExecutor): string | undefined {
  const maybeDialect = (db as { dialect?: unknown }).dialect;
  if (typeof maybeDialect === "string") {
    return maybeDialect;
  }

  const constructorName = (db as { constructor?: { name?: unknown } }).constructor?.name;
  if (typeof constructorName === "string" && constructorName.length > 0) {
    return constructorName;
  }

  const sessionName = (
    db as unknown as { _: { session?: { constructor?: { name?: unknown } } } }
  )._?.session?.constructor?.name;
  return typeof sessionName === "string" ? sessionName : undefined;
}

function normalizeDialectName(name: string | undefined): "postgres" | "sqlite" | null {
  if (!name) {
    return null;
  }
  const normalized = name.toLowerCase();
  if (normalized === "pg" || normalized === "postgres" || normalized.includes("pglite")) {
    return "postgres";
  }
  if (normalized === "sqlite" || normalized.includes("sqlite")) {
    return "sqlite";
  }
  return null;
}

async function executeDrizzlePlan<TContext>(
  plan: ProviderCompiledPlan,
  options: CreateDrizzleProviderOptions<TContext>,
  context: TContext,
): Promise<QueryRow[]> {
  const tableConfigs = options.tables as Record<string, DrizzleProviderTableConfig<TContext>>;
  const db = await resolveDrizzleDb(options, context);

  switch (plan.kind) {
    case "rel": {
      const compiled = plan.payload as DrizzleRelCompiledPlan;
      return executeDrizzleRelSingleQuery(compiled.rel, compiled.strategy, options, context, db);
    }
    case "scan": {
      const fragment = plan.payload as Extract<ProviderFragment, { kind: "scan" }>;
      const tableConfig = tableConfigs[fragment.table];
      if (!tableConfig) {
        throw new Error(`Unknown drizzle table config: ${fragment.table}`);
      }

      const scope = tableConfig.scope ? await tableConfig.scope(context) : undefined;
      return runDrizzleScan({
        db,
        tableName: fragment.table,
        table: tableConfig.table,
        columns: resolveColumns(tableConfig, fragment.table),
        request: fragment.request,
        ...(scope ? { scope } : {}),
      });
    }
    default:
      throw new Error(`Unsupported drizzle compiled plan kind: ${plan.kind}`);
  }
}

async function lookupManyWithDrizzle<TContext>(
  options: CreateDrizzleProviderOptions<TContext>,
  request: ProviderLookupManyRequest,
  context: TContext,
): Promise<QueryRow[]> {
  const tableConfigs = options.tables as Record<string, DrizzleProviderTableConfig<TContext>>;
  const db = await resolveDrizzleDb(options, context);
  const tableConfig = tableConfigs[request.table];
  if (!tableConfig) {
    throw new Error(`Unknown drizzle table config: ${request.table}`);
  }

  const where: ScanFilterClause[] = [
    ...(request.where ?? []),
    {
      op: "in",
      column: request.key,
      values: request.keys,
    } as ScanFilterClause,
  ];

  const scope = tableConfig.scope ? await tableConfig.scope(context) : undefined;
  return runDrizzleScan({
    db,
    tableName: request.table,
    table: tableConfig.table,
    columns: resolveColumns(tableConfig, request.table),
    request: {
      table: request.table,
      select: request.select,
      where,
    },
    ...(scope ? { scope } : {}),
  });
}

export interface RunDrizzleScanOptions<TTable extends string, TColumn extends string> {
  db: DrizzleQueryExecutor;
  tableName: TTable;
  table: object;
  columns: DrizzleColumnMap<TColumn>;
  request: TableScanRequest<TTable, TColumn>;
  scope?: SQL | SQL[];
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

function resolveColumns<TContext>(
  tableConfig: DrizzleProviderTableConfig<TContext>,
  tableName: string,
): DrizzleColumnMap<string> {
  if (tableConfig.columns) {
    return tableConfig.columns;
  }

  const derived = deriveColumnsFromTable(tableConfig.table);
  if (Object.keys(derived).length === 0) {
    throw new Error(
      `Unable to derive columns for table "${tableName}". Provide an explicit columns map.`,
    );
  }

  return derived;
}

function deriveColumnsFromTable(table: object): DrizzleColumnMap<string> {
  const out: DrizzleColumnMap<string> = {};

  for (const [propertyKey, raw] of Object.entries(table as Record<string, unknown>)) {
    if (!looksLikeDrizzleColumn(raw)) {
      continue;
    }

    const column = raw as AnyColumn;
    out[propertyKey] = column;

    const dbName = readColumnName(column);
    if (dbName) {
      out[dbName] = column;
    }
  }

  return out;
}

function deriveEntityColumnsFromTable(table: object): DataEntityHandle<string>["columns"] {
  const out: NonNullable<DataEntityHandle<string>["columns"]> = {};

  for (const [propertyKey, raw] of Object.entries(table as Record<string, unknown>)) {
    if (!looksLikeDrizzleColumn(raw)) {
      continue;
    }

    const column = raw as AnyColumn;
    const metadata: NonNullable<DataEntityHandle<string>["columns"]>[string] = {
      source: readColumnName(column) ?? propertyKey,
    };
    const inferredType = inferSqlqlTypeFromDrizzleColumn(column);
    if (inferredType) {
      metadata.type = inferredType;
    }
    if (column.notNull) {
      metadata.nullable = false;
    }
    if (column.primary) {
      metadata.primaryKey = true;
    } else if (column.isUnique) {
      metadata.unique = true;
    }
    if (Array.isArray(column.enumValues) && column.enumValues.length > 0) {
      metadata.enum = column.enumValues;
    }
    if (typeof column.dataType === "string") {
      metadata.physicalType = column.dataType;
    }
    out[propertyKey] = metadata;
  }

  return out;
}

function inferSqlqlTypeFromDrizzleColumn(column: AnyColumn): SqlScalarType | undefined {
  const dataType = String((column as { dataType?: unknown }).dataType ?? "").toLowerCase();
  const sqlType = typeof column.getSQLType === "function" ? column.getSQLType().toLowerCase() : "";
  const normalizedSqlType = sqlType.replace(/\s+/g, " ");

  if (dataType === "boolean" || sqlType === "boolean") {
    return "boolean";
  }
  if (dataType === "json" || normalizedSqlType.includes("json")) {
    return "json";
  }
  if (dataType === "arraybuffer" || normalizedSqlType.includes("blob") || normalizedSqlType.includes("bytea")) {
    return "blob";
  }
  if (normalizedSqlType.includes("datetime")) {
    return "datetime";
  }
  if (normalizedSqlType === "date") {
    return "date";
  }
  if (dataType === "date" || normalizedSqlType.includes("timestamp")) {
    return "timestamp";
  }
  if (
    normalizedSqlType.includes("real") ||
    normalizedSqlType.includes("double") ||
    normalizedSqlType.includes("float")
  ) {
    return "real";
  }
  if (
    dataType === "number" ||
    normalizedSqlType.includes("int") ||
    normalizedSqlType.includes("numeric") ||
    normalizedSqlType.includes("decimal")
  ) {
    return "integer";
  }
  if (dataType === "string" || sqlType.length > 0) {
    return "text";
  }
  return undefined;
}

function looksLikeDrizzleColumn(value: unknown): value is AnyColumn {
  return (
    !!value &&
    typeof value === "object" &&
    typeof (value as { name?: unknown }).name === "string"
  );
}

function readColumnName(column: AnyColumn): string | null {
  const maybeName = (column as unknown as { name?: unknown }).name;
  return typeof maybeName === "string" ? maybeName : null;
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
  const source = columns[clause.column as TColumn];
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
      return isNull(source);
    case "is_not_null":
      return isNotNull(source);
  }
}

function resolveDrizzleRelCompileStrategy(
  node: RelNode,
  tableConfigs: Record<string, DrizzleProviderTableConfig<any>>,
): DrizzleRelCompileStrategy | null {
  if (canCompileBasicRel(node, tableConfigs)) {
    try {
      buildSingleQueryPlan(node, tableConfigs);
      return "basic";
    } catch (error) {
      if (!(error instanceof UnsupportedSingleQueryPlanError)) {
        throw error;
      }
    }
  }
  if (canCompileSetOpRel(node, tableConfigs)) {
    return "set_op";
  }
  if (canCompileWithRel(node, tableConfigs)) {
    return "with";
  }
  return null;
}

function isStrategyAvailableOnDrizzleDb(
  strategy: DrizzleRelCompileStrategy,
  db: DrizzleQueryExecutor,
): boolean {
  if (strategy !== "with") {
    return true;
  }
  const candidate = db as {
    $with?: unknown;
    with?: unknown;
  };
  return typeof candidate.$with === "function" && typeof candidate.with === "function";
}

function canCompileBasicRel(
  node: RelNode,
  tableConfigs: Record<string, DrizzleProviderTableConfig<any>>,
): boolean {
  switch (node.kind) {
    case "scan":
      return !!tableConfigs[node.table];
    case "filter":
      return !node.expr && canCompileBasicRel(node.input, tableConfigs);
    case "project":
    case "aggregate":
    case "sort":
    case "limit_offset":
      return canCompileBasicRel(node.input, tableConfigs);
    case "join":
      return canCompileBasicRel(node.left, tableConfigs) && canCompileBasicRel(node.right, tableConfigs);
    case "window":
      return false;
    case "set_op":
    case "with":
    case "sql":
      return false;
  }
}

function canCompileSetOpRel(
  node: RelNode,
  tableConfigs: Record<string, DrizzleProviderTableConfig<any>>,
): boolean {
  const wrapper = unwrapSetOpRel(node);
  if (!wrapper) {
    return false;
  }
  if (!canCompileBasicRel(wrapper.setOp.left, tableConfigs)) {
    return false;
  }
  if (!canCompileBasicRel(wrapper.setOp.right, tableConfigs)) {
    return false;
  }

  const topProject = wrapper.project;
  if (topProject) {
    for (const rawColumn of topProject.columns) {
      const column = requireColumnProjectMapping(rawColumn);
      if (column.source.alias || column.source.table) {
        return false;
      }
      if (column.source.column !== column.output) {
        return false;
      }
    }
  }

  for (const term of wrapper.sort?.orderBy ?? []) {
    if (term.source.alias || term.source.table) {
      return false;
    }
  }

  return true;
}

function canCompileWithRel(
  node: RelNode,
  tableConfigs: Record<string, DrizzleProviderTableConfig<any>>,
): boolean {
  if (node.kind !== "with") {
    return false;
  }

  if (node.ctes.length === 0) {
    return false;
  }

  for (const cte of node.ctes) {
    const strategy = resolveDrizzleRelCompileStrategy(cte.query, tableConfigs);
    if (!strategy) {
      return false;
    }
  }

  const body = unwrapWithBodyRel(node.body);
  if (!body) {
    return false;
  }
  if (!body.cteScan.table || !node.ctes.some((cte) => cte.name === body.cteScan.table)) {
    return false;
  }
  for (const fn of body.window?.functions ?? []) {
    if (fn.fn !== "dense_rank" && fn.fn !== "rank" && fn.fn !== "row_number") {
      return false;
    }
  }
  return true;
}

function hasSqlNode(node: RelNode): boolean {
  switch (node.kind) {
    case "sql":
      return true;
    case "scan":
      return false;
    case "filter":
    case "project":
    case "aggregate":
    case "window":
    case "sort":
    case "limit_offset":
      return hasSqlNode(node.input);
    case "join":
    case "set_op":
      return hasSqlNode(node.left) || hasSqlNode(node.right);
    case "with":
      return node.ctes.some((cte) => hasSqlNode(cte.query)) || hasSqlNode(node.body);
  }
}

type InternalRow = Record<string, unknown>;

class UnsupportedSingleQueryPlanError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "UnsupportedSingleQueryPlanError";
  }
}

interface RelPipeline {
  base: RelNode;
  project?: Extract<RelNode, { kind: "project" }>;
  aggregate?: Extract<RelNode, { kind: "aggregate" }>;
  sort?: Extract<RelNode, { kind: "sort" }>;
  limitOffset?: Extract<RelNode, { kind: "limit_offset" }>;
  filters: Extract<RelNode, { kind: "filter" }>[];
}

interface SetOpWrapper {
  setOp: Extract<RelNode, { kind: "set_op" }>;
  project?: Extract<RelNode, { kind: "project" }>;
  sort?: Extract<RelNode, { kind: "sort" }>;
  limitOffset?: Extract<RelNode, { kind: "limit_offset" }>;
}

interface WithBodyWrapper {
  cteScan: Extract<RelNode, { kind: "scan" }>;
  project?: Extract<RelNode, { kind: "project" }>;
  sort?: Extract<RelNode, { kind: "sort" }>;
  limitOffset?: Extract<RelNode, { kind: "limit_offset" }>;
  window?: Extract<RelNode, { kind: "window" }>;
  filters: Extract<RelNode, { kind: "filter" }>[];
}

interface ScanBinding<TContext> {
  alias: string;
  scan: Extract<RelNode, { kind: "scan" }>;
  tableName: string;
  table: object;
  columns: DrizzleColumnMap<string>;
  tableConfig: DrizzleProviderTableConfig<TContext>;
}

interface RegularJoinStep<TContext> {
  joinType: Exclude<Extract<RelNode, { kind: "join" }>["joinType"], "semi">;
  right: ScanBinding<TContext>;
  leftKey: { alias: string; column: string };
  rightKey: { alias: string; column: string };
}

interface SemiJoinStep {
  joinType: "semi";
  right: RelNode;
  leftKey: { alias: string; column: string };
  rightKey: { alias?: string; column: string };
}

type JoinStep<TContext> = RegularJoinStep<TContext> | SemiJoinStep;

interface JoinPlan<TContext> {
  root: ScanBinding<TContext>;
  joins: JoinStep<TContext>[];
  aliases: Map<string, ScanBinding<TContext>>;
}

interface SingleQueryPlan<TContext> {
  joinPlan: JoinPlan<TContext>;
  pipeline: RelPipeline;
}

async function executeDrizzleRelSingleQuery<TContext>(
  rel: RelNode,
  strategy: DrizzleRelCompileStrategy,
  options: CreateDrizzleProviderOptions<TContext>,
  context: TContext,
  db: DrizzleQueryExecutor,
): Promise<QueryRow[]> {
  switch (strategy) {
    case "basic":
      return executeDrizzleBasicRelSingleQuery(rel, options, context, db);
    case "set_op":
      return executeDrizzleSetOpRelSingleQuery(rel, options, context, db);
    case "with":
      return executeDrizzleWithRelSingleQuery(rel, options, context, db);
  }
}

async function executeDrizzleBasicRelSingleQuery<TContext>(
  rel: RelNode,
  options: CreateDrizzleProviderOptions<TContext>,
  context: TContext,
  db: DrizzleQueryExecutor,
): Promise<QueryRow[]> {
  const { builder } = await buildDrizzleBasicRelSingleQueryBuilder(rel, options, context, db);
  return executeDrizzleQueryBuilder(builder, db);
}

async function buildDrizzleBasicRelSingleQueryBuilder<TContext>(
  rel: RelNode,
  options: CreateDrizzleProviderOptions<TContext>,
  context: TContext,
  db: DrizzleQueryExecutor,
): Promise<{
  builder: {
    execute: () => Promise<QueryRow[]>;
    orderBy: (...clauses: SQL[]) => unknown;
    limit: (value: number) => unknown;
    offset: (value: number) => unknown;
  };
}> {
  const tableConfigs = options.tables as Record<string, DrizzleProviderTableConfig<TContext>>;
  const plan = buildSingleQueryPlan(rel, tableConfigs);
  const selection = buildSingleQuerySelection(plan);
  const preferDistinctSelection =
    !!plan.pipeline.aggregate &&
    plan.pipeline.aggregate.metrics.length === 0 &&
    plan.pipeline.aggregate.groupBy.length > 0;
  const dbWithSelectDistinct = db as {
    select: (selection: Record<string, unknown>) => {
      from: (table: object) => {
        innerJoin: (table: object, on: SQL) => unknown;
        leftJoin: (table: object, on: SQL) => unknown;
        rightJoin: (table: object, on: SQL) => unknown;
        fullJoin: (table: object, on: SQL) => unknown;
        where: (condition: SQL) => unknown;
        groupBy: (...columns: AnyColumn[]) => unknown;
        orderBy: (...clauses: SQL[]) => unknown;
        limit: (value: number) => unknown;
        offset: (value: number) => unknown;
        execute: () => Promise<QueryRow[]>;
      };
    };
    selectDistinct?: (selection: Record<string, unknown>) => {
      from: (table: object) => {
        innerJoin: (table: object, on: SQL) => unknown;
        leftJoin: (table: object, on: SQL) => unknown;
        rightJoin: (table: object, on: SQL) => unknown;
        fullJoin: (table: object, on: SQL) => unknown;
        where: (condition: SQL) => unknown;
        groupBy: (...columns: AnyColumn[]) => unknown;
        orderBy: (...clauses: SQL[]) => unknown;
        limit: (value: number) => unknown;
        offset: (value: number) => unknown;
        execute: () => Promise<QueryRow[]>;
      };
    };
  };

  const selectFn =
    preferDistinctSelection && typeof dbWithSelectDistinct.selectDistinct === "function"
      ? dbWithSelectDistinct.selectDistinct.bind(dbWithSelectDistinct)
      : dbWithSelectDistinct.select.bind(dbWithSelectDistinct);

  let builder = selectFn(selection).from(plan.joinPlan.root.table) as {
    innerJoin: (table: object, on: SQL) => unknown;
    leftJoin: (table: object, on: SQL) => unknown;
    rightJoin: (table: object, on: SQL) => unknown;
    fullJoin: (table: object, on: SQL) => unknown;
    where: (condition: SQL) => unknown;
    groupBy: (...columns: AnyColumn[]) => unknown;
    orderBy: (...clauses: SQL[]) => unknown;
    limit: (value: number) => unknown;
    offset: (value: number) => unknown;
    execute: () => Promise<QueryRow[]>;
  };

  ensureJoinMethodsAvailable(builder, plan.joinPlan.joins);

  const whereClauses: SQL[] = [];

  for (const joinStep of plan.joinPlan.joins) {
    if (joinStep.joinType === "semi") {
      const leftColumn = resolveColumnRefFromAliasMap(plan.joinPlan.aliases, {
        alias: joinStep.leftKey.alias,
        column: joinStep.leftKey.column,
      });
      const { subquery } = await buildSemiJoinSubquery(
        joinStep,
        options,
        context,
        db,
      );
      whereClauses.push(sql`${leftColumn} in (${asDrizzleSubquerySql(subquery)})`);
      continue;
    }
    const leftColumn = resolveColumnRefFromAliasMap(plan.joinPlan.aliases, {
      alias: joinStep.leftKey.alias,
      column: joinStep.leftKey.column,
    });
    const rightColumn = resolveColumnRefFromAliasMap(plan.joinPlan.aliases, {
      alias: joinStep.rightKey.alias,
      column: joinStep.rightKey.column,
    });
    const onClause = eq(leftColumn, rightColumn);
    builder = (
      joinStep.joinType === "inner"
        ? builder.innerJoin(joinStep.right.table, onClause)
        : joinStep.joinType === "left"
          ? builder.leftJoin(joinStep.right.table, onClause)
          : joinStep.joinType === "right"
            ? builder.rightJoin(joinStep.right.table, onClause)
            : builder.fullJoin(joinStep.right.table, onClause)
    ) as typeof builder;
  }

  for (const binding of plan.joinPlan.aliases.values()) {
    whereClauses.push(...normalizeScope(binding.tableConfig.scope ? await binding.tableConfig.scope(context) : undefined));
    for (const clause of binding.scan.where ?? []) {
      whereClauses.push(toSqlCondition(clause, binding.columns, binding.tableName));
    }
  }

  for (const filterNode of plan.pipeline.filters) {
    for (const clause of filterNode.where ?? []) {
      whereClauses.push(toSqlConditionFromRelFilterClause(clause, plan));
    }
  }

  const where = and(...whereClauses);
  if (where) {
    builder = builder.where(where) as typeof builder;
  }

  if (
    plan.pipeline.aggregate &&
    plan.pipeline.aggregate.groupBy.length > 0 &&
    !preferDistinctSelection
  ) {
    const groupByColumns = plan.pipeline.aggregate.groupBy.map((columnRef) =>
      resolveColumnRefFromAliasMap(
        plan.joinPlan.aliases,
        toAliasColumnRef(columnRef.alias ?? columnRef.table, columnRef.column),
      ));
    builder = builder.groupBy(...groupByColumns) as typeof builder;
  }

  if (plan.pipeline.sort) {
    const orderBy = plan.pipeline.sort.orderBy.map((term) => {
      const source = resolveSingleQuerySortSource(term, plan);
      return term.direction === "asc" ? asc(source) : desc(source);
    });
    if (orderBy.length > 0) {
      builder = builder.orderBy(...orderBy) as typeof builder;
    }
  }

  if (plan.pipeline.limitOffset?.limit != null) {
    builder = builder.limit(plan.pipeline.limitOffset.limit) as typeof builder;
  }
  if (plan.pipeline.limitOffset?.offset != null) {
    builder = builder.offset(plan.pipeline.limitOffset.offset) as typeof builder;
  }

  return { builder };
}

interface DrizzleExecutableBuilder {
  execute: () => Promise<QueryRow[]>;
  orderBy?: (...clauses: SQL[]) => unknown;
  limit?: (value: number) => unknown;
  offset?: (value: number) => unknown;
  where?: (condition: SQL) => unknown;
}

function asDrizzleSubquerySql(subquery: unknown): SQL {
  if (!subquery || typeof subquery !== "object") {
    throw new UnsupportedSingleQueryPlanError("SEMI join subquery must be a Drizzle query object.");
  }
  const maybe = subquery as { getSQL?: unknown; toSQL?: unknown; execute?: unknown; then?: unknown };
  if (typeof maybe.getSQL !== "function") {
    throw new UnsupportedSingleQueryPlanError(
      "SEMI join subquery does not expose getSQL(), so it cannot be embedded as an IN subquery.",
    );
  }
  return sql`${subquery as { getSQL: () => SQL }}`;
}

async function executeDrizzleSetOpRelSingleQuery<TContext>(
  rel: RelNode,
  options: CreateDrizzleProviderOptions<TContext>,
  context: TContext,
  db: DrizzleQueryExecutor,
): Promise<QueryRow[]> {
  const { builder } = await buildDrizzleSetOpRelSingleQueryBuilder(rel, options, context, db);
  return executeDrizzleQueryBuilder(builder, db);
}

async function executeDrizzleWithRelSingleQuery<TContext>(
  rel: RelNode,
  options: CreateDrizzleProviderOptions<TContext>,
  context: TContext,
  db: DrizzleQueryExecutor,
): Promise<QueryRow[]> {
  const { builder } = await buildDrizzleWithRelSingleQueryBuilder(rel, options, context, db);
  return executeDrizzleQueryBuilder(builder, db);
}

async function executeDrizzleQueryBuilder(
  builder: unknown,
  db: DrizzleQueryExecutor,
): Promise<QueryRow[]> {
  if (builder && typeof builder === "object") {
    const execute = (builder as { execute?: unknown }).execute;
    if (typeof execute === "function") {
      return await execute.call(builder) as QueryRow[];
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
      throw new UnsupportedSingleQueryPlanError(
        `Drizzle fallback execute() expected getSQL() on query object. Received keys: ${keys}`,
      );
    }
    return await (dbExecute as (query: unknown) => Promise<QueryRow[]>)(builder);
  }

  throw new UnsupportedSingleQueryPlanError(
    "Drizzle query builder is not executable via execute(), promise semantics, or db.execute().",
  );
}

async function buildDrizzleRelBuilderForStrategy<TContext>(
  rel: RelNode,
  options: CreateDrizzleProviderOptions<TContext>,
  context: TContext,
  db: DrizzleQueryExecutor,
): Promise<{ builder: DrizzleExecutableBuilder }> {
  const tableConfigs = options.tables as Record<string, DrizzleProviderTableConfig<TContext>>;
  const strategy = resolveDrizzleRelCompileStrategy(rel, tableConfigs);
  if (!strategy) {
    throw new UnsupportedSingleQueryPlanError(
      `Rel node "${rel.kind}" is not supported in Drizzle single-query pushdown.`,
    );
  }
  switch (strategy) {
    case "basic":
      return buildDrizzleBasicRelSingleQueryBuilder(rel, options, context, db);
    case "set_op":
      return buildDrizzleSetOpRelSingleQueryBuilder(rel, options, context, db);
    case "with":
      return buildDrizzleWithRelSingleQueryBuilder(rel, options, context, db);
  }
}

async function buildSemiJoinSubquery<TContext>(
  joinStep: SemiJoinStep,
  options: CreateDrizzleProviderOptions<TContext>,
  context: TContext,
  db: DrizzleQueryExecutor,
): Promise<{ subquery: DrizzleExecutableBuilder }> {
  if (joinStep.right.output.length !== 1) {
    throw new UnsupportedSingleQueryPlanError(
      "SEMI join subquery must project exactly one output column.",
    );
  }
  return { subquery: (await buildDrizzleRelBuilderForStrategy(joinStep.right, options, context, db)).builder };
}

async function buildDrizzleSetOpRelSingleQueryBuilder<TContext>(
  rel: RelNode,
  options: CreateDrizzleProviderOptions<TContext>,
  context: TContext,
  db: DrizzleQueryExecutor,
): Promise<{ builder: DrizzleExecutableBuilder }> {
  const wrapper = unwrapSetOpRel(rel);
  if (!wrapper) {
    throw new UnsupportedSingleQueryPlanError("Expected set-op relational shape.");
  }

  const left = (await buildDrizzleRelBuilderForStrategy(wrapper.setOp.left, options, context, db)).builder;
  const right = (await buildDrizzleRelBuilderForStrategy(wrapper.setOp.right, options, context, db)).builder;
  const methodName =
    wrapper.setOp.op === "union_all"
      ? "unionAll"
      : wrapper.setOp.op === "union"
        ? "union"
        : wrapper.setOp.op === "intersect"
          ? "intersect"
          : "except";
  const applySetOp = (left as unknown as Record<string, unknown>)[methodName];
  if (typeof applySetOp !== "function") {
    throw new UnsupportedSingleQueryPlanError(
      `Drizzle query builder does not support ${methodName} for single-query pushdown.`,
    );
  }
  let builder = applySetOp.call(left, right) as DrizzleExecutableBuilder;

  if (wrapper.project) {
    for (const rawMapping of wrapper.project.columns) {
      const mapping = requireColumnProjectMapping(rawMapping);
      if ((mapping.source.alias || mapping.source.table) && mapping.source.column !== mapping.output) {
        throw new UnsupportedSingleQueryPlanError(
          "Set-op projections with qualified or renamed columns are not supported in single-query pushdown.",
        );
      }
    }
  }

  if (wrapper.sort) {
    if (typeof builder.orderBy !== "function") {
      throw new UnsupportedSingleQueryPlanError(
        "Drizzle query builder does not support ORDER BY on set-op fragments.",
      );
    }
    const orderByClauses = wrapper.sort.orderBy.map((term) => {
      if (term.source.alias || term.source.table) {
        throw new UnsupportedSingleQueryPlanError(
          "Set-op ORDER BY columns must be unqualified output columns.",
        );
      }
      const identifier = sql.identifier(term.source.column);
      return term.direction === "asc" ? asc(identifier) : desc(identifier);
    });
    if (orderByClauses.length > 0) {
      builder = builder.orderBy(...orderByClauses) as DrizzleExecutableBuilder;
    }
  }

  if (wrapper.limitOffset?.limit != null) {
    if (typeof builder.limit !== "function") {
      throw new UnsupportedSingleQueryPlanError(
        "Drizzle query builder does not support LIMIT on set-op fragments.",
      );
    }
    builder = builder.limit(wrapper.limitOffset.limit) as DrizzleExecutableBuilder;
  }
  if (wrapper.limitOffset?.offset != null) {
    if (typeof builder.offset !== "function") {
      throw new UnsupportedSingleQueryPlanError(
        "Drizzle query builder does not support OFFSET on set-op fragments.",
      );
    }
    builder = builder.offset(wrapper.limitOffset.offset) as DrizzleExecutableBuilder;
  }

  return { builder };
}

async function buildDrizzleWithRelSingleQueryBuilder<TContext>(
  rel: RelNode,
  options: CreateDrizzleProviderOptions<TContext>,
  context: TContext,
  db: DrizzleQueryExecutor,
): Promise<{ builder: DrizzleExecutableBuilder }> {
  if (rel.kind !== "with") {
    throw new UnsupportedSingleQueryPlanError(`Expected with node, received "${rel.kind}".`);
  }
  const dbWithCtes = db as {
    $with?: (name: string) => { as: (query: DrizzleExecutableBuilder) => unknown };
    with?: (...ctes: unknown[]) => {
      select: (selection: Record<string, unknown>) => {
        from: (source: unknown) => DrizzleExecutableBuilder;
      };
    };
  };
  if (typeof dbWithCtes.$with !== "function" || typeof dbWithCtes.with !== "function") {
    throw new UnsupportedSingleQueryPlanError(
      "Drizzle database instance does not support CTE builders required for WITH pushdown.",
    );
  }

  const cteBindings = new Map<string, unknown>();
  const cteRefs: unknown[] = [];
  for (const cte of rel.ctes) {
    const query = (await buildDrizzleRelBuilderForStrategy(cte.query, options, context, db)).builder;
    const cteRef = dbWithCtes.$with(cte.name).as(query);
    cteBindings.set(cte.name, cteRef);
    cteRefs.push(cteRef);
  }

  const body = unwrapWithBodyRel(rel.body);
  if (!body) {
    throw new UnsupportedSingleQueryPlanError("Unsupported WITH body shape for single-query pushdown.");
  }
  const source = cteBindings.get(body.cteScan.table);
  if (!source) {
    throw new UnsupportedSingleQueryPlanError(`Unknown CTE "${body.cteScan.table}" in WITH body.`);
  }
  const scanAlias = body.cteScan.alias ?? body.cteScan.table;

  const windowExpressions = new Map<string, unknown>();
  for (const fn of body.window?.functions ?? []) {
    windowExpressions.set(fn.as, buildWindowFunctionSql(fn, source as Record<string, unknown>, scanAlias));
  }

  const selection: Record<string, unknown> = {};
  if (body.project) {
    for (const rawMapping of body.project.columns) {
      const mapping = requireColumnProjectMapping(rawMapping);
      selection[mapping.output] = resolveWithBodyProjectionSource(
        mapping,
        source as Record<string, unknown>,
        windowExpressions,
        scanAlias,
      );
    }
  } else {
    for (const column of body.cteScan.select) {
      selection[column] = resolveWithBodySourceColumn(
        source as Record<string, unknown>,
        {
          alias: scanAlias,
          column,
        },
        scanAlias,
      );
    }
    for (const [name, exprSql] of windowExpressions.entries()) {
      selection[name] = exprSql;
    }
  }

  let builder = dbWithCtes.with(...cteRefs).select(selection).from(source) as DrizzleExecutableBuilder;

  const whereClauses: SQL[] = [];
  for (const clause of body.cteScan.where ?? []) {
    whereClauses.push(
      toSqlConditionFromSource(
        clause,
        resolveWithBodySourceColumn(
          source as Record<string, unknown>,
          toInlineColumnRef(clause.column),
          scanAlias,
        ),
      ),
    );
  }
  for (const filter of body.filters) {
    for (const clause of filter.where ?? []) {
      whereClauses.push(
        toSqlConditionFromSource(
          clause,
          resolveWithBodySourceColumn(
            source as Record<string, unknown>,
            toInlineColumnRef(clause.column),
            scanAlias,
          ),
        ),
      );
    }
  }

  const where = and(...whereClauses);
  if (where) {
    if (typeof builder.where !== "function") {
      throw new UnsupportedSingleQueryPlanError(
        "Drizzle query builder does not support WHERE on WITH fragments.",
      );
    }
    builder = builder.where(where) as DrizzleExecutableBuilder;
  }

  if (body.sort) {
    if (typeof builder.orderBy !== "function") {
      throw new UnsupportedSingleQueryPlanError(
        "Drizzle query builder does not support ORDER BY on WITH fragments.",
      );
    }
    const orderBy = body.sort.orderBy.map((term) => {
      const sourceColumn = windowExpressions.has(term.source.column)
        ? sql.identifier(term.source.column)
        : resolveWithBodySourceColumn(
          source as Record<string, unknown>,
          term.source,
          scanAlias,
        );
      return term.direction === "asc" ? asc(sourceColumn) : desc(sourceColumn);
    });
    if (orderBy.length > 0) {
      builder = builder.orderBy(...orderBy) as DrizzleExecutableBuilder;
    }
  }

  if (body.limitOffset?.limit != null) {
    if (typeof builder.limit !== "function") {
      throw new UnsupportedSingleQueryPlanError(
        "Drizzle query builder does not support LIMIT on WITH fragments.",
      );
    }
    builder = builder.limit(body.limitOffset.limit) as DrizzleExecutableBuilder;
  }
  if (body.limitOffset?.offset != null) {
    if (typeof builder.offset !== "function") {
      throw new UnsupportedSingleQueryPlanError(
        "Drizzle query builder does not support OFFSET on WITH fragments.",
      );
    }
    builder = builder.offset(body.limitOffset.offset) as DrizzleExecutableBuilder;
  }

  return { builder };
}

function resolveWithBodyProjectionSource(
  rawMapping: Extract<RelNode, { kind: "project" }>["columns"][number],
  source: Record<string, unknown>,
  windowExpressions: Map<string, unknown>,
  scanAlias: string,
): AnyColumn | unknown {
  const mapping = requireColumnProjectMapping(rawMapping);
  if (windowExpressions.has(mapping.source.column)) {
    return windowExpressions.get(mapping.source.column)!;
  }
  return resolveWithBodySourceColumn(source, mapping.source, scanAlias);
}

function resolveWithBodySourceColumn(
  source: Record<string, unknown>,
  ref: { alias?: string; table?: string; column: string },
  scanAlias: string,
): AnyColumn {
  const refAlias = ref.alias ?? ref.table;
  if (refAlias && refAlias !== scanAlias) {
    throw new UnsupportedSingleQueryPlanError(
      `WITH body column "${refAlias}.${ref.column}" must reference alias "${scanAlias}".`,
    );
  }
  const column = source[ref.column];
  if (!column || typeof column !== "object") {
    throw new UnsupportedSingleQueryPlanError(
      `Unknown WITH body column "${ref.column}".`,
    );
  }
  return column as AnyColumn;
}

function buildWindowFunctionSql(
  fn: Extract<RelNode, { kind: "window" }>["functions"][number],
  source: Record<string, unknown>,
  scanAlias: string,
): unknown {
  const call =
    fn.fn === "dense_rank"
      ? sql`dense_rank()`
      : fn.fn === "rank"
        ? sql`rank()`
        : sql`row_number()`;
  const partitionBy = fn.partitionBy.map((ref) => resolveWithBodySourceColumn(source, ref, scanAlias));
  const orderBy = fn.orderBy.map((term) => {
    const column = resolveWithBodySourceColumn(source, term.source, scanAlias);
    return sql`${column} ${term.direction === "asc" ? sql`asc` : sql`desc`}`;
  });
  const overParts: SQL[] = [];
  if (partitionBy.length > 0) {
    overParts.push(sql`partition by ${sql.join(partitionBy, sql`, `)}`);
  }
  if (orderBy.length > 0) {
    overParts.push(sql`order by ${sql.join(orderBy, sql`, `)}`);
  }
  return sql`${call} over (${sql.join(overParts, sql` `)})`.as(fn.as);
}

function resolveSingleQuerySortSource<TContext>(
  term: Extract<RelNode, { kind: "sort" }>["orderBy"][number],
  plan: SingleQueryPlan<TContext>,
): AnyColumn | SQL {
  const alias = term.source.alias ?? term.source.table;
  if (alias) {
    return resolveColumnRefFromAliasMap(plan.joinPlan.aliases, {
      alias,
      column: term.source.column,
    });
  }

  if (!plan.pipeline.aggregate) {
    const projected = resolveProjectedSelectionSource(term.source.column, plan);
    if (projected) {
      return projected;
    }
    return resolveColumnRefFromAliasMap(plan.joinPlan.aliases, {
      column: term.source.column,
    });
  }

  const metric = plan.pipeline.aggregate.metrics.find((entry) => entry.as === term.source.column);
  if (metric) {
    return buildAggregateMetricSql(metric, plan.joinPlan.aliases);
  }

  const groupBy = plan.pipeline.aggregate.groupBy.find((entry) => entry.column === term.source.column);
  if (groupBy) {
    return resolveColumnRefFromAliasMap(
      plan.joinPlan.aliases,
      toAliasColumnRef(groupBy.alias ?? groupBy.table, groupBy.column),
    );
  }

  throw new UnsupportedSingleQueryPlanError(
    `Unsupported ORDER BY reference "${term.source.column}" in aggregate rel fragment.`,
  );
}

function ensureJoinMethodsAvailable<TContext>(
  builder: {
    innerJoin?: unknown;
    leftJoin?: unknown;
    rightJoin?: unknown;
    fullJoin?: unknown;
  },
  joins: JoinStep<TContext>[],
): void {
  for (const join of joins) {
    if (join.joinType === "semi") {
      continue;
    }
    const methodName =
      join.joinType === "inner"
        ? "innerJoin"
        : join.joinType === "left"
          ? "leftJoin"
          : join.joinType === "right"
            ? "rightJoin"
            : "fullJoin";

    if (typeof builder[methodName] !== "function") {
      throw new UnsupportedSingleQueryPlanError(
        `Drizzle query builder does not support ${methodName} for single-query pushdown.`,
      );
    }
  }
}

function buildSingleQueryPlan<TContext>(
  rel: RelNode,
  tableConfigs: Record<string, DrizzleProviderTableConfig<TContext>>,
): SingleQueryPlan<TContext> {
  const pipeline = extractRelPipeline(rel);
  const joinPlan = buildJoinPlan(pipeline.base, tableConfigs);

  return {
    joinPlan,
    pipeline,
  };
}

function extractRelPipeline(node: RelNode): RelPipeline {
  let current = node;
  const filters: Extract<RelNode, { kind: "filter" }>[] = [];
  let project: Extract<RelNode, { kind: "project" }> | undefined;
  let aggregate: Extract<RelNode, { kind: "aggregate" }> | undefined;
  let sort: Extract<RelNode, { kind: "sort" }> | undefined;
  let limitOffset: Extract<RelNode, { kind: "limit_offset" }> | undefined;

  while (true) {
    switch (current.kind) {
      case "filter":
        filters.push(current);
        current = current.input;
        continue;
      case "project":
        if (project) {
          throw new UnsupportedSingleQueryPlanError("Multiple project nodes are not supported.");
        }
        project = current;
        current = current.input;
        continue;
      case "aggregate":
        if (aggregate) {
          throw new UnsupportedSingleQueryPlanError("Multiple aggregate nodes are not supported.");
        }
        aggregate = current;
        current = current.input;
        continue;
      case "sort":
        if (sort) {
          throw new UnsupportedSingleQueryPlanError("Multiple sort nodes are not supported.");
        }
        sort = current;
        current = current.input;
        continue;
      case "limit_offset":
        if (limitOffset) {
          throw new UnsupportedSingleQueryPlanError("Multiple limit/offset nodes are not supported.");
        }
        limitOffset = current;
        current = current.input;
        continue;
      case "scan":
      case "join":
        return {
          base: current,
          ...(project ? { project } : {}),
          ...(aggregate ? { aggregate } : {}),
          ...(sort ? { sort } : {}),
          ...(limitOffset ? { limitOffset } : {}),
          filters,
        };
      case "set_op":
      case "with":
      case "sql":
        throw new UnsupportedSingleQueryPlanError(
          `Rel node "${current.kind}" is not supported in single-query pushdown.`,
        );
    }
  }
}

function unwrapSetOpRel(node: RelNode): SetOpWrapper | null {
  let current = node;
  let project: Extract<RelNode, { kind: "project" }> | undefined;
  let sort: Extract<RelNode, { kind: "sort" }> | undefined;
  let limitOffset: Extract<RelNode, { kind: "limit_offset" }> | undefined;

  while (true) {
    switch (current.kind) {
      case "project":
        if (project) {
          return null;
        }
        project = current;
        current = current.input;
        continue;
      case "sort":
        if (sort) {
          return null;
        }
        sort = current;
        current = current.input;
        continue;
      case "limit_offset":
        if (limitOffset) {
          return null;
        }
        limitOffset = current;
        current = current.input;
        continue;
      case "set_op":
        return {
          setOp: current,
          ...(project ? { project } : {}),
          ...(sort ? { sort } : {}),
          ...(limitOffset ? { limitOffset } : {}),
        };
      default:
        return null;
    }
  }
}

function unwrapWithBodyRel(node: RelNode): WithBodyWrapper | null {
  let current = node;
  const filters: Extract<RelNode, { kind: "filter" }>[] = [];
  let project: Extract<RelNode, { kind: "project" }> | undefined;
  let sort: Extract<RelNode, { kind: "sort" }> | undefined;
  let limitOffset: Extract<RelNode, { kind: "limit_offset" }> | undefined;
  let window: Extract<RelNode, { kind: "window" }> | undefined;

  while (true) {
    switch (current.kind) {
      case "filter":
        filters.push(current);
        current = current.input;
        continue;
      case "project":
        if (project) {
          return null;
        }
        project = current;
        current = current.input;
        continue;
      case "sort":
        if (sort) {
          return null;
        }
        sort = current;
        current = current.input;
        continue;
      case "limit_offset":
        if (limitOffset) {
          return null;
        }
        limitOffset = current;
        current = current.input;
        continue;
      case "window":
        if (window) {
          return null;
        }
        window = current;
        current = current.input;
        continue;
      case "scan":
        return {
          cteScan: current,
          ...(project ? { project } : {}),
          ...(sort ? { sort } : {}),
          ...(limitOffset ? { limitOffset } : {}),
          ...(window ? { window } : {}),
          filters,
        };
      default:
        return null;
    }
  }
}

function buildJoinPlan<TContext>(
  node: RelNode,
  tableConfigs: Record<string, DrizzleProviderTableConfig<TContext>>,
): JoinPlan<TContext> {
  if (node.kind === "scan") {
    const root = createScanBinding(node, tableConfigs);
    return {
      root,
      joins: [],
      aliases: new Map([[root.alias, root]]),
    };
  }

  if (node.kind !== "join") {
    throw new UnsupportedSingleQueryPlanError(`Expected scan/join base node, received "${node.kind}".`);
  }

  const left = buildJoinPlan(node.left, tableConfigs);
  if (node.joinType === "semi") {
    const leftAlias = node.leftKey.alias ?? node.leftKey.table;
    const rightAlias = node.rightKey.alias ?? node.rightKey.table;
    if (!leftAlias) {
      throw new UnsupportedSingleQueryPlanError("Join keys must be qualified with table aliases.");
    }

    return {
      root: left.root,
      joins: [
        ...left.joins,
        {
          joinType: "semi",
          right: node.right,
          leftKey: {
            alias: leftAlias,
            column: node.leftKey.column,
          },
          rightKey: {
            ...(rightAlias ? { alias: rightAlias } : {}),
            column: node.rightKey.column,
          },
        },
      ],
      aliases: new Map(left.aliases),
    };
  }

  const right = buildJoinPlan(node.right, tableConfigs);
  if (right.joins.length > 0) {
    throw new UnsupportedSingleQueryPlanError("Only left-deep join trees are supported.");
  }

  const rightRoot = right.root;
  if (left.aliases.has(rightRoot.alias)) {
    throw new UnsupportedSingleQueryPlanError(`Duplicate alias "${rightRoot.alias}" in join tree.`);
  }

  const seenTables = new Set([...left.aliases.values()].map((binding) => binding.tableName));
  if (seenTables.has(rightRoot.tableName)) {
    throw new UnsupportedSingleQueryPlanError(
      "Joining the same physical table more than once is not supported without aliases.",
    );
  }

  const leftAlias = node.leftKey.alias ?? node.leftKey.table;
  const rightAlias = node.rightKey.alias ?? node.rightKey.table;
  if (!leftAlias || !rightAlias) {
    throw new UnsupportedSingleQueryPlanError("Join keys must be qualified with table aliases.");
  }

  const aliases = new Map(left.aliases);
  aliases.set(rightRoot.alias, rightRoot);
  for (const [alias, binding] of right.aliases.entries()) {
    aliases.set(alias, binding);
  }

  return {
    root: left.root,
    joins: [
      ...left.joins,
      {
        joinType: node.joinType,
        right: rightRoot,
        leftKey: {
          alias: leftAlias,
          column: node.leftKey.column,
        },
        rightKey: {
          alias: rightAlias,
          column: node.rightKey.column,
        },
      },
    ],
    aliases,
  };
}

function createScanBinding<TContext>(
  scan: Extract<RelNode, { kind: "scan" }>,
  tableConfigs: Record<string, DrizzleProviderTableConfig<TContext>>,
): ScanBinding<TContext> {
  const tableConfig = tableConfigs[scan.table];
  if (!tableConfig) {
    throw new UnsupportedSingleQueryPlanError(`Missing drizzle table config for "${scan.table}".`);
  }

  return {
    alias: scan.alias ?? scan.table,
    scan,
    tableName: scan.table,
    table: tableConfig.table,
    columns: resolveColumns(tableConfig, scan.table),
    tableConfig,
  };
}

function buildSingleQuerySelection<TContext>(
  plan: SingleQueryPlan<TContext>,
): Record<string, unknown> {
  const selection: Record<string, unknown> = {};

  if (plan.pipeline.aggregate) {
    const groupSources = new Map<string, AnyColumn>();
    const groupSourcesByKey = new Map<string, AnyColumn>();
    for (const groupBy of plan.pipeline.aggregate.groupBy) {
      const source = resolveColumnRefFromAliasMap(
        plan.joinPlan.aliases,
        toAliasColumnRef(groupBy.alias ?? groupBy.table, groupBy.column),
      );
      groupSources.set(groupBy.column, source);
      const keyAlias = groupBy.alias ?? groupBy.table ?? "";
      groupSourcesByKey.set(`${keyAlias}.${groupBy.column}`, source);
    }

    const metricSources = new Map<string, SQL>();
    for (const metric of plan.pipeline.aggregate.metrics) {
      metricSources.set(metric.as, buildAggregateMetricSql(metric, plan.joinPlan.aliases));
    }

    if (plan.pipeline.project) {
      for (const rawMapping of plan.pipeline.project.columns) {
        const mapping = requireColumnProjectMapping(rawMapping);
        const metricSource = metricSources.get(mapping.source.column);
        if (metricSource) {
          selection[mapping.output] = metricSource.as(mapping.output);
          continue;
        }
        const qualifiedSource = mapping.source.alias ?? mapping.source.table;
        if (qualifiedSource) {
          const groupSource = groupSourcesByKey.get(`${qualifiedSource}.${mapping.source.column}`);
          if (groupSource) {
            selection[mapping.output] = sql`${groupSource}`.as(mapping.output);
            continue;
          }
        }
        const groupSource = groupSources.get(mapping.source.column);
        if (groupSource) {
          selection[mapping.output] = sql`${groupSource}`.as(mapping.output);
          continue;
        }
        throw new UnsupportedSingleQueryPlanError(
          `Aggregate projection source "${mapping.source.column}" is not available in grouped output.`,
        );
      }
      return selection;
    }

    for (const [column, source] of groupSources.entries()) {
      selection[column] = source;
    }
    for (const [metricAlias, metricSource] of metricSources.entries()) {
      selection[metricAlias] = metricSource.as(metricAlias);
    }
    return selection;
  }

  if (plan.pipeline.project) {
    for (const rawMapping of plan.pipeline.project.columns) {
      const resolved = resolveProjectedSqlExpression(rawMapping, plan.joinPlan.aliases, true);
      selection[rawMapping.output] = isRelProjectColumnMapping(rawMapping)
        ? resolved
        : (resolved as SQL).as(rawMapping.output);
    }
    return selection;
  }

  for (const binding of plan.joinPlan.aliases.values()) {
    for (const column of binding.scan.select) {
      selection[`${binding.alias}.${column}`] = resolveColumnRefFromAliasMap(plan.joinPlan.aliases, {
        alias: binding.alias,
        column,
      });
    }
  }

  return selection;
}

function buildAggregateMetricSql<TContext>(
  metric: Extract<RelNode, { kind: "aggregate" }>["metrics"][number],
  aliases: Map<string, ScanBinding<TContext>>,
): SQL {
  if (metric.fn === "count" && !metric.column) {
    return sql`count(*)`;
  }

  if (!metric.column) {
    throw new UnsupportedSingleQueryPlanError(`Aggregate ${metric.fn} requires a column.`);
  }

  const source = resolveColumnRefFromAliasMap(aliases, {
    ...toAliasColumnRef(metric.column.alias ?? metric.column.table, metric.column.column),
  });

  switch (metric.fn) {
    case "count":
      return metric.distinct ? sql`count(distinct ${source})` : sql`count(${source})`;
    case "sum":
      return metric.distinct ? sql`sum(distinct ${source})` : sql`sum(${source})`;
    case "avg":
      return metric.distinct ? sql`avg(distinct ${source})` : sql`avg(${source})`;
    case "min":
      return sql`min(${source})`;
    case "max":
      return sql`max(${source})`;
  }
}

function resolveProjectedSelectionSource<TContext>(
  output: string,
  plan: SingleQueryPlan<TContext>,
): SQL | AnyColumn | null {
  const mapping = plan.pipeline.project?.columns.find((column) => column.output === output);
  if (!mapping) {
    return null;
  }

  return resolveProjectedSqlExpression(mapping, plan.joinPlan.aliases, false);
}

function resolveProjectedSqlExpression<TContext>(
  mapping: Extract<RelNode, { kind: "project" }>["columns"][number],
  aliases: Map<string, ScanBinding<TContext>>,
  allowSourceOnly: boolean,
): SQL | AnyColumn {
  if (isRelProjectColumnMapping(mapping)) {
    const source = resolveColumnRefFromAliasMap(
      aliases,
      toAliasColumnRef(mapping.source.alias ?? mapping.source.table, mapping.source.column),
    );
    return allowSourceOnly ? source : sql`${source}`;
  }

  return buildSqlExpressionFromRelExpr(mapping.expr, aliases);
}

function buildSqlExpressionFromRelExpr<TContext>(
  expr: RelExpr,
  aliases: Map<string, ScanBinding<TContext>>,
): SQL | AnyColumn {
  switch (expr.kind) {
    case "literal":
      return sql`${expr.value}`;
    case "column":
      return resolveColumnRefFromAliasMap(
        aliases,
        toAliasColumnRef(expr.ref.alias ?? expr.ref.table, expr.ref.column),
      );
    case "function": {
      const args = expr.args.map((arg) => buildSqlExpressionFromRelExpr(arg, aliases));
      switch (expr.name) {
        case "eq":
          return sql`${args[0]} = ${args[1]}`;
        case "neq":
          return sql`${args[0]} <> ${args[1]}`;
        case "gt":
          return sql`${args[0]} > ${args[1]}`;
        case "gte":
          return sql`${args[0]} >= ${args[1]}`;
        case "lt":
          return sql`${args[0]} < ${args[1]}`;
        case "lte":
          return sql`${args[0]} <= ${args[1]}`;
        case "add":
          return sql`(${args[0]} + ${args[1]})`;
        case "subtract":
          return sql`(${args[0]} - ${args[1]})`;
        case "multiply":
          return sql`(${args[0]} * ${args[1]})`;
        case "divide":
          return sql`(${args[0]} / ${args[1]})`;
        case "and":
          return sql`(${sql.join(args.map((arg) => sql`${arg}`), sql` and `)})`;
        case "or":
          return sql`(${sql.join(args.map((arg) => sql`${arg}`), sql` or `)})`;
        case "not":
          return sql`not (${args[0]})`;
        default:
          throw new UnsupportedSingleQueryPlanError(
            `Unsupported computed projection function "${expr.name}" in Drizzle single-query pushdown.`,
          );
      }
    }
  }
}

function resolveFilterSource<TContext>(
  column: string,
  plan: SingleQueryPlan<TContext>,
): AnyColumn | SQL {
  if (!plan.pipeline.aggregate) {
    const projected = resolveProjectedSelectionSource(column, plan);
    if (projected) {
      return projected;
    }
  }

  return resolveColumnRefFromFilterColumn(plan.joinPlan.aliases, column);
}

function toSqlConditionFromRelFilterClause<TContext>(
  clause: ScanFilterClause,
  plan: SingleQueryPlan<TContext>,
): SQL {
  const source = resolveFilterSource(clause.column, plan);
  return toSqlConditionFromSource(clause, source);
}

function toSqlConditionFromSource(
  clause: ScanFilterClause,
  source: AnyColumn | SQL,
): SQL {
  switch (clause.op) {
    case "eq":
      return sql`${source} = ${clause.value as never}`;
    case "neq":
      return sql`${source} <> ${clause.value as never}`;
    case "gt":
      return sql`${source} > ${clause.value as never}`;
    case "gte":
      return sql`${source} >= ${clause.value as never}`;
    case "lt":
      return sql`${source} < ${clause.value as never}`;
    case "lte":
      return sql`${source} <= ${clause.value as never}`;
    case "in": {
      const values = clause.values.filter((value) => value != null);
      if (values.length === 0) {
        return impossibleCondition();
      }
      return sql`${source} in ${values as never[]}`;
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
      return sql`${source} is null`;
    case "is_not_null":
      return sql`${source} is not null`;
  }
}

function resolveColumnRefFromFilterColumn<TContext>(
  aliases: Map<string, ScanBinding<TContext>>,
  column: string,
): AnyColumn {
  const idx = column.lastIndexOf(".");
  if (idx > 0) {
    const alias = column.slice(0, idx);
    const name = column.slice(idx + 1);
    return resolveColumnRefFromAliasMap(aliases, { alias, column: name });
  }

  return resolveColumnRefFromAliasMap(aliases, { column });
}

function toInlineColumnRef(column: string): { alias?: string; column: string } {
  const idx = column.lastIndexOf(".");
  if (idx > 0) {
    return {
      alias: column.slice(0, idx),
      column: column.slice(idx + 1),
    };
  }
  return { column };
}

function resolveColumnRefFromAliasMap<TContext>(
  aliases: Map<string, ScanBinding<TContext>>,
  ref: { alias?: string; column: string },
): AnyColumn {
  if (ref.alias) {
    const binding = aliases.get(ref.alias);
    if (!binding) {
      throw new UnsupportedSingleQueryPlanError(`Unknown alias "${ref.alias}" in rel fragment.`);
    }
    const source = binding.columns[ref.column];
    if (!source) {
      throw new UnsupportedSingleQueryPlanError(
        `Unknown column "${ref.column}" on alias "${ref.alias}" in rel fragment.`,
      );
    }
    return source;
  }

  let matched: AnyColumn | null = null;
  for (const binding of aliases.values()) {
    const source = binding.columns[ref.column];
    if (!source) {
      continue;
    }
    if (matched && matched !== source) {
      throw new UnsupportedSingleQueryPlanError(
        `Ambiguous unqualified column "${ref.column}" in rel fragment.`,
      );
    }
    matched = source;
  }

  if (!matched) {
    throw new UnsupportedSingleQueryPlanError(
      `Unknown unqualified column "${ref.column}" in rel fragment.`,
    );
  }

  return matched;
}

function toAliasColumnRef(
  alias: string | undefined,
  column: string,
): { alias?: string; column: string } {
  return alias ? { alias, column } : { column };
}

interface DrizzleRelExecutionContext<TContext> {
  options: CreateDrizzleProviderOptions<TContext>;
  db: DrizzleQueryExecutor;
  tableConfigs: Record<string, DrizzleProviderTableConfig<TContext>>;
  context: TContext;
  cteRows: Map<string, QueryRow[]>;
}

// oxlint-disable-next-line no-unused-vars
async function executeDrizzleRel<TContext>(
  rel: RelNode,
  options: CreateDrizzleProviderOptions<TContext>,
  context: TContext,
): Promise<QueryRow[]> {
  const db = await resolveDrizzleDb(options, context);
  const executionContext: DrizzleRelExecutionContext<TContext> = {
    options,
    db,
    tableConfigs: options.tables as Record<string, DrizzleProviderTableConfig<TContext>>,
    context,
    cteRows: new Map<string, QueryRow[]>(),
  };

  return executeDrizzleRelNode(rel, executionContext);
}

async function executeDrizzleRelNode<TContext>(
  node: RelNode,
  context: DrizzleRelExecutionContext<TContext>,
): Promise<QueryRow[]> {
  switch (node.kind) {
    case "scan":
      return executeDrizzleRelScan(node, context);
    case "filter":
      return executeDrizzleRelFilter(node, context);
    case "project":
      return executeDrizzleRelProject(node, context);
    case "join":
      return executeDrizzleRelJoin(node, context);
    case "aggregate":
      return executeDrizzleRelAggregate(node, context);
    case "window":
      throw new Error("Drizzle adapter local rel runtime does not support window nodes.");
    case "sort":
      return executeDrizzleRelSort(node, context);
    case "limit_offset":
      return executeDrizzleRelLimitOffset(node, context);
    case "set_op":
      return executeDrizzleRelSetOp(node, context);
    case "with":
      return executeDrizzleRelWith(node, context);
    case "sql":
      throw new Error("Drizzle adapter does not execute rel sql nodes.");
  }
}

async function executeDrizzleRelScan<TContext>(
  scan: Extract<RelNode, { kind: "scan" }>,
  context: DrizzleRelExecutionContext<TContext>,
): Promise<InternalRow[]> {
  const alias = scan.alias ?? scan.table;

  const cteRows = context.cteRows.get(scan.table);
  if (cteRows) {
    const scanned = scanLocalRows(cteRows, {
      table: scan.table,
      ...(scan.alias ? { alias: scan.alias } : {}),
      select: scan.select,
      ...(scan.where ? { where: scan.where } : {}),
      ...(scan.orderBy ? { orderBy: scan.orderBy } : {}),
      ...(scan.limit != null ? { limit: scan.limit } : {}),
      ...(scan.offset != null ? { offset: scan.offset } : {}),
    });
    return scanned.map((row) => prefixRow(row, alias));
  }

  const tableConfig = context.tableConfigs[scan.table];
  if (!tableConfig) {
    throw new Error(`Unknown drizzle table config: ${scan.table}`);
  }

  const scope = tableConfig.scope ? await tableConfig.scope(context.context) : undefined;
  const rows = await runDrizzleScan({
    db: context.db,
    tableName: scan.table,
    table: tableConfig.table,
    columns: resolveColumns(tableConfig, scan.table),
    request: {
      table: scan.table,
      ...(scan.alias ? { alias: scan.alias } : {}),
      select: scan.select,
      ...(scan.where ? { where: scan.where } : {}),
      ...(scan.orderBy ? { orderBy: scan.orderBy } : {}),
      ...(scan.limit != null ? { limit: scan.limit } : {}),
      ...(scan.offset != null ? { offset: scan.offset } : {}),
    },
    ...(scope ? { scope } : {}),
  });

  return rows.map((row) => prefixRow(row, alias));
}

async function executeDrizzleRelFilter<TContext>(
  filter: Extract<RelNode, { kind: "filter" }>,
  context: DrizzleRelExecutionContext<TContext>,
): Promise<QueryRow[]> {
  const rows = (await executeDrizzleRelNode(filter.input, context)) as InternalRow[];
  let out = [...rows];

  for (const clause of filter.where ?? []) {
    out = out.filter((row) => matchesClause(row, clause));
  }

  return out;
}

async function executeDrizzleRelProject<TContext>(
  project: Extract<RelNode, { kind: "project" }>,
  context: DrizzleRelExecutionContext<TContext>,
): Promise<QueryRow[]> {
  const rows = (await executeDrizzleRelNode(project.input, context)) as InternalRow[];

  return rows.map((row) => {
    const out: QueryRow = {};
    for (const rawMapping of project.columns) {
      if (isRelProjectColumnMapping(rawMapping)) {
        out[rawMapping.output] = readRowValue(row, toColumnKey(rawMapping.source)) ?? null;
        continue;
      }
      out[rawMapping.output] = evaluateRelExpr(rawMapping.expr, row);
    }
    return out;
  });
}

async function executeDrizzleRelJoin<TContext>(
  join: Extract<RelNode, { kind: "join" }>,
  context: DrizzleRelExecutionContext<TContext>,
): Promise<QueryRow[]> {
  const leftRows = (await executeDrizzleRelNode(join.left, context)) as InternalRow[];
  const rightRows = (await executeDrizzleRelNode(join.right, context)) as InternalRow[];
  return applyLocalHashJoin(join, leftRows, rightRows);
}

function applyLocalHashJoin(
  join: Extract<RelNode, { kind: "join" }>,
  leftRows: InternalRow[],
  rightRows: InternalRow[],
): InternalRow[] {
  const leftKey = `${join.leftKey.alias}.${join.leftKey.column}`;
  const rightKey = `${join.rightKey.alias}.${join.rightKey.column}`;

  const rightIndex = new Map<unknown, InternalRow[]>();
  rightRows.forEach((row) => {
    const key = row[rightKey];
    if (key == null) {
      return;
    }

    const bucket = rightIndex.get(key) ?? [];
    bucket.push(row);
    rightIndex.set(key, bucket);
  });

  const joined: InternalRow[] = [];
  const matchedRightRows = new Set<InternalRow>();

  for (const leftRow of leftRows) {
    const key = leftRow[leftKey];
    const matches = key == null ? [] : (rightIndex.get(key) ?? []);

    if (matches.length === 0) {
      if (join.joinType === "left" || join.joinType === "full") {
        joined.push({ ...leftRow });
      }
      continue;
    }

    for (const match of matches) {
      matchedRightRows.add(match);
      joined.push({
        ...leftRow,
        ...match,
      });
    }
  }

  if (join.joinType === "right" || join.joinType === "full") {
    for (const rightRow of rightRows) {
      if (matchedRightRows.has(rightRow)) {
        continue;
      }
      joined.push({ ...rightRow });
    }
  }

  return joined;
}

async function executeDrizzleRelAggregate<TContext>(
  aggregate: Extract<RelNode, { kind: "aggregate" }>,
  context: DrizzleRelExecutionContext<TContext>,
): Promise<QueryRow[]> {
  const rows = (await executeDrizzleRelNode(aggregate.input, context)) as InternalRow[];
  const groups = new Map<string, InternalRow[]>();

  for (const row of rows) {
    const key = JSON.stringify(
      aggregate.groupBy.map((ref) => readRowValue(row, toColumnKey(ref)) ?? null),
    );
    const bucket = groups.get(key) ?? [];
    bucket.push(row);
    groups.set(key, bucket);
  }

  if (groups.size === 0 && aggregate.groupBy.length === 0) {
    groups.set("__all__", []);
  }

  const out: QueryRow[] = [];

  for (const [groupKey, bucket] of groups.entries()) {
    const row: QueryRow = {};

    if (aggregate.groupBy.length > 0) {
      const values = JSON.parse(groupKey) as unknown[];
      aggregate.groupBy.forEach((ref, index) => {
        row[ref.column] = values[index] ?? null;
      });
    }

    for (const metric of aggregate.metrics) {
      const values = metric.column
        ? bucket.map((entry) => readRowValue(entry, toColumnKey(metric.column!)) ?? null)
        : bucket.map(() => 1);
      const metricValues = metric.distinct
        ? [...new Map(values.map((value) => [JSON.stringify(value), value])).values()]
        : values;

      row[metric.as] = evaluateAggregateMetric(metric.fn, metricValues, bucket.length, metric.column != null);
    }

    out.push(row);
  }

  return out;
}

async function executeDrizzleRelSort<TContext>(
  sort: Extract<RelNode, { kind: "sort" }>,
  context: DrizzleRelExecutionContext<TContext>,
): Promise<InternalRow[]> {
  const rows = (await executeDrizzleRelNode(sort.input, context)) as InternalRow[];
  const sorted = [...rows];

  sorted.sort((left, right) => {
    for (const term of sort.orderBy) {
      const comparison = compareNullableValues(
        readRowValue(left, toColumnKey(term.source)),
        readRowValue(right, toColumnKey(term.source)),
      );
      if (comparison !== 0) {
        return term.direction === "asc" ? comparison : -comparison;
      }
    }
    return 0;
  });

  return sorted;
}

async function executeDrizzleRelLimitOffset<TContext>(
  limitOffset: Extract<RelNode, { kind: "limit_offset" }>,
  context: DrizzleRelExecutionContext<TContext>,
): Promise<QueryRow[]> {
  let rows = await executeDrizzleRelNode(limitOffset.input, context);

  if (limitOffset.offset != null) {
    rows = rows.slice(limitOffset.offset);
  }

  if (limitOffset.limit != null) {
    rows = rows.slice(0, limitOffset.limit);
  }

  return rows;
}

async function executeDrizzleRelSetOp<TContext>(
  setOp: Extract<RelNode, { kind: "set_op" }>,
  context: DrizzleRelExecutionContext<TContext>,
): Promise<QueryRow[]> {
  const leftRows = await executeDrizzleRelNode(setOp.left, context);
  const rightRows = await executeDrizzleRelNode(setOp.right, context);

  switch (setOp.op) {
    case "union_all":
      return [...leftRows, ...rightRows];
    case "union":
      return dedupeRows([...leftRows, ...rightRows]);
    case "intersect": {
      const rightKeys = new Set(rightRows.map((row) => stableRowKey(row)));
      return dedupeRows(leftRows.filter((row) => rightKeys.has(stableRowKey(row))));
    }
    case "except": {
      const rightKeys = new Set(rightRows.map((row) => stableRowKey(row)));
      return dedupeRows(leftRows.filter((row) => !rightKeys.has(stableRowKey(row))));
    }
  }
}

async function executeDrizzleRelWith<TContext>(
  withNode: Extract<RelNode, { kind: "with" }>,
  context: DrizzleRelExecutionContext<TContext>,
): Promise<QueryRow[]> {
  const cteRows = new Map(context.cteRows);
  const nested: DrizzleRelExecutionContext<TContext> = {
    ...context,
    cteRows,
  };

  for (const cte of withNode.ctes) {
    const rows = await executeDrizzleRelNode(cte.query, nested);
    cteRows.set(cte.name, rows);
  }

  return executeDrizzleRelNode(withNode.body, nested);
}

function scanLocalRows(rows: QueryRow[], request: TableScanRequest): QueryRow[] {
  let out = [...rows];
  for (const clause of request.where ?? []) {
    out = out.filter((row) => matchesClause(row, clause));
  }

  if (request.orderBy && request.orderBy.length > 0) {
    out = [...out].sort((left, right) => {
      for (const term of request.orderBy ?? []) {
        const comparison = compareNullableValues(readRowValue(left, term.column), readRowValue(right, term.column));
        if (comparison !== 0) {
          return term.direction === "asc" ? comparison : -comparison;
        }
      }
      return 0;
    });
  }

  if (request.offset != null) {
    out = out.slice(request.offset);
  }

  if (request.limit != null) {
    out = out.slice(0, request.limit);
  }

  return out.map((row) => {
    const projected: QueryRow = {};
    for (const column of request.select) {
      projected[column] = readRowValue(row, column) ?? null;
    }
    return projected;
  });
}

function prefixRow(row: QueryRow, alias: string): InternalRow {
  const out: InternalRow = {};
  for (const [column, value] of Object.entries(row)) {
    out[`${alias}.${column}`] = value;
  }
  return out;
}

function evaluateRelExpr(expr: RelExpr, row: Record<string, unknown>): unknown {
  switch (expr.kind) {
    case "literal":
      return expr.value;
    case "column":
      return readRowValue(row, toColumnKey(expr.ref));
    case "function": {
      const args = expr.args.map((arg) => evaluateRelExpr(arg, row));
      switch (expr.name) {
        case "eq":
          return args[0] != null && args[0] === args[1];
        case "neq":
          return args[0] != null && args[0] !== args[1];
        case "gt":
          return args[0] != null && args[1] != null && compareNonNull(args[0], args[1]) > 0;
        case "gte":
          return args[0] != null && args[1] != null && compareNonNull(args[0], args[1]) >= 0;
        case "lt":
          return args[0] != null && args[1] != null && compareNonNull(args[0], args[1]) < 0;
        case "lte":
          return args[0] != null && args[1] != null && compareNonNull(args[0], args[1]) <= 0;
        case "add":
          return toFiniteNumber(args[0], "SUM") + toFiniteNumber(args[1], "SUM");
        case "subtract":
          return toFiniteNumber(args[0], "SUM") - toFiniteNumber(args[1], "SUM");
        case "multiply":
          return toFiniteNumber(args[0], "SUM") * toFiniteNumber(args[1], "SUM");
        case "divide":
          return toFiniteNumber(args[0], "SUM") / toFiniteNumber(args[1], "SUM");
        case "and":
          return args.every(Boolean);
        case "or":
          return args.some(Boolean);
        case "not":
          return !args[0];
        default:
          throw new Error(`Unsupported rel expression function: ${expr.name}`);
      }
    }
  }
}

function matchesClause(row: Record<string, unknown>, clause: ScanFilterClause): boolean {
  const value = readRowValue(row, clause.column);

  switch (clause.op) {
    case "eq":
      return value != null && value === clause.value;
    case "neq":
      return value != null && value !== clause.value;
    case "gt":
      return value != null && clause.value != null && compareNonNull(value, clause.value) > 0;
    case "gte":
      return value != null && clause.value != null && compareNonNull(value, clause.value) >= 0;
    case "lt":
      return value != null && clause.value != null && compareNonNull(value, clause.value) < 0;
    case "lte":
      return value != null && clause.value != null && compareNonNull(value, clause.value) <= 0;
    case "in": {
      const set = new Set(clause.values.filter((entry) => entry != null));
      return value != null && set.has(value);
    }
    case "not_in": {
      const set = new Set(clause.values.filter((entry) => entry != null));
      return value != null && !set.has(value);
    }
    case "like":
      return typeof value === "string" && typeof clause.value === "string"
        ? matchesLikePattern(value, clause.value)
        : false;
    case "not_like":
      return typeof value === "string" && typeof clause.value === "string"
        ? !matchesLikePattern(value, clause.value)
        : false;
    case "is_distinct_from":
      return value !== clause.value;
    case "is_not_distinct_from":
      return value === clause.value;
    case "is_null":
      return value == null;
    case "is_not_null":
      return value != null;
    default:
      return false;
  }
}

function matchesLikePattern(value: string, pattern: string): boolean {
  const escaped = pattern
    .replace(/[.*+?^${}()|[\]\\]/g, "\\$&")
    .replace(/%/g, ".*")
    .replace(/_/g, ".");
  return new RegExp(`^${escaped}$`, "su").test(value);
}

function evaluateAggregateMetric(
  fn: "count" | "sum" | "avg" | "min" | "max",
  values: unknown[],
  bucketSize: number,
  hasColumn: boolean,
): unknown {
  switch (fn) {
    case "count":
      return hasColumn ? values.filter((value) => value != null).length : bucketSize;
    case "sum": {
      const numeric = values.filter((value) => value != null).map((value) => toFiniteNumber(value, "SUM"));
      return numeric.length > 0 ? numeric.reduce((sum, value) => sum + value, 0) : null;
    }
    case "avg": {
      const numeric = values.filter((value) => value != null).map((value) => toFiniteNumber(value, "AVG"));
      return numeric.length > 0
        ? numeric.reduce((sum, value) => sum + value, 0) / numeric.length
        : null;
    }
    case "min": {
      const candidates = values.filter((value) => value != null);
      return candidates.length > 0
        ? candidates.reduce((left, right) =>
            compareNullableValues(left, right) <= 0 ? left : right,
          )
        : null;
    }
    case "max": {
      const candidates = values.filter((value) => value != null);
      return candidates.length > 0
        ? candidates.reduce((left, right) =>
            compareNullableValues(left, right) >= 0 ? left : right,
          )
        : null;
    }
  }
}

function dedupeRows(rows: QueryRow[]): QueryRow[] {
  const byKey = new Map<string, QueryRow>();
  for (const row of rows) {
    byKey.set(stableRowKey(row), row);
  }
  return [...byKey.values()];
}

function stableRowKey(row: QueryRow): string {
  const entries = Object.entries(row).sort(([left], [right]) => left.localeCompare(right));
  return JSON.stringify(entries);
}

function readRowValue(row: Record<string, unknown>, column: string): unknown {
  if (column in row) {
    return row[column];
  }

  const suffix = `.${column}`;
  const candidates = Object.entries(row).filter(([key]) => key.endsWith(suffix));
  if (candidates.length === 1) {
    return candidates[0]?.[1];
  }

  return undefined;
}

function toColumnKey(ref: { alias?: string; table?: string; column: string }): string {
  const prefix = ref.alias ?? ref.table;
  return prefix ? `${prefix}.${ref.column}` : ref.column;
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

  if (typeof left === "number" && typeof right === "number") {
    return left < right ? -1 : 1;
  }

  if (typeof left === "boolean" && typeof right === "boolean") {
    return Number(left) < Number(right) ? -1 : 1;
  }

  const leftString = String(left);
  const rightString = String(right);
  return leftString < rightString ? -1 : 1;
}

function compareNonNull(left: unknown, right: unknown): number {
  if (typeof left === "number" && typeof right === "number") {
    return left === right ? 0 : left < right ? -1 : 1;
  }

  if (typeof left === "boolean" && typeof right === "boolean") {
    const leftNumber = Number(left);
    const rightNumber = Number(right);
    return leftNumber === rightNumber ? 0 : leftNumber < rightNumber ? -1 : 1;
  }

  const leftString = String(left);
  const rightString = String(right);
  if (leftString === rightString) {
    return 0;
  }
  return leftString < rightString ? -1 : 1;
}

function toFiniteNumber(value: unknown, functionName: "SUM" | "AVG"): number {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    throw new Error(`${functionName} expects numeric values.`);
  }
  return parsed;
}
