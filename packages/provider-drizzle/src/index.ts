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
  AdapterResult,
  bindAdapterEntities,
  collectCapabilityAtomsForFragment,
  createDataEntityHandle,
  inferRouteFamilyForFragment,
  normalizeDataEntityShape,
  type DataEntityColumnMetadata,
  type DataEntityShape,
  type DataEntityHandle,
  type DataEntityReadMetadataMap,
  type FragmentProviderAdapter,
  type InferDataEntityShapeMetadata,
  type LookupProviderAdapter,
  type MaybePromise,
  type ProviderCapabilityAtom,
  type ProviderCapabilityReport,
  type ProviderCompiledPlan,
  type ProviderFragment,
  type ProviderLookupManyRequest,
  type ProviderRuntimeBinding,
} from "@tupl/core/provider";
import { isRelProjectColumnMapping, type RelExpr, type RelNode } from "@tupl/core/model/rel";
import {
  UnsupportedRelationalPlanError,
  canCompileBasicRel,
  canCompileSetOpRel,
  canCompileWithRel,
  extractRelPipeline,
  hasSqlNode,
  isSupportedRelationalPlan,
  resolveRelationalStrategy,
  unwrapSetOpRel,
  unwrapWithBodyRel,
  type RelationalJoinPlan,
  type RelationalJoinStep,
  type RelationalScanBindingBase,
  type RelationalSemiJoinStep,
  type RelationalSingleQueryPlan,
} from "@tupl/core/provider/shapes";
import type {
  QueryRow,
  ScanFilterClause,
  ScanOrderBy,
  SqlScalarType,
  TableScanRequest,
} from "@tupl/core/schema";

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

type InferDrizzleEntityRow<TConfig> =
  TConfig extends DrizzleProviderTableConfig<any, infer TTable, any>
    ? TTable extends Table
      ? InferSelectModel<TTable>
      : Record<string, unknown>
    : Record<string, unknown>;

type InferDrizzleTableColumns<TConfig> =
  TConfig extends DrizzleProviderTableConfig<any, any, infer TColumn>
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
            : TColumnType extends
                  | `${string}Real${string}`
                  | `${string}Double${string}`
                  | `${string}Float${string}`
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

type InferDrizzleColumnTuplType<TColumn> = TColumn extends {
  _: {
    columnType: infer TColumnType extends string;
    dataType: infer TDataType extends string;
  };
}
  ? InferDrizzleScalarTypeFromColumnMetadata<TColumnType, TDataType>
  : never;

type InferDrizzleEntityColumnMetadataFromColumns<TColumns extends Record<string, unknown>> = {
  [K in Extract<keyof TColumns, string>]: DataEntityColumnMetadata<
    InferDrizzleColumnRead<TColumns[K]>
  > & {
    source: K;
  } & ([InferDrizzleColumnTuplType<TColumns[K]>] extends [never]
      ? {}
      : {
          type: InferDrizzleColumnTuplType<TColumns[K]>;
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
): FragmentProviderAdapter<TContext> &
  LookupProviderAdapter<TContext> & {
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
          return AdapterResult.ok({
            provider: providerName,
            kind: fragment.kind,
            payload: fragment,
          });
        case "rel": {
          const strategy = resolveDrizzleRelCompileStrategy(fragment.rel, tableConfigs);
          if (!strategy) {
            return AdapterResult.err(
              new Error("Unsupported relational fragment for drizzle provider."),
            );
          }
          const db = await resolveDrizzleDb(options, context);
          if (!isStrategyAvailableOnDrizzleDb(strategy, db)) {
            return AdapterResult.err(
              new Error(
                `Drizzle database instance does not support required APIs for "${strategy}" rel pushdown.`,
              ),
            );
          }

          return AdapterResult.ok({
            provider: providerName,
            kind: fragment.kind,
            payload: {
              strategy,
              rel: fragment.rel,
            } satisfies DrizzleRelCompiledPlan,
          });
        }
        default:
          const fragmentKind = formatUnknownValue((fragment as { kind?: unknown }).kind);
          return AdapterResult.err(new Error(`Unsupported drizzle fragment kind: ${fragmentKind}`));
      }
    },
    async execute(plan, context) {
      return AdapterResult.tryPromise({
        try: () => executeDrizzlePlan(plan, options, context),
        catch: (error) => (error instanceof Error ? error : new Error(String(error))),
      });
    },
    async lookupMany(request, context) {
      return AdapterResult.tryPromise({
        try: () => lookupManyWithDrizzle(options, request, context),
        catch: (error) => (error instanceof Error ? error : new Error(String(error))),
      });
    },
  } satisfies FragmentProviderAdapter<TContext> &
    LookupProviderAdapter<TContext> & {
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
      ? normalizeDataEntityShape(
          tableConfig.shape as DataEntityShape<InferDrizzleTableColumns<TTables[typeof tableName]>>,
        )
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

  const sessionName = (db as unknown as { _: { session?: { constructor?: { name?: unknown } } } })._
    ?.session?.constructor?.name;
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

function formatUnknownValue(value: unknown): string {
  if (value == null) {
    return "";
  }
  if (typeof value === "string") {
    return value;
  }
  if (typeof value === "number" || typeof value === "boolean" || typeof value === "bigint") {
    return value.toString();
  }

  try {
    return JSON.stringify(value) ?? Object.prototype.toString.call(value);
  } catch {
    return Object.prototype.toString.call(value);
  }
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
    const inferredType = inferTuplTypeFromDrizzleColumn(column);
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

function inferTuplTypeFromDrizzleColumn(column: AnyColumn): SqlScalarType | undefined {
  const dataTypeValue = (column as { dataType?: unknown }).dataType;
  const dataType = typeof dataTypeValue === "string" ? dataTypeValue.toLowerCase() : "";
  const sqlType = typeof column.getSQLType === "function" ? column.getSQLType().toLowerCase() : "";
  const normalizedSqlType = sqlType.replace(/\s+/g, " ");

  if (dataType === "boolean" || sqlType === "boolean") {
    return "boolean";
  }
  if (dataType === "json" || normalizedSqlType.includes("json")) {
    return "json";
  }
  if (
    dataType === "arraybuffer" ||
    normalizedSqlType.includes("blob") ||
    normalizedSqlType.includes("bytea")
  ) {
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
    !!value && typeof value === "object" && typeof (value as { name?: unknown }).name === "string"
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
  return resolveRelationalStrategy(node, {
    basicStrategy: "basic",
    setOpStrategy: "set_op",
    withStrategy: "with",
    canCompileBasic: (current) => canCompileBasicRel(current, (table) => !!tableConfigs[table]),
    validateBasic: (current) =>
      isSupportedRelationalPlan(() => {
        buildSingleQueryPlan(current, tableConfigs);
      }),
    canCompileSetOp: (current) =>
      canCompileSetOpRel(
        current,
        (branch) => (canCompileBasicRel(branch, (table) => !!tableConfigs[table]) ? "basic" : null),
        requireColumnProjectMapping,
      ),
    canCompileWith: (current) =>
      canCompileWithRel(current, (branch) =>
        resolveDrizzleRelCompileStrategy(branch, tableConfigs),
      ),
  });
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

class UnsupportedSingleQueryPlanError extends UnsupportedRelationalPlanError {}

interface ScanBinding<TContext> extends RelationalScanBindingBase {
  alias: string;
  scan: Extract<RelNode, { kind: "scan" }>;
  tableName: string;
  table: object;
  scanColumns: DrizzleColumnMap<string>;
  columns: Record<string, AnyColumn | SQL>;
  outputColumns: string[];
  tableConfig: DrizzleProviderTableConfig<TContext>;
}

type SemiJoinStep = RelationalSemiJoinStep;
type JoinStep<TContext> = RelationalJoinStep<ScanBinding<TContext>>;
type JoinPlan<TContext> = RelationalJoinPlan<ScanBinding<TContext>>;

interface QualifiedJoinColumnRef {
  alias: string;
  column: string;
}

type SingleQueryPlan<TContext> = RelationalSingleQueryPlan<ScanBinding<TContext>>;

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
      const leftColumn = resolveJoinKeyColumnRefFromAliasMap(plan.joinPlan.aliases, {
        alias: joinStep.leftKey.alias,
        column: joinStep.leftKey.column,
      });
      const { subquery } = await buildSemiJoinSubquery(joinStep, options, context, db);
      whereClauses.push(sql`${leftColumn} in (${asDrizzleSubquerySql(subquery)})`);
      continue;
    }
    const leftColumn = resolveJoinKeyColumnRefFromAliasMap(plan.joinPlan.aliases, {
      alias: joinStep.leftKey.alias,
      column: joinStep.leftKey.column,
    });
    const rightColumn = resolveJoinKeyColumnRefFromAliasMap(plan.joinPlan.aliases, {
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
    whereClauses.push(
      ...normalizeScope(
        binding.tableConfig.scope ? await binding.tableConfig.scope(context) : undefined,
      ),
    );
    for (const clause of binding.scan.where ?? []) {
      whereClauses.push(toSqlCondition(clause, binding.scanColumns, binding.tableName));
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
      ),
    );
    builder = builder.groupBy(...(groupByColumns as AnyColumn[])) as typeof builder;
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
  const maybe = subquery as {
    getSQL?: unknown;
    toSQL?: unknown;
    execute?: unknown;
    then?: unknown;
  };
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
  return {
    subquery: (await buildDrizzleRelBuilderForStrategy(joinStep.right, options, context, db))
      .builder,
  };
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

  const left = (await buildDrizzleRelBuilderForStrategy(wrapper.setOp.left, options, context, db))
    .builder;
  const right = (await buildDrizzleRelBuilderForStrategy(wrapper.setOp.right, options, context, db))
    .builder;
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
      if (
        (mapping.source.alias || mapping.source.table) &&
        mapping.source.column !== mapping.output
      ) {
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
    const query = (await buildDrizzleRelBuilderForStrategy(cte.query, options, context, db))
      .builder;
    const cteRef = dbWithCtes.$with(cte.name).as(query);
    cteBindings.set(cte.name, cteRef);
    cteRefs.push(cteRef);
  }

  const body = unwrapWithBodyRel(rel.body);
  if (!body) {
    throw new UnsupportedSingleQueryPlanError(
      "Unsupported WITH body shape for single-query pushdown.",
    );
  }
  const source = cteBindings.get(body.cteScan.table);
  if (!source) {
    throw new UnsupportedSingleQueryPlanError(`Unknown CTE "${body.cteScan.table}" in WITH body.`);
  }
  const scanAlias = body.cteScan.alias ?? body.cteScan.table;

  const windowExpressions = new Map<string, unknown>();
  for (const fn of body.window?.functions ?? []) {
    windowExpressions.set(
      fn.as,
      buildWindowFunctionSql(fn, source as Record<string, unknown>, scanAlias),
    );
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

  let builder = dbWithCtes
    .with(...cteRefs)
    .select(selection)
    .from(source) as DrizzleExecutableBuilder;

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
        : resolveWithBodySourceColumn(source as Record<string, unknown>, term.source, scanAlias);
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
): unknown {
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
    throw new UnsupportedSingleQueryPlanError(`Unknown WITH body column "${ref.column}".`);
  }
  return column as AnyColumn;
}

function buildWindowFunctionSql(
  fn: Extract<RelNode, { kind: "window" }>["functions"][number],
  source: Record<string, unknown>,
  scanAlias: string,
): unknown {
  const call =
    fn.fn === "dense_rank" ? sql`dense_rank()` : fn.fn === "rank" ? sql`rank()` : sql`row_number()`;
  const partitionBy = fn.partitionBy.map((ref) =>
    resolveWithBodySourceColumn(source, ref, scanAlias),
  );
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

  const groupBy = plan.pipeline.aggregate.groupBy.find(
    (entry) => entry.column === term.source.column,
  );
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

  if (node.kind === "project") {
    const root = createProjectedScanBinding(node, tableConfigs);
    return {
      root,
      joins: [],
      aliases: new Map([[root.alias, root]]),
    };
  }

  if (node.kind !== "join") {
    throw new UnsupportedSingleQueryPlanError(
      `Expected scan/join base node, received "${node.kind}".`,
    );
  }

  const left = buildJoinPlan(node.left, tableConfigs);
  if (node.joinType === "semi") {
    const leftRef = qualifyJoinColumnRef(node.leftKey, left.aliases);
    const rightAlias = node.rightKey.alias ?? node.rightKey.table;

    return {
      root: left.root,
      joins: [
        ...left.joins,
        {
          joinType: "semi",
          right: node.right,
          leftKey: {
            alias: leftRef.alias,
            column: leftRef.column,
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
  if (
    right.joins.length > 0 &&
    (node.joinType !== "inner" || right.joins.some((join) => join.joinType !== "inner"))
  ) {
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

  const leftRef = qualifyJoinColumnRef(node.leftKey, left.aliases);
  const rightRef = qualifyJoinColumnRef(node.rightKey, right.aliases);

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
          alias: leftRef.alias,
          column: leftRef.column,
        },
        rightKey: {
          alias: rightRef.alias,
          column: rightRef.column,
        },
      },
      ...right.joins,
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
    scanColumns: resolveColumns(tableConfig, scan.table),
    columns: resolveColumns(tableConfig, scan.table),
    outputColumns: scan.select,
    tableConfig,
  };
}

function qualifyJoinColumnRef<TContext>(
  ref: { alias?: string; table?: string; column: string },
  aliases: Map<string, ScanBinding<TContext>>,
): QualifiedJoinColumnRef {
  const explicitAlias = ref.alias ?? ref.table;
  if (explicitAlias) {
    return {
      alias: explicitAlias,
      column: ref.column,
    };
  }

  let matchedAlias: string | null = null;
  for (const [alias, binding] of aliases.entries()) {
    if (!(ref.column in binding.columns)) {
      continue;
    }
    if (matchedAlias && matchedAlias !== alias) {
      throw new UnsupportedSingleQueryPlanError(
        `Ambiguous unqualified join key "${ref.column}" in rel fragment.`,
      );
    }
    matchedAlias = alias;
  }

  if (!matchedAlias) {
    throw new UnsupportedSingleQueryPlanError(
      `Unknown unqualified join key "${ref.column}" in rel fragment.`,
    );
  }

  return {
    alias: matchedAlias,
    column: ref.column,
  };
}

function createProjectedScanBinding<TContext>(
  project: Extract<RelNode, { kind: "project" }>,
  tableConfigs: Record<string, DrizzleProviderTableConfig<TContext>>,
): ScanBinding<TContext> {
  if (project.input.kind !== "scan") {
    throw new UnsupportedSingleQueryPlanError(
      "Projected join inputs must project directly from a scan.",
    );
  }

  const base = createScanBinding(project.input, tableConfigs);
  const aliases = new Map([[base.alias, base]]);
  const columns: Record<string, AnyColumn | SQL> = {};

  for (const rawMapping of project.columns) {
    if (isRelProjectColumnMapping(rawMapping)) {
      if (rawMapping.source.alias && rawMapping.source.alias !== base.alias) {
        throw new UnsupportedSingleQueryPlanError(
          `Projected scan column "${rawMapping.source.alias}.${rawMapping.source.column}" must reference alias "${base.alias}".`,
        );
      }
    }

    columns[rawMapping.output] = resolveProjectedSqlExpression(rawMapping, aliases, true);
  }

  return {
    ...base,
    columns,
    outputColumns: project.columns.map((column) => column.output),
  };
}

function buildSingleQuerySelection<TContext>(
  plan: SingleQueryPlan<TContext>,
): Record<string, unknown> {
  const selection: Record<string, unknown> = {};

  if (plan.pipeline.aggregate) {
    const groupSources = new Map<string, AnyColumn | SQL>();
    const groupSourcesByKey = new Map<string, AnyColumn | SQL>();
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
    for (const column of binding.outputColumns) {
      selection[`${binding.alias}.${column}`] = resolveColumnRefFromAliasMap(
        plan.joinPlan.aliases,
        {
          alias: binding.alias,
          column,
        },
      );
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
          return sql`(${sql.join(
            args.map((arg) => sql`${arg}`),
            sql` and `,
          )})`;
        case "or":
          return sql`(${sql.join(
            args.map((arg) => sql`${arg}`),
            sql` or `,
          )})`;
        case "not":
          return sql`not (${args[0]})`;
        default:
          throw new UnsupportedSingleQueryPlanError(
            `Unsupported computed projection function "${expr.name}" in Drizzle single-query pushdown.`,
          );
      }
    }
    case "subquery":
      throw new UnsupportedSingleQueryPlanError(
        "Subquery expressions are not supported in Drizzle single-query pushdown.",
      );
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

function toSqlConditionFromSource(clause: ScanFilterClause, source: AnyColumn | SQL): SQL {
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
): AnyColumn | SQL {
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
): AnyColumn | SQL {
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

  let matched: AnyColumn | SQL | null = null;
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

function resolveJoinKeyColumnRefFromAliasMap<TContext>(
  aliases: Map<string, ScanBinding<TContext>>,
  ref: { alias?: string; column: string },
): AnyColumn {
  const source = resolveColumnRefFromAliasMap(aliases, ref);
  if (looksLikeDrizzleColumn(source)) {
    return source;
  }
  const qualified = ref.alias ? `${ref.alias}.${ref.column}` : ref.column;
  throw new UnsupportedSingleQueryPlanError(
    `Join keys must resolve to physical columns. "${qualified}" resolved to a computed expression.`,
  );
}

function toAliasColumnRef(
  alias: string | undefined,
  column: string,
): { alias?: string; column: string } {
  return alias ? { alias, column } : { column };
}
