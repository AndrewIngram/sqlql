import * as drizzleAdapterModule from "../../../packages/drizzle/src/index";
import * as drizzleOrmModule from "drizzle-orm";
import * as ts from "typescript";
import {
  defaultSqlAstParser,
  lowerSqlToRel,
  planPhysicalQuery,
  resolveSchemaLinkedEnums,
  resolveTableColumnDefinition,
  type ExecutableSchema,
  type PhysicalPlan,
  type ProviderFragment,
  type ProviderAdapter,
  type QueryExecutionPlan,
  type QueryRow,
  type QuerySession,
  type QueryStepEvent,
  type RelNode,
  type SchemaDefinition,
} from "sqlql";
import * as sqlqlModule from "sqlql";

import {
  DOWNSTREAM_ROWS_SCHEMA,
  orderItemsTable,
  orgsTable,
  ordersTable,
  productsTable,
  usersTable,
  userProductAccessTable,
  vendorsTable,
} from "./downstream-model";
import {
  DB_PROVIDER_MODULE_ID,
  DEFAULT_DB_PROVIDER_CODE,
  DEFAULT_GENERATED_DB_FILE_CODE,
  DEFAULT_KV_PROVIDER_CODE,
  GENERATED_DB_MODULE_ID,
  KV_PROVIDER_MODULE_ID,
} from "./examples";
import {
  createKvProvider,
  KV_INPUT_TABLE_NAME,
  type KvInputRow,
} from "./kv-provider";
import {
  clearExecutedProviderOperations,
  getExecutedProviderOperations,
  getPlaygroundPgliteRuntime,
  recordExecutedProviderOperation,
  reseedDownstreamDatabase,
} from "./pglite-runtime";
import type { DownstreamRows, ExecutedProviderOperation, PlaygroundContext } from "./types";
import {
  parseDownstreamRowsText,
  parseFacadeSchemaCode,
} from "./validation";

export interface PlaygroundCompileSuccess {
  ok: true;
  schema: SchemaDefinition;
  schemaCode: string;
  downstreamRows: DownstreamRows;
  sql: string;
  modules: Record<string, string>;
}

export interface PlaygroundCompileFailure {
  ok: false;
  issues: string[];
}

export type PlaygroundCompileResult = PlaygroundCompileSuccess | PlaygroundCompileFailure;

export interface PlaygroundSchemaProgramOptions {
  modules?: Record<string, string>;
}

export interface SessionSnapshot {
  session: QuerySession;
  plan: QueryExecutionPlan;
  events: QueryStepEvent[];
  result: QueryRow[] | null;
  done: boolean;
  executedOperations: ExecutedProviderOperation[];
}

export interface TranslationFragment {
  stepId: string;
  provider: string;
  fragment: ProviderFragment;
}

export interface PlaygroundTranslation {
  userSql: string;
  facadeRel: RelNode;
  physicalPlan: PhysicalPlan;
  providerFragments: TranslationFragment[];
}

export interface PlaygroundSessionBundle {
  session: QuerySession;
  translation: PlaygroundTranslation;
}

interface PlaygroundOperationRecorder {
  clear: () => void;
  list: () => ExecutedProviderOperation[];
  record: (operation: Parameters<typeof recordExecutedProviderOperation>[0]) => ExecutedProviderOperation;
}

const operationRecorder: PlaygroundOperationRecorder = {
  clear: clearExecutedProviderOperations,
  list: getExecutedProviderOperations,
  record: recordExecutedProviderOperation,
};

export function getPlaygroundOperationRecorder(): PlaygroundOperationRecorder {
  return operationRecorder;
}

interface SqlBindingInfo {
  table: string;
  isCte: boolean;
}

function readCteName(raw: unknown): string | null {
  const name = (raw as { name?: unknown }).name;
  if (typeof name === "string" && name.length > 0) {
    return name;
  }
  const nested = (name as { value?: unknown })?.value;
  if (typeof nested === "string" && nested.length > 0) {
    return nested;
  }
  return null;
}

function validateWindowSpecification(
  rawSpec: unknown,
  bindings: Map<string, SqlBindingInfo>,
  schema: SchemaDefinition,
  availableCtes: Set<string>,
): string | null {
  if (!rawSpec || typeof rawSpec !== "object") {
    return null;
  }

  const partitionBy = (rawSpec as { partitionby?: unknown }).partitionby;
  if (Array.isArray(partitionBy)) {
    for (const part of partitionBy) {
      const issue = validateExpressionReferences(
        (part as { expr?: unknown }).expr,
        bindings,
        schema,
        availableCtes,
        true,
      );
      if (issue) {
        return issue;
      }
    }
  }

  const orderBy = (rawSpec as { orderby?: unknown }).orderby;
  if (Array.isArray(orderBy)) {
    for (const term of orderBy) {
      const issue = validateExpressionReferences(
        (term as { expr?: unknown }).expr,
        bindings,
        schema,
        availableCtes,
        true,
      );
      if (issue) {
        return issue;
      }
    }
  }

  return null;
}

function validateExpressionReferences(
  rawExpr: unknown,
  bindings: Map<string, SqlBindingInfo>,
  schema: SchemaDefinition,
  availableCtes: Set<string>,
  allowUnqualified: boolean,
): string | null {
  if (!rawExpr || typeof rawExpr !== "object") {
    return null;
  }

  const maybeSubquery = (rawExpr as { ast?: unknown }).ast;
  if (maybeSubquery && typeof maybeSubquery === "object") {
    return validateSelectReferences(maybeSubquery, schema, availableCtes);
  }

  const expr = rawExpr as { type?: unknown; table?: unknown; column?: unknown };
  if (expr.type === "column_ref") {
    const table = typeof expr.table === "string" && expr.table.length > 0 ? expr.table : null;
    const column = typeof expr.column === "string" ? expr.column : "";
    if (!column || column === "*") {
      return null;
    }

    if (table) {
      const binding = bindings.get(table);
      if (!binding) {
        return `Unknown table alias: ${table}`;
      }
      if (binding.isCte) {
        return null;
      }
      const tableDef = schema.tables[binding.table];
      if (!tableDef) {
        return `Unknown table: ${binding.table}`;
      }
      if (!(column in tableDef.columns)) {
        return `Unknown column: ${table}.${column}`;
      }
      return null;
    }

    if (!allowUnqualified) {
      return null;
    }

    if (bindings.size === 1) {
      const binding = [...bindings.values()][0];
      if (!binding || binding.isCte) {
        return null;
      }
      const tableDef = schema.tables[binding.table];
      if (!tableDef) {
        return `Unknown table: ${binding.table}`;
      }
      if (!(column in tableDef.columns)) {
        return `Unknown column: ${column}`;
      }
    }
    return null;
  }

  const binary = rawExpr as { type?: unknown; left?: unknown; right?: unknown };
  if (binary.type === "binary_expr") {
    const leftIssue = validateExpressionReferences(
      binary.left,
      bindings,
      schema,
      availableCtes,
      allowUnqualified,
    );
    if (leftIssue) {
      return leftIssue;
    }
    return validateExpressionReferences(
      binary.right,
      bindings,
      schema,
      availableCtes,
      allowUnqualified,
    );
  }

  const exprList = (rawExpr as { type?: unknown; value?: unknown }).type === "expr_list"
    ? (rawExpr as { value?: unknown }).value
    : undefined;
  if (Array.isArray(exprList)) {
    for (const item of exprList) {
      const issue = validateExpressionReferences(item, bindings, schema, availableCtes, allowUnqualified);
      if (issue) {
        return issue;
      }
    }
  }

  const argsRaw = (rawExpr as { args?: { value?: unknown } }).args?.value;
  const args = Array.isArray(argsRaw) ? argsRaw : argsRaw != null ? [argsRaw] : [];
  for (const arg of args) {
    const issue = validateExpressionReferences(arg, bindings, schema, availableCtes, allowUnqualified);
    if (issue) {
      return issue;
    }
  }

  const overClause = (rawExpr as { over?: unknown }).over;
  if (overClause && typeof overClause === "object") {
    const spec = (overClause as { as_window_specification?: unknown }).as_window_specification;
    if (spec && typeof spec === "object") {
      const issue = validateWindowSpecification(
        (spec as { window_specification?: unknown }).window_specification,
        bindings,
        schema,
        availableCtes,
      );
      if (issue) {
        return issue;
      }
    }
  }

  return null;
}

function validateSelectReferences(
  rawAst: unknown,
  schema: SchemaDefinition,
  parentCteNames: Set<string>,
): string | null {
  if (!rawAst || typeof rawAst !== "object") {
    return null;
  }

  const ast = rawAst as {
    with?: unknown;
    from?: unknown;
    columns?: unknown;
    where?: unknown;
    groupby?: { columns?: unknown };
    having?: unknown;
    orderby?: unknown;
    window?: unknown;
    _next?: unknown;
  };

  const withClauses = Array.isArray(ast.with) ? ast.with : [];
  const localCteNames = withClauses
    .map((entry) => readCteName(entry))
    .filter((name): name is string => typeof name === "string");
  const availableCtes = new Set<string>([...parentCteNames, ...localCteNames]);

  for (const entry of withClauses) {
    const cteAst = (entry as { stmt?: { ast?: unknown } }).stmt?.ast;
    const issue = validateSelectReferences(cteAst, schema, availableCtes);
    if (issue) {
      return issue;
    }
  }

  const bindings = new Map<string, SqlBindingInfo>();
  const fromEntries = Array.isArray(ast.from) ? ast.from : [];
  for (const rawFrom of fromEntries) {
    const entry = rawFrom as { table?: unknown; as?: unknown; on?: unknown };
    if (typeof entry.table === "string" && entry.table.length > 0) {
      const table = entry.table;
      const isCte = availableCtes.has(table);
      if (!isCte && !schema.tables[table]) {
        return `Unknown table: ${table}`;
      }

      const alias =
        typeof entry.as === "string" && entry.as.length > 0 ? entry.as : table;
      bindings.set(alias, {
        table,
        isCte,
      });
    }

    const onIssue = validateExpressionReferences(entry.on, bindings, schema, availableCtes, true);
    if (onIssue) {
      return onIssue;
    }
  }

  const columns = ast.columns;
  if (Array.isArray(columns)) {
    for (const column of columns) {
      const issue = validateExpressionReferences(
        (column as { expr?: unknown }).expr,
        bindings,
        schema,
        availableCtes,
        true,
      );
      if (issue) {
        return issue;
      }
    }
  }

  const whereIssue = validateExpressionReferences(ast.where, bindings, schema, availableCtes, true);
  if (whereIssue) {
    return whereIssue;
  }

  const groupByColumns = Array.isArray(ast.groupby?.columns) ? ast.groupby?.columns : [];
  for (const column of groupByColumns) {
    const issue = validateExpressionReferences(column, bindings, schema, availableCtes, true);
    if (issue) {
      return issue;
    }
  }

  const havingIssue = validateExpressionReferences(
    ast.having,
    bindings,
    schema,
    availableCtes,
    false,
  );
  if (havingIssue) {
    return havingIssue;
  }

  const orderByTerms = Array.isArray(ast.orderby) ? ast.orderby : [];
  for (const term of orderByTerms) {
    const issue = validateExpressionReferences(
      (term as { expr?: unknown }).expr,
      bindings,
      schema,
      availableCtes,
      false,
    );
    if (issue) {
      return issue;
    }
  }

  const windowEntries = Array.isArray(ast.window) ? ast.window : [];
  for (const entry of windowEntries) {
    const spec = (entry as { as_window_specification?: { window_specification?: unknown } })
      .as_window_specification?.window_specification;
    const issue = validateWindowSpecification(spec, bindings, schema, availableCtes);
    if (issue) {
      return issue;
    }
  }

  const nextIssue = validateSelectReferences(ast._next, schema, availableCtes);
  if (nextIssue) {
    return nextIssue;
  }

  return null;
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

interface ExecutableSchemaModuleExports<TContext> {
  executableSchema?: ExecutableSchema<TContext, SchemaDefinition>;
}

interface ProviderModuleExports<TContext> {
  dbProvider?: ProviderAdapter<TContext>;
  kvProvider?: ProviderAdapter<TContext>;
}

interface DbProviderRuntimeInput {
  db: Awaited<ReturnType<typeof getPlaygroundPgliteRuntime>>["db"];
  tables: {
    orgs: typeof orgsTable;
    users: typeof usersTable;
    vendors: typeof vendorsTable;
    products: typeof productsTable;
    orders: typeof ordersTable;
    order_items: typeof orderItemsTable;
    user_product_access: typeof userProductAccessTable;
  };
}

function transpilePlaygroundModuleOrThrow(source: string, moduleId: string): string {
  const transpiled = ts.transpileModule(source, {
    compilerOptions: {
      target: ts.ScriptTarget.ES2022,
      module: ts.ModuleKind.CommonJS,
      moduleResolution: ts.ModuleResolutionKind.NodeJs,
      strict: true,
      esModuleInterop: true,
    },
    reportDiagnostics: true,
    fileName: `${moduleId}.ts`,
  });

  const firstError = (transpiled.diagnostics ?? []).find(
    (diagnostic) => diagnostic.category === ts.DiagnosticCategory.Error,
  );
  if (firstError) {
    const message = ts.flattenDiagnosticMessageText(firstError.messageText, "\n");
    throw new Error(`[TS_PARSE_ERROR] ${moduleId}: ${message}`);
  }

  return transpiled.outputText;
}

function executePlaygroundModule(
  moduleId: string,
  sourceModules: Record<string, string>,
  staticModules: Record<string, unknown>,
  cache: Map<string, Record<string, unknown>>,
): Record<string, unknown> {
  const cached = cache.get(moduleId);
  if (cached) {
    return cached;
  }

  const staticModule = staticModules[moduleId];
  if (staticModule && typeof staticModule === "object") {
    const record = staticModule as Record<string, unknown>;
    cache.set(moduleId, record);
    return record;
  }

  const source = sourceModules[moduleId];
  if (typeof source !== "string") {
    throw new Error(`Unsupported import in playground module graph: ${moduleId}`);
  }

  const transpiledOutput = transpilePlaygroundModuleOrThrow(source, moduleId);
  const moduleRecord: { exports: Record<string, unknown> } = {
    exports: {},
  };

  const require = (id: string): unknown => {
    return executePlaygroundModule(id, sourceModules, staticModules, cache);
  };

  const runModule = new Function(
    "exports",
    "module",
    "require",
    `${transpiledOutput}\n//# sourceURL=playground-provider-${moduleId}.js`,
  ) as (
    exports: Record<string, unknown>,
    module: { exports: Record<string, unknown> },
    requireFn: (id: string) => unknown,
  ) => void;

  runModule(moduleRecord.exports, moduleRecord, require);
  cache.set(moduleId, moduleRecord.exports);
  return moduleRecord.exports;
}

function readExecutableSchemaOrThrow<TContext>(
  moduleId: string,
  exportsRecord: Record<string, unknown>,
): ExecutableSchema<TContext, SchemaDefinition> {
  const executableSchema = (exportsRecord as ExecutableSchemaModuleExports<TContext>).executableSchema;
  if (
    !executableSchema ||
    typeof executableSchema !== "object" ||
    !("schema" in executableSchema) ||
    typeof executableSchema.query !== "function" ||
    typeof executableSchema.createSession !== "function"
  ) {
    throw new Error(
      `${moduleId} must export executableSchema created via createExecutableSchema(...).`,
    );
  }

  return executableSchema;
}

function readProviderExportOrThrow<TContext>(
  moduleId: string,
  exportsRecord: Record<string, unknown>,
  exportName: "dbProvider" | "kvProvider",
): ProviderAdapter<TContext> {
  const provider = (exportsRecord as ProviderModuleExports<TContext>)[exportName];
  if (
    !provider ||
    typeof provider !== "object" ||
    typeof provider.name !== "string" ||
    typeof provider.canExecute !== "function" ||
    typeof provider.compile !== "function" ||
    typeof provider.execute !== "function"
  ) {
    throw new Error(`${moduleId} must export ${exportName} as a provider adapter instance.`);
  }

  return provider;
}

function readKvInputRows(rows: DownstreamRows): KvInputRow[] {
  return ((rows[KV_INPUT_TABLE_NAME] ?? []) as Array<Record<string, unknown>>)
    .flatMap((row) => {
      const key = row.key;
      if (typeof key !== "string" || key.trim().length === 0) {
        return [];
      }

      return [{ key, value: row.value }];
    });
}

async function buildExecutableSchemaFromModules(
  compiled: PlaygroundCompileSuccess,
): Promise<{
  executableSchema: ExecutableSchema<PlaygroundContext, SchemaDefinition>;
  dbProvider: ProviderAdapter<PlaygroundContext>;
  kvProvider: ProviderAdapter<PlaygroundContext>;
}> {
  const runtime = await getPlaygroundPgliteRuntime();
  const runtimeDbTables: DbProviderRuntimeInput["tables"] = {
    orgs: orgsTable,
    users: usersTable,
    vendors: vendorsTable,
    products: productsTable,
    orders: ordersTable,
    order_items: orderItemsTable,
    user_product_access: userProductAccessTable,
  };
  const staticModules: Record<string, unknown> = {
    sqlql: sqlqlModule,
    "@sqlql/drizzle": drizzleAdapterModule,
    "drizzle-orm": drizzleOrmModule,
    "@playground/kv-provider-core": {
      createKvProvider,
      playgroundKvRuntime: {
        rows: readKvInputRows(compiled.downstreamRows),
        recordOperation: operationRecorder.record,
      },
    },
    [GENERATED_DB_MODULE_ID]: {
      db: runtime.db,
      tables: runtimeDbTables,
    } satisfies DbProviderRuntimeInput,
  };

  const cache = new Map<string, Record<string, unknown>>();
  const schemaModuleExports = executePlaygroundModule(
    "__entry__",
    {
      __entry__: compiled.schemaCode,
      ...compiled.modules,
    },
    staticModules,
    cache,
  );
  const dbProviderExports = executePlaygroundModule(
    DB_PROVIDER_MODULE_ID,
    compiled.modules,
    staticModules,
    cache,
  );
  const kvProviderExports = executePlaygroundModule(
    KV_PROVIDER_MODULE_ID,
    compiled.modules,
    staticModules,
    cache,
  );

  const executableSchema = readExecutableSchemaOrThrow<PlaygroundContext>("schema.ts", schemaModuleExports);
  const dbProvider = readProviderExportOrThrow<PlaygroundContext>(
    DB_PROVIDER_MODULE_ID,
    dbProviderExports,
    "dbProvider",
  );
  const kvProvider = readProviderExportOrThrow<PlaygroundContext>(
    KV_PROVIDER_MODULE_ID,
    kvProviderExports,
    "kvProvider",
  );

  return {
    executableSchema,
    dbProvider,
    kvProvider,
  };
}

function resolveDownstreamEnumValues(ref: { table: string; column: string }): readonly string[] | undefined {
  const table = DOWNSTREAM_ROWS_SCHEMA.tables[ref.table];
  if (!table) {
    return undefined;
  }

  if (!(ref.column in table.columns)) {
    return undefined;
  }

  const column = resolveTableColumnDefinition(DOWNSTREAM_ROWS_SCHEMA, ref.table, ref.column);
  return column.enum;
}

function buildTranslation(
  userSql: string,
  facadeRel: RelNode,
  physicalPlan: PhysicalPlan,
): PlaygroundTranslation {
  const providerFragments: TranslationFragment[] = [];

  for (const step of physicalPlan.steps) {
    if (step.kind !== "remote_fragment" || !step.fragment) {
      continue;
    }
    providerFragments.push({
      stepId: step.id,
      provider: step.provider,
      fragment: step.fragment,
    });
  }

  return {
    userSql,
    facadeRel,
    physicalPlan,
    providerFragments,
  };
}

export async function compilePlaygroundInput(
  schemaCodeText: string,
  rowsText: string,
  sqlText: string,
  options: PlaygroundSchemaProgramOptions = {},
): Promise<PlaygroundCompileResult> {
  const modules = {
    [DB_PROVIDER_MODULE_ID]: DEFAULT_DB_PROVIDER_CODE,
    [GENERATED_DB_MODULE_ID]: DEFAULT_GENERATED_DB_FILE_CODE,
    [KV_PROVIDER_MODULE_ID]: DEFAULT_KV_PROVIDER_CODE,
    ...(options.modules ?? {}),
  };
  const schemaResult = await parseFacadeSchemaCode(schemaCodeText, {
    modules,
  });
  if (!schemaResult.ok || !schemaResult.schema) {
    return {
      ok: false,
      issues: schemaResult.issues.map((issue) => `${issue.path}: ${issue.message}`),
    };
  }

  let schema = schemaResult.schema;
  try {
    schema = resolveSchemaLinkedEnums(schema, {
      resolveEnumValues: (ref) => resolveDownstreamEnumValues(ref),
      onUnresolved: "throw",
      strictUnmapped: true,
    });
  } catch (error) {
    return {
      ok: false,
      issues: [error instanceof Error ? error.message : "Invalid enum linkage in schema."],
    };
  }

  const rowsResult = parseDownstreamRowsText(rowsText);
  const parsedRows = rowsResult.rows;
  if (!rowsResult.ok || !parsedRows) {
    return {
      ok: false,
      issues: rowsResult.issues.map((issue) => `${issue.path}: ${issue.message}`),
    };
  }

  const normalizedSql = sqlText.trim().replace(/;+$/u, "").trim();
  if (normalizedSql.length === 0) {
    return {
      ok: false,
      issues: ["SQL query cannot be empty."],
    };
  }

  let ast: unknown;
  try {
    ast = defaultSqlAstParser.astify(normalizedSql);
    if (Array.isArray(ast)) {
      throw new Error("Only a single SQL statement is supported.");
    }

    const type = (ast as { type?: unknown }).type;
    if (type !== "select") {
      throw new Error("Only SELECT statements are currently supported.");
    }
  } catch (error) {
    return {
      ok: false,
      issues: [error instanceof Error ? error.message : "Invalid SQL query."],
    };
  }

  const referenceIssue = validateSelectReferences(ast, schema, new Set<string>());
  if (referenceIssue) {
    return {
      ok: false,
      issues: [referenceIssue],
    };
  }

  const lowered = lowerSqlToRel(normalizedSql, schema);
  if (hasSqlNode(lowered.rel)) {
    return {
      ok: false,
      issues: [
        "This query shape is not executable in the current provider runtime yet (for example CTE/window, UNION, or subquery-heavy forms).",
      ],
    };
  }

  return {
    ok: true,
    schema,
    schemaCode: schemaCodeText,
    downstreamRows: parsedRows as DownstreamRows,
    sql: normalizedSql,
    modules: { ...modules },
  };
}

export async function createSession(
  compiled: PlaygroundCompileSuccess,
  context: PlaygroundContext,
): Promise<PlaygroundSessionBundle> {
  operationRecorder.clear();
  await reseedDownstreamDatabase(compiled.downstreamRows);
  operationRecorder.clear();

  const { executableSchema, dbProvider, kvProvider } = await buildExecutableSchemaFromModules(compiled);
  const providers = {
    [dbProvider.name]: dbProvider,
    [kvProvider.name]: kvProvider,
  };

  const lowered = lowerSqlToRel(compiled.sql, executableSchema.schema);
  const physicalPlan = await planPhysicalQuery(
    lowered.rel,
    executableSchema.schema,
    providers,
    context,
    compiled.sql,
  );

  const session = executableSchema.createSession({
    context,
    sql: compiled.sql,
    options: {
      maxConcurrency: 4,
      captureRows: "full",
    },
  });

  return {
    session,
    translation: buildTranslation(compiled.sql, lowered.rel, physicalPlan),
  };
}

export async function replaySession(
  compiled: PlaygroundCompileSuccess,
  eventCount: number,
  context: PlaygroundContext,
): Promise<SessionSnapshot> {
  const bundle = await createSession(compiled, context);
  const session = bundle.session;
  const events: QueryStepEvent[] = [];

  while (events.length < eventCount) {
    const next = await session.next();
    if ("done" in next) {
      const executedOperations = operationRecorder.list();
      return {
        session,
        plan: session.getPlan(),
        events,
        result: next.result,
        done: true,
        executedOperations,
      };
    }

    events.push(next);
  }

  const executedOperations = operationRecorder.list();
  return {
    session,
    plan: session.getPlan(),
    events,
    result: null,
    done: false,
    executedOperations,
  };
}

export async function runSessionToCompletion(
  session: QuerySession,
  existingEvents: QueryStepEvent[],
): Promise<SessionSnapshot> {
  const events = [...existingEvents];

  while (true) {
    const next = await session.next();
    if ("done" in next) {
      const executedOperations = operationRecorder.list();
      return {
        session,
        plan: session.getPlan(),
        events,
        result: next.result,
        done: true,
        executedOperations,
      };
    }
    events.push(next);
  }
}
