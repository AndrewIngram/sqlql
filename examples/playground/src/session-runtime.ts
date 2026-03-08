import {
  defaultSqlAstParser,
  lowerSqlToRel,
  resolveSchemaLinkedEnums,
  resolveTableColumnDefinition,
  type PhysicalPlan,
  type ProviderFragment,
  type QueryExecutionPlan,
  type QueryRow,
  type QuerySession,
  type QueryStepState,
  type QueryStepEvent,
  type RelNode,
  type SchemaDefinition,
} from "../../../src/index";

import { DOWNSTREAM_ROWS_SCHEMA } from "./downstream-model";
import { requestSandboxWorker } from "./playground-sandbox-client";
import {
  buildPlaygroundModules,
  serializeStringRecord,
  type PlaygroundSchemaProgramOptions,
} from "./playground-program-files";
import type { DownstreamRows, ExecutedProviderOperation, PlaygroundContext } from "./types";
import { parseDownstreamRowsText, parseFacadeSchemaCode } from "./validation";

export interface PlaygroundCompileSuccess {
  ok: true;
  schema: SchemaDefinition;
  schemaCode: string;
  downstreamRows: DownstreamRows;
  modules: Record<string, string>;
  sql: string;
}

export interface PlaygroundCompileFailure {
  ok: false;
  issues: string[];
}

export type PlaygroundCompileResult = PlaygroundCompileSuccess | PlaygroundCompileFailure;

export type PlaygroundPreparedInputSuccess = Omit<PlaygroundCompileSuccess, "sql">;

export type PlaygroundPreparedInputResult =
  | PlaygroundPreparedInputSuccess
  | PlaygroundCompileFailure;

export interface PlaygroundSessionOptions {
  reseed?: boolean;
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

interface PlaygroundPreparedInputCacheEntry extends PlaygroundPreparedInputSuccess {
  cacheKey: string;
}

const preparedInputCache = new Map<string, Promise<PlaygroundPreparedInputResult>>();
const MAX_PREPARED_INPUT_CACHE_ENTRIES = 16;

function createPreparedInputCacheKey(
  schemaCodeText: string,
  rowsText: string,
  modules: Record<string, string>,
): string {
  return `${schemaCodeText}\u0000${rowsText}\u0000${serializeStringRecord(modules)}`;
}

function setBoundedCacheEntry<T>(
  cache: Map<string, T>,
  key: string,
  value: T,
  maxEntries: number,
): void {
  if (!cache.has(key) && cache.size >= maxEntries) {
    const oldestKey = cache.keys().next().value;
    if (typeof oldestKey === "string") {
      cache.delete(oldestKey);
    }
  }
  cache.set(key, value);
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

  const exprList =
    (rawExpr as { type?: unknown; value?: unknown }).type === "expr_list"
      ? (rawExpr as { value?: unknown }).value
      : undefined;
  if (Array.isArray(exprList)) {
    for (const item of exprList) {
      const issue = validateExpressionReferences(
        item,
        bindings,
        schema,
        availableCtes,
        allowUnqualified,
      );
      if (issue) {
        return issue;
      }
    }
  }

  const argsRaw = (rawExpr as { args?: { value?: unknown } }).args?.value;
  const args = Array.isArray(argsRaw) ? argsRaw : argsRaw != null ? [argsRaw] : [];
  for (const arg of args) {
    const issue = validateExpressionReferences(
      arg,
      bindings,
      schema,
      availableCtes,
      allowUnqualified,
    );
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

      const alias = typeof entry.as === "string" && entry.as.length > 0 ? entry.as : table;
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

function resolveDownstreamEnumValues(ref: {
  table: string;
  column: string;
}): readonly string[] | undefined {
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

export async function preparePlaygroundInput(
  schemaCodeText: string,
  rowsText: string,
  options: PlaygroundSchemaProgramOptions = {},
): Promise<PlaygroundPreparedInputResult> {
  const modules = buildPlaygroundModules(options);
  const cacheKey = createPreparedInputCacheKey(schemaCodeText, rowsText, modules);

  let cached = preparedInputCache.get(cacheKey);
  if (!cached) {
    cached = (async () => {
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

      const prepared: PlaygroundPreparedInputCacheEntry = {
        ok: true,
        schema,
        schemaCode: schemaCodeText,
        downstreamRows: parsedRows as DownstreamRows,
        modules,
        cacheKey,
      };
      return prepared;
    })();
    setBoundedCacheEntry(preparedInputCache, cacheKey, cached, MAX_PREPARED_INPUT_CACHE_ENTRIES);
  }

  return cached;
}

export function compilePreparedPlaygroundQuery(
  prepared: PlaygroundPreparedInputSuccess,
  sqlText: string,
): PlaygroundCompileResult {
  const schema = prepared.schema;

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
    schemaCode: prepared.schemaCode,
    downstreamRows: prepared.downstreamRows,
    sql: normalizedSql,
    modules: { ...prepared.modules },
  };
}

export async function compilePlaygroundInput(
  schemaCodeText: string,
  rowsText: string,
  sqlText: string,
  options: PlaygroundSchemaProgramOptions = {},
): Promise<PlaygroundCompileResult> {
  const prepared = await preparePlaygroundInput(schemaCodeText, rowsText, options);
  if (!prepared.ok) {
    return prepared;
  }
  return compilePreparedPlaygroundQuery(prepared, sqlText);
}

const SANDBOX_SESSION_PROXY = Symbol("playgroundSandboxSessionProxy");

interface SandboxSessionState {
  sessionId: string;
  plan: QueryExecutionPlan;
  stepStates: Map<string, QueryStepState>;
  result: QueryRow[] | null;
  done: boolean;
  events: QueryStepEvent[];
}

type SandboxQuerySession = QuerySession & {
  [SANDBOX_SESSION_PROXY]: SandboxSessionState;
};

function createInitialStepStates(plan: QueryExecutionPlan): Map<string, QueryStepState> {
  return new Map(
    plan.steps.map((step) => [
      step.id,
      {
        id: step.id,
        kind: step.kind,
        status: "ready",
        summary: step.summary,
        dependsOn: step.dependsOn,
        ...(step.diagnostics ? { diagnostics: step.diagnostics } : {}),
      } satisfies QueryStepState,
    ]),
  );
}

function applyStepEvent(state: SandboxSessionState, event: QueryStepEvent): void {
  state.events.push(event);
  state.stepStates.set(event.id, {
    id: event.id,
    kind: event.kind,
    status: event.status === "failed" ? "failed" : "done",
    summary: event.summary,
    dependsOn: event.dependsOn,
    executionIndex: event.executionIndex,
    startedAt: event.startedAt,
    endedAt: event.endedAt,
    durationMs: event.durationMs,
    ...(typeof event.rowCount === "number" ? { rowCount: event.rowCount } : {}),
    ...(typeof event.inputRowCount === "number" ? { inputRowCount: event.inputRowCount } : {}),
    ...(typeof event.outputRowCount === "number" ? { outputRowCount: event.outputRowCount } : {}),
    ...(event.rows ? { rows: event.rows } : {}),
    ...(event.routeUsed ? { routeUsed: event.routeUsed } : {}),
    ...(event.notes ? { notes: event.notes } : {}),
    ...(event.error ? { error: event.error } : {}),
    ...(event.diagnostics ? { diagnostics: event.diagnostics } : {}),
  });
}

function isSandboxQuerySession(session: QuerySession): session is SandboxQuerySession {
  return SANDBOX_SESSION_PROXY in session;
}

function createSandboxQuerySession(
  sessionId: string,
  plan: QueryExecutionPlan,
  initialEvents: QueryStepEvent[] = [],
  initialResult: QueryRow[] | null = null,
  initialDone = false,
): SandboxQuerySession {
  const state: SandboxSessionState = {
    sessionId,
    plan,
    stepStates: createInitialStepStates(plan),
    result: initialResult,
    done: initialDone,
    events: [],
  };

  for (const event of initialEvents) {
    applyStepEvent(state, event);
  }

  const session: SandboxQuerySession = {
    [SANDBOX_SESSION_PROXY]: state,
    getPlan(): QueryExecutionPlan {
      return state.plan;
    },
    async next(): Promise<QueryStepEvent | { done: true; result: QueryRow[] }> {
      if (state.done) {
        return {
          done: true,
          result: state.result ?? [],
        };
      }

      const next = await requestSandboxWorker("session_next", {
        sessionId: state.sessionId,
      });
      if ("done" in next) {
        state.done = true;
        state.result = next.result;
        return next;
      }

      applyStepEvent(state, next);
      return next;
    },
    async runToCompletion(): Promise<QueryRow[]> {
      const snapshot = await requestSandboxWorker("session_run_to_completion", {
        sessionId: state.sessionId,
      });
      for (const event of snapshot.events) {
        applyStepEvent(state, event);
      }
      state.done = snapshot.done;
      state.result = snapshot.result;
      return snapshot.result ?? [];
    },
    getResult(): QueryRow[] | null {
      return state.result;
    },
    getStepState(stepId: string): QueryStepState | undefined {
      return state.stepStates.get(stepId);
    },
  };

  return session;
}

export async function createSession(
  compiled: PlaygroundCompileSuccess,
  context: PlaygroundContext,
  options: PlaygroundSessionOptions = {},
): Promise<PlaygroundSessionBundle> {
  const bundle = await requestSandboxWorker("create_session", {
    compiled,
    context,
    options,
  });
  if (!bundle.ok) {
    throw new Error(bundle.error.message);
  }
  const session = createSandboxQuerySession(bundle.sessionId, bundle.plan);

  return {
    session,
    translation: bundle.translation,
  };
}

export async function replaySession(
  compiled: PlaygroundCompileSuccess,
  eventCount: number,
  context: PlaygroundContext,
  options: PlaygroundSessionOptions = {},
): Promise<SessionSnapshot> {
  const snapshot = await requestSandboxWorker("replay_session", {
    compiled,
    context,
    eventCount,
    options,
  });
  const session = createSandboxQuerySession(
    snapshot.sessionId,
    snapshot.plan,
    snapshot.events,
    snapshot.result,
    snapshot.done,
  );
  return {
    session,
    plan: snapshot.plan,
    events: snapshot.events,
    result: snapshot.result,
    done: snapshot.done,
    executedOperations: snapshot.executedOperations,
  };
}

export async function runSessionToCompletion(
  session: QuerySession,
  existingEvents: QueryStepEvent[],
): Promise<SessionSnapshot> {
  if (isSandboxQuerySession(session)) {
    const snapshot = await requestSandboxWorker("session_run_to_completion", {
      sessionId: session[SANDBOX_SESSION_PROXY].sessionId,
    });
    for (const event of snapshot.events) {
      applyStepEvent(session[SANDBOX_SESSION_PROXY], event);
    }
    session[SANDBOX_SESSION_PROXY].done = snapshot.done;
    session[SANDBOX_SESSION_PROXY].result = snapshot.result;
    return {
      session,
      plan: snapshot.plan,
      events: [...existingEvents, ...snapshot.events],
      result: snapshot.result,
      done: snapshot.done,
      executedOperations: snapshot.executedOperations,
    };
  }

  const events = [...existingEvents];

  while (true) {
    const next = await session.next();
    if ("done" in next) {
      return {
        session,
        plan: session.getPlan(),
        events,
        result: next.result,
        done: true,
        executedOperations: [],
      };
    }
    events.push(next);
  }
}
