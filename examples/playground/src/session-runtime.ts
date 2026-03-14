import { Result } from "better-result";
import { defaultSqlAstParser, lowerSqlToRel } from "@tupl/planner";
import type { QueryExecutionPlan, QuerySession, QueryStepEvent } from "@tupl/runtime/session";
import type { ExplainResult, QueryRow, SchemaDefinition } from "@tupl/schema";
import { resolveSchemaLinkedEnums, resolveTableColumnDefinition } from "@tupl/schema-model";

import { DOWNSTREAM_ROWS_SCHEMA } from "./downstream-model";
import {
  applySandboxCompletionSnapshot,
  createSandboxQuerySession,
  isSandboxQuerySession,
  readSandboxSessionId,
} from "./playground-sandbox-session";
import { hasSqlNode } from "./playground-sql-validation";
import { requestSandboxWorker } from "./playground-sandbox-client";
import {
  buildPlaygroundModules,
  serializeStringRecord,
  type PlaygroundSchemaProgramOptions,
} from "./playground-program-files";
import type { SandboxCompiledInput } from "./playground-sandbox";
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

export interface PlaygroundSessionBundle {
  session: QuerySession;
  explain: ExplainResult;
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
      const linkedEnumsResult = resolveSchemaLinkedEnums(schema, {
        resolveEnumValues: (ref) => resolveDownstreamEnumValues(ref),
        onUnresolved: "error",
        strictUnmapped: true,
      });
      if (Result.isError(linkedEnumsResult)) {
        return {
          ok: false,
          issues: [linkedEnumsResult.error.message],
        };
      }
      schema = linkedEnumsResult.value;

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

  let lowered;
  try {
    lowered = lowerSqlToRel(normalizedSql, schema);
  } catch (error) {
    return {
      ok: false,
      issues: [error instanceof Error ? error.message : "Invalid SQL query."],
    };
  }
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

function toSandboxCompiledInput(compiled: PlaygroundCompileSuccess): SandboxCompiledInput {
  return {
    schemaCode: compiled.schemaCode,
    downstreamRows: compiled.downstreamRows,
    sql: compiled.sql,
    ...(compiled.modules ? { modules: compiled.modules } : {}),
  };
}

export async function createSession(
  compiled: PlaygroundCompileSuccess,
  context: PlaygroundContext,
  options: PlaygroundSessionOptions = {},
): Promise<PlaygroundSessionBundle> {
  const bundle = await requestSandboxWorker("create_session", {
    compiled: toSandboxCompiledInput(compiled),
    context,
    options,
  });
  if (!bundle.ok) {
    throw new Error(bundle.error.message);
  }
  const session = createSandboxQuerySession(bundle.sessionId, bundle.plan);

  return {
    session,
    explain: bundle.explain,
  };
}

export async function replaySession(
  compiled: PlaygroundCompileSuccess,
  eventCount: number,
  context: PlaygroundContext,
  options: PlaygroundSessionOptions = {},
): Promise<SessionSnapshot> {
  const snapshot = await requestSandboxWorker("replay_session", {
    compiled: toSandboxCompiledInput(compiled),
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
      sessionId: readSandboxSessionId(session),
    });
    applySandboxCompletionSnapshot(session, snapshot);
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
