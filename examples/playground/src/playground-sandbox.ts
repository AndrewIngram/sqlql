import * as drizzleOrmModule from "drizzle-orm";
import * as drizzlePgCoreModule from "drizzle-orm/pg-core";
import * as drizzlePgliteModule from "drizzle-orm/pglite";
import * as pgliteModule from "@electric-sql/pglite";
import * as betterResultModule from "better-result";
import type { ProviderAdapter } from "@tupl/provider-kit";
import { lowerSqlToRelResult, planPhysicalQueryResult } from "@tupl/planner";
import {
  createExecutableSchemaSession,
  type QueryExecutionPlan,
  type QuerySession,
  type QueryStepEvent,
} from "@tupl/runtime/session";
import type { ExecutableSchema, ExplainResult, QueryRow, SchemaDefinition } from "@tupl/schema";

import { createVirtualModuleRuntime } from "./playground-module-runtime";
import {
  buildPlaygroundWorkspaceFiles,
  serializeStringRecord,
  type PlaygroundSchemaProgramOptions,
} from "./playground-program-files";
import {
  buildPlaygroundWorkspaceSnapshot,
  PLAYGROUND_CONTEXT_FILE_PATH,
  PLAYGROUND_DB_PROVIDER_FILE_PATH,
  PLAYGROUND_GENERATED_DB_FILE_PATH,
  PLAYGROUND_REDIS_PROVIDER_FILE_PATH,
  PLAYGROUND_SCHEMA_FILE_PATH,
  type PlaygroundWorkspaceSnapshot,
} from "./playground-workspace";
import {
  clearExecutedProviderOperations,
  getExecutedProviderOperations,
  getPlaygroundPgliteRuntime,
  getPlaygroundRedisRuntime,
  recordExecutedProviderOperation,
  reseedDownstreamDatabase,
} from "./pglite-runtime";
import type {
  DownstreamRows,
  ExecutedProviderOperation,
  PlaygroundContext,
  PlaygroundRuntimeContext,
  SchemaParseResult,
} from "./types";

export interface SandboxCompiledInput {
  schemaCode: string;
  downstreamRows: DownstreamRows;
  sql: string;
  modules?: Record<string, string>;
}

export interface SandboxSessionSnapshot {
  sessionId: string;
  plan: QueryExecutionPlan;
  events: QueryStepEvent[];
  result: QueryRow[] | null;
  done: boolean;
  executedOperations: ExecutedProviderOperation[];
}

export interface SandboxCreateSessionSuccess {
  ok: true;
  sessionId: string;
  plan: QueryExecutionPlan;
  explain: ExplainResult;
}

export interface SandboxCreateSessionFailure {
  ok: false;
  error: {
    message: string;
    tag?: string;
  };
}

export type SandboxCreateSessionResult = SandboxCreateSessionSuccess | SandboxCreateSessionFailure;

export type SandboxSessionNextResult = QueryStepEvent | { done: true; result: QueryRow[] };

interface SessionRecord {
  session: QuerySession;
}

interface ExecutableSchemaModuleExports<TContext> {
  executableSchema?:
    | ExecutableSchema<TContext, SchemaDefinition>
    | betterResultModule.Result<ExecutableSchema<TContext, SchemaDefinition>, unknown>;
}

interface ProviderModuleExports<TContext> {
  dbProvider?: ProviderAdapter<TContext>;
  redisProvider?: ProviderAdapter<TContext>;
}

interface TuplRuntimeModule {
  lowerSqlToRelResult: typeof lowerSqlToRelResult;
  planPhysicalQueryResult: typeof planPhysicalQueryResult;
}

interface PlaygroundRuntimeModule {
  getPlaygroundIoredisRuntime: () => {
    redis: Awaited<ReturnType<typeof getPlaygroundRedisRuntime>>["redis"];
    recordOperation: typeof recordExecutedProviderOperation;
  };
  getPlaygroundDb: () => PlaygroundRuntimeContext["db"];
}

interface SandboxProviderRuntime<TContext> {
  tuplModule: TuplRuntimeModule;
  executableSchema: ExecutableSchema<TContext, SchemaDefinition>;
  dbProvider: ProviderAdapter<TContext>;
  redisProvider: ProviderAdapter<TContext>;
  db: PlaygroundRuntimeContext["db"];
  redis: PlaygroundRuntimeContext["redis"];
}

const sessionStore = new Map<string, SessionRecord>();
const providerRuntimeCache = new Map<
  string,
  Promise<SandboxProviderRuntime<PlaygroundRuntimeContext>>
>();
let nextSessionId = 1;
const MAX_PROVIDER_RUNTIME_CACHE_ENTRIES = 8;
const MAX_SANDBOX_SESSION_ENTRIES = 32;

function makeSessionId(): string {
  const sessionId = `sandbox_session_${nextSessionId}`;
  nextSessionId += 1;
  return sessionId;
}

function setBoundedSessionStoreEntry(sessionId: string, record: SessionRecord): void {
  if (!sessionStore.has(sessionId) && sessionStore.size >= MAX_SANDBOX_SESSION_ENTRIES) {
    const oldestKey = sessionStore.keys().next().value;
    if (typeof oldestKey === "string") {
      sessionStore.delete(oldestKey);
    }
  }

  sessionStore.set(sessionId, record);
}

function asErrorMessage(error: unknown): string {
  if (error instanceof Error && error.message.length > 0) {
    return error.message;
  }
  if (typeof error === "string" && error.length > 0) {
    return error;
  }
  return "Sandbox execution failed.";
}

function unwrapResult<T, E>(result: import("better-result").Result<T, E>) {
  if (betterResultModule.Result.isError(result)) {
    throw result.error;
  }
  return result.value;
}

function isResultLike<T>(value: unknown): value is betterResultModule.Result<T, unknown> {
  return typeof value === "object" && value != null && ("value" in value || "error" in value);
}

async function runSandboxPhase<T>(phase: string, fn: () => Promise<T>): Promise<T> {
  try {
    return await fn();
  } catch (error) {
    const message = `[SANDBOX_${phase}] ${asErrorMessage(error)}`;
    console.error(message, error);
    throw new Error(message);
  }
}

function extractSchemaExport(
  exportsRecord: Record<string, unknown>,
): ExecutableSchema<PlaygroundRuntimeContext, SchemaDefinition> {
  if (!("executableSchema" in exportsRecord)) {
    throw new Error(
      "[SCHEMA_EXPORT_MISSING] Schema module must export `executableSchema` via `export const executableSchema = createExecutableSchema(...)`.",
    );
  }
  const exportedSchema = (exportsRecord as ExecutableSchemaModuleExports<PlaygroundRuntimeContext>)
    .executableSchema;
  const executableSchema = isResultLike<
    ExecutableSchema<PlaygroundRuntimeContext, SchemaDefinition>
  >(exportedSchema)
    ? unwrapResult(exportedSchema)
    : exportedSchema;
  if (
    !executableSchema ||
    typeof executableSchema !== "object" ||
    !("schema" in executableSchema) ||
    typeof executableSchema.query !== "function"
  ) {
    throw new Error(
      "[SCHEMA_EXPORT_INVALID] Schema module must export `executableSchema` created via createExecutableSchema(...).",
    );
  }
  return executableSchema;
}

function readProviderExportOrThrow<TContext>(
  moduleId: string,
  exportsRecord: Record<string, unknown>,
  exportName: "dbProvider" | "redisProvider",
): ProviderAdapter<TContext> {
  const provider = (exportsRecord as ProviderModuleExports<TContext>)[exportName];
  if (
    !provider ||
    typeof provider !== "object" ||
    typeof provider.name !== "string" ||
    typeof provider.canExecute !== "function"
  ) {
    throw new Error(
      `[SCHEMA_EXEC_ERROR] ${moduleId} must export ${exportName} as a provider adapter.`,
    );
  }

  return provider;
}

async function buildExternalRuntimeModules(): Promise<Record<string, unknown>> {
  const [dbRuntime, redisRuntime] = await Promise.all([
    getPlaygroundPgliteRuntime(),
    getPlaygroundRedisRuntime(),
  ]);
  return {
    "better-result": betterResultModule,
    "drizzle-orm": drizzleOrmModule,
    "drizzle-orm/pg-core": drizzlePgCoreModule,
    "drizzle-orm/pglite": drizzlePgliteModule,
    "@electric-sql/pglite": pgliteModule,
    "@playground/runtime": {
      getPlaygroundIoredisRuntime: () => ({
        redis: redisRuntime.redis,
        recordOperation: recordExecutedProviderOperation,
      }),
      getPlaygroundDb: () => dbRuntime.db,
    },
  };
}

function buildWorkspace(
  schemaCode: string,
  options: PlaygroundSchemaProgramOptions = {},
): PlaygroundWorkspaceSnapshot {
  return buildPlaygroundWorkspaceSnapshot(buildPlaygroundWorkspaceFiles(schemaCode, options));
}

function setBoundedProviderRuntimeCacheEntry(
  key: string,
  value: Promise<SandboxProviderRuntime<PlaygroundRuntimeContext>>,
): void {
  if (
    !providerRuntimeCache.has(key) &&
    providerRuntimeCache.size >= MAX_PROVIDER_RUNTIME_CACHE_ENTRIES
  ) {
    const oldestKey = providerRuntimeCache.keys().next().value;
    if (typeof oldestKey === "string") {
      providerRuntimeCache.delete(oldestKey);
    }
  }
  providerRuntimeCache.set(key, value);
}

function createProviderRuntimeCacheKey(
  schemaCode: string,
  downstreamRows: DownstreamRows,
  options: PlaygroundSchemaProgramOptions = {},
): string {
  return `${schemaCode}\u0000${JSON.stringify(downstreamRows)}\u0000${serializeStringRecord(
    options.modules ?? {},
  )}`;
}

function createProviderRuntime<TContext>(
  workspace: PlaygroundWorkspaceSnapshot,
  externalModules: Record<string, unknown>,
): SandboxProviderRuntime<TContext> {
  const runtime = createVirtualModuleRuntime({
    workspace,
    externalModules,
  });

  const schemaModule = runtime.executeModule(PLAYGROUND_SCHEMA_FILE_PATH);
  runtime.executeModule(PLAYGROUND_CONTEXT_FILE_PATH);
  runtime.executeModule(PLAYGROUND_GENERATED_DB_FILE_PATH);
  const dbProviderModule = runtime.executeModule(PLAYGROUND_DB_PROVIDER_FILE_PATH);
  const redisProviderModule = runtime.executeModule(PLAYGROUND_REDIS_PROVIDER_FILE_PATH);
  const tuplModule = runtime.executeModule(
    `${workspace.rootPath}/node_modules/@tupl/planner/index.ts`,
  ) as unknown as TuplRuntimeModule;
  const playgroundRuntimeModule = externalModules["@playground/runtime"] as
    | PlaygroundRuntimeModule
    | undefined;
  const db = playgroundRuntimeModule?.getPlaygroundDb();
  const ioredisRuntime = playgroundRuntimeModule?.getPlaygroundIoredisRuntime();
  const redis = ioredisRuntime?.redis;
  if (!db || typeof db !== "object" || typeof db.select !== "function") {
    throw new Error(
      "[SCHEMA_EXEC_ERROR] Playground runtime must provide a Drizzle database instance.",
    );
  }
  if (!redis || typeof redis !== "object" || typeof redis.pipeline !== "function") {
    throw new Error("[SCHEMA_EXEC_ERROR] Playground runtime must provide a Redis client instance.");
  }

  return {
    tuplModule,
    executableSchema: extractSchemaExport(schemaModule) as ExecutableSchema<
      TContext,
      SchemaDefinition
    >,
    dbProvider: readProviderExportOrThrow<TContext>(
      PLAYGROUND_DB_PROVIDER_FILE_PATH,
      dbProviderModule,
      "dbProvider",
    ) as ProviderAdapter<TContext>,
    redisProvider: readProviderExportOrThrow<TContext>(
      PLAYGROUND_REDIS_PROVIDER_FILE_PATH,
      redisProviderModule,
      "redisProvider",
    ),
    db,
    redis,
  };
}

async function getOrCreateProviderRuntime(
  compiled: SandboxCompiledInput,
): Promise<SandboxProviderRuntime<PlaygroundRuntimeContext>> {
  const options = compiled.modules ? { modules: compiled.modules } : undefined;
  const cacheKey = createProviderRuntimeCacheKey(
    compiled.schemaCode,
    compiled.downstreamRows,
    options,
  );

  let cached = providerRuntimeCache.get(cacheKey);
  if (!cached) {
    cached = (async () => {
      const workspace = buildWorkspace(compiled.schemaCode, options);
      const externalModules = await buildExternalRuntimeModules();
      return createProviderRuntime<PlaygroundRuntimeContext>(workspace, externalModules);
    })();
    setBoundedProviderRuntimeCacheEntry(cacheKey, cached);
  }

  return cached;
}

function toRuntimeContext(
  context: PlaygroundContext,
  runtime: SandboxProviderRuntime<PlaygroundRuntimeContext>,
): PlaygroundRuntimeContext {
  return {
    ...context,
    db: runtime.db,
    redis: runtime.redis,
  };
}

function normalizeSchemaError(message: string): SchemaParseResult {
  const normalized = message.startsWith("[") ? message : `[SCHEMA_EXEC_ERROR] ${message}`;
  return {
    ok: false,
    issues: [
      {
        path: "schema.ts",
        message: normalized,
      },
    ],
  };
}

export async function validateSchemaInSandbox(
  schemaCode: string,
  options: PlaygroundSchemaProgramOptions = {},
): Promise<SchemaParseResult> {
  const trimmed = schemaCode.trim();
  if (trimmed.length === 0) {
    return {
      ok: false,
      issues: [
        {
          path: "schema.ts",
          message: "[SCHEMA_EXPORT_MISSING] Schema module cannot be empty.",
        },
      ],
    };
  }

  try {
    const workspace = buildWorkspace(schemaCode, options);
    const externalModules = await buildExternalRuntimeModules();
    const { executableSchema } = createProviderRuntime<PlaygroundRuntimeContext>(
      workspace,
      externalModules,
    );
    return {
      ok: true,
      schema: executableSchema.schema,
      issues: [],
    };
  } catch (error) {
    const message = asErrorMessage(error);
    if (message.includes("[SCHEMA_EXPORT_MISSING]")) {
      return {
        ok: false,
        issues: [
          {
            path: "schema.ts",
            message,
          },
        ],
      };
    }
    if (message.includes("[SCHEMA_EXPORT_INVALID]")) {
      return normalizeSchemaError(message);
    }
    return normalizeSchemaError(message);
  }
}

export async function createSandboxSession(
  compiled: SandboxCompiledInput,
  context: PlaygroundContext,
  options: { reseed?: boolean } = {},
): Promise<SandboxCreateSessionResult> {
  clearExecutedProviderOperations();
  if (options.reseed ?? true) {
    await runSandboxPhase("RESEED", () => reseedDownstreamDatabase(compiled.downstreamRows));
  }
  clearExecutedProviderOperations();

  const runtime = await runSandboxPhase("RUNTIME_INIT", () => getOrCreateProviderRuntime(compiled));
  const runtimeContext = toRuntimeContext(context, runtime);
  const { executableSchema } = runtime;
  const explain = await runSandboxPhase("EXPLAIN", async () =>
    unwrapResult(
      await executableSchema.explain({
        context: runtimeContext,
        sql: compiled.sql,
      }),
    ),
  );

  const sessionResult = await runSandboxPhase("CREATE_SESSION", async () =>
    createExecutableSchemaSession(executableSchema, {
      context: runtimeContext,
      sql: compiled.sql,
      options: {
        maxConcurrency: 4,
        captureRows: "full",
      },
    }),
  );
  if (betterResultModule.Result.isError(sessionResult)) {
    const error = sessionResult.error as {
      message?: unknown;
      _tag?: unknown;
    };
    return {
      ok: false,
      error: {
        message: typeof error.message === "string" ? error.message : "Sandbox session failed.",
        ...(typeof error._tag === "string" ? { tag: error._tag } : {}),
      },
    };
  }

  const session = sessionResult.value;

  const sessionId = makeSessionId();
  setBoundedSessionStoreEntry(sessionId, { session });

  return {
    ok: true,
    sessionId,
    plan: session.getPlan(),
    explain,
  };
}

function readSessionRecord(sessionId: string): SessionRecord {
  const record = sessionStore.get(sessionId);
  if (!record) {
    throw new Error(`Unknown sandbox session: ${sessionId}`);
  }
  return record;
}

export async function nextSandboxSessionEvent(
  sessionId: string,
): Promise<SandboxSessionNextResult> {
  const record = readSessionRecord(sessionId);
  const next = await record.session.next();
  if ("done" in next) {
    disposeSandboxSession(sessionId);
    return {
      done: true,
      result: next.result,
    };
  }
  return next;
}

export async function runSandboxSessionToCompletion(
  sessionId: string,
): Promise<Omit<SandboxSessionSnapshot, "sessionId">> {
  const record = readSessionRecord(sessionId);
  const events: QueryStepEvent[] = [];

  while (true) {
    const next = await record.session.next();
    if ("done" in next) {
      disposeSandboxSession(sessionId);
      return {
        plan: record.session.getPlan(),
        events,
        result: next.result,
        done: true,
        executedOperations: getExecutedProviderOperations(),
      };
    }
    events.push(next);
  }
}

export async function replaySandboxSession(
  compiled: SandboxCompiledInput,
  context: PlaygroundContext,
  eventCount: number,
  options: { reseed?: boolean } = {},
): Promise<SandboxSessionSnapshot> {
  const bundle = await createSandboxSession(compiled, context, options);
  if (!bundle.ok) {
    throw new Error(bundle.error.message);
  }
  const record = readSessionRecord(bundle.sessionId);
  const events: QueryStepEvent[] = [];

  while (events.length < eventCount) {
    const next = await record.session.next();
    if ("done" in next) {
      disposeSandboxSession(bundle.sessionId);
      return {
        sessionId: bundle.sessionId,
        plan: record.session.getPlan(),
        events,
        result: next.result,
        done: true,
        executedOperations: getExecutedProviderOperations(),
      };
    }
    events.push(next);
  }

  return {
    sessionId: bundle.sessionId,
    plan: record.session.getPlan(),
    events,
    result: null,
    done: false,
    executedOperations: getExecutedProviderOperations(),
  };
}

export function disposeSandboxSession(sessionId: string): void {
  sessionStore.delete(sessionId);
}

export interface SandboxRpcRequestMap {
  validate_schema: {
    schemaCode: string;
    options?: PlaygroundSchemaProgramOptions;
  };
  create_session: {
    compiled: SandboxCompiledInput;
    context: PlaygroundContext;
    options?: { reseed?: boolean };
  };
  session_next: {
    sessionId: string;
  };
  session_run_to_completion: {
    sessionId: string;
  };
  replay_session: {
    compiled: SandboxCompiledInput;
    context: PlaygroundContext;
    eventCount: number;
    options?: { reseed?: boolean };
  };
  dispose_session: {
    sessionId: string;
  };
}

export interface SandboxRpcResponseMap {
  validate_schema: SchemaParseResult;
  create_session: SandboxCreateSessionResult;
  session_next: SandboxSessionNextResult;
  session_run_to_completion: Omit<SandboxSessionSnapshot, "sessionId">;
  replay_session: SandboxSessionSnapshot;
  dispose_session: null;
}
