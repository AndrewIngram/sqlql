import * as drizzleOrmModule from "drizzle-orm";
import * as drizzlePgCoreModule from "drizzle-orm/pg-core";
import * as drizzlePgliteModule from "drizzle-orm/pglite";
import * as pgliteModule from "@electric-sql/pglite";
import {
  defineSchema,
  type ExecutableSchema,
  type PhysicalPlan,
  type ProviderAdapter,
  type ProviderFragment,
  type QueryExecutionPlan,
  type QueryRow,
  type QuerySession,
  type QueryStepEvent,
  type RelNode,
  type SchemaDefinition,
} from "../../../src/index";

import { createVirtualModuleRuntime } from "./playground-module-runtime";
import {
  buildPlaygroundWorkspaceFiles,
  type PlaygroundSchemaProgramOptions,
} from "./playground-program-files";
import {
  buildPlaygroundWorkspaceSnapshot,
  PLAYGROUND_DB_PROVIDER_FILE_PATH,
  PLAYGROUND_KV_PROVIDER_FILE_PATH,
  PLAYGROUND_SCHEMA_FILE_PATH,
  type PlaygroundWorkspaceSnapshot,
} from "./playground-workspace";
import { KV_INPUT_TABLE_NAME, type KvInputRow } from "./kv-provider";
import {
  clearExecutedProviderOperations,
  getExecutedProviderOperations,
  getPlaygroundPgliteRuntime,
  recordExecutedProviderOperation,
  reseedDownstreamDatabase,
} from "./pglite-runtime";
import type {
  DownstreamRows,
  ExecutedProviderOperation,
  PlaygroundContext,
  SchemaParseResult,
} from "./types";

export interface SandboxCompiledInput {
  schemaCode: string;
  downstreamRows: DownstreamRows;
  sql: string;
  modules?: Record<string, string>;
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

export interface SandboxSessionSnapshot {
  sessionId: string;
  plan: QueryExecutionPlan;
  events: QueryStepEvent[];
  result: QueryRow[] | null;
  done: boolean;
  executedOperations: ExecutedProviderOperation[];
}

export interface SandboxCreateSessionResult {
  sessionId: string;
  plan: QueryExecutionPlan;
  translation: PlaygroundTranslation;
}

export type SandboxSessionNextResult = QueryStepEvent | { done: true; result: QueryRow[] };

interface SessionRecord {
  session: QuerySession;
}

interface ExecutableSchemaModuleExports<TContext> {
  executableSchema?: ExecutableSchema<TContext, SchemaDefinition>;
}

interface ProviderModuleExports<TContext> {
  dbProvider?: ProviderAdapter<TContext>;
  kvProvider?: ProviderAdapter<TContext>;
}

interface SqlqlRuntimeModule {
  lowerSqlToRel: typeof import("../../../src/index").lowerSqlToRel;
  planPhysicalQuery: typeof import("../../../src/index").planPhysicalQuery;
}

const sessionStore = new Map<string, SessionRecord>();
let nextSessionId = 1;

function makeSessionId(): string {
  const sessionId = `sandbox_session_${nextSessionId}`;
  nextSessionId += 1;
  return sessionId;
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

function extractSchemaExport(
  exportsRecord: Record<string, unknown>,
): ExecutableSchema<PlaygroundContext, SchemaDefinition> {
  if (!("executableSchema" in exportsRecord)) {
    throw new Error(
      "[SCHEMA_EXPORT_MISSING] Schema module must export `executableSchema` via `export const executableSchema = createExecutableSchema(...)`.",
    );
  }
  const executableSchema = (exportsRecord as ExecutableSchemaModuleExports<PlaygroundContext>).executableSchema;
  if (
    !executableSchema ||
    typeof executableSchema !== "object" ||
    !("schema" in executableSchema) ||
    typeof executableSchema.query !== "function" ||
    typeof executableSchema.createSession !== "function"
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
    throw new Error(`[SCHEMA_EXEC_ERROR] ${moduleId} must export ${exportName} as a provider adapter.`);
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

async function buildExternalRuntimeModules(
  downstreamRows: DownstreamRows,
): Promise<Record<string, unknown>> {
  const runtime = await getPlaygroundPgliteRuntime();
  return {
    "drizzle-orm": drizzleOrmModule,
    "drizzle-orm/pg-core": drizzlePgCoreModule,
    "drizzle-orm/pglite": drizzlePgliteModule,
    "@electric-sql/pglite": pgliteModule,
    "@playground/runtime": {
      getPlaygroundKvRuntime: () => ({
        rows: readKvInputRows(downstreamRows),
        recordOperation: recordExecutedProviderOperation,
      }),
      getPlaygroundDbRuntime: <TTables extends object>(input: { tables: TTables }) => ({
        db: runtime.db,
        tables: input.tables,
      }),
    },
  };
}

function buildWorkspace(
  schemaCode: string,
  options: PlaygroundSchemaProgramOptions = {},
): PlaygroundWorkspaceSnapshot {
  return buildPlaygroundWorkspaceSnapshot(buildPlaygroundWorkspaceFiles(schemaCode, options));
}

function createProviderRuntime<TContext>(
  workspace: PlaygroundWorkspaceSnapshot,
  externalModules: Record<string, unknown>,
): {
  sqlqlModule: SqlqlRuntimeModule;
  executableSchema: ExecutableSchema<TContext, SchemaDefinition>;
  dbProvider: ProviderAdapter<TContext>;
  kvProvider: ProviderAdapter<TContext>;
} {
  const runtime = createVirtualModuleRuntime({
    workspace,
    externalModules,
  });

  const schemaModule = runtime.executeModule(PLAYGROUND_SCHEMA_FILE_PATH);
  const dbProviderModule = runtime.executeModule(PLAYGROUND_DB_PROVIDER_FILE_PATH);
  const kvProviderModule = runtime.executeModule(PLAYGROUND_KV_PROVIDER_FILE_PATH);
  const sqlqlModule = runtime.executeModule(
    `${workspace.rootPath}/node_modules/sqlql/index.ts`,
  ) as unknown as SqlqlRuntimeModule;

  return {
    sqlqlModule,
    executableSchema: extractSchemaExport(schemaModule) as ExecutableSchema<TContext, SchemaDefinition>,
    dbProvider: readProviderExportOrThrow<TContext>(
      PLAYGROUND_DB_PROVIDER_FILE_PATH,
      dbProviderModule,
      "dbProvider",
    ),
    kvProvider: readProviderExportOrThrow<TContext>(
      PLAYGROUND_KV_PROVIDER_FILE_PATH,
      kvProviderModule,
      "kvProvider",
    ),
  };
}

function normalizeSchemaError(message: string): SchemaParseResult {
  const normalized = message.startsWith("[")
    ? message
    : `[SCHEMA_EXEC_ERROR] ${message}`;
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
    const externalModules = await buildExternalRuntimeModules({});
    const { executableSchema } = createProviderRuntime<PlaygroundContext>(workspace, externalModules);
    return {
      ok: true,
      schema: defineSchema(executableSchema.schema),
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
    await reseedDownstreamDatabase(compiled.downstreamRows);
  }
  clearExecutedProviderOperations();

  const workspace = buildWorkspace(
    compiled.schemaCode,
    compiled.modules ? { modules: compiled.modules } : undefined,
  );
  const externalModules = await buildExternalRuntimeModules(compiled.downstreamRows);
  const { sqlqlModule, executableSchema, dbProvider, kvProvider } = createProviderRuntime<PlaygroundContext>(
    workspace,
    externalModules,
  );

  const providers = {
    [dbProvider.name]: dbProvider,
    [kvProvider.name]: kvProvider,
  };
  const lowered = sqlqlModule.lowerSqlToRel(compiled.sql, executableSchema.schema);
  const physicalPlan = await sqlqlModule.planPhysicalQuery(
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

  const sessionId = makeSessionId();
  sessionStore.set(sessionId, { session });

  return {
    sessionId,
    plan: session.getPlan(),
    translation: buildTranslation(compiled.sql, lowered.rel, physicalPlan),
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
): Promise<SandboxSessionSnapshot> {
  const bundle = await createSandboxSession(compiled, context);
  const record = readSessionRecord(bundle.sessionId);
  const events: QueryStepEvent[] = [];

  while (events.length < eventCount) {
    const next = await record.session.next();
    if ("done" in next) {
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
