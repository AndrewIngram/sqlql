import { TuplProviderBindingError } from "@tupl/foundation";
import { AdapterResult, type ProviderRuntimeBinding, type MaybePromise } from "@tupl/provider-kit";

import type {
  CreateDrizzleProviderOptions,
  DrizzleProviderTableConfig,
  DrizzleQueryExecutor,
} from "../types";

export function isRuntimeBindingResolver<TContext, TValue>(
  binding: ProviderRuntimeBinding<TContext, TValue>,
): binding is (context: TContext) => MaybePromise<TValue> {
  return typeof binding === "function";
}

export function isPromiseLike<T>(value: unknown): value is PromiseLike<T> {
  return (
    (typeof value === "object" || typeof value === "function") &&
    value !== null &&
    typeof (value as { then?: unknown }).then === "function"
  );
}

export function validateDrizzleDb(
  db: DrizzleQueryExecutor | null | undefined,
): import("@tupl/provider-kit").ProviderOperationResult<
  DrizzleQueryExecutor,
  TuplProviderBindingError
> {
  if (!db || typeof db.select !== "function") {
    return AdapterResult.err(
      new TuplProviderBindingError({
        provider: "drizzle",
        message:
          "Drizzle provider runtime binding did not resolve to a valid database instance. Check your context and db callback.",
      }),
    );
  }
  return AdapterResult.ok(db);
}

export function resolveDrizzleDbMaybeSyncResult<TContext>(
  options: CreateDrizzleProviderOptions<TContext>,
  context: TContext,
): MaybePromise<
  import("@tupl/provider-kit").ProviderOperationResult<
    DrizzleQueryExecutor,
    TuplProviderBindingError
  >
> {
  if (!isRuntimeBindingResolver(options.db)) {
    return validateDrizzleDb(options.db);
  }

  const db = options.db(context);
  return isPromiseLike(db) ? db.then(validateDrizzleDb) : validateDrizzleDb(db);
}

export async function resolveDrizzleDbResult<TContext>(
  options: CreateDrizzleProviderOptions<TContext>,
  context: TContext,
): Promise<
  import("@tupl/provider-kit").ProviderOperationResult<
    DrizzleQueryExecutor,
    TuplProviderBindingError
  >
> {
  return await Promise.resolve(resolveDrizzleDbMaybeSyncResult(options, context));
}

export function resolveDrizzleDbMaybeSync<TContext>(
  options: CreateDrizzleProviderOptions<TContext>,
  context: TContext,
): MaybePromise<DrizzleQueryExecutor> {
  const result = resolveDrizzleDbMaybeSyncResult(options, context);
  if (isPromiseLike(result)) {
    return result.then((resolved) => resolved.unwrap());
  }
  return result.unwrap();
}

export async function resolveDrizzleDb<TContext>(
  options: CreateDrizzleProviderOptions<TContext>,
  context: TContext,
): Promise<DrizzleQueryExecutor> {
  return await Promise.resolve(resolveDrizzleDbMaybeSync(options, context));
}

export function inferDrizzleDialect<TContext>(
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

export function isStrategyAvailableOnDrizzleDb(
  strategy: "basic" | "set_op" | "with",
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
