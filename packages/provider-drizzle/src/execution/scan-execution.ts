import { TuplExecutionError, TuplProviderBindingError } from "@tupl/foundation";
import { AdapterResult, type ProviderOperationResult, type QueryRow } from "@tupl/provider-kit";

import { runDrizzleScan } from "../backend/query-helpers";
import { resolveColumns } from "../backend/table-columns";
import type {
  CreateDrizzleProviderOptions,
  DrizzleProviderTableConfig,
  DrizzleQueryExecutor,
} from "../types";

function normalizeDrizzleScanError(
  error: unknown,
  provider: string,
  table: string,
): TuplProviderBindingError | TuplExecutionError {
  if (error instanceof TuplProviderBindingError || error instanceof TuplExecutionError) {
    return error;
  }

  if (error instanceof Error) {
    if (
      error.message.startsWith("Unable to derive columns for table ") ||
      error.message.startsWith("Unsupported column ") ||
      error.message.startsWith("Unsupported ORDER BY column ") ||
      error.message.startsWith("Unsupported filter column ")
    ) {
      return new TuplProviderBindingError({
        provider,
        table,
        message: error.message,
        cause: error,
      });
    }

    return new TuplExecutionError({
      operation: "execute drizzle scan",
      message: error.message,
      cause: error,
    });
  }

  return new TuplExecutionError({
    operation: "execute drizzle scan",
    message: String(error),
    cause: error,
  });
}

export async function executeScanResult<TContext>(
  db: DrizzleQueryExecutor,
  options: CreateDrizzleProviderOptions<TContext>,
  request: import("@tupl/provider-kit").TableScanRequest,
  context: TContext,
): Promise<ProviderOperationResult<QueryRow[], TuplProviderBindingError | TuplExecutionError>> {
  const tableConfigs = options.tables as Record<string, DrizzleProviderTableConfig<TContext>>;
  const tableConfig = tableConfigs[request.table];
  if (!tableConfig) {
    return AdapterResult.err(
      new TuplProviderBindingError({
        provider: options.name ?? "drizzle",
        table: request.table,
        message: `Unknown drizzle table config: ${request.table}`,
      }),
    );
  }

  return AdapterResult.tryPromise({
    try: async () => {
      const scope = tableConfig.scope ? await tableConfig.scope(context) : undefined;
      return await runDrizzleScan({
        db,
        tableName: request.table,
        table: tableConfig.table,
        columns: resolveColumns(tableConfig, request.table),
        request,
        ...(scope ? { scope } : {}),
      });
    },
    catch: (error) => normalizeDrizzleScanError(error, options.name ?? "drizzle", request.table),
  });
}

export async function executeScan<TContext>(
  db: DrizzleQueryExecutor,
  options: CreateDrizzleProviderOptions<TContext>,
  request: import("@tupl/provider-kit").TableScanRequest,
  context: TContext,
): Promise<QueryRow[]> {
  return (await executeScanResult(db, options, request, context)).unwrap();
}
