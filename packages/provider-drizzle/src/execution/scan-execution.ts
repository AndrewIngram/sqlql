import { TuplProviderBindingError } from "@tupl/foundation";
import { AdapterResult, type ProviderOperationResult, type QueryRow } from "@tupl/provider-kit";

import { runDrizzleScan } from "../backend/query-helpers";
import { resolveColumns } from "../backend/table-columns";
import type {
  CreateDrizzleProviderOptions,
  DrizzleProviderTableConfig,
  DrizzleQueryExecutor,
} from "../types";

export async function executeScanResult<TContext>(
  db: DrizzleQueryExecutor,
  options: CreateDrizzleProviderOptions<TContext>,
  request: import("@tupl/provider-kit").TableScanRequest,
  context: TContext,
): Promise<ProviderOperationResult<QueryRow[], TuplProviderBindingError>> {
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

  const scope = tableConfig.scope ? await tableConfig.scope(context) : undefined;
  return AdapterResult.ok(
    await runDrizzleScan({
      db,
      tableName: request.table,
      table: tableConfig.table,
      columns: resolveColumns(tableConfig, request.table),
      request,
      ...(scope ? { scope } : {}),
    }),
  );
}

export async function executeScan<TContext>(
  db: DrizzleQueryExecutor,
  options: CreateDrizzleProviderOptions<TContext>,
  request: import("@tupl/provider-kit").TableScanRequest,
  context: TContext,
): Promise<QueryRow[]> {
  return (await executeScanResult(db, options, request, context)).unwrap();
}
