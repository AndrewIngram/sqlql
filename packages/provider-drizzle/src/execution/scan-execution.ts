import type { QueryRow } from "@tupl/provider-kit";

import { runDrizzleScan } from "../backend/query-helpers";
import { resolveColumns } from "../backend/table-columns";
import type {
  CreateDrizzleProviderOptions,
  DrizzleProviderTableConfig,
  DrizzleQueryExecutor,
} from "../types";

export async function executeScan<TContext>(
  db: DrizzleQueryExecutor,
  options: CreateDrizzleProviderOptions<TContext>,
  request: import("@tupl/provider-kit").TableScanRequest,
  context: TContext,
): Promise<QueryRow[]> {
  const tableConfigs = options.tables as Record<string, DrizzleProviderTableConfig<TContext>>;
  const tableConfig = tableConfigs[request.table];
  if (!tableConfig) {
    throw new Error(`Unknown drizzle table config: ${request.table}`);
  }

  const scope = tableConfig.scope ? await tableConfig.scope(context) : undefined;
  return runDrizzleScan({
    db,
    tableName: request.table,
    table: tableConfig.table,
    columns: resolveColumns(tableConfig, request.table),
    request,
    ...(scope ? { scope } : {}),
  });
}
