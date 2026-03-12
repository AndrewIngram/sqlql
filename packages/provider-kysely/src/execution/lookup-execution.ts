import { type QueryRow, type ScanFilterClause, type TableScanRequest } from "@tupl/provider-kit";

import type { KyselyDatabaseLike, ResolvedEntityConfig } from "../types";
import { executeScan } from "./scan-execution";

export async function executeLookupMany<TContext>(
  db: KyselyDatabaseLike,
  entityConfigs: Record<string, ResolvedEntityConfig<TContext>>,
  request: {
    table: string;
    key: string;
    keys: unknown[];
    select: string[];
    where?: ScanFilterClause[];
  },
  context: TContext,
): Promise<QueryRow[]> {
  const scanRequest: TableScanRequest = {
    table: request.table,
    select: request.select,
    where: [
      ...(request.where ?? []),
      {
        op: "in",
        column: request.key,
        values: request.keys,
      } as ScanFilterClause,
    ],
  };

  return executeScan(db, entityConfigs, scanRequest, context);
}
