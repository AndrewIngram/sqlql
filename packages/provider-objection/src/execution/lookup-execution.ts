import { type QueryRow, type ScanFilterClause, type TableScanRequest } from "@tupl/provider-kit";

import type { KnexLike, ResolvedEntityConfig } from "../types";
import { executeScan } from "./scan-execution";

export async function executeLookupMany<TContext>(
  knex: KnexLike,
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

  return executeScan(knex, entityConfigs, scanRequest, context);
}
