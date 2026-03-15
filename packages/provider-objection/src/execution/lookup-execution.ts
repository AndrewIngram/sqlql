import type {
  ProviderOperationResult,
  QueryRow,
  ScanFilterClause,
  TableScanRequest,
} from "@tupl/provider-kit";
import type { TuplProviderBindingError } from "@tupl/foundation";

import type { KnexLike, ResolvedEntityConfig } from "../types";
import { executeScanResult } from "./scan-execution";

export async function executeLookupManyResult<TContext>(
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
): Promise<ProviderOperationResult<QueryRow[], TuplProviderBindingError>> {
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

  return executeScanResult(knex, entityConfigs, scanRequest, context);
}
