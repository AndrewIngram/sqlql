import type { QueryRow } from "@tupl/foundation";

import type { ProviderOperationResult } from "../../contracts";
import type { ScanFilterClause, TableScanRequest } from "../../contracts";

/**
 * Shared lookup execution owns the common "lookupMany as keyed scan" scaffold used by ordinary
 * SQL-like providers. Backends provide scan execution; provider-kit owns the request shaping so
 * providers do not duplicate the same lookup-to-scan translation logic.
 */
export function executeLookupManyViaScanResult<TRuntime, TResolvedEntity, TContext, TError>(
  runtime: TRuntime,
  entityConfigs: Record<string, TResolvedEntity>,
  request: {
    table: string;
    key: string;
    keys: unknown[];
    select: string[];
    where?: ScanFilterClause[];
  },
  context: TContext,
  executeScanResult: (
    runtime: TRuntime,
    entityConfigs: Record<string, TResolvedEntity>,
    request: TableScanRequest,
    context: TContext,
  ) => Promise<ProviderOperationResult<QueryRow[], TError>>,
): Promise<ProviderOperationResult<QueryRow[], TError>> {
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

  return executeScanResult(runtime, entityConfigs, scanRequest, context);
}
