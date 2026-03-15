import type { QueryRow, ProviderOperationResult, ScanFilterClause } from "@tupl/provider-kit";
import type { TuplProviderBindingError } from "@tupl/foundation";
import type { ProviderLookupManyRequest } from "@tupl/provider-kit/shapes";

import type { CreateDrizzleProviderOptions } from "../types";
import type { DrizzleQueryExecutor } from "../types";
import { executeScanResult } from "./scan-execution";

export async function executeLookupManyResult<TContext>(
  db: DrizzleQueryExecutor,
  options: CreateDrizzleProviderOptions<TContext>,
  request: ProviderLookupManyRequest,
  context: TContext,
): Promise<ProviderOperationResult<QueryRow[], TuplProviderBindingError>> {
  const where: ScanFilterClause[] = [
    ...(request.where ?? []),
    {
      op: "in",
      column: request.key,
      values: request.keys,
    } as ScanFilterClause,
  ];

  return executeScanResult(
    db,
    options,
    {
      table: request.table,
      select: request.select,
      where,
    },
    context,
  );
}
