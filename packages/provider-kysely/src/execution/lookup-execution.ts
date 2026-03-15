import type { ProviderOperationResult, QueryRow, ScanFilterClause } from "@tupl/provider-kit";
import { executeLookupManyViaScanResult } from "@tupl/provider-kit/relational-sql";
import type { TuplExecutionError, TuplProviderBindingError } from "@tupl/foundation";

import type { KyselyDatabaseLike, ResolvedEntityConfig } from "../types";
import { executeScanResult } from "./scan-execution";

export async function executeLookupManyResult<TContext>(
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
): Promise<ProviderOperationResult<QueryRow[], TuplProviderBindingError | TuplExecutionError>> {
  return executeLookupManyViaScanResult(db, entityConfigs, request, context, executeScanResult);
}
