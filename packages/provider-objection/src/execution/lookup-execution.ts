import type { ProviderOperationResult, QueryRow, ScanFilterClause } from "@tupl/provider-kit";
import { executeLookupManyViaScanResult } from "@tupl/provider-kit/relational-sql";
import type { TuplExecutionError, TuplProviderBindingError } from "@tupl/foundation";

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
): Promise<ProviderOperationResult<QueryRow[], TuplProviderBindingError | TuplExecutionError>> {
  return executeLookupManyViaScanResult(knex, entityConfigs, request, context, executeScanResult);
}
