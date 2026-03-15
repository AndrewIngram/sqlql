import type { QueryRow, ScanFilterClause } from "@tupl/foundation";

import type { ProviderOperationResult } from "../contracts";
import type { MaybePromise } from "../operations";

/**
 * Lookup optimization types live under shapes because they are optional provider-author helpers,
 * not part of the primary rel compile/execute adapter contract.
 */
export interface ProviderLookupManyRequest {
  table: string;
  alias?: string;
  key: string;
  keys: unknown[];
  select: string[];
  where?: ScanFilterClause[];
}

export interface LookupManyCapableProviderAdapter<TContext = unknown> {
  lookupMany(
    request: ProviderLookupManyRequest,
    context: TContext,
  ): MaybePromise<ProviderOperationResult<QueryRow[]>>;
}

export function supportsLookupMany<TContext>(
  provider: object,
): provider is LookupManyCapableProviderAdapter<TContext> {
  return "lookupMany" in provider && typeof provider.lookupMany === "function";
}
