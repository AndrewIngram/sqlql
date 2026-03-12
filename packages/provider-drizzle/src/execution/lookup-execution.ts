import type { ProviderLookupManyRequest, QueryRow, ScanFilterClause } from "@tupl/provider-kit";

import type { CreateDrizzleProviderOptions } from "../types";
import { resolveDrizzleDb } from "../backend/runtime-checks";
import { executeScan } from "./scan-execution";

export async function executeLookupMany<TContext>(
  options: CreateDrizzleProviderOptions<TContext>,
  request: ProviderLookupManyRequest,
  context: TContext,
): Promise<QueryRow[]> {
  const db = await resolveDrizzleDb(options, context);
  const where: ScanFilterClause[] = [
    ...(request.where ?? []),
    {
      op: "in",
      column: request.key,
      values: request.keys,
    } as ScanFilterClause,
  ];

  return executeScan(
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
