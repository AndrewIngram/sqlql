import { TuplProviderBindingError } from "@tupl/foundation";
import { AdapterResult, type ProviderOperationResult } from "@tupl/provider-kit";

import type { CreateKyselyProviderOptions, KyselyDatabaseLike } from "../types";

export function validateKyselyDb(
  db: unknown,
): ProviderOperationResult<KyselyDatabaseLike, TuplProviderBindingError> {
  const candidate = db as Partial<KyselyDatabaseLike> | null | undefined;
  if (!candidate || typeof candidate.selectFrom !== "function") {
    return AdapterResult.err(
      new TuplProviderBindingError({
        provider: "kysely",
        message:
          "Kysely provider runtime binding did not resolve to a valid database instance. Check your context and db callback.",
      }),
    );
  }

  return AdapterResult.ok(candidate as KyselyDatabaseLike);
}

export async function resolveKyselyDbResult<TContext>(
  options: CreateKyselyProviderOptions<TContext>,
  context: TContext,
): Promise<ProviderOperationResult<KyselyDatabaseLike, TuplProviderBindingError>> {
  const db = typeof options.db === "function" ? await options.db(context) : options.db;
  return validateKyselyDb(db);
}

export async function resolveKyselyDb<TContext>(
  options: CreateKyselyProviderOptions<TContext>,
  context: TContext,
): Promise<KyselyDatabaseLike> {
  return (await resolveKyselyDbResult(options, context)).unwrap();
}
