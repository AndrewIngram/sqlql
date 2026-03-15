import { TuplProviderBindingError } from "@tupl/foundation";
import { AdapterResult, type ProviderOperationResult } from "@tupl/provider-kit";

import type { CreateObjectionProviderOptions, KnexLike } from "../types";

export function validateKnex(
  knex: unknown,
): ProviderOperationResult<KnexLike, TuplProviderBindingError> {
  const candidate = knex as Partial<KnexLike> | null | undefined;
  if (
    !candidate ||
    typeof candidate.table !== "function" ||
    typeof candidate.queryBuilder !== "function"
  ) {
    return AdapterResult.err(
      new TuplProviderBindingError({
        provider: "objection",
        message:
          "Objection provider runtime binding did not resolve to a valid knex instance. Check your context and knex callback.",
      }),
    );
  }

  return AdapterResult.ok(candidate as KnexLike);
}

export async function resolveKnexResult<TContext>(
  options: CreateObjectionProviderOptions<TContext>,
  context: TContext,
): Promise<ProviderOperationResult<KnexLike, TuplProviderBindingError>> {
  const knex = typeof options.knex === "function" ? await options.knex(context) : options.knex;
  return validateKnex(knex);
}

export async function resolveKnex<TContext>(
  options: CreateObjectionProviderOptions<TContext>,
  context: TContext,
): Promise<KnexLike> {
  return (await resolveKnexResult(options, context)).unwrap();
}
