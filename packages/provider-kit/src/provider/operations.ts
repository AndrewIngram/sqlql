import { Result, type Result as BetterResult } from "better-result";

import type { ProviderOperationResult } from "./contracts";

/**
 * Provider operations centralize Result handling and capability predicates for adapter contracts.
 */
export const AdapterResult = Result;
export type AdapterResult<T, E = Error> = BetterResult<T, E>;
export type MaybePromise<T> = T | PromiseLike<T>;
export type ProviderRuntimeBinding<TContext, TValue> =
  | TValue
  | ((context: TContext) => MaybePromise<TValue>);

export function unwrapProviderOperationResult<T, E>(outcome: ProviderOperationResult<T, E>): T {
  if (Result.isError(outcome)) {
    throw outcome.error;
  }

  return outcome.value;
}
