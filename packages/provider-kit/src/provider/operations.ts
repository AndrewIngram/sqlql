import { Result, type Result as BetterResult } from "better-result";

import type {
  FragmentProviderAdapter,
  LookupProviderAdapter,
  ProviderAdapter,
  ProviderOperationResult,
} from "./contracts";

/**
 * Provider operations centralize Result handling and capability predicates for provider contracts.
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

export function supportsFragmentExecution<TContext>(
  provider: ProviderAdapter<TContext>,
): provider is FragmentProviderAdapter<TContext> {
  return (
    "compile" in provider &&
    typeof provider.compile === "function" &&
    "execute" in provider &&
    typeof provider.execute === "function"
  );
}

export function supportsLookupMany<TContext>(
  provider: ProviderAdapter<TContext>,
): provider is LookupProviderAdapter<TContext> {
  return "lookupMany" in provider && typeof provider.lookupMany === "function";
}
