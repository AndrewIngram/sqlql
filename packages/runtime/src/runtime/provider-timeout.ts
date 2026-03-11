import type { Result as BetterResult } from "better-result";

import type { TuplError } from "@tupl/foundation";
import { TuplTimeoutError } from "@tupl/foundation";

import { tryQueryStepAsync } from "./diagnostics";

/**
 * Provider timeout owns async timeout wrapping for provider-facing runtime operations.
 */
export async function withTimeoutResult<T>(
  operation: string,
  promiseFactory: () => Promise<T>,
  timeoutMs: number,
): Promise<BetterResult<T, TuplError>> {
  if (timeoutMs <= 0 || !Number.isFinite(timeoutMs)) {
    return tryQueryStepAsync(operation, promiseFactory);
  }

  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeoutPromise = new Promise<never>((_, reject) => {
    timer = setTimeout(() => {
      reject(
        new TuplTimeoutError({
          operation,
          timeoutMs,
          message: `Query timed out after ${timeoutMs}ms.`,
        }),
      );
    }, timeoutMs);
  });

  try {
    return await tryQueryStepAsync(operation, () =>
      Promise.race([promiseFactory(), timeoutPromise]),
    );
  } finally {
    if (timer) {
      clearTimeout(timer);
    }
  }
}
