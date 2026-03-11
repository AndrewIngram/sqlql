import { Result, type Result as BetterResult } from "better-result";

import { TuplExecutionError } from "@tupl/foundation";

import { compareNullableValues } from "./row-ops";
import { toFiniteNumberResult } from "./expression-scalar-functions";

/**
 * Aggregate metric eval owns local aggregate metric semantics.
 */
export function evaluateAggregateMetricResult(
  fn: "count" | "sum" | "avg" | "min" | "max",
  values: unknown[],
  bucketSize: number,
  hasColumn: boolean,
): BetterResult<unknown, TuplExecutionError> {
  switch (fn) {
    case "count":
      return Result.ok(hasColumn ? values.filter((value) => value != null).length : bucketSize);
    case "sum": {
      const numeric: number[] = [];
      for (const value of values.filter((entry) => entry != null)) {
        const numericResult = toFiniteNumberResult(value, "SUM");
        if (Result.isError(numericResult)) {
          return numericResult;
        }
        numeric.push(numericResult.value);
      }
      return Result.ok(numeric.length > 0 ? numeric.reduce((sum, value) => sum + value, 0) : null);
    }
    case "avg": {
      const numeric: number[] = [];
      for (const value of values.filter((entry) => entry != null)) {
        const numericResult = toFiniteNumberResult(value, "AVG");
        if (Result.isError(numericResult)) {
          return numericResult;
        }
        numeric.push(numericResult.value);
      }
      return Result.ok(
        numeric.length > 0 ? numeric.reduce((sum, value) => sum + value, 0) / numeric.length : null,
      );
    }
    case "min": {
      const candidates = values.filter((value) => value != null);
      return Result.ok(
        candidates.length > 0
          ? candidates.reduce((left, right) =>
              compareNullableValues(left, right) <= 0 ? left : right,
            )
          : null,
      );
    }
    case "max": {
      const candidates = values.filter((value) => value != null);
      return Result.ok(
        candidates.length > 0
          ? candidates.reduce((left, right) =>
              compareNullableValues(left, right) >= 0 ? left : right,
            )
          : null,
      );
    }
  }
}
