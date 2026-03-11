import { Result, type Result as BetterResult } from "better-result";

import { stringifyUnknownValue, TuplExecutionError } from "@tupl/foundation";

import { compareNonNull, testSqlLikePattern } from "../row-ops";

/**
 * Expression scalar functions own local scalar SQL function semantics, casts, and numeric coercion.
 */
export function evaluateScalarFunctionResult(
  name: string,
  args: unknown[],
): BetterResult<unknown, TuplExecutionError> {
  switch (name) {
    case "eq":
      return Result.ok(args[0] != null && args[0] === args[1]);
    case "neq":
      return Result.ok(args[0] != null && args[0] !== args[1]);
    case "gt":
      return Result.ok(args[0] != null && args[1] != null && compareNonNull(args[0], args[1]) > 0);
    case "gte":
      return Result.ok(args[0] != null && args[1] != null && compareNonNull(args[0], args[1]) >= 0);
    case "lt":
      return Result.ok(args[0] != null && args[1] != null && compareNonNull(args[0], args[1]) < 0);
    case "lte":
      return Result.ok(args[0] != null && args[1] != null && compareNonNull(args[0], args[1]) <= 0);
    case "and":
      return Result.ok(args.every(Boolean));
    case "or":
      return Result.ok(args.some(Boolean));
    case "not":
      return Result.ok(!args[0]);
    case "add":
      return evaluateNumericBinaryResult(args[0], args[1], "ADD", (left, right) => left + right);
    case "subtract":
      return evaluateNumericBinaryResult(
        args[0],
        args[1],
        "SUBTRACT",
        (left, right) => left - right,
      );
    case "multiply":
      return evaluateNumericBinaryResult(
        args[0],
        args[1],
        "MULTIPLY",
        (left, right) => left * right,
      );
    case "divide":
      return evaluateNumericBinaryResult(args[0], args[1], "DIVIDE", (left, right) => left / right);
    case "mod":
      return evaluateNumericBinaryResult(args[0], args[1], "MOD", (left, right) => left % right);
    case "concat":
      return Result.ok(args.map((arg) => stringifyUnknownValue(arg)).join(""));
    case "like":
      return Result.ok(
        typeof args[0] === "string" && typeof args[1] === "string"
          ? testSqlLikePattern(args[0], args[1])
          : false,
      );
    case "not_like":
      return Result.ok(
        typeof args[0] === "string" && typeof args[1] === "string"
          ? !testSqlLikePattern(args[0], args[1])
          : false,
      );
    case "in":
      return Result.ok(args[0] != null && args.slice(1).some((arg) => arg === args[0]));
    case "not_in":
      return Result.ok(args[0] != null && args.slice(1).every((arg) => arg !== args[0]));
    case "is_null":
      return Result.ok(args[0] == null);
    case "is_not_null":
      return Result.ok(args[0] != null);
    case "is_distinct_from":
      return Result.ok(args[0] !== args[1]);
    case "is_not_distinct_from":
      return Result.ok(args[0] === args[1]);
    case "between":
      return Result.ok(
        args[0] != null && args[1] != null && args[2] != null
          ? compareNonNull(args[0], args[1]) >= 0 && compareNonNull(args[0], args[2]) <= 0
          : false,
      );
    case "lower":
      return Result.ok(args[0] == null ? null : stringifyUnknownValue(args[0]).toLowerCase());
    case "upper":
      return Result.ok(args[0] == null ? null : stringifyUnknownValue(args[0]).toUpperCase());
    case "trim":
      return Result.ok(args[0] == null ? null : stringifyUnknownValue(args[0]).trim());
    case "length":
      return Result.ok(args[0] == null ? null : stringifyUnknownValue(args[0]).length);
    case "substr":
      return evaluateSubstrResult(args);
    case "coalesce":
      return Result.ok(args.find((arg) => arg != null) ?? null);
    case "nullif":
      return Result.ok(args[0] === args[1] ? null : (args[0] ?? null));
    case "abs":
      return evaluateAbsResult(args[0]);
    case "round":
      return evaluateRoundResult(args);
    case "cast":
      return castValueResult(args[0], args[1]);
    case "case":
      return evaluateCaseResult(args);
    default:
      return Result.err(
        new TuplExecutionError({
          operation: "evaluate relational expression",
          message: `Unsupported computed expression function: ${name}`,
        }),
      );
  }
}

export function toFiniteNumberResult(
  value: unknown,
  functionName:
    | "SUM"
    | "AVG"
    | "ADD"
    | "SUBTRACT"
    | "MULTIPLY"
    | "DIVIDE"
    | "MOD"
    | "SUBSTR"
    | "ABS"
    | "ROUND",
) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return Result.err(
      new TuplExecutionError({
        operation: "evaluate relational expression",
        message: `${functionName} expects numeric values.`,
      }),
    );
  }
  return Result.ok(parsed);
}

function evaluateNumericBinaryResult(
  left: unknown,
  right: unknown,
  op: "ADD" | "SUBTRACT" | "MULTIPLY" | "DIVIDE" | "MOD",
  apply: (left: number, right: number) => number,
) {
  const leftResult = toFiniteNumberResult(left, op);
  if (Result.isError(leftResult)) {
    return leftResult;
  }
  const rightResult = toFiniteNumberResult(right, op);
  if (Result.isError(rightResult)) {
    return rightResult;
  }
  return Result.ok(apply(leftResult.value, rightResult.value));
}

function evaluateSubstrResult(args: unknown[]) {
  if (args[0] == null || args[1] == null) {
    return Result.ok(null);
  }
  const input = stringifyUnknownValue(args[0]);
  const startResult = toFiniteNumberResult(args[1], "SUBSTR");
  if (Result.isError(startResult)) {
    return startResult;
  }
  const start = Math.trunc(startResult.value);
  const begin = start >= 0 ? Math.max(0, start - 1) : Math.max(input.length + start, 0);
  if (args[2] == null) {
    return Result.ok(input.slice(begin));
  }
  const lengthResult = toFiniteNumberResult(args[2], "SUBSTR");
  if (Result.isError(lengthResult)) {
    return lengthResult;
  }
  const length = Math.max(0, Math.trunc(lengthResult.value));
  return Result.ok(input.slice(begin, begin + length));
}

function evaluateAbsResult(value: unknown) {
  if (value == null) {
    return Result.ok(null);
  }
  const valueResult = toFiniteNumberResult(value, "ABS");
  if (Result.isError(valueResult)) {
    return valueResult;
  }
  return Result.ok(Math.abs(valueResult.value));
}

function evaluateRoundResult(args: unknown[]) {
  if (args[0] == null) {
    return Result.ok(null);
  }
  const valueResult = toFiniteNumberResult(args[0], "ROUND");
  if (Result.isError(valueResult)) {
    return valueResult;
  }
  let precision = 0;
  if (args[1] != null) {
    const precisionResult = toFiniteNumberResult(args[1], "ROUND");
    if (Result.isError(precisionResult)) {
      return precisionResult;
    }
    precision = Math.trunc(precisionResult.value);
  }
  const scale = 10 ** precision;
  return Result.ok(Math.round(valueResult.value * scale) / scale);
}

function evaluateCaseResult(args: unknown[]) {
  const lastIndex = args.length - 1;
  const hasElse = args.length % 2 === 1;
  for (let index = 0; index < (hasElse ? lastIndex : args.length); index += 2) {
    if (args[index]) {
      return Result.ok(args[index + 1] ?? null);
    }
  }
  return Result.ok(hasElse ? (args[lastIndex] ?? null) : null);
}

function castValueResult(value: unknown, target: unknown) {
  if (value == null) {
    return Result.ok(null);
  }
  const normalized = typeof target === "string" ? target.trim().toLowerCase() : "";
  switch (normalized) {
    case "text":
      return Result.ok(stringifyUnknownValue(value));
    case "integer":
    case "int":
      return Result.ok(Math.trunc(Number(value)));
    case "real":
    case "numeric":
    case "float":
      return Result.ok(Number(value));
    case "boolean":
      return Result.ok(Boolean(value));
    default:
      return Result.err(
        new TuplExecutionError({
          operation: "evaluate relational expression",
          message: `Unsupported CAST target type: ${String(target)}`,
        }),
      );
  }
}
