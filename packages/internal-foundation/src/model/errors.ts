import { TaggedError, type Result as BetterResult } from "better-result";

import type { TuplDiagnostic } from "./diagnostics";

export class TuplDiagnosticError extends TaggedError("TuplDiagnosticError")<{
  diagnostics: TuplDiagnostic[];
  message: string;
}>() {}

export class TuplGuardrailError extends TaggedError("TuplGuardrailError")<{
  actual: number;
  guardrail: string;
  limit: number;
  message: string;
}>() {}

export class TuplTimeoutError extends TaggedError("TuplTimeoutError")<{
  cause?: unknown;
  message: string;
  operation: string;
  timeoutMs: number;
}>() {}

export class TuplRuntimeError extends TaggedError("TuplRuntimeError")<{
  cause?: unknown;
  message: string;
  operation: string;
}>() {}

export class TuplParseError extends TaggedError("TuplParseError")<{
  cause?: unknown;
  message: string;
  sql: string;
}>() {}

export class TuplPlanningError extends TaggedError("TuplPlanningError")<{
  cause?: unknown;
  message: string;
  operation: string;
}>() {}

export class TuplExecutionError extends TaggedError("TuplExecutionError")<{
  cause?: unknown;
  message: string;
  operation: string;
}>() {}

export class TuplProviderBindingError extends TaggedError("TuplProviderBindingError")<{
  cause?: unknown;
  message: string;
  provider?: string;
  table?: string;
}>() {}

export type TuplError =
  | TuplDiagnosticError
  | TuplGuardrailError
  | TuplTimeoutError
  | TuplRuntimeError
  | TuplParseError
  | TuplPlanningError
  | TuplExecutionError
  | TuplProviderBindingError;

export type TuplResult<T> = BetterResult<T, TuplError>;
