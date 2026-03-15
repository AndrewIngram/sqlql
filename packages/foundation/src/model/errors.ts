import { TaggedError, type Result as BetterResult } from "better-result";

import type { TuplDiagnostic } from "./diagnostics";

export interface TuplSchemaIssue {
  code: string;
  message: string;
  table?: string;
  column?: string;
  constraint?: string;
  path?: readonly string[];
}

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

export class UnsupportedQueryShapeError extends TaggedError("UnsupportedQueryShapeError")<{
  cause?: unknown;
  message: string;
  operation: string;
}>() {}

export class RelLoweringError extends TaggedError("RelLoweringError")<{
  cause?: unknown;
  message: string;
  operation: string;
}>() {}

export class RelRewriteError extends TaggedError("RelRewriteError")<{
  cause?: unknown;
  message: string;
  operation: string;
}>() {}

export class PhysicalPlanningError extends TaggedError("PhysicalPlanningError")<{
  cause?: unknown;
  message: string;
  operation: string;
}>() {}

export class ProviderFragmentBuildError extends TaggedError("ProviderFragmentBuildError")<{
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

export class TuplSchemaValidationError extends TaggedError("TuplSchemaValidationError")<{
  issues: readonly TuplSchemaIssue[];
  message: string;
}>() {}

export class TuplSchemaNormalizationError extends TaggedError("TuplSchemaNormalizationError")<{
  cause?: unknown;
  message: string;
  operation: string;
  table?: string;
  column?: string;
}>() {}

export type TuplError =
  | TuplDiagnosticError
  | TuplGuardrailError
  | TuplTimeoutError
  | TuplRuntimeError
  | TuplParseError
  | TuplPlanningError
  | UnsupportedQueryShapeError
  | RelLoweringError
  | RelRewriteError
  | PhysicalPlanningError
  | ProviderFragmentBuildError
  | TuplExecutionError
  | TuplProviderBindingError
  | TuplSchemaValidationError
  | TuplSchemaNormalizationError;

export type TuplResult<T> = BetterResult<T, TuplError>;
