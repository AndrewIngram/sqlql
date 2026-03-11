/**
 * Schema is the canonical application-facing surface for building and querying tupl facades.
 * It exposes logical schema APIs plus the stable runtime contracts needed by schema consumers.
 */
export {
  asIso8601Timestamp,
  createSchemaBuilder,
  defineTableMethods,
  resolveTableColumnDefinition,
  toSqlDDL,
} from "@tupl/schema-model";
export type {
  QueryRow,
  ScanFilterClause,
  ScanOrderBy,
  SchemaBuilder,
  SchemaDefinition,
  SqlScalarType,
  TableAggregateMetric,
  TableAggregateRequest,
  TableColumnDefinition,
  TableConstraints,
  TableDefinition,
  TableLookupRequest,
  TableMethods,
  TableMethodsForSchema,
  TableMethodsMap,
  TableScanRequest,
} from "@tupl/schema-model";
export {
  DEFAULT_QUERY_FALLBACK_POLICY,
  DEFAULT_QUERY_GUARDRAILS,
  TuplDiagnosticError,
  createExecutableSchema,
  createExecutableSchemaResult,
} from "@tupl/runtime";
export type {
  ExecutableSchema,
  ExecutableSchemaQueryInput,
  ExecutableSchemaSessionInput,
  ExplainResult,
  QueryExecutionPlan,
  QueryExecutionPlanScope,
  QueryExecutionPlanStep,
  QueryFallbackPolicy,
  QueryGuardrails,
  QuerySession,
  QuerySessionOptions,
  QueryStepEvent,
  QueryStepKind,
  QueryStepOperation,
  QueryStepPhase,
  QueryStepRoute,
  QueryStepState,
  QueryStepStatus,
  QuerySqlOrigin,
} from "@tupl/runtime";
export {
  type TuplResult,
  type ConstraintValidationMode,
  type ConstraintValidationOptions,
  type ConstraintViolation,
  type ConstraintViolationType,
  type ValidateTableConstraintsInput,
  validateTableConstraintRows,
} from "@tupl/runtime";
