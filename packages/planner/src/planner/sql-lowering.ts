import { Result, type Result as BetterResult } from "better-result";

import {
  type RelNode,
  type TuplParseError,
  type TuplPlanningError,
  type TuplSchemaNormalizationError,
} from "@tupl/foundation";
import { validateRelAgainstSchema, type SchemaDefinition } from "@tupl/schema-model";
import { toTuplPlanningError } from "./planner-errors";
import { parseSqliteSelectAstResult } from "./sqlite-parser/parser";
import { assertNoUnsupportedQueryShapes } from "./query-shape-validation";
import { collectTablesFromSelectAst, tryLowerStructuredSelect } from "./structured-select-lowering";

export interface RelLoweringResult {
  rel: RelNode;
  tables: string[];
}

/**
 * SQL lowering is the thin public entrypoint for SQL parsing and relational lowering.
 * The implementation lives in planner-owned modules organized by owned knowledge.
 */
export function lowerSqlToRel(sql: string, schema: SchemaDefinition): RelLoweringResult {
  const result = lowerSqlToRelResult(sql, schema);
  if (Result.isError(result)) {
    throw result.error;
  }
  return result.value;
}

export function lowerSqlToRelResult(
  sql: string,
  schema: SchemaDefinition,
): BetterResult<
  RelLoweringResult,
  TuplParseError | TuplPlanningError | TuplSchemaNormalizationError
> {
  return Result.gen(function* () {
    const ast = yield* parseSqliteSelectAstResult(sql);
    const lowered = yield* lowerSqlAstToRel(ast, sql, schema);
    return Result.ok(lowered);
  });
}

function lowerSqlAstToRel(
  ast: import("./sqlite-parser/ast").SelectAst,
  sql: string,
  schema: SchemaDefinition,
): BetterResult<RelLoweringResult, TuplPlanningError | TuplSchemaNormalizationError> {
  const lowered = Result.try({
    try: () => lowerSqlAstToRelUnchecked(ast, sql, schema),
    catch: (error) => toTuplPlanningError(error, "lower SQL to relational plan"),
  });
  if (Result.isError(lowered)) {
    return lowered;
  }

  return Result.gen(function* () {
    yield* validateRelAgainstSchema(lowered.value.rel, schema);
    return Result.ok(lowered.value);
  });
}

function lowerSqlAstToRelUnchecked(
  ast: import("./sqlite-parser/ast").SelectAst,
  sql: string,
  schema: SchemaDefinition,
): RelLoweringResult {
  assertNoUnsupportedQueryShapes(ast);

  const structured = tryLowerStructuredSelect(ast, schema, new Set<string>());
  if (structured) {
    return {
      rel: structured,
      tables: collectTablesFromSelectAst(ast),
    };
  }

  throw new Error("Query could not be lowered into canonical relational operators.");
}

export { expandRelViews, expandRelViewsResult } from "./view-expansion";
export {
  buildProviderFragmentForRel,
  buildProviderFragmentForRelResult,
} from "./provider-fragments";
export { planPhysicalQuery, planPhysicalQueryResult } from "./physical-planning";
