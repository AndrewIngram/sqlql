import { Result } from "better-result";

import { createSqlRel, type RelNode } from "@tupl/foundation";
import { validateRelAgainstSchema, type SchemaDefinition } from "@tupl/schema-model";
import { parseSqliteSelectAstResult } from "./sqlite-parser/parser";
import { toTuplPlanningError } from "./planner-errors";
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

export function lowerSqlToRelResult(sql: string, schema: SchemaDefinition) {
  return Result.gen(function* () {
    const ast = yield* parseSqliteSelectAstResult(sql);
    return Result.try({
      try: () => lowerSqlAstToRel(ast, sql, schema),
      catch: (error) => toTuplPlanningError(error, "lower SQL to relational plan"),
    });
  });
}

function lowerSqlAstToRel(
  ast: import("./sqlite-parser/ast").SelectAst,
  sql: string,
  schema: SchemaDefinition,
): RelLoweringResult {
  assertNoUnsupportedQueryShapes(ast);

  const structured = tryLowerStructuredSelect(ast, schema, new Set<string>());
  if (structured) {
    validateRelAgainstSchema(structured, schema);
    return {
      rel: structured,
      tables: collectTablesFromSelectAst(ast),
    };
  }

  const tables = collectTablesFromSelectAst(ast);
  const rel = createSqlRel(sql, tables);
  validateRelAgainstSchema(rel, schema);
  return {
    rel,
    tables,
  };
}

export { expandRelViews, expandRelViewsResult } from "./view-expansion";
export {
  buildProviderFragmentForRel,
  buildProviderFragmentForRelResult,
} from "./provider-fragments";
export { planPhysicalQuery, planPhysicalQueryResult } from "./physical-planning";
