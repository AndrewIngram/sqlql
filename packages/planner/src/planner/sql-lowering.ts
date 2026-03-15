import { Result } from "better-result";

import { type RelNode } from "@tupl/foundation";
import type { SchemaDefinition } from "@tupl/schema-model";
import { validateRelAgainstSchema } from "@tupl/schema-model/constraints";
import { parseSqliteSelectAstResult } from "./sqlite-parser/parser";
import { toRelLoweringError } from "./planner-errors";
import { validateQueryShapeResult } from "./query-shape-validation";
import { collectTablesFromSelectAst, tryLowerStructuredSelect } from "./structured-select-lowering";

export interface RelLoweringResult {
  rel: RelNode;
  tables: string[];
}

export function lowerSqlToRelResult(sql: string, schema: SchemaDefinition) {
  return Result.gen(function* () {
    const ast = yield* parseSqliteSelectAstResult(sql);
    yield* validateQueryShapeResult(ast);
    return Result.try({
      try: () => lowerSqlAstToRel(ast, sql, schema),
      catch: (error) => toRelLoweringError(error, "lower SQL to relational plan"),
    });
  });
}

function lowerSqlAstToRel(
  ast: import("./sqlite-parser/ast").SelectAst,
  sql: string,
  schema: SchemaDefinition,
): RelLoweringResult {
  const structured = tryLowerStructuredSelect(ast, schema, new Set<string>());
  if (structured) {
    validateRelAgainstSchema(structured, schema);
    return {
      rel: structured,
      tables: collectTablesFromSelectAst(ast),
    };
  }

  throw toRelLoweringError(
    new Error("Query could not be lowered into canonical relational operators."),
    "lower SQL to relational plan",
  );
}

export { expandRelViewsResult } from "./view-expansion";
export { buildProviderFragmentForRelResult } from "./provider-fragments";
export { planPhysicalQueryResult } from "./physical-planning";
