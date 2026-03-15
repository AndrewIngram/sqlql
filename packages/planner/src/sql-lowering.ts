import { Result, type Result as BetterResult } from "better-result";

import {
  RelLoweringError,
  TuplParseError,
  UnsupportedQueryShapeError,
  type RelNode,
} from "@tupl/foundation";
import type { SchemaDefinition } from "@tupl/schema-model";
import { validateRelAgainstSchema } from "@tupl/schema-model/constraints";
import { parseSqliteSelectAstResult } from "./sqlite-parser/parser";
import { validateQueryShapeResult } from "./query-shape-validation";
import { collectTablesFromSelectAst, tryLowerStructuredSelect } from "./structured-select-lowering";

export interface RelLoweringResult {
  rel: RelNode;
  tables: string[];
}

export function lowerSqlToRelResult(
  sql: string,
  schema: SchemaDefinition,
): BetterResult<RelLoweringResult, TuplParseError | UnsupportedQueryShapeError | RelLoweringError> {
  const astResult = parseSqliteSelectAstResult(sql);
  if (Result.isError(astResult)) {
    return astResult;
  }

  const queryShapeResult = validateQueryShapeResult(astResult.value);
  if (Result.isError(queryShapeResult)) {
    return queryShapeResult;
  }

  return lowerSqlAstToRel(astResult.value, sql, schema);
}

function lowerSqlAstToRel(
  ast: import("./sqlite-parser/ast").SelectAst,
  sql: string,
  schema: SchemaDefinition,
): BetterResult<RelLoweringResult, RelLoweringError> {
  return Result.gen(function* () {
    const structured = yield* tryLowerStructuredSelect(ast, schema, new Set<string>());
    if (structured) {
      yield* validateRelAgainstSchema(structured, schema);
      return Result.ok({
        rel: structured,
        tables: collectTablesFromSelectAst(ast),
      });
    }

    return Result.err(
      new RelLoweringError({
        operation: "lower SQL to relational plan",
        message: "Query could not be lowered into canonical relational operators.",
        cause: sql,
      }),
    );
  });
}

export { expandRelViewsResult } from "./view-expansion";
export { buildProviderFragmentForRelResult } from "./provider-fragments";
export { planPhysicalQueryResult } from "./physical-planning";
