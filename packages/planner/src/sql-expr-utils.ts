/**
 * SQL expr utilities own the curated internal export surface shared by expression lowering,
 * window parsing, and query-shape analysis.
 */
export {
  parseLimitAndOffset,
  parseLiteral,
  parsePositiveOrdinalLiteral,
  tryParseLiteralExpressionList,
} from "./expr/expr-literals";
export {
  collectRelExprRefs,
  collectTablesFromSelectAst,
  resolveColumnRef,
  toRawColumnRef,
} from "./expr/expr-column-refs";
export { isCorrelatedSubquery, parseSubqueryAst } from "./subqueries/analysis";
export {
  parseNamedWindowSpecifications,
  parseWindowFrameClause,
  parseWindowOver,
} from "./windows/window-specifications";
export {
  mapBinaryOperatorToRelFunction,
  readWindowFunctionArgs,
  readWindowFunctionName,
  supportsRankWindowArgs,
} from "./expr/expr-functions";
