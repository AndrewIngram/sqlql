/**
 * Planning is the curated planner surface for SQL lowering, view expansion, and physical planning.
 * Internal planner modules should depend on narrower implementation modules rather than this root.
 */
export {
  buildProviderFragmentForRel,
  buildProviderFragmentForRelResult,
  expandRelViews,
  expandRelViewsResult,
  lowerSqlToRel,
  lowerSqlToRelResult,
  planPhysicalQuery,
  planPhysicalQueryResult,
  type RelLoweringResult,
} from "./sql-lowering";
export {
  normalizePhysicalPlanForSnapshot,
  normalizeRelForSnapshot,
} from "./translation-normalization";
