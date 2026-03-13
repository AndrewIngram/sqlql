/**
 * Local operators is the curated runtime surface for in-memory relational operators over materialized rows.
 */
export { executeFilterResult, executeJoinResult } from "./local-filter-join";
export { executeAggregateResult, executeProjectResult } from "./local-projection-aggregation";
export {
  executeLimitOffsetResult,
  executeRepeatUnionResult,
  executeSetOpResult,
  executeSortResult,
  executeWithResult,
} from "./local-ordering-cte";
