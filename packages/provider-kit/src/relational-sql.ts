/**
 * Advanced SQL-relational adapter types live on their own subpath so the package root can keep
 * the ordinary provider-authoring path small. Adapter authors that need backend translation hooks
 * or custom SQL-relational strategy helpers can opt into this surface explicitly.
 */
export {
  UnsupportedSqlRelationalPlanError,
  type SqlRelationalOrderTerm,
  type SqlRelationalCompileStrategy,
  type SqlRelationalQueryTranslationBackend,
  type SqlRelationalResolvedEntity,
  type SqlRelationalScanBinding,
  type SqlRelationalSelection,
  type SqlRelationalWithSelection,
} from "./provider/relational/sql-relational/types";
export {
  buildSqlRelationalSingleQueryPlan,
  createSqlRelationalScanBinding,
  resolveSqlRelationalCompileStrategy,
} from "./provider/relational/sql-relational/planning";
export { executeLookupManyViaScanResult } from "./provider/relational/sql-relational/lookup-execution";
