/**
 * Normalized schema contracts expose post-DSL binding shapes for internal packages and advanced
 * tooling. They are intentionally off the root because ordinary schema authoring should not depend
 * on normalization artifacts.
 */
export type {
  NormalizedCalculatedColumnBinding,
  NormalizedColumnBinding,
  NormalizedPhysicalTableBinding,
  NormalizedSourceColumnBinding,
  NormalizedTableBinding,
  NormalizedViewTableBinding,
  TableColumnName,
  TableName,
} from "./contracts/normalized-contracts";
