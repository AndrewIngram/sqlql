/**
 * DSL contracts expose schema-builder token and view-shape types that planner internals and
 * advanced tooling may need explicitly. They are intentionally off the root to keep the ordinary
 * schema-model surface focused on authoring behavior rather than DSL implementation detail.
 */
export type {
  DslTableDefinition,
  DslViewDefinition,
  SchemaCalculatedColumnDefinition,
  SchemaColumnLensDefinition,
  SchemaDslViewRelHelpers,
  SchemaTypedColumnBuilder,
  SchemaTypedColumnDefinition,
  SchemaViewAggregateMetric,
  SchemaViewAggregateNode,
  SchemaViewEqExpr,
  SchemaViewJoinNode,
  SchemaViewRelNode,
  SchemaViewRelNodeInput,
  SchemaViewScanNode,
} from "./contracts/dsl-contracts";
export type { SchemaColRefToken, SchemaDslTableToken } from "./contracts/schema-contracts";
