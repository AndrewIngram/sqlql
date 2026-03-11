/**
 * DSL contracts is the curated schema-model surface for builder-facing contracts.
 * Internal modules should prefer the narrower table/view contract files when they only need one side.
 */
export type {
  DslTableDefinition,
  DslViewDefinition,
  SchemaBuilder,
  SchemaCalculatedColumnDefinition,
  SchemaColumnLensDefinition,
  SchemaDslRelationRef,
  SchemaTypedColumnBuilder,
  SchemaTypedColumnDefinition,
} from "./table-dsl-contracts";
export type {
  SchemaDslViewRelHelpers,
  SchemaViewAggregateMetric,
  SchemaViewAggregateNode,
  SchemaViewEqExpr,
  SchemaViewJoinNode,
  SchemaViewRelNode,
  SchemaViewRelNodeInput,
  SchemaViewScanNode,
} from "./schema-view-contracts";
