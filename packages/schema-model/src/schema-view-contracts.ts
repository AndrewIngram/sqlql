import type {
  SchemaColRefToken,
  SchemaDataEntityHandle,
  SchemaDslTableToken,
} from "./schema-contracts";
import type { AggregateFunction } from "./query-contracts";
import type {
  DslTableDefinition,
  DslViewDefinition,
  SchemaDslRelationRef,
} from "./table-dsl-contracts";

/**
 * Schema view contracts own logical view-rel node shapes and the builder-facing helper types used to author them.
 */
export interface SchemaViewEqExpr {
  kind: "eq";
  left: SchemaColRefToken;
  right: SchemaColRefToken;
}

export interface SchemaViewScanNode {
  kind: "scan";
  table: string;
  entity?: SchemaDataEntityHandle<string>;
}

export interface SchemaViewJoinNode {
  kind: "join";
  left: SchemaViewRelNode;
  right: SchemaViewRelNode;
  on: SchemaViewEqExpr;
  type: "inner" | "left" | "right" | "full";
}

export interface SchemaViewAggregateMetric {
  kind: "metric";
  fn: AggregateFunction;
  column?: SchemaColRefToken;
  distinct?: boolean;
}

export interface SchemaViewAggregateNode {
  kind: "aggregate";
  from: SchemaViewRelNode;
  groupBy: Record<string, SchemaColRefToken>;
  measures: Record<string, SchemaViewAggregateMetric>;
}

export type SchemaViewRelNode = SchemaViewScanNode | SchemaViewJoinNode | SchemaViewAggregateNode;

interface SchemaViewRelNodeInputBase<TColumns extends string> {
  readonly __columns__?: TColumns;
}

export interface SchemaViewScanNodeInput<
  TColumns extends string = string,
> extends SchemaViewRelNodeInputBase<TColumns> {
  kind: "scan";
  table: string | SchemaDslTableToken<string> | SchemaDataEntityHandle<TColumns>;
}

export interface SchemaViewJoinNodeInput<
  TColumns extends string = string,
> extends SchemaViewRelNodeInputBase<TColumns> {
  kind: "join";
  left: SchemaViewRelNodeInput;
  right: SchemaViewRelNodeInput;
  on: SchemaViewEqExpr;
  type: "inner" | "left" | "right" | "full";
}

export interface SchemaViewAggregateNodeInput<
  TColumns extends string = string,
> extends SchemaViewRelNodeInputBase<TColumns> {
  kind: "aggregate";
  from: SchemaViewRelNodeInput;
  groupBy: Record<string, SchemaColRefToken>;
  measures: Record<string, SchemaViewAggregateMetric>;
}

export type SchemaViewRelNodeInput<TColumns extends string = string> =
  | SchemaViewScanNodeInput<TColumns>
  | SchemaViewJoinNodeInput<TColumns>
  | SchemaViewAggregateNodeInput<TColumns>;

interface SchemaDslRelExprHelpers {
  eq: (left: SchemaColRefToken, right: SchemaColRefToken) => SchemaViewEqExpr;
}

interface SchemaDslAggHelpers {
  count: () => SchemaViewAggregateMetric;
  countDistinct: (column: SchemaColRefToken) => SchemaViewAggregateMetric;
  sum: (column: SchemaColRefToken) => SchemaViewAggregateMetric;
  sumDistinct: (column: SchemaColRefToken) => SchemaViewAggregateMetric;
  avg: (column: SchemaColRefToken) => SchemaViewAggregateMetric;
  avgDistinct: (column: SchemaColRefToken) => SchemaViewAggregateMetric;
  min: (column: SchemaColRefToken) => SchemaViewAggregateMetric;
  max: (column: SchemaColRefToken) => SchemaViewAggregateMetric;
}

interface SchemaDslRelHelpers {
  scan: {
    (table: string): SchemaViewScanNodeInput<string>;
    (table: SchemaDslTableToken<string>): SchemaViewScanNodeInput<string>;
    <TColumns extends string>(
      table: SchemaDslTableToken<TColumns>,
    ): SchemaViewScanNodeInput<TColumns>;
    <TColumns extends string>(
      entity: SchemaDataEntityHandle<TColumns>,
    ): SchemaViewScanNodeInput<TColumns>;
    <TColumns extends string>(
      table: DslTableDefinition<TColumns, string>,
    ): SchemaViewScanNodeInput<TColumns>;
    <TColumns extends string>(
      table: DslViewDefinition<any, TColumns, string>,
    ): SchemaViewScanNodeInput<TColumns>;
  };
  join: <TLeftColumns extends string, TRightColumns extends string>(input: {
    left: SchemaViewRelNodeInput<TLeftColumns>;
    right: SchemaViewRelNodeInput<TRightColumns>;
    on: SchemaViewEqExpr;
    type?: "inner" | "left" | "right" | "full";
  }) => SchemaViewJoinNodeInput<TLeftColumns | TRightColumns>;
  aggregate: <
    TGroupBy extends Record<string, SchemaColRefToken>,
    TMeasures extends Record<string, SchemaViewAggregateMetric>,
  >(input: {
    from: SchemaViewRelNodeInput<string>;
    groupBy: TGroupBy;
    measures: TMeasures;
  }) => SchemaViewAggregateNodeInput<Extract<keyof TGroupBy | keyof TMeasures, string>>;
}

interface SchemaDslRelColHelpers {
  (ref: string): SchemaColRefToken;
  <TColumns extends string, TColumn extends TColumns>(
    entity: SchemaDataEntityHandle<TColumns>,
    column: TColumn,
  ): SchemaColRefToken;
  <TColumns extends string, TColumn extends TColumns>(
    table: SchemaDslRelationRef<TColumns>,
    column: TColumn,
  ): SchemaColRefToken;
}

export interface SchemaDslViewRelHelpers extends SchemaDslRelHelpers {
  col: SchemaDslRelColHelpers;
  expr: SchemaDslRelExprHelpers;
  agg: SchemaDslAggHelpers;
}
