import type { SchemaDefinition } from "./schema-contracts";
import type { TableColumnName, TableName } from "./normalized-contracts";
import type {
  AggregatePlanDecision,
  LookupPlanDecision,
  PlannedAggregateRequest,
  PlannedLookupRequest,
  PlannedScanRequest,
  ScanPlanDecision,
} from "./planning-contracts";
import type { TableColumns } from "./schema-contracts";
import type { TableMethods } from "./query-contracts";

/**
 * Table-planning contracts are the lower-level extension seam for runtimes and advanced tests that
 * want explicit remote-vs-residual planning hints. They are intentionally off the root because
 * ordinary schema authoring should only need the simple scan/lookup/aggregate behavior contract.
 */
export interface TablePlanningMethods<
  TContext = unknown,
  TTable extends string = string,
  TColumn extends string = string,
  TColumns extends TableColumns = any,
> extends TableMethods<TContext, TTable, TColumn, TColumns> {
  planScan?(
    request: PlannedScanRequest<TTable, TColumn, TColumns>,
    context: TContext,
  ): ScanPlanDecision<TTable, TColumn, TColumns>;
  planLookup?(
    request: PlannedLookupRequest<TTable, TColumn, TColumns>,
    context: TContext,
  ): LookupPlanDecision<TTable, TColumn, TColumns>;
  planAggregate?(
    request: PlannedAggregateRequest<TTable, TColumn, TColumns>,
    context: TContext,
  ): AggregatePlanDecision<TTable, TColumn, TColumns>;
}

export type TablePlanningMethodsMap<TContext = unknown> = Record<
  string,
  TablePlanningMethods<TContext, any, any, any>
>;

export type TablePlanningMethodsForSchema<TSchema extends SchemaDefinition, TContext = unknown> = {
  [TTableName in TableName<TSchema>]: TablePlanningMethods<
    TContext,
    TTableName,
    TableColumnName<TSchema, TTableName>,
    TSchema["tables"][TTableName]["columns"]
  >;
};
