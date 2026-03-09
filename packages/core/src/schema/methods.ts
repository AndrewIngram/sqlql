import type {
  AggregatePlanDecision,
  PlannedAggregateRequest,
  PlannedLookupRequest,
  PlannedScanRequest,
  QueryRow,
  ScanPlanDecision,
  LookupPlanDecision,
  SchemaDefinition,
  TableAggregateRequest,
  TableColumnName,
  TableColumns,
  TableLookupRequest,
  TableName,
  TableScanRequest,
} from "./definition";

export interface TableMethods<
  TContext = unknown,
  TTable extends string = string,
  TColumn extends string = string,
  TColumns extends TableColumns = any,
> {
  scan(
    request: TableScanRequest<TTable, TColumn, TColumns>,
    context: TContext,
  ): Promise<QueryRow[]>;
  lookup?(
    request: TableLookupRequest<TTable, TColumn, TColumns>,
    context: TContext,
  ): Promise<QueryRow[]>;
  aggregate?(
    request: TableAggregateRequest<TTable, TColumn, TColumns>,
    context: TContext,
  ): Promise<QueryRow[]>;
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

export type TableMethodsMap<TContext = unknown> = Record<
  string,
  TableMethods<TContext, any, any, any>
>;

export type TableMethodsForSchema<TSchema extends SchemaDefinition, TContext = unknown> = {
  [TTableName in TableName<TSchema>]: TableMethods<
    TContext,
    TTableName,
    TableColumnName<TSchema, TTableName>,
    TSchema["tables"][TTableName]["columns"]
  >;
};

export function defineTableMethods<TContext, TMethods extends TableMethodsMap<TContext>>(
  methods: TMethods,
): TMethods;

export function defineTableMethods<TSchema extends SchemaDefinition, TContext>(
  schema: TSchema,
  methods: TableMethodsForSchema<TSchema, TContext>,
): TableMethodsForSchema<TSchema, TContext>;

export function defineTableMethods(...args: unknown[]): unknown {
  if (args.length === 1) {
    return args[0];
  }

  if (args.length === 2) {
    return args[1];
  }

  throw new Error("defineTableMethods expects either (methods) or (schema, methods).");
}
