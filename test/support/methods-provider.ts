import { Result } from "better-result";
import { aggregateArrayRows, scanArrayRows } from "../../src/array-methods";
import {
  type AggregatePlanDecision,
  bindAdapterEntities,
  type ConstraintValidationOptions,
  createDataEntityHandle,
  createSchemaBuilder,
  createExecutableSchema,
  type LookupPlanDecision,
  type PlannedAggregateRequest,
  type PlannedLookupRequest,
  type PlannedScanRequest,
  type QueryGuardrails,
  type QuerySessionOptions,
  type ProviderAdapter,
  type ProviderFragment,
  type QueryRow,
  type ScanPlanDecision,
  type ScanFilterClause,
  type ScanOrderBy,
  type SchemaDefinition,
  type TableAggregateMetric,
  type TableAggregateRequest,
  type TableLookupRequest,
  type TableMethodsMap,
  type TableScanRequest,
} from "../../src";

function toEntityColumns(schema: SchemaDefinition, tableName: string) {
  const table = schema.tables[tableName];
  if (!table) {
    throw new Error(`Unknown table: ${tableName}`);
  }

  return Object.fromEntries(
    Object.entries(table.columns).map(([columnName, definition]) => [
      columnName,
      typeof definition === "string"
        ? {
            source: columnName,
            type: definition,
          }
        : {
            source: columnName,
            type: definition.type,
            ...(definition.nullable != null ? { nullable: definition.nullable } : {}),
            ...(definition.primaryKey != null ? { primaryKey: definition.primaryKey } : {}),
            ...(definition.unique != null ? { unique: definition.unique } : {}),
            ...(definition.enum ? { enum: definition.enum } : {}),
            ...(definition.physicalType ? { physicalType: definition.physicalType } : {}),
            ...(definition.physicalDialect ? { physicalDialect: definition.physicalDialect } : {}),
          },
    ]),
  );
}

export function createMethodsProvider<TContext>(
  schema: SchemaDefinition,
  methods: TableMethodsMap<TContext>,
  providerName = "memory",
): ProviderAdapter<TContext> {
  const adapter: ProviderAdapter<TContext> = {
    name: providerName,
    entities: {},
    canExecute(fragment) {
      switch (fragment.kind) {
        case "scan":
          return !!methods[fragment.table]?.scan;
        case "aggregate":
          return !!methods[fragment.table]?.aggregate;
        case "rel":
          return false;
        default:
          return false;
      }
    },
    async compile(fragment) {
      return Result.ok({
        provider: providerName,
        kind: fragment.kind,
        payload: fragment,
      });
    },
    async execute(plan, context) {
      const fragment = plan.payload as ProviderFragment;
      switch (fragment.kind) {
        case "scan": {
          const method = methods[fragment.table];
          if (!method?.scan) {
            return Result.err(
              new Error(`No table methods registered for table: ${fragment.table}`),
            );
          }
          return Result.ok(await executePlannedScan(method, fragment.request, context));
        }
        case "aggregate": {
          const method = methods[fragment.table];
          if (!method?.aggregate) {
            return Result.err(
              new Error(`No aggregate method registered for table: ${fragment.table}`),
            );
          }
          return Result.ok(await executePlannedAggregate(method, fragment.request, context));
        }
        case "rel":
          return Result.err(new Error("Methods-based provider does not support rel fragments."));
      }
    },
    async lookupMany(request, context) {
      const method = methods[request.table];
      if (!method?.lookup) {
        return Result.ok([]);
      }

      return Result.ok(await executePlannedLookup(method, {
        table: request.table,
        ...(request.alias ? { alias: request.alias } : {}),
        key: request.key,
        values: request.keys,
        select: request.select,
        ...(request.where ? { where: request.where } : {}),
      }, context));
    },
  };

  for (const tableName of Object.keys(schema.tables)) {
    adapter.entities![tableName] = createDataEntityHandle({
      entity: tableName,
      provider: providerName,
      adapter,
      columns: toEntityColumns(schema, tableName),
    });
  }

  return bindAdapterEntities(adapter);
}

export function createExecutableMethodsSchema<TContext, TSchema extends SchemaDefinition>(
  schema: TSchema,
  methods: TableMethodsMap<TContext>,
  providerName = "memory",
) {
  const provider = createMethodsProvider(schema, methods, providerName);
  const builder = createSchemaBuilder<TContext>();

  for (const [tableName, tableDefinition] of Object.entries(schema.tables)) {
    builder.table(tableName, provider.entities![tableName]!, {
      columns: () =>
        Object.fromEntries(
          Object.entries(tableDefinition.columns).map(([columnName, definition]) => [
            columnName,
            typeof definition === "string"
              ? { source: columnName, type: definition }
              : { source: columnName, ...definition },
          ]),
        ),
      ...(tableDefinition.constraints ? { constraints: tableDefinition.constraints } : {}),
    });
  }

  return createExecutableSchema(builder);
}

async function executePlannedScan<TContext>(
  method: NonNullable<TableMethodsMap<TContext>[string]>,
  request: TableScanRequest,
  context: TContext,
): Promise<QueryRow[]> {
  if (!method.planScan) {
    return method.scan(request, context);
  }

  const planned = toPlannedScanRequest(request);
  const decision = method.planScan(planned, context);
  if (decision.reject) {
    throw new Error(decision.reject.message);
  }

  const split = splitScanRequest(request, decision);
  const remoteRows = await method.scan(split.remote, context);
  if (!split.residual) {
    return remoteRows;
  }

  return scanArrayRows(remoteRows, {
    table: request.table,
    ...(request.alias ? { alias: request.alias } : {}),
    select: request.select,
    ...(split.residual.where ? { where: split.residual.where } : {}),
    ...(split.residual.orderBy ? { orderBy: split.residual.orderBy } : {}),
    ...(split.residual.limit != null ? { limit: split.residual.limit } : {}),
    ...(split.residual.offset != null ? { offset: split.residual.offset } : {}),
  });
}

async function executePlannedLookup<TContext>(
  method: TableMethodsMap<TContext>[string],
  request: TableLookupRequest,
  context: TContext,
): Promise<QueryRow[]> {
  if (!method.lookup) {
    return [];
  }
  if (!method.planLookup) {
    return method.lookup(request, context);
  }

  const planned: PlannedLookupRequest = {
    table: request.table,
    ...(request.alias ? { alias: request.alias } : {}),
    key: request.key,
    values: request.values,
    select: request.select,
    ...(request.where
      ? {
          where: request.where.map((clause, index) => ({
            id: `where_${index}`,
            clause,
          })),
        }
      : {}),
  };
  const decision = method.planLookup(planned, context);
  if (decision.reject) {
    throw new Error(decision.reject.message);
  }

  const split = splitLookupRequest(request, decision);
  const remoteRows = await method.lookup(split.remote, context);
  if (!split.residual?.where?.length) {
    return remoteRows;
  }

  return scanArrayRows(remoteRows, {
    table: request.table,
    ...(request.alias ? { alias: request.alias } : {}),
    select: request.select,
    where: split.residual.where,
  });
}

function toPlannedScanRequest(request: TableScanRequest): PlannedScanRequest {
  return {
    table: request.table,
    ...(request.alias ? { alias: request.alias } : {}),
    select: request.select,
    ...(request.where
      ? {
          where: request.where.map((clause, index) => ({
            id: `where_${index}`,
            clause,
          })),
        }
      : {}),
    ...(request.orderBy
      ? {
          orderBy: request.orderBy.map((term, index) => ({
            id: `order_${index}`,
            term,
          })),
        }
      : {}),
    ...(request.limit != null ? { limit: request.limit } : {}),
    ...(request.offset != null ? { offset: request.offset } : {}),
  };
}

function splitScanRequest(
  request: TableScanRequest,
  decision: ScanPlanDecision,
): {
  remote: TableScanRequest;
  residual?: {
    where?: ScanFilterClause[];
    orderBy?: ScanOrderBy[];
    limit?: number;
    offset?: number;
  };
} {
  if (decision.mode === "remote_residual") {
    return {
      remote: {
        table: request.table,
        ...(request.alias ? { alias: request.alias } : {}),
        select: request.select,
        ...(decision.remote?.where ? { where: decision.remote.where } : {}),
        ...(decision.remote?.orderBy ? { orderBy: decision.remote.orderBy } : {}),
        ...(decision.remote?.limit != null ? { limit: decision.remote.limit } : {}),
        ...(decision.remote?.offset != null ? { offset: decision.remote.offset } : {}),
      },
      ...(decision.residual
        ? {
            residual: {
              ...(decision.residual.where ? { where: decision.residual.where } : {}),
              ...(decision.residual.orderBy ? { orderBy: decision.residual.orderBy } : {}),
              ...(decision.residual.limit != null ? { limit: decision.residual.limit } : {}),
              ...(decision.residual.offset != null ? { offset: decision.residual.offset } : {}),
            },
          }
        : {}),
    };
  }

  const planned = toPlannedScanRequest(request);
  const whereIds = new Set(decision.whereIds ?? []);
  const orderByIds = new Set(decision.orderByIds ?? []);
  const remoteWhere = planned.where
    ?.filter((term) => whereIds.has(term.id))
    .map((term) => term.clause);
  const residualWhere = planned.where
    ?.filter((term) => !whereIds.has(term.id))
    .map((term) => term.clause);
  const remoteOrderBy = planned.orderBy
    ?.filter((term) => orderByIds.has(term.id))
    .map((term) => term.term);
  const residualOrderBy = planned.orderBy
    ?.filter((term) => !orderByIds.has(term.id))
    .map((term) => term.term);

  return {
    remote: {
      table: request.table,
      ...(request.alias ? { alias: request.alias } : {}),
      select: request.select,
      ...(remoteWhere && remoteWhere.length > 0 ? { where: remoteWhere } : {}),
      ...(remoteOrderBy && remoteOrderBy.length > 0 ? { orderBy: remoteOrderBy } : {}),
      ...(decision.limitOffset !== "residual" && request.limit != null ? { limit: request.limit } : {}),
      ...(decision.limitOffset !== "residual" && request.offset != null ? { offset: request.offset } : {}),
    },
    ...((residualWhere && residualWhere.length > 0) ||
    (residualOrderBy && residualOrderBy.length > 0) ||
    decision.limitOffset === "residual"
      ? {
          residual: {
            ...(residualWhere && residualWhere.length > 0 ? { where: residualWhere } : {}),
            ...(residualOrderBy && residualOrderBy.length > 0 ? { orderBy: residualOrderBy } : {}),
            ...(decision.limitOffset === "residual" && request.limit != null
              ? { limit: request.limit }
              : {}),
            ...(decision.limitOffset === "residual" && request.offset != null
              ? { offset: request.offset }
              : {}),
          },
        }
      : {}),
  };
}

function splitLookupRequest(
  request: TableLookupRequest,
  decision: LookupPlanDecision,
): {
  remote: TableLookupRequest;
  residual?: {
    where?: ScanFilterClause[];
  };
} {
  if (decision.mode === "remote_residual") {
    return {
      remote: {
        table: request.table,
        ...(request.alias ? { alias: request.alias } : {}),
        key: request.key,
        values: request.values,
        select: request.select,
        ...(decision.remote?.where ? { where: decision.remote.where } : {}),
      },
      ...(decision.residual?.where ? { residual: { where: decision.residual.where } } : {}),
    };
  }

  const whereIds = new Set(decision.whereIds ?? []);
  const plannedWhere =
    request.where?.map((clause, index) => ({
      id: `where_${index}`,
      clause,
    })) ?? [];
  const remoteWhere = plannedWhere.filter((term) => whereIds.has(term.id)).map((term) => term.clause);
  const residualWhere = plannedWhere
    .filter((term) => !whereIds.has(term.id))
    .map((term) => term.clause);

  return {
    remote: {
      table: request.table,
      ...(request.alias ? { alias: request.alias } : {}),
      key: request.key,
      values: request.values,
      select: request.select,
      ...(remoteWhere.length > 0 ? { where: remoteWhere } : {}),
    },
    ...(residualWhere.length > 0 ? { residual: { where: residualWhere } } : {}),
  };
}

async function executePlannedAggregate<TContext>(
  method: NonNullable<TableMethodsMap<TContext>[string]>,
  request: TableAggregateRequest,
  context: TContext,
): Promise<QueryRow[]> {
  if (!method.aggregate) {
    return [];
  }

  if (!method.planAggregate) {
    return method.aggregate(request, context);
  }

  const planned = toPlannedAggregateRequest(request);
  const decision = method.planAggregate(planned, context);
  if (decision.reject) {
    throw new Error(decision.reject.message);
  }

  const split = splitAggregateRequest(request, decision);
  if (!split.residual) {
    return method.aggregate(split.remote, context);
  }

  const scannedRows = await method.scan(
    {
      table: request.table,
      ...(request.alias ? { alias: request.alias } : {}),
      select: collectAggregateScanColumns(request),
      ...(split.remote.where?.length ? { where: split.remote.where } : {}),
    },
    context,
  );

  return aggregateArrayRows(scannedRows, request);
}

export function queryWithMethods<TContext>(input: {
  schema: SchemaDefinition;
  methods: TableMethodsMap<TContext>;
  context: TContext;
  sql: string;
  queryGuardrails?: Partial<QueryGuardrails>;
  constraintValidation?: ConstraintValidationOptions;
}): Promise<QueryRow[]> {
  return createExecutableMethodsSchema(input.schema, input.methods).query({
    context: input.context,
    sql: input.sql,
    ...(input.queryGuardrails ? { queryGuardrails: input.queryGuardrails } : {}),
    ...(input.constraintValidation ? { constraintValidation: input.constraintValidation } : {}),
  });
}

function toPlannedAggregateRequest(request: TableAggregateRequest): PlannedAggregateRequest {
  return {
    table: request.table,
    ...(request.alias ? { alias: request.alias } : {}),
    ...(request.where
      ? {
          where: request.where.map((clause, index) => ({
            id: `where_${index}`,
            clause,
          })),
        }
      : {}),
    ...(request.groupBy?.length ? { groupBy: request.groupBy } : {}),
    metrics: request.metrics.map((metric, index) => ({
      id: `metric_${index}`,
      metric,
    })),
    ...(request.limit != null ? { limit: request.limit } : {}),
  };
}

function splitAggregateRequest(
  request: TableAggregateRequest,
  decision: AggregatePlanDecision,
): {
  remote: TableAggregateRequest;
  residual?: {
    where?: ScanFilterClause[];
    groupBy?: string[];
    metrics?: TableAggregateMetric[];
    limit?: number;
  };
} {
  if (decision.mode === "remote_residual") {
    return {
      remote: {
        table: request.table,
        ...(request.alias ? { alias: request.alias } : {}),
        ...(decision.remote?.where ? { where: decision.remote.where } : {}),
        ...(decision.remote?.groupBy ? { groupBy: decision.remote.groupBy } : {}),
        metrics: decision.remote?.metrics ?? [],
        ...(decision.remote?.limit != null ? { limit: decision.remote.limit } : {}),
      },
      ...(decision.residual
        ? {
            residual: {
              ...(decision.residual.where ? { where: decision.residual.where } : {}),
              ...(decision.residual.groupBy ? { groupBy: decision.residual.groupBy } : {}),
              ...(decision.residual.metrics ? { metrics: decision.residual.metrics } : {}),
              ...(decision.residual.limit != null ? { limit: decision.residual.limit } : {}),
            },
          }
        : {}),
    };
  }

  const planned = toPlannedAggregateRequest(request);
  const whereIds = new Set(decision.whereIds ?? []);
  const metricIds = new Set(decision.metricIds ?? []);
  const remoteWhere = planned.where
    ?.filter((term) => whereIds.has(term.id))
    .map((term) => term.clause);
  const residualWhere = planned.where
    ?.filter((term) => !whereIds.has(term.id))
    .map((term) => term.clause);
  const remoteMetrics = planned.metrics
    .filter((term) => metricIds.has(term.id))
    .map((term) => term.metric);
  const residualMetrics = planned.metrics
    .filter((term) => !metricIds.has(term.id))
    .map((term) => term.metric);

  return {
    remote: {
      table: request.table,
      ...(request.alias ? { alias: request.alias } : {}),
      ...(remoteWhere?.length ? { where: remoteWhere } : {}),
      ...(decision.groupBy !== "residual" && request.groupBy?.length
        ? { groupBy: request.groupBy }
        : {}),
      metrics: remoteMetrics,
      ...(decision.limit !== "residual" && request.limit != null ? { limit: request.limit } : {}),
    },
    ...((residualWhere?.length ?? 0) > 0 ||
    residualMetrics.length > 0 ||
    decision.groupBy === "residual" ||
    decision.limit === "residual"
      ? {
          residual: {
            ...(residualWhere?.length ? { where: residualWhere } : {}),
            ...(decision.groupBy === "residual" && request.groupBy?.length
              ? { groupBy: request.groupBy }
              : {}),
            ...(residualMetrics.length ? { metrics: residualMetrics } : {}),
            ...(decision.limit === "residual" && request.limit != null ? { limit: request.limit } : {}),
          },
        }
      : {}),
  };
}

function collectAggregateScanColumns(request: TableAggregateRequest): string[] {
  const columns = new Set<string>();

  for (const clause of request.where ?? []) {
    columns.add(clause.column);
  }
  for (const column of request.groupBy ?? []) {
    columns.add(column);
  }
  for (const metric of request.metrics) {
    if (metric.column) {
      columns.add(metric.column);
    }
  }

  return [...columns];
}

export function createMethodsSession<TContext>(input: {
  schema: SchemaDefinition;
  methods: TableMethodsMap<TContext>;
  context: TContext;
  sql: string;
  queryGuardrails?: Partial<QueryGuardrails>;
  constraintValidation?: ConstraintValidationOptions;
  options?: QuerySessionOptions;
}) {
  return createExecutableMethodsSchema(input.schema, input.methods).createSession({
    context: input.context,
    sql: input.sql,
    ...(input.queryGuardrails ? { queryGuardrails: input.queryGuardrails } : {}),
    ...(input.constraintValidation ? { constraintValidation: input.constraintValidation } : {}),
    ...(input.options ? { options: input.options } : {}),
  });
}
