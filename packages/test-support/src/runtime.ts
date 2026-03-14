import Database from "better-sqlite3";

import { Result } from "better-result";
import { describe, expect, it } from "vitest";
import {
  stringifyUnknownValue,
  type DataEntityColumnMap,
  type RelNode,
  type RelScanNode,
} from "@tupl/foundation";
import {
  bindAdapterEntities,
  createDataEntityHandle,
  extractSimpleRelScanRequest,
  type ProviderAdapter,
  type ProvidersMap,
} from "@tupl/provider-kit";
import type {
  LookupManyCapableProviderAdapter,
  ProviderLookupManyRequest,
} from "@tupl/provider-kit/shapes";
import {
  type ConstraintValidationOptions,
  createExecutableSchema,
  type ExecutableSchema,
  type QueryGuardrails,
} from "@tupl/runtime";
import {
  createExecutableSchemaSession,
  type ExecutableSchemaSessionInput,
  type QuerySessionOptions,
} from "@tupl/runtime/session";
import {
  createSchemaBuilder,
  getNormalizedTableBinding,
  isNormalizedSourceColumnBinding,
  toSqlDDL,
  type AggregatePlanDecision,
  type LookupPlanDecision,
  type NormalizedColumnBinding,
  type PlannedAggregateRequest,
  type PlannedLookupRequest,
  type PlannedScanRequest,
  type QueryRow,
  type ScanFilterClause,
  type ScanOrderBy,
  type ScanPlanDecision,
  type SchemaColumnLensDefinition,
  type SchemaDefinition,
  type TableAggregateMetric,
  type TableAggregateRequest,
  type TableColumnDefinition,
  type TableLookupRequest,
  type TableMethodsMap,
  type TableName,
  type TableScanRequest,
} from "@tupl/schema-model";

import { aggregateArrayRows, scanArrayRows } from "./methods";

/**
 * Runtime test support owns shared execution harnesses and parity helpers for internal tests.
 * It is stable only inside this workspace and must not be imported by product source.
 */
type ProviderInput<TContext> = {
  name?: string;
  entities?: Record<string, unknown>;
  capabilityAtoms?: readonly string[];
  fallbackPolicy?: unknown;
  canExecute(rel: RelNode, context: TContext): unknown;
  estimate?(rel: RelNode, context: TContext): unknown;
  compile?(rel: RelNode, context: TContext): unknown;
  describeCompiledPlan?(plan: unknown, context: TContext): unknown;
  execute?(plan: unknown, context: TContext): unknown;
} & Partial<LookupManyCapableProviderAdapter<TContext>>;

export function finalizeProviders<TContext>(
  providers: Record<string, ProviderInput<TContext>>,
): Record<string, ProviderAdapter<TContext>> {
  for (const [providerName, adapter] of Object.entries(providers)) {
    const boundAdapter = adapter as ProviderAdapter<TContext>;
    if (!boundAdapter.name) {
      boundAdapter.name = providerName;
    }
    bindAdapterEntities(boundAdapter);
  }

  return providers as Record<string, ProviderAdapter<TContext>>;
}

function toEntityColumns(
  columns: Record<string, TableColumnDefinition>,
): DataEntityColumnMap<string> {
  return Object.fromEntries(
    Object.entries(columns).map(([columnName, definition]) => [
      columnName,
      toEntityColumnMetadata(columnName, definition),
    ]),
  );
}

function toEntityColumnsFromBindings(
  bindings: Record<string, NormalizedColumnBinding>,
  fallbackDefinitions?: Record<string, TableColumnDefinition>,
): DataEntityColumnMap<string> {
  return Object.fromEntries(
    Object.entries(bindings).flatMap(([columnName, binding]) =>
      isNormalizedSourceColumnBinding(binding)
        ? [
            [
              columnName,
              toEntityColumnMetadata(
                columnName,
                binding.definition ?? fallbackDefinitions?.[columnName],
                binding.source,
              ),
            ] as const,
          ]
        : [],
    ),
  );
}

function toEntityColumnMetadata(
  columnName: string,
  definition?: TableColumnDefinition,
  source = columnName,
) {
  if (!definition) {
    return { source };
  }
  if (typeof definition === "string") {
    return {
      source,
      type: definition,
    };
  }
  return {
    source,
    type: definition.type,
    ...(definition.nullable != null ? { nullable: definition.nullable } : {}),
    ...(definition.primaryKey != null ? { primaryKey: definition.primaryKey } : {}),
    ...(definition.unique != null ? { unique: definition.unique } : {}),
    ...(definition.enum ? { enum: definition.enum } : {}),
    ...(definition.physicalType ? { physicalType: definition.physicalType } : {}),
    ...(definition.physicalDialect ? { physicalDialect: definition.physicalDialect } : {}),
  };
}

function toLensDefinition(
  columnName: string,
  definition: TableColumnDefinition,
): SchemaColumnLensDefinition {
  return toLensDefinitionFromSource(definition, columnName);
}

function toLensDefinitionFromSource(
  definition: TableColumnDefinition,
  source: string,
): SchemaColumnLensDefinition {
  if (typeof definition === "string") {
    return {
      source,
      type: definition,
    };
  }

  const lens: SchemaColumnLensDefinition = {
    source,
  };
  lens.type = definition.type;
  if (definition.nullable != null) {
    lens.nullable = definition.nullable;
  }
  if (definition.primaryKey != null) {
    lens.primaryKey = definition.primaryKey;
  }
  if (definition.unique != null) {
    lens.unique = definition.unique;
  }
  if (definition.enum) {
    lens.enum = definition.enum;
  }
  if (definition.enumFrom) {
    lens.enumFrom = definition.enumFrom;
  }
  if (definition.enumMap) {
    lens.enumMap = definition.enumMap;
  }
  if (definition.physicalType) {
    lens.physicalType = definition.physicalType;
  }
  if (definition.physicalDialect) {
    lens.physicalDialect = definition.physicalDialect;
  }
  if (definition.foreignKey) {
    lens.foreignKey = definition.foreignKey;
  }
  if (definition.description) {
    lens.description = definition.description;
  }
  return lens;
}

function getCalculatedColumnMethodName(definition: TableColumnDefinition): string {
  const type = typeof definition === "string" ? definition : definition.type;
  return type === "text" ? "string" : type;
}

function getCalculatedColumnOptions(
  binding: Extract<NormalizedColumnBinding, { kind: "expr" }>,
  definition: TableColumnDefinition,
) {
  if (typeof definition === "string") {
    return binding.coerce ? { coerce: binding.coerce } : {};
  }

  return {
    ...(definition.nullable != null ? { nullable: definition.nullable } : {}),
    ...(definition.physicalType ? { physicalType: definition.physicalType } : {}),
    ...(definition.physicalDialect ? { physicalDialect: definition.physicalDialect } : {}),
    ...(definition.foreignKey ? { foreignKey: definition.foreignKey } : {}),
    ...(definition.description ? { description: definition.description } : {}),
    ...(binding.coerce ? { coerce: binding.coerce } : {}),
  };
}

export function createExecutableSchemaFromProviders<TContext, TSchema extends SchemaDefinition>(
  schema: TSchema,
  providers: Record<string, ProviderInput<TContext>>,
) {
  const providerEntries = Object.entries(providers);
  const singleProviderName = providerEntries.length === 1 ? providerEntries[0]?.[0] : undefined;

  for (const [providerName, adapter] of providerEntries) {
    const boundAdapter = adapter as ProviderAdapter<TContext>;
    if (!boundAdapter.name) {
      boundAdapter.name = providerName;
    }
    boundAdapter.entities ??= {};
  }

  const builder = createSchemaBuilder<TContext>();

  for (const [tableName, tableDefinition] of Object.entries(schema.tables)) {
    const binding = getNormalizedTableBinding(schema, tableName);
    if (binding?.kind === "view") {
      builder.view(
        tableName,
        ((context: TContext) => binding.rel(context)) as any,
        {
          columns: () =>
            Object.fromEntries(
              Object.entries(binding.columnBindings).flatMap(([columnName, columnBinding]) =>
                isNormalizedSourceColumnBinding(columnBinding)
                  ? [
                      [
                        columnName,
                        {
                          source: columnBinding.source,
                          ...(typeof columnBinding.definition === "string"
                            ? { type: columnBinding.definition }
                            : (columnBinding.definition ?? {})),
                          ...(columnBinding.coerce ? { coerce: columnBinding.coerce } : {}),
                        },
                      ] as const,
                    ]
                  : [],
              ),
            ),
          ...("constraints" in tableDefinition && tableDefinition.constraints
            ? { constraints: tableDefinition.constraints }
            : {}),
        } as any,
      );
      continue;
    }

    const providerName = binding?.provider ?? tableDefinition.provider ?? singleProviderName;
    if (!providerName) {
      throw new Error(
        `Table ${tableName} must declare table.provider when more than one provider is involved.`,
      );
    }

    const adapter = providers[providerName];
    if (!adapter) {
      throw new Error(`No provider registered for table ${tableName}: ${providerName}`);
    }
    const boundAdapter = adapter as ProviderAdapter<TContext>;

    if (!boundAdapter.entities?.[tableName]) {
      boundAdapter.entities ??= {};
      boundAdapter.entities[tableName] = createDataEntityHandle({
        entity: binding?.kind === "physical" ? binding.entity : tableName,
        provider: providerName,
        adapter: boundAdapter,
        columns:
          binding?.kind === "physical"
            ? toEntityColumnsFromBindings(binding.columnBindings, tableDefinition.columns)
            : toEntityColumns(tableDefinition.columns),
      });
    }

    bindAdapterEntities(boundAdapter);

    builder.table(tableName, boundAdapter.entities[tableName], {
      columns: ({ col }) =>
        Object.fromEntries(
          binding?.kind === "physical"
            ? Object.entries(binding.columnBindings).map(([columnName, columnBinding]) => {
                if (isNormalizedSourceColumnBinding(columnBinding)) {
                  return [
                    columnName,
                    {
                      ...toLensDefinitionFromSource(
                        columnBinding.definition ?? tableDefinition.columns[columnName] ?? "text",
                        columnBinding.source,
                      ),
                      ...(columnBinding.coerce ? { coerce: columnBinding.coerce } : {}),
                    },
                  ] as const;
                }

                if (columnBinding.kind !== "expr") {
                  throw new Error(`Unsupported column binding kind for ${tableName}.${columnName}`);
                }

                const definition =
                  columnBinding.definition ?? tableDefinition.columns[columnName] ?? "text";
                const methodName = getCalculatedColumnMethodName(definition);
                const calcMethod = (
                  col as unknown as Record<string, (expr: any, options?: any) => unknown>
                )[methodName];
                if (typeof calcMethod !== "function") {
                  throw new Error(
                    `Unsupported calculated column type for ${tableName}.${columnName}`,
                  );
                }

                return [
                  columnName,
                  calcMethod(
                    columnBinding.expr,
                    getCalculatedColumnOptions(columnBinding, definition),
                  ),
                ] as const;
              })
            : Object.entries(tableDefinition.columns).map(([columnName, definition]) => [
                columnName,
                toLensDefinition(columnName, definition),
              ]),
        ) as Record<string, any>,
      ...(tableDefinition.constraints ? { constraints: tableDefinition.constraints } : {}),
    });
  }

  return createExecutableSchema(builder);
}

export function createMethodsProvider<TContext>(
  schema: SchemaDefinition,
  methods: TableMethodsMap<TContext>,
  providerName = "memory",
): ProviderAdapter<TContext> & LookupManyCapableProviderAdapter<TContext> {
  const adapter: ProviderAdapter<TContext> & LookupManyCapableProviderAdapter<TContext> = {
    name: providerName,
    entities: {},
    canExecute(rel) {
      const executable = extractMethodsExecutableRel(rel);
      if (!executable) {
        return false;
      }

      const method = methods[executable.table];
      if (!method) {
        return false;
      }

      return executable.kind === "scan" ? !!method.scan : !!method.aggregate;
    },
    async compile(rel) {
      return Result.ok({
        provider: providerName,
        kind: "rel",
        payload: rel,
      });
    },
    async execute(plan, context) {
      if (plan.kind !== "rel") {
        return Result.err(new Error(`Unsupported methods provider compiled plan: ${plan.kind}`));
      }

      const rel = plan.payload as RelNode;
      const executable = extractMethodsExecutableRel(rel);
      if (!executable) {
        return Result.err(new Error("Methods-based provider does not support this rel fragment."));
      }

      const method = methods[executable.table];
      if (!method) {
        return Result.err(new Error(`No table methods registered for table: ${executable.table}`));
      }

      if (executable.kind === "scan") {
        if (!method.scan) {
          return Result.err(
            new Error(`No table scan method registered for table: ${executable.table}`),
          );
        }
        return Result.ok(await executePlannedScan(method, executable.request, context));
      }

      if (!method.aggregate) {
        return Result.err(
          new Error(`No aggregate method registered for table: ${executable.table}`),
        );
      }
      return Result.ok(await executePlannedAggregate(method, executable.request, context));
    },
    async lookupMany(request: ProviderLookupManyRequest, context: TContext) {
      const method = methods[request.table];
      if (!method?.lookup) {
        return Result.ok([]);
      }

      return Result.ok(
        await executePlannedLookup(
          method,
          {
            table: request.table,
            ...(request.alias ? { alias: request.alias } : {}),
            key: request.key,
            values: request.keys,
            select: request.select,
            ...(request.where ? { where: request.where } : {}),
          },
          context,
        ),
      );
    },
  };

  for (const tableName of Object.keys(schema.tables)) {
    adapter.entities![tableName] = createDataEntityHandle({
      entity: tableName,
      provider: providerName,
      adapter,
      columns: toEntityColumns(schema.tables[tableName]!.columns),
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

function extractMethodsExecutableRel(rel: RelNode):
  | {
      kind: "scan";
      table: string;
      request: TableScanRequest;
    }
  | {
      kind: "aggregate";
      table: string;
      request: TableAggregateRequest;
    }
  | null {
  if (rel.kind === "scan") {
    return {
      kind: "scan",
      table: rel.table,
      request: toMethodsScanRequest(rel),
    };
  }

  if (rel.kind !== "aggregate") {
    return null;
  }

  const extracted = extractAggregateMethodsInput(rel.input);
  if (!extracted) {
    return null;
  }

  return {
    kind: "aggregate",
    table: extracted.scan.table,
    request: {
      table: extracted.scan.table,
      ...(extracted.scan.alias ? { alias: extracted.scan.alias } : {}),
      ...(extracted.where.length > 0 || (extracted.scan.where?.length ?? 0) > 0
        ? {
            where: [...(extracted.scan.where ?? []), ...extracted.where],
          }
        : {}),
      ...(rel.groupBy.length > 0
        ? {
            groupBy: rel.groupBy.map((column) => column.column),
          }
        : {}),
      metrics: rel.metrics.map((metric) => ({
        fn: metric.fn,
        as: metric.as,
        ...(metric.distinct ? { distinct: true } : {}),
        ...(metric.column ? { column: metric.column.column } : {}),
      })),
    },
  };
}

function toMethodsScanRequest(scan: RelScanNode): TableScanRequest {
  return {
    table: scan.table,
    ...(scan.alias ? { alias: scan.alias } : {}),
    select: scan.select,
    ...(scan.where ? { where: scan.where } : {}),
    ...(scan.orderBy ? { orderBy: scan.orderBy } : {}),
    ...(scan.limit != null ? { limit: scan.limit } : {}),
    ...(scan.offset != null ? { offset: scan.offset } : {}),
  };
}

function extractAggregateMethodsInput(node: RelNode): {
  scan: RelScanNode;
  where: ScanFilterClause[];
} | null {
  const where: ScanFilterClause[] = [];
  let current = node;

  while (current.kind === "filter") {
    if (current.expr) {
      return null;
    }
    if (current.where) {
      where.push(...current.where);
    }
    current = current.input;
  }

  if (current.kind !== "scan") {
    return null;
  }

  if (current.orderBy?.length || current.limit != null || current.offset != null) {
    return null;
  }

  return {
    scan: current,
    where,
  };
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
      ...(decision.limitOffset !== "residual" && request.limit != null
        ? { limit: request.limit }
        : {}),
      ...(decision.limitOffset !== "residual" && request.offset != null
        ? { offset: request.offset }
        : {}),
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
  const remoteWhere = plannedWhere
    .filter((term) => whereIds.has(term.id))
    .map((term) => term.clause);
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
            ...(decision.limit === "residual" && request.limit != null
              ? { limit: request.limit }
              : {}),
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
  return createExecutableSchemaSession(createExecutableMethodsSchema(input.schema, input.methods), {
    context: input.context,
    sql: input.sql,
    ...(input.queryGuardrails ? { queryGuardrails: input.queryGuardrails } : {}),
    ...(input.constraintValidation ? { constraintValidation: input.constraintValidation } : {}),
    ...(input.options ? { options: input.options } : {}),
  });
}

export function createSessionFromExecutableSchema<TContext>(
  executableSchema: ExecutableSchema<TContext>,
  input: ExecutableSchemaSessionInput<TContext>,
) {
  return createExecutableSchemaSession(executableSchema, input);
}

export type RowsByTable<TSchema extends SchemaDefinition> = {
  [TTable in TableName<TSchema>]: QueryRow<TSchema, TTable>[];
};

export interface QueryHarness<TSchema extends SchemaDefinition, TContext> {
  schema: TSchema;
  executableSchema: ReturnType<typeof createExecutableSchemaFromProviders<TContext, TSchema>>;
  runTupl: (sql: string, context: TContext) => Promise<QueryRow[]>;
  runSqlite: (sql: string) => QueryRow[];
  runAgainstBoth: (
    sql: string,
    context: TContext,
  ) => Promise<{ actual: QueryRow[]; expected: QueryRow[] }>;
  close: () => void;
}

export function createQueryHarness<
  TSchema extends SchemaDefinition,
  TContext = Record<string, never>,
>(options: {
  schema: TSchema;
  rowsByTable: RowsByTable<TSchema>;
  providers?: ProvidersMap<TContext>;
}): QueryHarness<TSchema, TContext> {
  const schema = options.schema;
  const rowsByTable = options.rowsByTable as Record<string, QueryRow[]>;
  const controlDb = createControlDatabase(schema, options.rowsByTable);

  const providers = options.providers ?? {
    memory: createMemoryProvider<TContext>(rowsByTable),
  };
  const executableSchema = createExecutableSchemaFromProviders(schema, providers);

  return {
    schema,
    executableSchema,
    runTupl: (sql, context) => executableSchema.query({ context, sql }),
    runSqlite: (sql) => controlDb.prepare(sql).all() as QueryRow[],
    runAgainstBoth: async (sql, context) => {
      const actual = await executableSchema.query({ context, sql });

      const expected = controlDb.prepare(sql).all() as QueryRow[];
      return { actual, expected };
    },
    close: () => {
      controlDb.close();
    },
  };
}

export async function withQueryHarness<
  TSchema extends SchemaDefinition,
  TContext = Record<string, never>,
  TResult = void,
>(
  options: {
    schema: TSchema;
    rowsByTable: RowsByTable<TSchema>;
    providers?: ProvidersMap<TContext>;
  },
  fn: (harness: QueryHarness<TSchema, TContext>) => Promise<TResult>,
): Promise<TResult> {
  const harness = createQueryHarness<TSchema, TContext>(options);

  try {
    return await fn(harness);
  } finally {
    harness.close();
  }
}

function createControlDatabase<TSchema extends SchemaDefinition>(
  schema: TSchema,
  rowsByTable: RowsByTable<TSchema>,
): InstanceType<typeof Database> {
  const db = new Database(":memory:");
  db.exec(toSqlDDL(schema, { ifNotExists: true }));

  for (const [tableName, table] of Object.entries(schema.tables)) {
    const columns = Object.keys(table.columns);
    const quotedColumns = columns.map(quoteIdentifier).join(", ");
    const placeholders = columns.map(() => "?").join(", ");

    const insert = db.prepare(
      `INSERT INTO ${quoteIdentifier(tableName)} (${quotedColumns}) VALUES (${placeholders})`,
    );

    const rows = rowsByTable[tableName as keyof RowsByTable<TSchema>] as QueryRow[];
    const tx = db.transaction((batch: QueryRow[]) => {
      for (const row of batch) {
        insert.run(...columns.map((column) => normalizeSqliteValue(row[column])));
      }
    });

    tx(rows);
  }

  return db;
}

function quoteIdentifier(name: string): string {
  return `"${name.replaceAll('"', '""')}"`;
}

function normalizeSqliteValue(value: unknown): unknown {
  if (typeof value === "boolean") {
    return value ? 1 : 0;
  }

  if (value instanceof Date) {
    return value.toISOString();
  }

  return value ?? null;
}

function createMemoryProvider<TContext>(
  rowsByTable: Record<string, QueryRow[]>,
): ProviderAdapter<TContext> & LookupManyCapableProviderAdapter<TContext> {
  return {
    name: "memory",
    canExecute(rel) {
      return (
        extractSimpleRelScanRequest(rel) !== null ||
        extractMethodsExecutableRel(rel)?.kind === "aggregate"
      );
    },
    async compile(rel) {
      return Result.ok({
        provider: "memory",
        kind: "rel",
        payload: rel,
      });
    },
    async execute(plan) {
      if (plan.kind !== "rel") {
        return Result.err(new Error(`Unsupported memory provider compiled plan: ${plan.kind}`));
      }

      const rel = plan.payload as RelNode;
      const directScan = extractSimpleRelScanRequest(rel);
      if (directScan) {
        return Result.ok(scanRows(rowsByTable[directScan.table] ?? [], directScan));
      }

      const executable = extractMethodsExecutableRel(rel);
      if (!executable || executable.kind !== "aggregate") {
        return Result.err(new Error("Unsupported memory provider rel fragment."));
      }
      return Result.ok(aggregateArrayRows(rowsByTable[executable.table] ?? [], executable.request));
    },
    async lookupMany(request: ProviderLookupManyRequest) {
      const scanRequest: TableScanRequest = {
        table: request.table,
        select: request.select,
        where: [
          ...(request.where ?? []),
          {
            op: "in",
            column: request.key,
            values: request.keys,
          } as ScanFilterClause,
        ],
      };

      return Result.ok(scanRows(rowsByTable[request.table] ?? [], scanRequest));
    },
  };
}

function scanRows(rows: QueryRow[], request: TableScanRequest): QueryRow[] {
  const normalizedRows = rows.map((row) => {
    const next: QueryRow = {};
    for (const [key, value] of Object.entries(row)) {
      next[key] = value instanceof Date ? value.toISOString() : value;
    }
    return next;
  });
  let out = normalizedRows.filter((row) => matchesFilters(row, request.where ?? []));

  if (request.orderBy && request.orderBy.length > 0) {
    out = [...out].sort((left, right) => {
      for (const term of request.orderBy ?? []) {
        const leftValue = left[term.column] ?? null;
        const rightValue = right[term.column] ?? null;
        if (leftValue === rightValue) {
          continue;
        }

        if (leftValue == null) {
          return term.direction === "asc" ? -1 : 1;
        }
        if (rightValue == null) {
          return term.direction === "asc" ? 1 : -1;
        }

        const comparison = stringifyUnknownValue(leftValue).localeCompare(
          stringifyUnknownValue(rightValue),
        );
        if (comparison !== 0) {
          return term.direction === "asc" ? comparison : -comparison;
        }
      }

      return 0;
    });
  }

  if (request.offset != null) {
    out = out.slice(request.offset);
  }

  if (request.limit != null) {
    out = out.slice(0, request.limit);
  }

  return out.map((row) => {
    const projected: QueryRow = {};
    for (const column of request.select) {
      projected[column] = row[column] ?? null;
    }
    return projected;
  });
}

function matchesFilters(row: QueryRow, filters: ScanFilterClause[]): boolean {
  for (const clause of filters) {
    const value = row[clause.column];

    switch (clause.op) {
      case "eq":
        if (value == null || clause.value == null || value !== clause.value) {
          return false;
        }
        break;
      case "neq":
        if (value == null || clause.value == null || value === clause.value) {
          return false;
        }
        break;
      case "gt":
        if (value == null || clause.value == null || compareNonNull(value, clause.value) <= 0) {
          return false;
        }
        break;
      case "gte":
        if (value == null || clause.value == null || compareNonNull(value, clause.value) < 0) {
          return false;
        }
        break;
      case "lt":
        if (value == null || clause.value == null || compareNonNull(value, clause.value) >= 0) {
          return false;
        }
        break;
      case "lte":
        if (value == null || clause.value == null || compareNonNull(value, clause.value) > 0) {
          return false;
        }
        break;
      case "in":
        if (value == null || !clause.values.filter((entry) => entry != null).includes(value)) {
          return false;
        }
        break;
      case "not_in":
        if (value == null || clause.values.filter((entry) => entry != null).includes(value)) {
          return false;
        }
        break;
      case "like":
        if (
          typeof value !== "string" ||
          typeof clause.value !== "string" ||
          !matchesLike(value, clause.value)
        ) {
          return false;
        }
        break;
      case "not_like":
        if (
          typeof value !== "string" ||
          typeof clause.value !== "string" ||
          matchesLike(value, clause.value)
        ) {
          return false;
        }
        break;
      case "is_distinct_from":
        if (value === clause.value) {
          return false;
        }
        break;
      case "is_not_distinct_from":
        if (value !== clause.value) {
          return false;
        }
        break;
      case "is_null":
        if (value != null) {
          return false;
        }
        break;
      case "is_not_null":
        if (value == null) {
          return false;
        }
        break;
    }
  }

  return true;
}

function matchesLike(value: string, pattern: string): boolean {
  const escaped = pattern
    .replace(/[.*+?^${}()|[\]\\]/g, "\\$&")
    .replace(/%/g, ".*")
    .replace(/_/g, ".");
  return new RegExp(`^${escaped}$`, "su").test(value);
}

function compareNonNull(left: unknown, right: unknown): number {
  if (typeof left === "number" && typeof right === "number") {
    return left === right ? 0 : left < right ? -1 : 1;
  }

  return stringifyUnknownValue(left).localeCompare(stringifyUnknownValue(right));
}

export interface ComplianceCase {
  name: string;
  sql: string;
  expectedRows?: QueryRow[];
}

const EMPTY_CONTEXT = {} as const;

export function registerParityCases<TSchema extends SchemaDefinition>(
  title: string,
  options: {
    schema: TSchema;
    rowsByTable: RowsByTable<TSchema>;
  },
  cases: ComplianceCase[],
): void {
  describe(title, () => {
    for (const testCase of cases) {
      it(testCase.name, async () => {
        const { actual, expected } = await withQueryHarness(options, (harness) =>
          harness.runAgainstBoth(testCase.sql, EMPTY_CONTEXT),
        );

        expect(actual).toEqual(expected);
        if (testCase.expectedRows) {
          expect(actual).toEqual(testCase.expectedRows);
        }
      });
    }
  });
}
