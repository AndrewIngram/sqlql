import {
  getTable,
  resolveColumnType,
  resolveTableQueryBehavior,
  type AggregateFunction,
  type QueryRow,
  type ScanFilterClause,
  type ScanOrderBy,
  type SchemaDefinition,
  type TableAggregateMetric,
  type TableAggregateRequest,
  type TableMethods,
  type TableMethodsMap,
  type TableScanRequest,
} from "./schema";
import { defaultSqlAstParser } from "./parser";

export interface SqlQuery {
  text: string;
}

export interface PlannedQuery {
  source: string;
  selectAll: boolean;
}

export interface QueryInput<TContext> {
  schema: SchemaDefinition;
  methods: TableMethodsMap<TContext>;
  context: TContext;
  sql: string;
}

interface SelectAst {
  with?: unknown;
  type?: unknown;
  distinct?: unknown;
  set_op?: unknown;
  _next?: unknown;
  from?: unknown;
  where?: unknown;
  having?: unknown;
  columns?: unknown;
  orderby?: unknown;
  limit?: unknown;
  groupby?: unknown;
}

interface TableBinding {
  table: string;
  alias: string;
  index: number;
  isCte: boolean;
}

interface JoinCondition {
  leftAlias: string;
  leftColumn: string;
  rightAlias: string;
  rightColumn: string;
}

interface ParsedJoin {
  alias: string;
  join: "inner" | "left" | "right" | "full";
  condition: JoinCondition;
}

interface SelectColumn {
  alias: string;
  column: string;
  output: string;
}

interface AggregateMetric {
  fn: AggregateFunction;
  output: string;
  signature: string;
  hidden: boolean;
  column?: {
    alias: string;
    column: string;
  };
  distinct: boolean;
}

interface SourceOrderColumn {
  kind: "source";
  alias: string;
  column: string;
  direction: "asc" | "desc";
}

interface OutputOrderColumn {
  kind: "output";
  output: string;
  direction: "asc" | "desc";
}

type OrderColumn = SourceOrderColumn | OutputOrderColumn;

interface LiteralFilter {
  alias: string;
  clause: ScanFilterClause;
}

interface AggregateOutputColumn {
  source: {
    alias: string;
    column: string;
  };
  output: string;
}

interface ParsedSelectQuery {
  bindings: TableBinding[];
  joins: ParsedJoin[];
  joinEdges: JoinCondition[];
  filters: LiteralFilter[];
  where?: unknown;
  whereColumns: Array<{
    alias: string;
    column: string;
  }>;
  wherePushdownSafe: boolean;
  having?: unknown;
  distinct: boolean;
  selectAll: boolean;
  selectColumns: SelectColumn[];
  scalarSelectItems: Array<{
    expr: unknown;
    output: string;
  }>;
  groupBy: Array<{
    alias: string;
    column: string;
  }>;
  aggregateMetrics: AggregateMetric[];
  aggregateOutputColumns: AggregateOutputColumn[];
  isAggregate: boolean;
  orderBy: OrderColumn[];
  limit?: number;
  offset?: number;
}

interface JoinedRowBundle {
  [alias: string]: QueryRow;
}

interface MetricAccumulator {
  count: number;
  sum: number;
  hasValue: boolean;
  min: unknown;
  max: unknown;
  distinctValues?: Set<string>;
}

export function parseSql(query: SqlQuery, schema: SchemaDefinition): PlannedQuery {
  const ast = astifySingleSelect(query.text);
  const parsed = parseSelectAst(ast, schema, new Map());
  const source = parsed.bindings[0];
  if (!source) {
    throw new Error("SELECT queries must include a FROM clause.");
  }

  return {
    source: source.table,
    selectAll: parsed.selectAll,
  };
}

export async function query<TContext>(input: QueryInput<TContext>): Promise<QueryRow[]> {
  const ast = astifySingleSelect(input.sql);
  return executeSelectAst(ast, input, new Map());
}

async function executeSelectAst<TContext>(
  ast: SelectAst,
  input: QueryInput<TContext>,
  parentCtes: Map<string, QueryRow[]>,
): Promise<QueryRow[]> {
  if (ast.type !== "select") {
    throw new Error("Only SELECT statements are currently supported.");
  }

  if (ast.set_op != null || ast._next != null) {
    return executeSetOperation(ast, input, parentCtes);
  }

  const cteRows = new Map(parentCtes);
  const rawCtes = Array.isArray(ast.with) ? ast.with : [];
  for (const rawCte of rawCtes) {
    if ((rawCte as { recursive?: unknown }).recursive === true) {
      throw new Error("Recursive CTEs are not yet supported.");
    }

    const cteName = readCteName(rawCte);
    const cteStatement = (rawCte as { stmt?: { ast?: unknown } }).stmt?.ast;
    if (!cteStatement || typeof cteStatement !== "object") {
      throw new Error(`Unable to parse CTE statement for: ${cteName}`);
    }

    const cteAst = cteStatement as SelectAst;
    if (cteAst.type !== "select") {
      throw new Error("Only SELECT CTE statements are currently supported.");
    }

    const rows = await executeSelectAst(cteAst, input, cteRows);
    cteRows.set(cteName, rows);
  }

  const parsed = parseSelectAst(ast, input.schema, cteRows);
  return executeParsedSelect(parsed, input, cteRows);
}

async function executeSetOperation<TContext>(
  ast: SelectAst,
  input: QueryInput<TContext>,
  parentCtes: Map<string, QueryRow[]>,
): Promise<QueryRow[]> {
  const operation = typeof ast.set_op === "string" ? ast.set_op.toLowerCase() : "";
  const nextRaw = readSetOperationNext(ast._next);
  const next =
    nextRaw && ast.with && !nextRaw.with
      ? {
          ...nextRaw,
          with: ast.with,
        }
      : nextRaw;
  if (!next) {
    throw new Error("Invalid set operation: missing right-hand SELECT.");
  }

  const leftAst = cloneSelectWithoutSetOperation(ast);
  const leftRows = await executeSelectAst(leftAst, input, parentCtes);
  const rightRows = await executeSelectAst(next, input, parentCtes);

  switch (operation) {
    case "union all":
      return [...leftRows, ...rightRows];
    case "union":
      return dedupeRows([...leftRows, ...rightRows]);
    case "intersect":
      return intersectRows(leftRows, rightRows);
    case "except":
    case "minus":
      return exceptRows(leftRows, rightRows);
    default:
      throw new Error(`Unsupported set operation: ${String(ast.set_op)}`);
  }
}

async function executeParsedSelect<TContext>(
  parsed: ParsedSelectQuery,
  input: QueryInput<TContext>,
  cteRows: Map<string, QueryRow[]>,
): Promise<QueryRow[]> {
  const rootBinding = parsed.bindings[0];
  if (!rootBinding) {
    throw new Error("SELECT queries must include a FROM clause.");
  }

  for (const binding of parsed.bindings) {
    if (binding.isCte) {
      continue;
    }

    getTable(input.schema, binding.table);
    if (!input.methods[binding.table]) {
      throw new Error(`No table methods registered for table: ${binding.table}`);
    }
  }

  if (parsed.isAggregate) {
    return executeAggregateSelect(parsed, input, cteRows);
  }

  const joinedRows = await executeJoinedRows(parsed, input, cteRows, {
    applyFinalSortAndLimit: !parsed.distinct,
  });
  const whereFiltered = await applyWhereFilter(joinedRows, parsed, input, cteRows);
  let projected = await projectResultRows(whereFiltered, parsed, input, cteRows);

  if (parsed.distinct) {
    projected = dedupeRows(projected);
    projected = applyProjectedSortLimit(projected, parsed);
  }

  return projected;
}

async function executeAggregateSelect<TContext>(
  parsed: ParsedSelectQuery,
  input: QueryInput<TContext>,
  cteRows: Map<string, QueryRow[]>,
): Promise<QueryRow[]> {
  let aggregateRows =
    (await tryRunAggregateRoute(parsed, input)) ??
    (await runLocalAggregate(parsed, input, cteRows));

  if (parsed.having) {
    aggregateRows = await applyHavingFilter(aggregateRows, parsed, input, cteRows);
  }

  if (parsed.distinct) {
    aggregateRows = dedupeRows(aggregateRows);
  }

  let out = aggregateRows;
  if (parsed.orderBy.length > 0) {
    out = applyOutputSort(out, parsed.orderBy);
  }

  if (parsed.offset != null) {
    out = out.slice(parsed.offset);
  }

  if (parsed.limit != null) {
    out = out.slice(0, parsed.limit);
  }

  return out.map((row) => projectAggregateOutputRow(row, parsed));
}

async function tryRunAggregateRoute<TContext>(
  parsed: ParsedSelectQuery,
  input: QueryInput<TContext>,
): Promise<QueryRow[] | null> {
  if (parsed.bindings.length !== 1 || parsed.joins.length > 0) {
    return null;
  }

  if (parsed.having) {
    return null;
  }

  if (!parsed.wherePushdownSafe) {
    return null;
  }

  const binding = parsed.bindings[0];
  if (!binding || binding.isCte) {
    return null;
  }

  const method = input.methods[binding.table];
  if (!method?.aggregate) {
    return null;
  }

  const filtersByAlias = groupFiltersByAlias(parsed.filters);
  const where = filtersByAlias.get(binding.alias) ?? [];

  if (parsed.groupBy.some((column) => column.alias !== binding.alias)) {
    return null;
  }

  if (
    parsed.aggregateMetrics.some((metric) => metric.column && metric.column.alias !== binding.alias)
  ) {
    return null;
  }

  const metrics: TableAggregateMetric[] = parsed.aggregateMetrics.map((metric) => ({
    fn: metric.fn,
    as: metric.output,
    ...(metric.column ? { column: metric.column.column } : {}),
    ...(metric.distinct ? { distinct: true } : {}),
  }));

  const request: TableAggregateRequest = {
    table: binding.table,
    alias: binding.alias,
    metrics,
  };

  if (where.length > 0) {
    request.where = where;
  }

  if (parsed.groupBy.length > 0) {
    request.groupBy = parsed.groupBy.map((column) => column.column);
  }

  if (parsed.orderBy.length === 0 && parsed.offset == null && parsed.limit != null) {
    request.limit = parsed.limit;
  }

  const rows = await method.aggregate(request, input.context);
  return rows.map((row) => normalizeAggregateRowFromRoute(row, parsed));
}

function normalizeAggregateRowFromRoute(row: QueryRow, parsed: ParsedSelectQuery): QueryRow {
  const out: QueryRow = {};

  for (const column of parsed.aggregateOutputColumns) {
    const direct = row[column.output];
    out[column.output] = direct ?? row[column.source.column] ?? null;
  }

  for (const metric of parsed.aggregateMetrics) {
    if (metric.hidden) {
      continue;
    }
    out[metric.output] = row[metric.output] ?? null;
  }

  return out;
}

async function runLocalAggregate<TContext>(
  parsed: ParsedSelectQuery,
  input: QueryInput<TContext>,
  cteRows: Map<string, QueryRow[]>,
): Promise<QueryRow[]> {
  const joinedRows = await executeJoinedRows(parsed, input, cteRows, {
    applyFinalSortAndLimit: false,
  });
  const whereFiltered = await applyWhereFilter(joinedRows, parsed, input, cteRows);

  return aggregateJoinedRows(whereFiltered, parsed);
}

function aggregateJoinedRows(rows: JoinedRowBundle[], parsed: ParsedSelectQuery): QueryRow[] {
  const groupSourceKeys = parsed.groupBy.map((column) =>
    sourceColumnKey(column.alias, column.column),
  );

  const groups = new Map<
    string,
    {
      groupValues: Map<string, unknown>;
      metricState: MetricAccumulator[];
    }
  >();

  for (const bundle of rows) {
    const groupValues = groupSourceKeys.map((key) => {
      const [alias, column] = key.split(".");
      if (!alias || !column) {
        return null;
      }
      return bundle[alias]?.[column] ?? null;
    });

    const groupKey = JSON.stringify(groupValues);
    let state = groups.get(groupKey);

    if (!state) {
      const stateGroupValues = new Map<string, unknown>();
      parsed.groupBy.forEach((column, index) => {
        stateGroupValues.set(
          sourceColumnKey(column.alias, column.column),
          groupValues[index] ?? null,
        );
      });

      state = {
        groupValues: stateGroupValues,
        metricState: parsed.aggregateMetrics.map((metric) => createMetricAccumulator(metric)),
      };

      groups.set(groupKey, state);
    }

    parsed.aggregateMetrics.forEach((metric, index) => {
      const accumulator = state.metricState[index];
      if (!accumulator) {
        return;
      }

      const value = metric.column
        ? (bundle[metric.column.alias]?.[metric.column.column] ?? null)
        : null;

      applyMetricValue(accumulator, metric, value);
    });
  }

  if (groups.size === 0 && parsed.groupBy.length === 0 && parsed.aggregateMetrics.length > 0) {
    const state = {
      groupValues: new Map<string, unknown>(),
      metricState: parsed.aggregateMetrics.map((metric) => createMetricAccumulator(metric)),
    };
    groups.set("__all__", state);
  }

  const out: QueryRow[] = [];

  for (const state of groups.values()) {
    const row: QueryRow = {};

    for (const column of parsed.aggregateOutputColumns) {
      const sourceKey = sourceColumnKey(column.source.alias, column.source.column);
      row[column.output] = state.groupValues.get(sourceKey) ?? null;
    }

    parsed.aggregateMetrics.forEach((metric, index) => {
      const accumulator = state.metricState[index];
      row[metric.output] = accumulator ? finalizeMetricValue(metric, accumulator) : null;
    });

    out.push(row);
  }

  return out;
}

function createMetricAccumulator(metric: AggregateMetric): MetricAccumulator {
  const accumulator: MetricAccumulator = {
    count: 0,
    sum: 0,
    hasValue: false,
    min: null,
    max: null,
  };

  if (metric.distinct) {
    accumulator.distinctValues = new Set<string>();
  }

  return accumulator;
}

function applyMetricValue(
  accumulator: MetricAccumulator,
  metric: AggregateMetric,
  value: unknown,
): void {
  if (metric.distinct) {
    const distinctKey = JSON.stringify(value);
    if (accumulator.distinctValues?.has(distinctKey)) {
      return;
    }
    accumulator.distinctValues?.add(distinctKey);
  }

  switch (metric.fn) {
    case "count": {
      if (!metric.column) {
        accumulator.count += 1;
      } else if (value != null) {
        accumulator.count += 1;
      }
      return;
    }
    case "sum": {
      if (value == null) {
        return;
      }
      accumulator.sum += toFiniteNumber(value, "SUM");
      accumulator.hasValue = true;
      return;
    }
    case "avg": {
      if (value == null) {
        return;
      }
      accumulator.sum += toFiniteNumber(value, "AVG");
      accumulator.count += 1;
      accumulator.hasValue = true;
      return;
    }
    case "min": {
      if (value == null) {
        return;
      }
      if (!accumulator.hasValue || compareNullableValues(value, accumulator.min) < 0) {
        accumulator.min = value;
      }
      accumulator.hasValue = true;
      return;
    }
    case "max": {
      if (value == null) {
        return;
      }
      if (!accumulator.hasValue || compareNullableValues(value, accumulator.max) > 0) {
        accumulator.max = value;
      }
      accumulator.hasValue = true;
      return;
    }
  }
}

function finalizeMetricValue(metric: AggregateMetric, accumulator: MetricAccumulator): unknown {
  switch (metric.fn) {
    case "count":
      return accumulator.count;
    case "sum":
      return accumulator.hasValue ? accumulator.sum : null;
    case "avg":
      return accumulator.count > 0 ? accumulator.sum / accumulator.count : null;
    case "min":
      return accumulator.hasValue ? accumulator.min : null;
    case "max":
      return accumulator.hasValue ? accumulator.max : null;
  }
}

function toFiniteNumber(value: unknown, functionName: string): number {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    throw new Error(`${functionName} expects numeric values.`);
  }

  return parsed;
}

async function executeJoinedRows<TContext>(
  parsed: ParsedSelectQuery,
  input: QueryInput<TContext>,
  cteRows: Map<string, QueryRow[]>,
  options: {
    applyFinalSortAndLimit: boolean;
  },
): Promise<JoinedRowBundle[]> {
  const rootBinding = parsed.bindings[0];
  if (!rootBinding) {
    throw new Error("SELECT queries must include a FROM clause.");
  }

  const canPushFinalSortAndLimitAll =
    options.applyFinalSortAndLimit &&
    parsed.bindings.length === 1 &&
    parsed.orderBy.every((term) => term.kind === "source" && term.alias === rootBinding.alias);

  const projectionByAlias = buildProjection(parsed, input.schema, cteRows);
  const filtersByAlias = groupFiltersByAlias(parsed.filters);
  const executionOrder = buildExecutionOrder(parsed.bindings, parsed.joinEdges, filtersByAlias);
  const rowsByAlias = new Map<string, QueryRow[]>();

  for (const alias of executionOrder) {
    const binding = parsed.bindings.find((candidate) => candidate.alias === alias);
    if (!binding) {
      throw new Error(`Unknown alias in execution order: ${alias}`);
    }

    const dependencyFilters = buildDependencyFilters(
      alias,
      parsed.joins,
      parsed.joinEdges,
      rowsByAlias,
    );
    const localFilters = filtersByAlias.get(alias) ?? [];

    if (dependencyFilters.some((filter) => filter.op === "in" && filter.values.length === 0)) {
      rowsByAlias.set(alias, []);
      continue;
    }

    const requestWhere: ScanFilterClause[] = [...localFilters, ...dependencyFilters];

    const canPushFinalSortAndLimit = canPushFinalSortAndLimitAll && alias === rootBinding.alias;

    const requestOrderBy: ScanOrderBy[] | undefined = canPushFinalSortAndLimit
      ? parsed.orderBy
          .filter((term): term is SourceOrderColumn => term.kind === "source")
          .map((term) => ({
            column: term.column,
            direction: term.direction,
          }))
      : undefined;

    let requestLimit = canPushFinalSortAndLimit ? parsed.limit : undefined;
    const requestOffset = canPushFinalSortAndLimit ? parsed.offset : undefined;

    if (!binding.isCte) {
      const tableBehavior = resolveTableQueryBehavior(input.schema, binding.table);
      const defaultMaxRows = tableBehavior.maxRows;

      if (requestLimit == null && defaultMaxRows != null) {
        requestLimit = defaultMaxRows;
      }

      if (requestLimit != null && defaultMaxRows != null && requestLimit > defaultMaxRows) {
        throw new Error(
          `Requested limit ${requestLimit} exceeds maxRows ${defaultMaxRows} for table ${binding.table}`,
        );
      }
    }

    const projection = projectionByAlias.get(alias);
    if (!projection) {
      throw new Error(`Unable to resolve projection columns for alias: ${alias}`);
    }

    const request: TableScanRequest = {
      table: binding.table,
      alias,
      select: [...projection],
    };

    if (requestWhere.length > 0) {
      request.where = requestWhere;
    }

    if (requestOrderBy && requestOrderBy.length > 0) {
      request.orderBy = requestOrderBy;
    }

    if (requestLimit != null) {
      request.limit = requestLimit;
    }

    if (requestOffset != null) {
      request.offset = requestOffset;
    }

    const rows = await runSourceScan(binding, request, cteRows, input);
    rowsByAlias.set(alias, rows);
  }

  let joinedRows = initializeJoinedRows(rowsByAlias, rootBinding.alias);
  for (const join of parsed.joins) {
    switch (join.join) {
      case "left":
        joinedRows = applyLeftJoin(joinedRows, join, rowsByAlias);
        break;
      case "right":
        joinedRows = applyRightJoin(joinedRows, join, rowsByAlias);
        break;
      case "full":
        joinedRows = applyFullJoin(joinedRows, join, rowsByAlias);
        break;
      default:
        joinedRows = applyInnerJoin(joinedRows, join, rowsByAlias);
    }
  }

  if (options.applyFinalSortAndLimit && !canPushFinalSortAndLimitAll) {
    if (parsed.orderBy.length > 0) {
      joinedRows = applyFinalSort(joinedRows, parsed.orderBy);
    }

    if (parsed.offset != null) {
      joinedRows = joinedRows.slice(parsed.offset);
    }

    if (parsed.limit != null) {
      joinedRows = joinedRows.slice(0, parsed.limit);
    }
  }

  return joinedRows;
}

async function runSourceScan<TContext>(
  binding: TableBinding,
  request: TableScanRequest,
  cteRows: Map<string, QueryRow[]>,
  input: QueryInput<TContext>,
): Promise<QueryRow[]> {
  if (binding.isCte) {
    const rows = cteRows.get(binding.table) ?? [];
    return scanRows(rows, request);
  }

  const method = input.methods[binding.table];
  if (!method) {
    throw new Error(`No table methods registered for table: ${binding.table}`);
  }

  return runScan(method, request, input.context);
}

function scanRows(rows: QueryRow[], request: TableScanRequest): QueryRow[] {
  let out = applyScanFilters(rows, request.where ?? []);

  if (request.orderBy && request.orderBy.length > 0) {
    out = [...out].sort((left, right) => {
      for (const term of request.orderBy ?? []) {
        const comparison = compareNullableValues(
          left[term.column] ?? null,
          right[term.column] ?? null,
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

function applyScanFilters(rows: QueryRow[], clauses: ScanFilterClause[]): QueryRow[] {
  let out = [...rows];

  for (const clause of clauses) {
    switch (clause.op) {
      case "eq":
        out = out.filter((row) => {
          if (clause.value == null) {
            return false;
          }

          const value = row[clause.column];
          return value != null && value === clause.value;
        });
        break;
      case "neq":
        out = out.filter((row) => {
          if (clause.value == null) {
            return false;
          }

          const value = row[clause.column];
          return value != null && value !== clause.value;
        });
        break;
      case "gt":
        out = out.filter((row) => {
          if (clause.value == null) {
            return false;
          }

          const value = row[clause.column];
          return value != null && compareNonNull(value, clause.value) > 0;
        });
        break;
      case "gte":
        out = out.filter((row) => {
          if (clause.value == null) {
            return false;
          }

          const value = row[clause.column];
          return value != null && compareNonNull(value, clause.value) >= 0;
        });
        break;
      case "lt":
        out = out.filter((row) => {
          if (clause.value == null) {
            return false;
          }

          const value = row[clause.column];
          return value != null && compareNonNull(value, clause.value) < 0;
        });
        break;
      case "lte":
        out = out.filter((row) => {
          if (clause.value == null) {
            return false;
          }

          const value = row[clause.column];
          return value != null && compareNonNull(value, clause.value) <= 0;
        });
        break;
      case "in": {
        const set = new Set(clause.values.filter((value) => value != null));
        out = out.filter((row) => {
          const value = row[clause.column];
          return value != null && set.has(value);
        });
        break;
      }
      case "is_null":
        out = out.filter((row) => row[clause.column] == null);
        break;
      case "is_not_null":
        out = out.filter((row) => row[clause.column] != null);
        break;
    }
  }

  return out;
}

async function runScan<TContext>(
  method: TableMethods<TContext>,
  request: TableScanRequest,
  context: TContext,
): Promise<QueryRow[]> {
  const dependencyFilters = request.where?.filter((clause) => clause.op === "in") ?? [];

  if (
    dependencyFilters.length === 1 &&
    method.lookup &&
    dependencyFilters[0] &&
    dependencyFilters[0].values.length > 0 &&
    request.orderBy == null &&
    request.limit == null &&
    request.offset == null
  ) {
    const lookup = dependencyFilters[0];
    if (!lookup) {
      return method.scan(request, context);
    }

    const nonDependencyFilters = request.where?.filter((clause) => clause !== lookup);
    const lookupRequest = {
      table: request.table,
      key: lookup.column,
      values: lookup.values,
      select: request.select,
    } as const;
    const fullLookupRequest: Parameters<NonNullable<typeof method.lookup>>[0] = {
      ...lookupRequest,
    };

    if (request.alias) {
      fullLookupRequest.alias = request.alias;
    }

    if (nonDependencyFilters && nonDependencyFilters.length > 0) {
      fullLookupRequest.where = nonDependencyFilters;
    }

    return method.lookup(fullLookupRequest, context);
  }

  return method.scan(request, context);
}

function parseSelectAst(
  ast: SelectAst,
  schema: SchemaDefinition,
  cteRows: Map<string, QueryRow[]>,
): ParsedSelectQuery {
  if (ast.type !== "select") {
    throw new Error("Only SELECT statements are currently supported.");
  }

  const rawFrom: unknown[] = Array.isArray(ast.from) ? ast.from : ast.from ? [ast.from] : [];
  if (rawFrom.length === 0) {
    throw new Error("SELECT queries must include a FROM clause.");
  }

  const bindings = rawFrom.map((entry: unknown, index: number) => {
    if (!entry || typeof entry !== "object" || !("table" in entry)) {
      throw new Error("Unsupported FROM clause entry.");
    }

    const table = (entry as { table?: unknown }).table;
    const alias = (entry as { as?: unknown }).as;
    if (typeof table !== "string" || table.length === 0) {
      throw new Error("Unable to resolve table name from query.");
    }

    return {
      table,
      alias: typeof alias === "string" && alias.length > 0 ? alias : table,
      index,
      isCte: cteRows.has(table),
    } as TableBinding;
  });

  const aliasToBinding = new Map(bindings.map((binding) => [binding.alias, binding]));

  const joins: ParsedJoin[] = [];
  const joinEdges: JoinCondition[] = [];

  for (let i = 1; i < rawFrom.length; i += 1) {
    const entry = rawFrom[i] as { join?: unknown; on?: unknown; as?: unknown; table?: unknown };
    const joinType = typeof entry.join === "string" ? entry.join.toUpperCase() : "";
    if (
      joinType !== "INNER JOIN" &&
      joinType !== "JOIN" &&
      joinType !== "LEFT JOIN" &&
      joinType !== "LEFT OUTER JOIN" &&
      joinType !== "RIGHT JOIN" &&
      joinType !== "RIGHT OUTER JOIN" &&
      joinType !== "FULL JOIN" &&
      joinType !== "FULL OUTER JOIN"
    ) {
      throw new Error(`Unsupported join type: ${String(entry.join ?? "unknown")}`);
    }

    const parsedJoin = parseJoinCondition(entry.on, bindings, aliasToBinding);
    const joinedAlias =
      typeof entry.as === "string" && entry.as.length > 0 ? entry.as : String(entry.table);

    joins.push({
      alias: joinedAlias,
      join:
        joinType === "LEFT JOIN" || joinType === "LEFT OUTER JOIN"
          ? "left"
          : joinType === "RIGHT JOIN" || joinType === "RIGHT OUTER JOIN"
            ? "right"
            : joinType === "FULL JOIN" || joinType === "FULL OUTER JOIN"
              ? "full"
              : "inner",
      condition: parsedJoin,
    });

    joinEdges.push(parsedJoin);
  }
  const where = ast.where;
  const whereColumns = collectColumnReferences(where, bindings, aliasToBinding);
  const filters: LiteralFilter[] = [];
  const wherePushdown = tryParseConjunctivePushdownFilters(where, bindings, aliasToBinding);
  if (wherePushdown) {
    filters.push(...wherePushdown.filters);
    joinEdges.push(...wherePushdown.joinEdges);
  }
  const wherePushdownSafe = where == null || wherePushdown != null;

  const groupBy = parseGroupBy(ast.groupby, bindings, aliasToBinding);

  const selectColumnsRaw: unknown = ast.columns;
  const selectAll =
    selectColumnsRaw === "*" ||
    (Array.isArray(selectColumnsRaw) &&
      selectColumnsRaw.length === 1 &&
      isStarColumn(selectColumnsRaw[0] as { expr?: unknown }));

  const selectColumns: SelectColumn[] = [];
  const scalarSelectItems: Array<{ expr: unknown; output: string }> = [];
  const aggregateMetrics: AggregateMetric[] = [];
  const aggregateOutputColumns: AggregateOutputColumn[] = [];

  if (!selectAll) {
    if (!Array.isArray(selectColumnsRaw)) {
      throw new Error("Unsupported SELECT clause.");
    }

    for (const item of selectColumnsRaw) {
      if (!item || typeof item !== "object") {
        throw new Error("Unsupported SELECT item.");
      }

      const expr = (item as { expr?: unknown }).expr;
      const as = (item as { as?: unknown }).as;
      const explicitOutput = typeof as === "string" && as.length > 0 ? as : undefined;

      const aggregateMetric = parseAggregateMetric(
        expr,
        explicitOutput,
        bindings,
        aliasToBinding,
        schema,
      );
      if (aggregateMetric) {
        if (aggregateMetrics.some((existing) => existing.output === aggregateMetric.output)) {
          throw new Error(`Duplicate aggregate output alias: ${aggregateMetric.output}`);
        }
        aggregateMetrics.push(aggregateMetric);
        continue;
      }

      const colRef = resolveColumnRef(expr, bindings, aliasToBinding);
      if (colRef) {
        const output =
          explicitOutput ??
          (selectColumns.some((existing) => existing.output === colRef.column)
            ? `${colRef.alias}.${colRef.column}`
            : colRef.column);

        selectColumns.push({
          alias: colRef.alias,
          column: colRef.column,
          output,
        });
        continue;
      }

      if (toSubqueryAst(expr)) {
        const output =
          explicitOutput ?? `expr_${selectColumns.length + scalarSelectItems.length + 1}`;
        scalarSelectItems.push({
          expr,
          output,
        });
        continue;
      }

      throw new Error(
        "Only direct column references, scalar subqueries, and aggregate functions are currently supported in SELECT.",
      );
    }
  }

  const having = ast.having;
  const havingMetrics = collectHavingAggregateMetrics(
    having,
    bindings,
    aliasToBinding,
    schema,
    aggregateMetrics,
  );
  aggregateMetrics.push(...havingMetrics);

  const isAggregate = groupBy.length > 0 || aggregateMetrics.length > 0;

  if (selectAll && bindings.length > 1) {
    throw new Error("SELECT * is only supported for single-table queries.");
  }

  if (isAggregate && selectAll) {
    throw new Error("SELECT * is not supported for aggregate queries.");
  }

  const groupByKeys = new Set(
    groupBy.map((column) => sourceColumnKey(column.alias, column.column)),
  );

  if (isAggregate) {
    for (const column of selectColumns) {
      const key = sourceColumnKey(column.alias, column.column);
      if (!groupByKeys.has(key)) {
        throw new Error(
          `Column ${column.alias}.${column.column} must appear in GROUP BY or be aggregated.`,
        );
      }
    }

    for (const column of selectColumns) {
      aggregateOutputColumns.push({
        source: {
          alias: column.alias,
          column: column.column,
        },
        output: column.output,
      });
    }
  }

  const selectableOutputByName = new Map<string, SelectColumn>();
  for (const column of selectColumns) {
    selectableOutputByName.set(column.output, column);
  }

  const aggregateOutputNames = new Set<string>();
  for (const column of aggregateOutputColumns) {
    aggregateOutputNames.add(column.output);
  }
  for (const metric of aggregateMetrics) {
    aggregateOutputNames.add(metric.output);
  }

  const groupOutputBySource = new Map<string, string>();
  for (const outputColumn of aggregateOutputColumns) {
    groupOutputBySource.set(
      sourceColumnKey(outputColumn.source.alias, outputColumn.source.column),
      outputColumn.output,
    );
  }

  const orderBy = parseOrderBy(
    ast.orderby,
    bindings,
    aliasToBinding,
    isAggregate,
    selectableOutputByName,
    aggregateOutputNames,
    groupOutputBySource,
  );

  const { limit, offset } = parseLimitAndOffset(ast.limit);

  const parsedQuery: ParsedSelectQuery = {
    bindings,
    joins,
    joinEdges: uniqueJoinEdges(joinEdges),
    filters,
    whereColumns,
    wherePushdownSafe,
    ...(where != null ? { where } : {}),
    ...(having != null ? { having } : {}),
    distinct: ast.distinct != null,
    selectAll,
    selectColumns,
    scalarSelectItems,
    groupBy,
    aggregateMetrics,
    aggregateOutputColumns,
    isAggregate,
    orderBy,
    ...(limit != null ? { limit } : {}),
    ...(offset != null ? { offset } : {}),
  };

  return parsedQuery;
}

function parseGroupBy(
  rawGroupBy: unknown,
  bindings: TableBinding[],
  aliasToBinding: Map<string, TableBinding>,
): Array<{ alias: string; column: string }> {
  if (!rawGroupBy || typeof rawGroupBy !== "object") {
    return [];
  }

  const columns = (rawGroupBy as { columns?: unknown }).columns;
  if (!Array.isArray(columns)) {
    return [];
  }

  return columns.map((columnExpr) => {
    const column = resolveColumnRef(columnExpr, bindings, aliasToBinding);
    if (!column) {
      throw new Error("GROUP BY currently supports only direct column references.");
    }
    return column;
  });
}

function parseAggregateMetric(
  expr: unknown,
  explicitOutput: string | undefined,
  bindings: TableBinding[],
  aliasToBinding: Map<string, TableBinding>,
  schema: SchemaDefinition,
): AggregateMetric | null {
  const aggregateExpr = expr as {
    type?: unknown;
    name?: unknown;
    args?: {
      expr?: unknown;
      distinct?: unknown;
    };
  };

  if (aggregateExpr.type !== "aggr_func") {
    return null;
  }

  const rawName = typeof aggregateExpr.name === "string" ? aggregateExpr.name.toUpperCase() : "";
  const fn = mapAggregateFunction(rawName);
  if (!fn) {
    throw new Error(`Unsupported aggregate function: ${String(aggregateExpr.name)}`);
  }

  const args = aggregateExpr.args;
  const distinct = args?.distinct === "DISTINCT";

  const argExpr = args?.expr;
  let column: { alias: string; column: string } | undefined;

  if (isStarExpr(argExpr)) {
    if (fn !== "count") {
      throw new Error(`${rawName}(*) is not supported.`);
    }

    if (distinct) {
      throw new Error("COUNT(DISTINCT *) is not supported.");
    }
  } else {
    column = resolveColumnRef(argExpr, bindings, aliasToBinding);
    if (!column) {
      throw new Error(`${rawName} must reference a column or *.`);
    }

    if ((fn === "sum" || fn === "avg") && !isNumericColumn(column, aliasToBinding, schema)) {
      throw new Error(`${rawName} requires a numeric column.`);
    }
  }

  if (distinct && fn !== "count") {
    throw new Error(`DISTINCT is currently only supported for COUNT.`);
  }

  const output =
    explicitOutput ??
    (column ? `${fn}_${column.column}` : fn === "count" ? "count" : `${fn}_value`);

  const signature = buildAggregateMetricSignature({
    fn,
    distinct,
    ...(column ? { column } : {}),
  });

  return {
    fn,
    output,
    signature,
    hidden: false,
    ...(column ? { column } : {}),
    distinct,
  };
}

function isNumericColumn(
  column: { alias: string; column: string },
  aliasToBinding: Map<string, TableBinding>,
  schema: SchemaDefinition,
): boolean {
  const binding = aliasToBinding.get(column.alias);
  if (!binding || binding.isCte) {
    return true;
  }

  const table = getTable(schema, binding.table);
  const columnDefinition = table.columns[column.column];
  if (!columnDefinition) {
    return false;
  }

  const columnType = resolveColumnType(columnDefinition);
  return columnType === "integer";
}

function parseOrderBy(
  rawOrderBy: unknown,
  bindings: TableBinding[],
  aliasToBinding: Map<string, TableBinding>,
  isAggregate: boolean,
  selectByOutput: Map<string, SelectColumn>,
  aggregateOutputNames: Set<string>,
  aggregateGroupOutputBySource: Map<string, string>,
): OrderColumn[] {
  if (!Array.isArray(rawOrderBy)) {
    return [];
  }

  const out: OrderColumn[] = [];

  for (const item of rawOrderBy) {
    const expr = (item as { expr?: unknown }).expr;
    const rawType = (item as { type?: unknown }).type;
    const direction: "asc" | "desc" = rawType === "DESC" ? "desc" : "asc";

    const rawColumnRef = toRawColumnRef(expr);
    if (!rawColumnRef) {
      throw new Error("Only column references are currently supported in ORDER BY.");
    }

    if (isAggregate) {
      if (!rawColumnRef.table && aggregateOutputNames.has(rawColumnRef.column)) {
        out.push({
          kind: "output",
          output: rawColumnRef.column,
          direction,
        });
        continue;
      }

      const source = resolveColumnRef(expr, bindings, aliasToBinding);
      if (!source) {
        throw new Error("Unable to resolve ORDER BY column.");
      }

      const groupOutput = aggregateGroupOutputBySource.get(
        sourceColumnKey(source.alias, source.column),
      );
      if (!groupOutput) {
        throw new Error(
          `Aggregate ORDER BY on ${source.alias}.${source.column} must reference a grouped selected column or output alias.`,
        );
      }

      out.push({
        kind: "output",
        output: groupOutput,
        direction,
      });
      continue;
    }

    if (!rawColumnRef.table) {
      const selectedColumn = selectByOutput.get(rawColumnRef.column);
      if (selectedColumn) {
        out.push({
          kind: "source",
          alias: selectedColumn.alias,
          column: selectedColumn.column,
          direction,
        });
        continue;
      }
    }

    const source = resolveColumnRef(expr, bindings, aliasToBinding);
    if (!source) {
      throw new Error("Unable to resolve ORDER BY column.");
    }

    out.push({
      kind: "source",
      alias: source.alias,
      column: source.column,
      direction,
    });
  }

  return out;
}

function mapAggregateFunction(raw: string): AggregateFunction | null {
  switch (raw) {
    case "COUNT":
      return "count";
    case "SUM":
      return "sum";
    case "AVG":
      return "avg";
    case "MIN":
      return "min";
    case "MAX":
      return "max";
    default:
      return null;
  }
}

function parseLimitAndOffset(rawLimit: unknown): { limit?: number; offset?: number } {
  if (!rawLimit || typeof rawLimit !== "object") {
    return {};
  }

  const limitNode = rawLimit as {
    value?: Array<{ value?: unknown }>;
    seperator?: unknown;
  };

  if (!Array.isArray(limitNode.value) || limitNode.value.length === 0) {
    return {};
  }

  const first = parseNumericLiteral(limitNode.value[0]?.value);
  const second = parseNumericLiteral(limitNode.value[1]?.value);
  const separator = limitNode.seperator;

  if (first == null) {
    throw new Error("Unable to parse LIMIT value.");
  }

  if (separator === "offset") {
    const out: { limit?: number; offset?: number } = { limit: first };
    if (second != null) {
      out.offset = second;
    }
    return out;
  }

  if (separator === ",") {
    const out: { limit?: number; offset?: number } = { offset: first };
    if (second != null) {
      out.limit = second;
    }
    return out;
  }

  return {
    limit: first,
  };
}

function parseNumericLiteral(value: unknown): number | undefined {
  if (typeof value === "number") {
    return Number.isFinite(value) ? value : undefined;
  }

  if (typeof value === "string") {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : undefined;
  }

  return undefined;
}

interface ParsedPushdownWhere {
  filters: LiteralFilter[];
  joinEdges: JoinCondition[];
}

function tryParseConjunctivePushdownFilters(
  where: unknown,
  bindings: TableBinding[],
  aliasToBinding: Map<string, TableBinding>,
): ParsedPushdownWhere | null {
  if (!where) {
    return {
      filters: [],
      joinEdges: [],
    };
  }

  const whereParts = flattenConjunctiveWhere(where);
  if (!whereParts) {
    return null;
  }

  const filters: LiteralFilter[] = [];
  const joinEdges: JoinCondition[] = [];

  for (const part of whereParts) {
    if (!part || typeof part !== "object") {
      return null;
    }

    const binary = part as { type?: unknown; operator?: unknown; left?: unknown; right?: unknown };
    if (binary.type !== "binary_expr") {
      return null;
    }

    const operator = normalizeBinaryOperator(binary.operator);
    if (operator === "in") {
      const colRef = resolveColumnRef(binary.left, bindings, aliasToBinding);
      if (!colRef) {
        return null;
      }

      const values = tryParseLiteralExpressionList(binary.right);
      if (!values) {
        return null;
      }

      filters.push({
        alias: colRef.alias,
        clause: {
          op: "in",
          column: colRef.column,
          values,
        },
      });
      continue;
    }

    const leftCol = resolveColumnRef(binary.left, bindings, aliasToBinding);
    const rightCol = resolveColumnRef(binary.right, bindings, aliasToBinding);
    const leftLiteral = parseLiteral(binary.left);
    const rightLiteral = parseLiteral(binary.right);

    if (operator === "is_null" || operator === "is_not_null") {
      if (leftCol && rightLiteral === null) {
        filters.push({
          alias: leftCol.alias,
          clause: {
            op: operator,
            column: leftCol.column,
          },
        });
        continue;
      }

      if (rightCol && leftLiteral === null) {
        filters.push({
          alias: rightCol.alias,
          clause: {
            op: operator,
            column: rightCol.column,
          },
        });
        continue;
      }

      return null;
    }

    if (operator === "eq" && leftCol && rightCol) {
      joinEdges.push({
        leftAlias: leftCol.alias,
        leftColumn: leftCol.column,
        rightAlias: rightCol.alias,
        rightColumn: rightCol.column,
      });
      continue;
    }

    if (leftCol && rightLiteral !== undefined) {
      filters.push({
        alias: leftCol.alias,
        clause: {
          op: operator,
          column: leftCol.column,
          value: rightLiteral,
        },
      });
      continue;
    }

    if (rightCol && leftLiteral !== undefined) {
      filters.push({
        alias: rightCol.alias,
        clause: {
          op: invertOperator(operator),
          column: rightCol.column,
          value: leftLiteral,
        },
      });
      continue;
    }

    return null;
  }

  return {
    filters,
    joinEdges,
  };
}

function collectHavingAggregateMetrics(
  having: unknown,
  bindings: TableBinding[],
  aliasToBinding: Map<string, TableBinding>,
  schema: SchemaDefinition,
  existingMetrics: AggregateMetric[],
): AggregateMetric[] {
  if (!having) {
    return [];
  }

  const seen = new Set(existingMetrics.map((metric) => metric.signature));
  const out: AggregateMetric[] = [];
  let counter = 1;

  const visit = (node: unknown): void => {
    if (!node || typeof node !== "object") {
      return;
    }

    const aggregateMetric = parseAggregateMetric(
      node,
      `__having_${counter}`,
      bindings,
      aliasToBinding,
      schema,
    );
    if (aggregateMetric) {
      if (!seen.has(aggregateMetric.signature)) {
        aggregateMetric.hidden = true;
        out.push(aggregateMetric);
        seen.add(aggregateMetric.signature);
        counter += 1;
      }
      return;
    }

    const expr = node as {
      type?: unknown;
      left?: unknown;
      right?: unknown;
      args?: { value?: unknown };
    };

    if (expr.type === "binary_expr") {
      visit(expr.left);
      visit(expr.right);
      return;
    }

    if (expr.type === "function") {
      const args = expr.args?.value;
      if (Array.isArray(args)) {
        for (const arg of args) {
          visit(arg);
        }
      } else {
        visit(args);
      }
    }
  };

  visit(having);
  return out;
}

function collectColumnReferences(
  raw: unknown,
  bindings: TableBinding[],
  aliasToBinding: Map<string, TableBinding>,
): Array<{ alias: string; column: string }> {
  if (!raw) {
    return [];
  }

  const out = new Map<string, { alias: string; column: string }>();

  const visit = (node: unknown): void => {
    if (!node || typeof node !== "object") {
      return;
    }

    if (toSubqueryAst(node)) {
      return;
    }

    const ref = toRawColumnRef(node);
    if (ref) {
      try {
        const resolved = resolveColumnRef(node, bindings, aliasToBinding);
        if (resolved) {
          out.set(sourceColumnKey(resolved.alias, resolved.column), resolved);
        }
      } catch {
        // Ignore columns that do not bind in this query scope.
      }
    }

    const expr = node as {
      type?: unknown;
      left?: unknown;
      right?: unknown;
      args?: { expr?: unknown; value?: unknown };
    };

    if (expr.type === "binary_expr") {
      visit(expr.left);
      visit(expr.right);
      return;
    }

    if (expr.type === "aggr_func") {
      visit(expr.args?.expr);
      return;
    }

    if (expr.type === "function") {
      const values = expr.args?.value;
      if (Array.isArray(values)) {
        for (const value of values) {
          visit(value);
        }
      } else {
        visit(values);
      }
    }
  };

  visit(raw);
  return [...out.values()];
}

function buildAggregateMetricSignature(metric: {
  fn: AggregateFunction;
  distinct: boolean;
  column?: {
    alias: string;
    column: string;
  };
}): string {
  const source = metric.column ? `${metric.column.alias}.${metric.column.column}` : "*";
  return `${metric.fn}|${metric.distinct ? "distinct" : "all"}|${source}`;
}

function buildProjection(
  parsed: ParsedSelectQuery,
  schema: SchemaDefinition,
  cteRows: Map<string, QueryRow[]>,
): Map<string, Set<string>> {
  const projections = new Map<string, Set<string>>();
  for (const binding of parsed.bindings) {
    projections.set(binding.alias, new Set());
  }

  if (parsed.selectAll) {
    const base = parsed.bindings[0];
    if (!base) {
      throw new Error("SELECT queries must include a FROM clause.");
    }

    for (const column of getBindingColumns(base, schema, cteRows)) {
      projections.get(base.alias)?.add(column);
    }
  } else {
    for (const item of parsed.selectColumns) {
      projections.get(item.alias)?.add(item.column);
    }

    for (const groupColumn of parsed.groupBy) {
      projections.get(groupColumn.alias)?.add(groupColumn.column);
    }

    for (const metric of parsed.aggregateMetrics) {
      if (metric.column) {
        projections.get(metric.column.alias)?.add(metric.column.column);
      }
    }
  }

  for (const join of parsed.joinEdges) {
    projections.get(join.leftAlias)?.add(join.leftColumn);
    projections.get(join.rightAlias)?.add(join.rightColumn);
  }

  for (const filter of parsed.filters) {
    projections.get(filter.alias)?.add(filter.clause.column);
  }

  for (const reference of parsed.whereColumns) {
    projections.get(reference.alias)?.add(reference.column);
  }

  for (const term of parsed.orderBy) {
    if (term.kind === "source") {
      projections.get(term.alias)?.add(term.column);
    }
  }

  for (const [alias, columns] of projections) {
    if (columns.size > 0) {
      continue;
    }

    const binding = parsed.bindings.find((candidate) => candidate.alias === alias);
    if (!binding) {
      continue;
    }

    const bindingColumns = getBindingColumns(binding, schema, cteRows);
    const firstColumn = bindingColumns[0];
    if (firstColumn) {
      columns.add(firstColumn);
    }
  }

  return projections;
}

function getBindingColumns(
  binding: TableBinding,
  schema: SchemaDefinition,
  cteRows: Map<string, QueryRow[]>,
): string[] {
  if (binding.isCte) {
    const rows = cteRows.get(binding.table) ?? [];
    const first = rows[0];
    return first ? Object.keys(first) : [];
  }

  return Object.keys(getTable(schema, binding.table).columns);
}

function groupFiltersByAlias(filters: LiteralFilter[]): Map<string, ScanFilterClause[]> {
  const grouped = new Map<string, ScanFilterClause[]>();
  for (const filter of filters) {
    const existing = grouped.get(filter.alias) ?? [];
    existing.push(filter.clause);
    grouped.set(filter.alias, existing);
  }
  return grouped;
}

function buildExecutionOrder(
  bindings: TableBinding[],
  joinEdges: JoinCondition[],
  filtersByAlias: Map<string, ScanFilterClause[]>,
): string[] {
  const score = new Map<string, number>();
  for (const binding of bindings) {
    score.set(binding.alias, filtersByAlias.get(binding.alias)?.length ?? 0);
  }

  const unvisited = new Set(bindings.map((binding) => binding.alias));
  const visited = new Set<string>();
  const order: string[] = [];

  while (unvisited.size > 0) {
    const candidates = [...unvisited].filter((alias) => {
      if (visited.size === 0) {
        return true;
      }
      return joinEdges.some(
        (edge) =>
          (edge.leftAlias === alias && visited.has(edge.rightAlias)) ||
          (edge.rightAlias === alias && visited.has(edge.leftAlias)),
      );
    });

    const pool = candidates.length > 0 ? candidates : [...unvisited];
    pool.sort((a, b) => {
      const aScore = score.get(a) ?? 0;
      const bScore = score.get(b) ?? 0;
      if (aScore !== bScore) {
        return bScore - aScore;
      }

      const aIndex = bindings.find((binding) => binding.alias === a)?.index ?? 0;
      const bIndex = bindings.find((binding) => binding.alias === b)?.index ?? 0;
      return bIndex - aIndex;
    });

    const next = pool[0];
    if (!next) {
      break;
    }

    order.push(next);
    visited.add(next);
    unvisited.delete(next);
  }

  return order;
}

function buildDependencyFilters(
  alias: string,
  joins: ParsedJoin[],
  joinEdges: JoinCondition[],
  rowsByAlias: Map<string, QueryRow[]>,
): ScanFilterClause[] {
  const clauses: ScanFilterClause[] = [];
  for (const edge of joinEdges) {
    const join = joins.find(
      (candidate) =>
        (candidate.condition.leftAlias === edge.leftAlias &&
          candidate.condition.leftColumn === edge.leftColumn &&
          candidate.condition.rightAlias === edge.rightAlias &&
          candidate.condition.rightColumn === edge.rightColumn) ||
        (candidate.condition.leftAlias === edge.rightAlias &&
          candidate.condition.leftColumn === edge.rightColumn &&
          candidate.condition.rightAlias === edge.leftAlias &&
          candidate.condition.rightColumn === edge.leftColumn),
    );
    const preservedAliases = join ? getPreservedAliases(join) : [];

    if (edge.leftAlias === alias && rowsByAlias.has(edge.rightAlias)) {
      if (preservedAliases.includes(alias)) {
        continue;
      }
      clauses.push({
        op: "in",
        column: edge.leftColumn,
        values: uniqueValues(rowsByAlias.get(edge.rightAlias) ?? [], edge.rightColumn),
      });
      continue;
    }

    if (edge.rightAlias === alias && rowsByAlias.has(edge.leftAlias)) {
      if (preservedAliases.includes(alias)) {
        continue;
      }
      clauses.push({
        op: "in",
        column: edge.rightColumn,
        values: uniqueValues(rowsByAlias.get(edge.leftAlias) ?? [], edge.leftColumn),
      });
    }
  }

  return dedupeInClauses(clauses);
}

function getExistingJoinAlias(join: ParsedJoin): string {
  return join.condition.leftAlias === join.alias
    ? join.condition.rightAlias
    : join.condition.leftAlias;
}

function getJoinAliasColumn(join: ParsedJoin): string {
  return join.condition.leftAlias === join.alias
    ? join.condition.leftColumn
    : join.condition.rightColumn;
}

function getExistingJoinColumn(join: ParsedJoin): string {
  return join.condition.leftAlias === join.alias
    ? join.condition.rightColumn
    : join.condition.leftColumn;
}

function getPreservedAliases(join: ParsedJoin): string[] {
  const existingAlias = getExistingJoinAlias(join);
  switch (join.join) {
    case "left":
      return [existingAlias];
    case "right":
      return [join.alias];
    case "full":
      return [existingAlias, join.alias];
    default:
      return [];
  }
}

function initializeJoinedRows(
  rowsByAlias: Map<string, QueryRow[]>,
  baseAlias: string,
): Array<Record<string, QueryRow>> {
  const baseRows = rowsByAlias.get(baseAlias) ?? [];
  return baseRows.map((row) => ({
    [baseAlias]: row,
  }));
}

function applyInnerJoin(
  existing: Array<Record<string, QueryRow>>,
  join: ParsedJoin,
  rowsByAlias: Map<string, QueryRow[]>,
): Array<Record<string, QueryRow>> {
  const rightRows = rowsByAlias.get(join.alias) ?? [];
  const joinAliasColumn = getJoinAliasColumn(join);
  const existingAlias = getExistingJoinAlias(join);
  const existingColumn = getExistingJoinColumn(join);

  const index = new Map<unknown, QueryRow[]>();
  for (const row of rightRows) {
    const key = row[joinAliasColumn];
    if (key == null) {
      continue;
    }
    const bucket = index.get(key) ?? [];
    bucket.push(row);
    index.set(key, bucket);
  }

  const joined: Array<Record<string, QueryRow>> = [];
  for (const bundle of existing) {
    const leftRow = bundle[existingAlias];
    if (!leftRow) {
      continue;
    }

    const key = leftRow[existingColumn];
    if (key == null) {
      continue;
    }
    const matches = index.get(key) ?? [];
    for (const match of matches) {
      joined.push({
        ...bundle,
        [join.alias]: match,
      });
    }
  }

  return joined;
}

function applyLeftJoin(
  existing: Array<Record<string, QueryRow>>,
  join: ParsedJoin,
  rowsByAlias: Map<string, QueryRow[]>,
): Array<Record<string, QueryRow>> {
  const rightRows = rowsByAlias.get(join.alias) ?? [];
  const joinAliasColumn = getJoinAliasColumn(join);
  const existingAlias = getExistingJoinAlias(join);
  const existingColumn = getExistingJoinColumn(join);

  const index = new Map<unknown, QueryRow[]>();
  for (const row of rightRows) {
    const key = row[joinAliasColumn];
    const bucket = index.get(key) ?? [];
    bucket.push(row);
    index.set(key, bucket);
  }

  const joined: Array<Record<string, QueryRow>> = [];
  for (const bundle of existing) {
    const leftRow = bundle[existingAlias];
    if (!leftRow) {
      joined.push({
        ...bundle,
        [join.alias]: {},
      });
      continue;
    }

    const key = leftRow[existingColumn];
    const matches = key == null ? [] : (index.get(key) ?? []);
    if (matches.length === 0) {
      joined.push({
        ...bundle,
        [join.alias]: {},
      });
      continue;
    }

    for (const match of matches) {
      joined.push({
        ...bundle,
        [join.alias]: match,
      });
    }
  }

  return joined;
}

function applyRightJoin(
  existing: Array<Record<string, QueryRow>>,
  join: ParsedJoin,
  rowsByAlias: Map<string, QueryRow[]>,
): Array<Record<string, QueryRow>> {
  const rightRows = rowsByAlias.get(join.alias) ?? [];
  const joinAliasColumn = getJoinAliasColumn(join);
  const existingAlias = getExistingJoinAlias(join);
  const existingColumn = getExistingJoinColumn(join);

  const index = new Map<unknown, Array<Record<string, QueryRow>>>();
  for (const bundle of existing) {
    const row = bundle[existingAlias];
    if (!row) {
      continue;
    }

    const key = row[existingColumn];
    if (key == null) {
      continue;
    }

    const bucket = index.get(key) ?? [];
    bucket.push(bundle);
    index.set(key, bucket);
  }

  const joined: Array<Record<string, QueryRow>> = [];
  for (const rightRow of rightRows) {
    const key = rightRow[joinAliasColumn];
    const matches = key == null ? [] : (index.get(key) ?? []);
    if (matches.length === 0) {
      joined.push({
        [join.alias]: rightRow,
      });
      continue;
    }

    for (const bundle of matches) {
      joined.push({
        ...bundle,
        [join.alias]: rightRow,
      });
    }
  }

  return joined;
}

function applyFullJoin(
  existing: Array<Record<string, QueryRow>>,
  join: ParsedJoin,
  rowsByAlias: Map<string, QueryRow[]>,
): Array<Record<string, QueryRow>> {
  const rightRows = rowsByAlias.get(join.alias) ?? [];
  const joinAliasColumn = getJoinAliasColumn(join);
  const existingAlias = getExistingJoinAlias(join);
  const existingColumn = getExistingJoinColumn(join);

  const index = new Map<unknown, number[]>();
  rightRows.forEach((row, idx) => {
    const key = row[joinAliasColumn];
    if (key == null) {
      return;
    }

    const bucket = index.get(key) ?? [];
    bucket.push(idx);
    index.set(key, bucket);
  });

  const matchedRight = new Set<number>();
  const joined: Array<Record<string, QueryRow>> = [];

  for (const bundle of existing) {
    const leftRow = bundle[existingAlias];
    const key = leftRow?.[existingColumn];
    const matchIndexes = key == null ? [] : (index.get(key) ?? []);

    if (matchIndexes.length === 0) {
      joined.push({
        ...bundle,
        [join.alias]: {},
      });
      continue;
    }

    for (const idx of matchIndexes) {
      matchedRight.add(idx);
      const rightRow = rightRows[idx];
      joined.push({
        ...bundle,
        [join.alias]: rightRow ?? {},
      });
    }
  }

  rightRows.forEach((rightRow, idx) => {
    if (matchedRight.has(idx)) {
      return;
    }

    joined.push({
      [join.alias]: rightRow,
    });
  });

  return joined;
}

function applyFinalSort(
  rows: Array<Record<string, QueryRow>>,
  orderBy: OrderColumn[],
): Array<Record<string, QueryRow>> {
  const sourceTerms = orderBy.filter((term): term is SourceOrderColumn => term.kind === "source");
  if (sourceTerms.length === 0) {
    return rows;
  }

  const sorted = [...rows];
  sorted.sort((left, right) => {
    for (const term of sourceTerms) {
      const leftValue = left[term.alias]?.[term.column] as
        | string
        | number
        | boolean
        | null
        | undefined;
      const rightValue = right[term.alias]?.[term.column] as
        | string
        | number
        | boolean
        | null
        | undefined;
      if (leftValue === rightValue) {
        continue;
      }

      const leftNorm = leftValue ?? null;
      const rightNorm = rightValue ?? null;
      const comparison = compareNullableValues(leftNorm, rightNorm);
      return term.direction === "asc" ? comparison : -comparison;
    }

    return 0;
  });

  return sorted;
}

function applyOutputSort(rows: QueryRow[], orderBy: OrderColumn[]): QueryRow[] {
  const sorted = [...rows];

  sorted.sort((left, right) => {
    for (const term of orderBy) {
      const key = term.kind === "output" ? term.output : term.column;
      const leftValue = left[key] as string | number | boolean | null | undefined;
      const rightValue = right[key] as string | number | boolean | null | undefined;

      if (leftValue === rightValue) {
        continue;
      }

      const comparison = compareNullableValues(leftValue ?? null, rightValue ?? null);
      return term.direction === "asc" ? comparison : -comparison;
    }

    return 0;
  });

  return sorted;
}

async function applyWhereFilter<TContext>(
  rows: JoinedRowBundle[],
  parsed: ParsedSelectQuery,
  input: QueryInput<TContext>,
  cteRows: Map<string, QueryRow[]>,
): Promise<JoinedRowBundle[]> {
  if (!parsed.where) {
    return rows;
  }

  const out: JoinedRowBundle[] = [];
  for (const bundle of rows) {
    const truth = await evaluatePredicateTruth(parsed.where, {
      parsed,
      input,
      cteRows,
      bundle,
    });
    if (truth === true) {
      out.push(bundle);
    }
  }

  return out;
}

async function applyHavingFilter<TContext>(
  rows: QueryRow[],
  parsed: ParsedSelectQuery,
  input: QueryInput<TContext>,
  cteRows: Map<string, QueryRow[]>,
): Promise<QueryRow[]> {
  if (!parsed.having) {
    return rows;
  }

  const out: QueryRow[] = [];
  for (const row of rows) {
    const truth = await evaluatePredicateTruth(parsed.having, {
      parsed,
      input,
      cteRows,
      aggregateRow: row,
    });
    if (truth === true) {
      out.push(row);
    }
  }

  return out;
}

function applyProjectedSortLimit(rows: QueryRow[], parsed: ParsedSelectQuery): QueryRow[] {
  let out = rows;

  if (parsed.orderBy.length > 0) {
    const mapped = parsed.orderBy.map((term) => {
      if (term.kind === "output") {
        return {
          key: term.output,
          direction: term.direction,
        };
      }

      const selected = parsed.selectColumns.find(
        (candidate) => candidate.alias === term.alias && candidate.column === term.column,
      );
      if (!selected) {
        throw new Error(
          `ORDER BY ${term.alias}.${term.column} must reference a selected output when DISTINCT is used.`,
        );
      }

      return {
        key: selected.output,
        direction: term.direction,
      };
    });

    out = [...out].sort((left, right) => {
      for (const term of mapped) {
        const leftValue = left[term.key] as string | number | boolean | null | undefined;
        const rightValue = right[term.key] as string | number | boolean | null | undefined;
        const comparison = compareNullableValues(leftValue ?? null, rightValue ?? null);
        if (comparison !== 0) {
          return term.direction === "asc" ? comparison : -comparison;
        }
      }
      return 0;
    });
  }

  if (parsed.offset != null) {
    out = out.slice(parsed.offset);
  }

  if (parsed.limit != null) {
    out = out.slice(0, parsed.limit);
  }

  return out;
}

interface PredicateEvalScope<TContext> {
  parsed: ParsedSelectQuery;
  input: QueryInput<TContext>;
  cteRows: Map<string, QueryRow[]>;
  bundle?: JoinedRowBundle;
  aggregateRow?: QueryRow;
}

type SqlTruth = true | false | null;

async function evaluatePredicateTruth<TContext>(
  expr: unknown,
  scope: PredicateEvalScope<TContext>,
): Promise<SqlTruth> {
  if (!expr || typeof expr !== "object") {
    return null;
  }

  const node = expr as {
    type?: unknown;
    operator?: unknown;
    left?: unknown;
    right?: unknown;
    name?: unknown;
    args?: { value?: unknown };
  };

  if (node.type === "function") {
    const fn = readFunctionName(node.name);
    const args = readFunctionArgs(node.args);

    if (fn === "NOT") {
      const arg = args[0];
      const truth = await evaluatePredicateTruth(arg, scope);
      if (truth === null) {
        return null;
      }
      return !truth;
    }

    if (fn === "EXISTS") {
      const subquery = toSubqueryAst(args[0]);
      if (!subquery) {
        throw new Error("EXISTS requires a subquery argument.");
      }

      const rows = await executeSubquery(subquery, scope);
      return rows.length > 0;
    }

    throw new Error(`Unsupported predicate function: ${fn}`);
  }

  if (node.type !== "binary_expr") {
    throw new Error("Only binary predicates are currently supported in WHERE/HAVING clauses.");
  }

  const operator = typeof node.operator === "string" ? node.operator.toUpperCase() : "";
  switch (operator) {
    case "AND": {
      const left = await evaluatePredicateTruth(node.left, scope);
      const right = await evaluatePredicateTruth(node.right, scope);
      if (left === false || right === false) {
        return false;
      }
      if (left === null || right === null) {
        return null;
      }
      return true;
    }
    case "OR": {
      const left = await evaluatePredicateTruth(node.left, scope);
      const right = await evaluatePredicateTruth(node.right, scope);
      if (left === true || right === true) {
        return true;
      }
      if (left === null || right === null) {
        return null;
      }
      return false;
    }
    case "IN": {
      const left = await evaluateExpressionValue(node.left, scope);
      if (left == null) {
        return null;
      }

      const candidates = await evaluateInCandidates(node.right, scope);
      let sawNull = false;
      for (const candidate of candidates) {
        if (candidate == null) {
          sawNull = true;
          continue;
        }
        if (candidate === left) {
          return true;
        }
      }

      return sawNull ? null : false;
    }
    case "IS":
    case "IS NOT": {
      const left = await evaluateExpressionValue(node.left, scope);
      const right = await evaluateExpressionValue(node.right, scope);
      const isNull = right == null;
      if (!isNull) {
        throw new Error("IS/IS NOT currently only support NULL checks.");
      }
      return operator === "IS" ? left == null : left != null;
    }
    case "=":
    case "!=":
    case "<>":
    case ">":
    case ">=":
    case "<":
    case "<=": {
      const left = await evaluateExpressionValue(node.left, scope);
      const right = await evaluateExpressionValue(node.right, scope);
      return compareSqlValues(left, right, operator);
    }
    case "BETWEEN": {
      const left = await evaluateExpressionValue(node.left, scope);
      const range = node.right as { value?: unknown } | undefined;
      const values = Array.isArray(range?.value) ? range.value : [];
      const low = await evaluateExpressionValue(values[0], scope);
      const high = await evaluateExpressionValue(values[1], scope);
      if (left == null || low == null || high == null) {
        return null;
      }
      return compareNonNull(left, low) >= 0 && compareNonNull(left, high) <= 0;
    }
    default:
      throw new Error(`Unsupported predicate operator: ${String(node.operator)}`);
  }
}

async function evaluateExpressionValue<TContext>(
  expr: unknown,
  scope: PredicateEvalScope<TContext>,
): Promise<unknown> {
  const literal = parseLiteral(expr);
  if (literal !== undefined) {
    return literal;
  }

  const columnRef = toRawColumnRef(expr);
  if (columnRef) {
    return evaluateColumnReference(columnRef, scope);
  }

  const aggregateExpr = expr as {
    type?: unknown;
    name?: unknown;
    args?: { expr?: unknown; distinct?: unknown };
  };
  if (aggregateExpr.type === "aggr_func") {
    if (!scope.aggregateRow) {
      throw new Error("Aggregate expressions are only valid in aggregate contexts.");
    }

    const metric = parseAggregateMetric(
      expr,
      undefined,
      scope.parsed.bindings,
      new Map(scope.parsed.bindings.map((binding) => [binding.alias, binding])),
      scope.input.schema,
    );
    if (!metric) {
      throw new Error("Unable to resolve aggregate expression.");
    }

    const existing = scope.parsed.aggregateMetrics.find(
      (candidate) => candidate.signature === metric.signature,
    );
    if (!existing) {
      throw new Error("HAVING references an aggregate that is not available.");
    }

    return scope.aggregateRow[existing.output] ?? null;
  }

  const subquery = toSubqueryAst(expr);
  if (subquery) {
    const rows = await executeSubquery(subquery, scope);
    if (rows.length === 0) {
      return null;
    }
    if (rows.length > 1) {
      throw new Error("Scalar subquery returned more than one row.");
    }

    const row = rows[0];
    if (!row) {
      return null;
    }

    const firstKey = Object.keys(row)[0];
    return firstKey ? (row[firstKey] ?? null) : null;
  }

  const functionExpr = expr as { type?: unknown; name?: unknown; args?: { value?: unknown } };
  if (functionExpr.type === "function") {
    const name = readFunctionName(functionExpr.name);
    if (name === "NOT" || name === "EXISTS") {
      const truth = await evaluatePredicateTruth(expr, scope);
      return truth === null ? null : truth;
    }
  }

  throw new Error("Unsupported expression.");
}

function evaluateColumnReference<TContext>(
  ref: { table: string | null; column: string },
  scope: PredicateEvalScope<TContext>,
): unknown {
  if (scope.aggregateRow) {
    if (!ref.table) {
      const direct = scope.aggregateRow[ref.column];
      if (direct !== undefined) {
        return direct ?? null;
      }

      const fromGroup = scope.parsed.aggregateOutputColumns.find(
        (column) => column.source.column === ref.column,
      );
      return fromGroup ? (scope.aggregateRow[fromGroup.output] ?? null) : null;
    }

    const fromGroup = scope.parsed.aggregateOutputColumns.find(
      (column) => column.source.alias === ref.table && column.source.column === ref.column,
    );
    return fromGroup ? (scope.aggregateRow[fromGroup.output] ?? null) : null;
  }

  const bundle = scope.bundle;
  if (!bundle) {
    return null;
  }

  if (ref.table) {
    return bundle[ref.table]?.[ref.column] ?? null;
  }

  if (scope.parsed.bindings.length === 1) {
    const alias = scope.parsed.bindings[0]?.alias;
    return alias ? (bundle[alias]?.[ref.column] ?? null) : null;
  }

  const matches = scope.parsed.bindings.filter(
    (binding) => bundle[binding.alias] && ref.column in (bundle[binding.alias] ?? {}),
  );
  if (matches.length === 1) {
    const alias = matches[0]?.alias;
    return alias ? (bundle[alias]?.[ref.column] ?? null) : null;
  }
  if (matches.length > 1) {
    throw new Error(`Ambiguous unqualified column reference: ${ref.column}`);
  }

  return null;
}

async function evaluateInCandidates<TContext>(
  raw: unknown,
  scope: PredicateEvalScope<TContext>,
): Promise<unknown[]> {
  const subquery = toSubqueryAst(raw);
  if (subquery) {
    const rows = await executeSubquery(subquery, scope);
    return rows.map((row) => {
      const key = Object.keys(row)[0];
      return key ? (row[key] ?? null) : null;
    });
  }

  const expr = raw as { type?: unknown; value?: unknown };
  if (expr?.type !== "expr_list" || !Array.isArray(expr.value)) {
    throw new Error("IN predicates must use literal lists or subqueries.");
  }

  if (expr.value.length === 1) {
    const directSubquery = toSubqueryAst(expr.value[0]);
    if (directSubquery) {
      const rows = await executeSubquery(directSubquery, scope);
      return rows.map((row) => {
        const key = Object.keys(row)[0];
        return key ? (row[key] ?? null) : null;
      });
    }
  }

  const values: unknown[] = [];
  for (const item of expr.value) {
    values.push(await evaluateExpressionValue(item, scope));
  }
  return values;
}

async function executeSubquery<TContext>(
  subquery: SelectAst,
  scope: PredicateEvalScope<TContext>,
): Promise<QueryRow[]> {
  try {
    return await executeSelectAst(subquery, scope.input, scope.cteRows);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    if (message.startsWith("Unknown table alias:")) {
      throw new Error("Correlated subqueries are not yet supported.");
    }
    throw error;
  }
}

function readFunctionName(raw: unknown): string {
  const node = raw as { name?: unknown } | undefined;
  const nameParts = Array.isArray(node?.name) ? node?.name : [];
  const first = nameParts[0] as { value?: unknown } | undefined;
  return typeof first?.value === "string" ? first.value.toUpperCase() : "";
}

function readFunctionArgs(raw: { value?: unknown } | undefined): unknown[] {
  const value = raw?.value;
  if (Array.isArray(value)) {
    return value;
  }
  return value != null ? [value] : [];
}

function compareSqlValues(left: unknown, right: unknown, operator: string): SqlTruth {
  if (left == null || right == null) {
    return null;
  }

  switch (operator) {
    case "=":
      return left === right;
    case "!=":
    case "<>":
      return left !== right;
    case ">":
      return compareNonNull(left, right) > 0;
    case ">=":
      return compareNonNull(left, right) >= 0;
    case "<":
      return compareNonNull(left, right) < 0;
    case "<=":
      return compareNonNull(left, right) <= 0;
    default:
      throw new Error(`Unsupported comparison operator: ${operator}`);
  }
}

function compareNonNull(left: unknown, right: unknown): number {
  if (typeof left === "number" && typeof right === "number") {
    return left === right ? 0 : left < right ? -1 : 1;
  }
  if (typeof left === "boolean" && typeof right === "boolean") {
    const leftNum = Number(left);
    const rightNum = Number(right);
    return leftNum === rightNum ? 0 : leftNum < rightNum ? -1 : 1;
  }

  const leftString = String(left);
  const rightString = String(right);
  if (leftString === rightString) {
    return 0;
  }
  return leftString < rightString ? -1 : 1;
}

async function projectResultRows<TContext>(
  rows: JoinedRowBundle[],
  parsed: ParsedSelectQuery,
  input: QueryInput<TContext>,
  cteRows: Map<string, QueryRow[]>,
): Promise<QueryRow[]> {
  if (parsed.selectAll) {
    const baseAlias = parsed.bindings[0]?.alias;
    if (!baseAlias) {
      return [];
    }

    return rows.map((row) => {
      const baseRow = row[baseAlias];
      return baseRow ? { ...baseRow } : {};
    });
  }

  return Promise.all(
    rows.map(async (bundle) => {
      const out: QueryRow = {};
      for (const item of parsed.selectColumns) {
        out[item.output] = bundle[item.alias]?.[item.column] ?? null;
      }
      for (const item of parsed.scalarSelectItems) {
        out[item.output] = await evaluateExpressionValue(item.expr, {
          parsed,
          input,
          cteRows,
          bundle,
        });
      }
      return out;
    }),
  );
}

function projectAggregateOutputRow(row: QueryRow, parsed: ParsedSelectQuery): QueryRow {
  const out: QueryRow = {};

  for (const column of parsed.aggregateOutputColumns) {
    out[column.output] = row[column.output] ?? null;
  }

  for (const metric of parsed.aggregateMetrics) {
    if (metric.hidden) {
      continue;
    }
    out[metric.output] = row[metric.output] ?? null;
  }

  return out;
}

function parseJoinCondition(
  raw: unknown,
  bindings: TableBinding[],
  aliasToBinding: Map<string, TableBinding>,
): JoinCondition {
  const expr = raw as {
    type?: unknown;
    operator?: unknown;
    left?: unknown;
    right?: unknown;
  };

  if (expr?.type !== "binary_expr" || expr.operator !== "=") {
    throw new Error("Only equality join conditions are currently supported.");
  }

  const left = resolveColumnRef(expr.left, bindings, aliasToBinding);
  const right = resolveColumnRef(expr.right, bindings, aliasToBinding);
  if (!left || !right) {
    throw new Error("JOIN conditions must compare two columns.");
  }

  return {
    leftAlias: left.alias,
    leftColumn: left.column,
    rightAlias: right.alias,
    rightColumn: right.column,
  };
}

function flattenConjunctiveWhere(where: unknown): unknown[] | null {
  if (!where) {
    return [];
  }

  const expr = where as {
    type?: unknown;
    operator?: unknown;
    left?: unknown;
    right?: unknown;
  };

  if (expr.type === "binary_expr" && expr.operator === "AND") {
    const left = flattenConjunctiveWhere(expr.left);
    const right = flattenConjunctiveWhere(expr.right);
    if (!left || !right) {
      return null;
    }

    return [...left, ...right];
  }

  if (expr.type === "binary_expr" && expr.operator === "OR") {
    return null;
  }

  if (expr.type === "function") {
    return null;
  }

  return [expr];
}

function normalizeBinaryOperator(raw: unknown): Exclude<ScanFilterClause["op"], never> {
  switch (raw) {
    case "=":
      return "eq";
    case "!=":
    case "<>":
      return "neq";
    case ">":
      return "gt";
    case ">=":
      return "gte";
    case "<":
      return "lt";
    case "<=":
      return "lte";
    case "IN":
      return "in";
    case "IS":
      return "is_null";
    case "IS NOT":
      return "is_not_null";
    default:
      throw new Error(`Unsupported operator: ${String(raw)}`);
  }
}

function invertOperator(
  op: Exclude<ScanFilterClause["op"], "in" | "is_null" | "is_not_null">,
): Exclude<ScanFilterClause["op"], "in" | "is_null" | "is_not_null"> {
  switch (op) {
    case "eq":
      return "eq";
    case "neq":
      return "neq";
    case "gt":
      return "lt";
    case "gte":
      return "lte";
    case "lt":
      return "gt";
    case "lte":
      return "gte";
  }
}

function toRawColumnRef(raw: unknown): { table: string | null; column: string } | undefined {
  const expr = raw as { type?: unknown; table?: unknown; column?: unknown };
  if (expr?.type !== "column_ref") {
    return undefined;
  }

  if (typeof expr.column !== "string" || expr.column.length === 0) {
    return undefined;
  }

  const table = typeof expr.table === "string" && expr.table.length > 0 ? expr.table : null;
  return {
    table,
    column: expr.column,
  };
}

function resolveColumnRef(
  raw: unknown,
  bindings: TableBinding[],
  aliasToBinding: Map<string, TableBinding>,
): { alias: string; column: string } | undefined {
  const rawRef = toRawColumnRef(raw);
  if (!rawRef) {
    return undefined;
  }

  if (rawRef.table) {
    if (!aliasToBinding.has(rawRef.table)) {
      throw new Error(`Unknown table alias: ${rawRef.table}`);
    }

    return {
      alias: rawRef.table,
      column: rawRef.column,
    };
  }

  if (bindings.length === 1) {
    return {
      alias: bindings[0]?.alias ?? "",
      column: rawRef.column,
    };
  }

  throw new Error(`Ambiguous unqualified column reference: ${rawRef.column}`);
}

function isStarColumn(raw: { expr?: unknown }): boolean {
  const expr = raw.expr as { type?: unknown; column?: unknown } | undefined;
  return expr?.type === "column_ref" && expr.column === "*";
}

function isStarExpr(raw: unknown): boolean {
  const expr = raw as { type?: unknown; value?: unknown } | undefined;
  return expr?.type === "star" && expr.value === "*";
}

function parseLiteral(raw: unknown): unknown | undefined {
  const expr = raw as { type?: unknown; value?: unknown };

  switch (expr?.type) {
    case "single_quote_string":
    case "double_quote_string":
    case "string":
      return String(expr.value ?? "");
    case "number": {
      const value = expr.value;
      if (typeof value === "number") {
        return value;
      }
      if (typeof value === "string") {
        const parsed = Number(value);
        return Number.isFinite(parsed) ? parsed : undefined;
      }
      return undefined;
    }
    case "bool":
      return Boolean(expr.value);
    case "null":
      return null;
    default:
      return undefined;
  }
}

function tryParseLiteralExpressionList(raw: unknown): unknown[] | undefined {
  const expr = raw as { type?: unknown; value?: unknown };
  if (expr?.type !== "expr_list" || !Array.isArray(expr.value)) {
    return undefined;
  }

  const values = expr.value.map((entry) => parseLiteral(entry));
  if (values.some((value) => value === undefined)) {
    return undefined;
  }

  return values;
}

function uniqueJoinEdges(edges: JoinCondition[]): JoinCondition[] {
  const seen = new Set<string>();
  const out: JoinCondition[] = [];

  for (const edge of edges) {
    const key = `${edge.leftAlias}.${edge.leftColumn}=${edge.rightAlias}.${edge.rightColumn}`;
    const reverseKey = `${edge.rightAlias}.${edge.rightColumn}=${edge.leftAlias}.${edge.leftColumn}`;
    if (seen.has(key) || seen.has(reverseKey)) {
      continue;
    }
    seen.add(key);
    out.push(edge);
  }

  return out;
}

function uniqueValues(rows: QueryRow[], column: string): unknown[] {
  const seen = new Set<unknown>();
  const out: unknown[] = [];
  for (const row of rows) {
    const value = row[column] ?? null;
    if (seen.has(value)) {
      continue;
    }

    seen.add(value);
    out.push(value);
  }
  return out;
}

function compareNullableValues(left: unknown, right: unknown): number {
  if (left === right) {
    return 0;
  }
  if (left == null) {
    return -1;
  }
  if (right == null) {
    return 1;
  }

  if (typeof left === "number" && typeof right === "number") {
    return left < right ? -1 : 1;
  }

  if (typeof left === "boolean" && typeof right === "boolean") {
    return Number(left) < Number(right) ? -1 : 1;
  }

  const leftString = String(left);
  const rightString = String(right);
  return leftString < rightString ? -1 : 1;
}

function dedupeInClauses(clauses: ScanFilterClause[]): ScanFilterClause[] {
  const out: ScanFilterClause[] = [];
  const seen = new Set<string>();

  for (const clause of clauses) {
    if (clause.op !== "in") {
      out.push(clause);
      continue;
    }

    const key = `${clause.column}:${JSON.stringify(clause.values)}`;
    if (seen.has(key)) {
      continue;
    }

    seen.add(key);
    out.push(clause);
  }

  return out;
}

function cloneSelectWithoutSetOperation(ast: SelectAst): SelectAst {
  const clone = {
    ...ast,
  };
  delete clone.set_op;
  delete clone._next;
  return clone;
}

function readSetOperationNext(raw: unknown): SelectAst | undefined {
  if (!raw || typeof raw !== "object") {
    return undefined;
  }

  const next = raw as SelectAst;
  return next.type === "select" ? next : undefined;
}

function dedupeRows(rows: QueryRow[]): QueryRow[] {
  const seen = new Set<string>();
  const out: QueryRow[] = [];
  for (const row of rows) {
    const key = rowSignature(row);
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    out.push(row);
  }
  return out;
}

function intersectRows(left: QueryRow[], right: QueryRow[]): QueryRow[] {
  const rightSet = new Set(right.map((row) => rowSignature(row)));
  return dedupeRows(left).filter((row) => rightSet.has(rowSignature(row)));
}

function exceptRows(left: QueryRow[], right: QueryRow[]): QueryRow[] {
  const rightSet = new Set(right.map((row) => rowSignature(row)));
  return dedupeRows(left).filter((row) => !rightSet.has(rowSignature(row)));
}

function rowSignature(row: QueryRow): string {
  const keys = Object.keys(row).sort();
  const payload = keys.map((key) => [key, row[key] ?? null]);
  return JSON.stringify(payload);
}

function toSubqueryAst(raw: unknown): SelectAst | undefined {
  if (!raw || typeof raw !== "object") {
    return undefined;
  }

  const wrapped = raw as { ast?: unknown; type?: unknown };
  if (wrapped.type === "select") {
    return wrapped as SelectAst;
  }

  if (!wrapped.ast || typeof wrapped.ast !== "object") {
    return undefined;
  }

  const ast = wrapped.ast as SelectAst;
  return ast.type === "select" ? ast : undefined;
}

function sourceColumnKey(alias: string, column: string): string {
  return `${alias}.${column}`;
}

function astifySingleSelect(sql: string): SelectAst {
  const astRaw = defaultSqlAstParser.astify(sql);
  if (Array.isArray(astRaw)) {
    throw new Error("Only a single SQL statement is supported.");
  }

  if (!astRaw || typeof astRaw !== "object") {
    throw new Error("Unable to parse SQL statement.");
  }

  return astRaw as SelectAst;
}

function readCteName(rawCte: unknown): string {
  const nameNode = (rawCte as { name?: { value?: unknown } | unknown }).name;

  if (typeof nameNode === "string" && nameNode.length > 0) {
    return nameNode;
  }

  if (
    nameNode &&
    typeof nameNode === "object" &&
    "value" in nameNode &&
    typeof (nameNode as { value?: unknown }).value === "string"
  ) {
    return (nameNode as { value: string }).value;
  }

  throw new Error("Unable to parse CTE name.");
}
