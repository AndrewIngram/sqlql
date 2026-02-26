import {
  getTable,
  resolveTableQueryBehavior,
  type AggregateFunction,
  type NullFilterClause,
  type QueryRow,
  type ScanFilterClause,
  type ScanOrderBy,
  type ScalarFilterClause,
  type SchemaDefinition,
  type SetFilterClause,
  type TableAggregateMetric,
  type TableAggregateRequest,
  type TableMethods,
  type TableMethodsMap,
  type TableScanRequest,
} from "@sqlql/core";
import nodeSqlParser from "node-sql-parser";

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

interface TableBinding {
  table: string;
  alias: string;
  index: number;
}

interface JoinCondition {
  leftAlias: string;
  leftColumn: string;
  rightAlias: string;
  rightColumn: string;
}

interface ParsedJoin {
  alias: string;
  join: "inner" | "left";
  condition: JoinCondition;
}

interface SelectColumn {
  alias: string;
  column: string;
  output: string;
}

interface OrderColumn {
  alias: string;
  column: string;
  direction: "asc" | "desc";
}

interface LiteralFilter {
  alias: string;
  clause: ScanFilterClause;
}

interface ParsedAggregate {
  fn: AggregateFunction;
  alias: string;
  tableAlias: string;
  column?: string;
  distinct?: boolean;
}

interface ParsedGroupBy {
  alias: string;
  column: string;
}

// WhereNode represents the full WHERE predicate tree, including OR/NOT branches
// that cannot be pushed down to individual scan calls.
type WhereNode =
  | { kind: "and"; parts: WhereNode[] }
  | { kind: "or"; parts: WhereNode[] }
  | { kind: "not"; inner: WhereNode }
  | { kind: "filter"; alias: string; clause: ScanFilterClause };

interface ParsedSelectQuery {
  bindings: TableBinding[];
  joins: ParsedJoin[];
  joinEdges: JoinCondition[];
  filters: LiteralFilter[];
  postWhere?: WhereNode;
  selectAll: boolean;
  selectColumns: SelectColumn[];
  aggregates?: ParsedAggregate[];
  groupBy?: ParsedGroupBy[];
  isAggregate: boolean;
  orderBy: OrderColumn[];
  limit?: number;
  offset?: number;
  distinct: boolean;
}

const { Parser } = nodeSqlParser as { Parser: new () => { astify: (sql: string) => unknown } };
const parser = new Parser();

export function parseSql(query: SqlQuery, schema: SchemaDefinition): PlannedQuery {
  const parsed = parseSelectAst(query.text, schema);
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
  const parsed = parseSelectAst(input.sql, input.schema);
  const rootBinding = parsed.bindings[0];
  if (!rootBinding) {
    throw new Error("SELECT queries must include a FROM clause.");
  }

  for (const binding of parsed.bindings) {
    getTable(input.schema, binding.table);
    if (!input.methods[binding.table]) {
      throw new Error(`No table methods registered for table: ${binding.table}`);
    }
  }

  if (parsed.isAggregate) {
    return executeAggregateQuery(parsed, input);
  }

  return executeScanQuery(parsed, input);
}

async function executeScanQuery<TContext>(
  parsed: ParsedSelectQuery,
  input: QueryInput<TContext>,
): Promise<QueryRow[]> {
  const rootBinding = parsed.bindings[0];
  if (!rootBinding) {
    throw new Error("SELECT queries must include a FROM clause.");
  }

  const projectionByAlias = buildProjection(parsed, input.schema);
  const filtersByAlias = groupFiltersByAlias(parsed.filters);
  const executionOrder = buildExecutionOrder(parsed.bindings, parsed.joinEdges, filtersByAlias);
  const rowsByAlias = new Map<string, QueryRow[]>();

  for (const alias of executionOrder) {
    const binding = parsed.bindings.find((candidate) => candidate.alias === alias);
    if (!binding) {
      throw new Error(`Unknown alias in execution order: ${alias}`);
    }

    const dependencyFilters = buildDependencyFilters(alias, parsed.joinEdges, rowsByAlias);
    const localFilters = filtersByAlias.get(alias) ?? [];

    if (dependencyFilters.some((filter) => filter.op === "in" && (filter as SetFilterClause).values.length === 0)) {
      rowsByAlias.set(alias, []);
      continue;
    }

    const tableBehavior = resolveTableQueryBehavior(input.schema, binding.table);
    const defaultMaxRows = tableBehavior.maxRows;
    const requestWhere: ScanFilterClause[] = [...localFilters, ...dependencyFilters];

    const isSingleTable = parsed.bindings.length === 1;
    const canPushFinalSort =
      isSingleTable && parsed.orderBy.every((term) => term.alias === alias);
    const requestOrderBy: ScanOrderBy[] | undefined = canPushFinalSort
      ? parsed.orderBy.map((term) => ({
          column: term.column,
          direction: term.direction,
        }))
      : undefined;

    const canPushFinalLimit = isSingleTable && !parsed.distinct && !parsed.postWhere;
    let requestLimit = canPushFinalLimit ? parsed.limit : undefined;
    if (requestLimit == null && defaultMaxRows != null) {
      requestLimit = defaultMaxRows;
    }
    if (requestLimit != null && defaultMaxRows != null && requestLimit > defaultMaxRows) {
      throw new Error(
        `Requested limit ${requestLimit} exceeds maxRows ${defaultMaxRows} for table ${binding.table}`,
      );
    }

    const canPushOffset = isSingleTable && !parsed.distinct && !parsed.postWhere;
    const requestOffset = canPushOffset ? parsed.offset : undefined;

    const method = input.methods[binding.table];
    if (!method) {
      throw new Error(`No table methods registered for table: ${binding.table}`);
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

    const rows = await runScan(method, request, input.context);
    rowsByAlias.set(alias, rows);
  }

  let joinedRows = initializeJoinedRows(rowsByAlias, rootBinding.alias);
  for (const join of parsed.joins) {
    joinedRows =
      join.join === "left"
        ? applyLeftJoin(joinedRows, join, rowsByAlias)
        : applyInnerJoin(joinedRows, join, rowsByAlias);
  }

  if (parsed.postWhere) {
    const node = parsed.postWhere;
    joinedRows = joinedRows.filter((bundle) => evalWhereNode(node, bundle));
  }

  if (parsed.orderBy.length > 0) {
    joinedRows = applyFinalSort(joinedRows, parsed.orderBy);
  }

  // For multi-table joins, DISTINCT, or complex WHERE, LIMIT/OFFSET must be applied
  // in memory after all results are assembled. Single-table queries push them to the scan.
  const needsPostSlice = parsed.bindings.length > 1 || parsed.distinct || Boolean(parsed.postWhere);
  if (needsPostSlice) {
    const start = parsed.offset ?? 0;
    joinedRows =
      parsed.limit != null
        ? joinedRows.slice(start, start + parsed.limit)
        : start > 0
          ? joinedRows.slice(start)
          : joinedRows;
  }

  let result = projectResultRows(joinedRows, parsed);

  if (parsed.distinct) {
    result = applyDistinct(result);
  }

  return result;
}

async function executeAggregateQuery<TContext>(
  parsed: ParsedSelectQuery,
  input: QueryInput<TContext>,
): Promise<QueryRow[]> {
  const aggregates = parsed.aggregates ?? [];
  const groupBy = parsed.groupBy ?? [];
  const rootBinding = parsed.bindings[0];
  if (!rootBinding) {
    throw new Error("SELECT queries must include a FROM clause.");
  }

  // For single-table queries, try to delegate to the aggregate method.
  if (parsed.bindings.length === 1) {
    const method = input.methods[rootBinding.table];
    if (!method) {
      throw new Error(`No table methods registered for table: ${rootBinding.table}`);
    }

    if (method.aggregate) {
      const metrics: TableAggregateMetric[] = aggregates.map((agg) => {
        const metric: TableAggregateMetric = { fn: agg.fn, as: agg.alias };
        if (agg.column != null) {
          metric.column = agg.column;
        }
        if (agg.distinct) {
          metric.distinct = agg.distinct;
        }
        return metric;
      });

      const localFilters = parsed.filters
        .filter((f) => f.alias === rootBinding.alias)
        .map((f) => f.clause);

      const aggRequest: TableAggregateRequest = {
        table: rootBinding.table,
        alias: rootBinding.alias,
        metrics,
      };
      if (localFilters.length > 0) {
        aggRequest.where = localFilters;
      }
      if (groupBy.length > 0) {
        aggRequest.groupBy = groupBy.map((g) => g.column);
      }
      if (parsed.limit != null) {
        aggRequest.limit = parsed.limit;
      }

      return method.aggregate(aggRequest, input.context);
    }
  }

  // Fallback: scan (+ join if multi-table) and aggregate in memory.
  const projectionByAlias = buildAggregateProjection(parsed, input.schema);
  const filtersByAlias = groupFiltersByAlias(parsed.filters);
  const executionOrder = buildExecutionOrder(parsed.bindings, parsed.joinEdges, filtersByAlias);
  const rowsByAlias = new Map<string, QueryRow[]>();

  for (const alias of executionOrder) {
    const binding = parsed.bindings.find((b) => b.alias === alias);
    if (!binding) {
      throw new Error(`Unknown alias in execution order: ${alias}`);
    }

    const dependencyFilters = buildDependencyFilters(alias, parsed.joinEdges, rowsByAlias);
    const localFilters = filtersByAlias.get(alias) ?? [];

    if (dependencyFilters.some((f) => f.op === "in" && (f as SetFilterClause).values.length === 0)) {
      rowsByAlias.set(alias, []);
      continue;
    }

    const requestWhere: ScanFilterClause[] = [...localFilters, ...dependencyFilters];
    const method = input.methods[binding.table];
    if (!method) {
      throw new Error(`No table methods registered for table: ${binding.table}`);
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

    const rows = await runScan(method, request, input.context);
    rowsByAlias.set(alias, rows);
  }

  let joinedRows = initializeJoinedRows(rowsByAlias, rootBinding.alias);
  for (const join of parsed.joins) {
    joinedRows =
      join.join === "left"
        ? applyLeftJoin(joinedRows, join, rowsByAlias)
        : applyInnerJoin(joinedRows, join, rowsByAlias);
  }

  if (parsed.postWhere) {
    const node = parsed.postWhere;
    joinedRows = joinedRows.filter((bundle) => evalWhereNode(node, bundle));
  }

  return applyInMemoryAggregation(joinedRows, parsed);
}

function applyInMemoryAggregation(
  rows: Array<Record<string, QueryRow>>,
  parsed: ParsedSelectQuery,
): QueryRow[] {
  const groupBy = parsed.groupBy ?? [];
  const aggregates = parsed.aggregates ?? [];

  // If no GROUP BY and no rows, return a single empty aggregate row (SQL semantics)
  if (groupBy.length === 0 && rows.length === 0) {
    const emptyRow: QueryRow = {};
    for (const agg of aggregates) {
      emptyRow[agg.alias] = agg.fn === "count" ? 0 : null;
    }
    return [emptyRow];
  }

  // Build groups
  const groups = new Map<string, Array<Record<string, QueryRow>>>();
  const groupKeyOrder: string[] = [];

  if (groupBy.length === 0) {
    // No GROUP BY: treat all rows as one group
    groups.set("__all__", rows);
    groupKeyOrder.push("__all__");
  } else {
    for (const bundle of rows) {
      const keyParts = groupBy.map((g) => {
        const val = bundle[g.alias]?.[g.column];
        return val === null || val === undefined ? "\x1Enull" : String(val);
      });
      const key = keyParts.join("\x1E");

      if (!groups.has(key)) {
        groups.set(key, []);
        groupKeyOrder.push(key);
      }
      groups.get(key)!.push(bundle);
    }
  }

  const result: QueryRow[] = [];

  for (const key of groupKeyOrder) {
    const groupRows = groups.get(key) ?? [];
    const outRow: QueryRow = {};

    // Add GROUP BY columns
    for (const g of groupBy) {
      const val = groupRows[0]?.[g.alias]?.[g.column] ?? null;
      const selectCol = parsed.selectColumns.find(
        (c) => c.alias === g.alias && c.column === g.column,
      );
      outRow[selectCol?.output ?? g.column] = val;
    }

    // Compute aggregates
    for (const agg of aggregates) {
      outRow[agg.alias] = computeAggregate(agg, groupRows);
    }

    result.push(outRow);
  }

  if (parsed.limit != null) {
    return result.slice(0, parsed.limit);
  }

  return result;
}

function computeAggregate(
  agg: ParsedAggregate,
  rows: Array<Record<string, QueryRow>>,
): number | null {
  if (agg.fn === "count") {
    if (agg.column == null) {
      return rows.length;
    }
    const values = rows
      .map((b) => b[agg.tableAlias]?.[agg.column!])
      .filter((v) => v !== null && v !== undefined);
    if (agg.distinct) {
      return new Set(values).size;
    }
    return values.length;
  }

  const values = rows
    .map((b) => b[agg.tableAlias]?.[agg.column!])
    .filter((v) => v !== null && v !== undefined) as number[];

  const nums = agg.distinct ? [...new Set(values)] : values;

  if (nums.length === 0) {
    return null;
  }

  switch (agg.fn) {
    case "sum":
      return nums.reduce((a, b) => Number(a) + Number(b), 0);
    case "avg":
      return nums.reduce((a, b) => Number(a) + Number(b), 0) / nums.length;
    case "min":
      return Math.min(...nums.map(Number));
    case "max":
      return Math.max(...nums.map(Number));
  }

  return null;
}

async function runScan<TContext>(
  method: TableMethods<TContext>,
  request: TableScanRequest,
  context: TContext,
): Promise<QueryRow[]> {
  const dependencyFilters = (request.where ?? []).filter(
    (clause) => clause.op === "in",
  ) as SetFilterClause[];

  if (
    dependencyFilters.length === 1 &&
    method.lookup &&
    dependencyFilters[0] &&
    dependencyFilters[0].values.length > 0 &&
    request.orderBy == null &&
    request.limit == null
  ) {
    const lookup = dependencyFilters[0];
    const nonDependencyFilters = request.where?.filter((clause) => clause !== lookup);
    const fullLookupRequest: Parameters<NonNullable<typeof method.lookup>>[0] = {
      table: request.table,
      key: lookup.column,
      values: lookup.values,
      select: request.select,
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

function parseSelectAst(sql: string, _schema: SchemaDefinition): ParsedSelectQuery {
  const astRaw = parser.astify(sql);
  if (Array.isArray(astRaw)) {
    throw new Error("Only a single SQL statement is supported.");
  }

  const ast = astRaw as {
    type?: unknown;
    from?: unknown;
    where?: unknown;
    columns?: unknown;
    orderby?: unknown;
    limit?: unknown;
    distinct?: unknown;
    groupby?: unknown;
  };

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
    };
  });

  const aliasToTable = new Map(
    bindings.map((binding: TableBinding) => [binding.alias, binding.table]),
  );

  const joins: ParsedJoin[] = [];
  const joinEdges: JoinCondition[] = [];

  for (let i = 1; i < rawFrom.length; i += 1) {
    const entry = rawFrom[i] as { join?: unknown; on?: unknown; as?: unknown; table?: unknown };
    const joinType = typeof entry.join === "string" ? entry.join.toUpperCase() : "";
    if (
      joinType !== "INNER JOIN" &&
      joinType !== "JOIN" &&
      joinType !== "LEFT JOIN" &&
      joinType !== "LEFT OUTER JOIN"
    ) {
      throw new Error(`Unsupported join type: ${String(entry.join ?? "unknown")}`);
    }

    const isLeft = joinType === "LEFT JOIN" || joinType === "LEFT OUTER JOIN";
    const parsedJoin = parseJoinCondition(entry.on);
    if (!aliasToTable.has(parsedJoin.leftAlias) || !aliasToTable.has(parsedJoin.rightAlias)) {
      throw new Error("JOIN condition references an unknown table alias.");
    }

    const joinedAlias =
      typeof entry.as === "string" && entry.as.length > 0 ? entry.as : String(entry.table);
    joins.push({
      alias: joinedAlias,
      join: isLeft ? "left" : "inner",
      condition: parsedJoin,
    });
    // Only inner join conditions become dependency edges for scan ordering
    if (!isLeft) {
      joinEdges.push(parsedJoin);
    }
  }

  // Parse WHERE into a rich predicate tree that supports OR/NOT/LIKE/etc.
  const whereNode = ast.where ? parseWhereNode(ast.where) : null;

  const filters: LiteralFilter[] = [];
  if (whereNode) {
    extractPushdownFilters(whereNode, filters);
  }

  let postWhere: WhereNode | undefined;
  if (whereNode && hasComplexPredicates(whereNode)) {
    postWhere = whereNode;
  }

  // Parse SELECT items: column refs and aggregate functions
  const selectColumnsRaw: unknown = ast.columns;
  const selectAll =
    selectColumnsRaw === "*" ||
    (Array.isArray(selectColumnsRaw) &&
      selectColumnsRaw.length === 1 &&
      isStarColumn(selectColumnsRaw[0] as { expr?: unknown }));

  const selectColumns: SelectColumn[] = [];
  const parsedAggregates: ParsedAggregate[] = [];
  let isAggregate = false;

  if (!selectAll) {
    if (!Array.isArray(selectColumnsRaw)) {
      throw new Error("Unsupported SELECT clause.");
    }

    for (const item of selectColumnsRaw) {
      if (!item || typeof item !== "object") {
        throw new Error("Unsupported SELECT item.");
      }

      const expr = (item as { expr?: unknown }).expr;
      const asAlias = (item as { as?: unknown }).as;

      // Check for aggregate function (aggr_func)
      const aggExpr = expr as { type?: unknown; name?: unknown; args?: unknown; over?: unknown };
      if (aggExpr?.type === "aggr_func") {
        if (aggExpr.over != null) {
          throw new Error("Window functions (OVER) are not yet supported.");
        }
        const fn = parseAggregateFunction(aggExpr.name);
        const outputAlias =
          typeof asAlias === "string" && asAlias.length > 0 ? asAlias : fn;
        const argExpr = (aggExpr.args as { expr?: unknown })?.expr;
        const argRef = toColumnRef(argExpr);
        const isDistinct = Boolean((aggExpr as { distinct?: unknown }).distinct);

        const aggEntry: ParsedAggregate = {
          fn,
          alias: outputAlias,
          tableAlias: argRef?.alias ?? bindings[0]?.alias ?? "",
          distinct: isDistinct,
        };
        if (argRef?.column != null) {
          aggEntry.column = argRef.column;
        }
        parsedAggregates.push(aggEntry);
        isAggregate = true;
        continue;
      }

      const colRef = toColumnRef(expr);
      if (!colRef) {
        throw new Error(
          "Only direct column references and aggregate functions are supported in SELECT.",
        );
      }

      const as = (item as { as?: unknown }).as;
      selectColumns.push({
        alias: colRef.alias,
        column: colRef.column,
        output:
          typeof as === "string" && as.length > 0
            ? as
            : selectColumns.some((existing) => existing.column === colRef.column)
              ? `${colRef.alias}.${colRef.column}`
              : colRef.column,
      });
    }
  }

  // Parse GROUP BY clause
  const groupByRaw = ast.groupby as { columns?: unknown[] } | null;
  const parsedGroupBy: ParsedGroupBy[] = [];
  if (groupByRaw?.columns && Array.isArray(groupByRaw.columns)) {
    for (const col of groupByRaw.columns) {
      const ref = toColumnRef(col);
      if (!ref) {
        throw new Error("Only column references are supported in GROUP BY.");
      }
      parsedGroupBy.push({ alias: ref.alias, column: ref.column });
    }
    if (parsedGroupBy.length > 0) {
      isAggregate = true;
    }
  }

  if (isAggregate && selectAll) {
    throw new Error("SELECT * is not supported with GROUP BY or aggregate functions.");
  }

  const distinct = ast.distinct === "DISTINCT";

  const orderBy: OrderColumn[] = [];
  if (Array.isArray(ast.orderby)) {
    for (const item of ast.orderby) {
      const colRef = toColumnRef((item as { expr?: unknown }).expr);
      if (!colRef) {
        throw new Error("Only column references are currently supported in ORDER BY.");
      }

      const rawType = (item as { type?: unknown }).type;
      orderBy.push({
        alias: colRef.alias,
        column: colRef.column,
        direction: rawType === "DESC" ? "desc" : "asc",
      });
    }
  }

  let limit: number | undefined;
  let offset: number | undefined;
  const rawLimit = ast.limit as { value?: Array<{ value?: unknown }> } | null;
  if (rawLimit && Array.isArray(rawLimit.value) && rawLimit.value.length > 0) {
    const first = rawLimit.value[0]?.value;
    if (typeof first === "number") {
      limit = first;
    } else if (typeof first === "string") {
      const parsed = Number(first);
      if (Number.isFinite(parsed)) {
        limit = parsed;
      }
    }
    if (limit == null) {
      throw new Error("Unable to parse LIMIT value.");
    }

    // OFFSET is the second entry in the limit value array (node-sql-parser format)
    if (rawLimit.value.length > 1) {
      const second = rawLimit.value[1]?.value;
      if (typeof second === "number") {
        offset = second;
      } else if (typeof second === "string") {
        const parsed = Number(second);
        if (Number.isFinite(parsed)) {
          offset = parsed;
        }
      }
    }
  }

  if (selectAll && bindings.length > 1) {
    throw new Error("SELECT * is only supported for single-table queries.");
  }

  const parsedQuery: ParsedSelectQuery = {
    bindings,
    joins,
    joinEdges: uniqueJoinEdges(joinEdges),
    filters,
    selectAll,
    selectColumns,
    isAggregate,
    orderBy,
    distinct,
  };
  if (postWhere) {
    parsedQuery.postWhere = postWhere;
  }
  if (parsedAggregates.length > 0) {
    parsedQuery.aggregates = parsedAggregates;
  }
  if (parsedGroupBy.length > 0) {
    parsedQuery.groupBy = parsedGroupBy;
  }
  if (limit != null) {
    parsedQuery.limit = limit;
  }
  if (offset != null) {
    parsedQuery.offset = offset;
  }

  return parsedQuery;
}

// ---------------------------------------------------------------------------
// WHERE tree parsing
// ---------------------------------------------------------------------------

function parseWhereNode(raw: unknown): WhereNode {
  const expr = raw as {
    type?: unknown;
    operator?: unknown;
    left?: unknown;
    right?: unknown;
  };

  if (!expr || typeof expr !== "object") {
    throw new Error("Unsupported WHERE clause.");
  }

  if (expr.type === "binary_expr") {
    const op = String(expr.operator ?? "").toUpperCase();

    if (op === "AND") {
      const leftNode = parseWhereNode(expr.left);
      const rightNode = parseWhereNode(expr.right);
      return flattenAnd([leftNode, rightNode]);
    }

    if (op === "OR") {
      const leftNode = parseWhereNode(expr.left);
      const rightNode = parseWhereNode(expr.right);
      return flattenOr([leftNode, rightNode]);
    }

    if (op === "IS") {
      const colRef = toColumnRef(expr.left);
      if (!colRef) {
        throw new Error("IS NULL must have a column reference on the left-hand side.");
      }
      const nullClause: NullFilterClause = { op: "is_null", column: colRef.column };
      return { kind: "filter", alias: colRef.alias, clause: nullClause };
    }

    if (op === "IS NOT") {
      const colRef = toColumnRef(expr.left);
      if (!colRef) {
        throw new Error("IS NOT NULL must have a column reference on the left-hand side.");
      }
      const nullClause: NullFilterClause = { op: "is_not_null", column: colRef.column };
      return { kind: "filter", alias: colRef.alias, clause: nullClause };
    }

    if (op === "IN") {
      const colRef = toColumnRef(expr.left);
      if (!colRef) {
        throw new Error("IN predicates must use a column on the left-hand side.");
      }
      const values = parseExpressionList(expr.right);
      const clause: SetFilterClause = { op: "in", column: colRef.column, values };
      return { kind: "filter", alias: colRef.alias, clause };
    }

    if (op === "NOT IN") {
      const colRef = toColumnRef(expr.left);
      if (!colRef) {
        throw new Error("NOT IN predicates must use a column on the left-hand side.");
      }
      const values = parseExpressionList(expr.right);
      const clause: SetFilterClause = { op: "not_in", column: colRef.column, values };
      return { kind: "filter", alias: colRef.alias, clause };
    }

    if (op === "LIKE") {
      const colRef = toColumnRef(expr.left);
      if (!colRef) {
        throw new Error("LIKE predicates must use a column on the left-hand side.");
      }
      const pattern = parseLiteral(expr.right);
      if (typeof pattern !== "string") {
        throw new Error("LIKE pattern must be a string literal.");
      }
      const clause: ScalarFilterClause = { op: "like", column: colRef.column, value: pattern };
      return { kind: "filter", alias: colRef.alias, clause };
    }

    if (op === "NOT LIKE") {
      const colRef = toColumnRef(expr.left);
      if (!colRef) {
        throw new Error("NOT LIKE predicates must use a column on the left-hand side.");
      }
      const pattern = parseLiteral(expr.right);
      if (typeof pattern !== "string") {
        throw new Error("NOT LIKE pattern must be a string literal.");
      }
      const clause: ScalarFilterClause = { op: "not_like", column: colRef.column, value: pattern };
      return { kind: "filter", alias: colRef.alias, clause };
    }

    if (op === "BETWEEN") {
      // Desugar to col >= low AND col <= high
      const colRef = toColumnRef(expr.left);
      if (!colRef) {
        throw new Error("BETWEEN must have a column reference on the left-hand side.");
      }
      const [low, high] = parseBetweenBounds(expr.right);
      const gteClause: ScalarFilterClause = { op: "gte", column: colRef.column, value: low };
      const lteClause: ScalarFilterClause = { op: "lte", column: colRef.column, value: high };
      return {
        kind: "and",
        parts: [
          { kind: "filter", alias: colRef.alias, clause: gteClause },
          { kind: "filter", alias: colRef.alias, clause: lteClause },
        ],
      };
    }

    if (op === "NOT BETWEEN") {
      // Desugar to col < low OR col > high
      const colRef = toColumnRef(expr.left);
      if (!colRef) {
        throw new Error("NOT BETWEEN must have a column reference on the left-hand side.");
      }
      const [low, high] = parseBetweenBounds(expr.right);
      const ltClause: ScalarFilterClause = { op: "lt", column: colRef.column, value: low };
      const gtClause: ScalarFilterClause = { op: "gt", column: colRef.column, value: high };
      return {
        kind: "or",
        parts: [
          { kind: "filter", alias: colRef.alias, clause: ltClause },
          { kind: "filter", alias: colRef.alias, clause: gtClause },
        ],
      };
    }

    // Standard comparison operators
    const normalizedOp = normalizeBinaryOperator(expr.operator);

    const leftCol = toColumnRef(expr.left);
    const rightCol = toColumnRef(expr.right);

    // Column = Column from different tables: join edge (handled separately; return pass-through)
    if (normalizedOp === "eq" && leftCol && rightCol) {
      return { kind: "and", parts: [] };
    }

    const leftLiteral = parseLiteral(expr.left);
    const rightLiteral = parseLiteral(expr.right);

    if (leftCol && rightLiteral !== undefined) {
      const clause: ScalarFilterClause = {
        op: normalizedOp as ScalarFilterClause["op"],
        column: leftCol.column,
        value: rightLiteral,
      };
      return { kind: "filter", alias: leftCol.alias, clause };
    }

    if (rightCol && leftLiteral !== undefined) {
      const inverted = invertOperator(
        normalizedOp as Exclude<ScalarFilterClause["op"], "like" | "not_like">,
      );
      const clause: ScalarFilterClause = {
        op: inverted,
        column: rightCol.column,
        value: leftLiteral,
      };
      return { kind: "filter", alias: rightCol.alias, clause };
    }

    throw new Error(
      "WHERE predicates must compare columns to literals (or column equality joins).",
    );
  }

  throw new Error("Unsupported WHERE clause type.");
}

function flattenAnd(parts: WhereNode[]): WhereNode {
  const flattened: WhereNode[] = [];
  for (const part of parts) {
    if (part.kind === "and") {
      flattened.push(...part.parts);
    } else {
      flattened.push(part);
    }
  }
  if (flattened.length === 0) {
    return { kind: "and", parts: [] };
  }
  if (flattened.length === 1) {
    return flattened[0]!;
  }
  return { kind: "and", parts: flattened };
}

function flattenOr(parts: WhereNode[]): WhereNode {
  const flattened: WhereNode[] = [];
  for (const part of parts) {
    if (part.kind === "or") {
      flattened.push(...part.parts);
    } else {
      flattened.push(part);
    }
  }
  if (flattened.length === 1) {
    return flattened[0]!;
  }
  return { kind: "or", parts: flattened };
}

/**
 * Extracts pushdownable (simple, non-OR, non-NOT) filters from a WhereNode.
 * Only AND-connected leaf filters are pushed to the scan method.
 */
function extractPushdownFilters(node: WhereNode, out: LiteralFilter[]): void {
  switch (node.kind) {
    case "filter":
      out.push({ alias: node.alias, clause: node.clause });
      break;
    case "and":
      for (const part of node.parts) {
        if (!hasComplexPredicates(part)) {
          extractPushdownFilters(part, out);
        }
      }
      break;
    // "or" and "not" are not pushed down to individual scan calls
  }
}

/**
 * Returns true if the WhereNode contains OR or NOT predicates, requiring
 * post-join evaluation that cannot be pushed down to individual scans.
 */
function hasComplexPredicates(node: WhereNode): boolean {
  switch (node.kind) {
    case "filter":
      return false;
    case "and":
      return node.parts.some(hasComplexPredicates);
    case "or":
      return true;
    case "not":
      return true;
  }
}

/**
 * Evaluates a WhereNode against a joined row bundle.
 * Each bundle maps table alias to the row returned by that table's scan.
 */
function evalWhereNode(node: WhereNode, bundle: Record<string, QueryRow>): boolean {
  switch (node.kind) {
    case "and":
      return node.parts.every((p) => evalWhereNode(p, bundle));
    case "or":
      return node.parts.some((p) => evalWhereNode(p, bundle));
    case "not":
      return !evalWhereNode(node.inner, bundle);
    case "filter": {
      const row = bundle[node.alias] ?? {};
      return evalFilterOnRow(node.clause, row);
    }
  }
}

function evalFilterOnRow(clause: ScanFilterClause, row: QueryRow): boolean {
  const val = row[clause.column];

  switch (clause.op) {
    case "eq":
      return val === (clause as ScalarFilterClause).value;
    case "neq":
      return val !== (clause as ScalarFilterClause).value;
    case "gt":
      return Number(val) > Number((clause as ScalarFilterClause).value);
    case "gte":
      return Number(val) >= Number((clause as ScalarFilterClause).value);
    case "lt":
      return Number(val) < Number((clause as ScalarFilterClause).value);
    case "lte":
      return Number(val) <= Number((clause as ScalarFilterClause).value);
    case "in": {
      const set = new Set((clause as SetFilterClause).values);
      return set.has(val);
    }
    case "not_in": {
      const set = new Set((clause as SetFilterClause).values);
      return !set.has(val);
    }
    case "is_null":
      return val === null || val === undefined;
    case "is_not_null":
      return val !== null && val !== undefined;
    case "like":
      return evalLikePattern(
        String(val ?? ""),
        String((clause as ScalarFilterClause).value ?? ""),
      );
    case "not_like":
      return !evalLikePattern(
        String(val ?? ""),
        String((clause as ScalarFilterClause).value ?? ""),
      );
  }
}

/**
 * Evaluates a SQL LIKE pattern against a value.
 * % matches any sequence of characters; _ matches any single character.
 */
function evalLikePattern(value: string, pattern: string): boolean {
  const escaped = pattern.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const regexStr = escaped.replace(/%/g, ".*").replace(/_/g, ".");
  return new RegExp(`^${regexStr}$`).test(value);
}

// ---------------------------------------------------------------------------
// Projection helpers
// ---------------------------------------------------------------------------

function buildProjection(
  parsed: ParsedSelectQuery,
  schema: SchemaDefinition,
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

    const allColumns = Object.keys(getTable(schema, base.table).columns);
    for (const column of allColumns) {
      projections.get(base.alias)?.add(column);
    }
  } else {
    for (const item of parsed.selectColumns) {
      projections.get(item.alias)?.add(item.column);
    }
  }

  for (const join of parsed.joinEdges) {
    projections.get(join.leftAlias)?.add(join.leftColumn);
    projections.get(join.rightAlias)?.add(join.rightColumn);
  }

  for (const join of parsed.joins) {
    if (join.join === "left") {
      projections.get(join.condition.leftAlias)?.add(join.condition.leftColumn);
      projections.get(join.condition.rightAlias)?.add(join.condition.rightColumn);
    }
  }

  for (const filter of parsed.filters) {
    projections.get(filter.alias)?.add(filter.clause.column);
  }

  if (parsed.postWhere) {
    collectWhereNodeColumns(parsed.postWhere, projections);
  }

  for (const term of parsed.orderBy) {
    projections.get(term.alias)?.add(term.column);
  }

  for (const [alias, cols] of projections) {
    if (cols.size === 0) {
      const binding = parsed.bindings.find((candidate) => candidate.alias === alias);
      if (binding) {
        const firstColumn = Object.keys(getTable(schema, binding.table).columns)[0];
        if (!firstColumn) {
          throw new Error(`Table ${binding.table} has no columns.`);
        }
        cols.add(firstColumn);
      }
    }
  }

  return projections;
}

function buildAggregateProjection(
  parsed: ParsedSelectQuery,
  schema: SchemaDefinition,
): Map<string, Set<string>> {
  const projections = new Map<string, Set<string>>();
  for (const binding of parsed.bindings) {
    projections.set(binding.alias, new Set());
  }

  for (const g of parsed.groupBy ?? []) {
    projections.get(g.alias)?.add(g.column);
  }

  for (const agg of parsed.aggregates ?? []) {
    if (agg.column != null) {
      projections.get(agg.tableAlias)?.add(agg.column);
    }
  }

  for (const filter of parsed.filters) {
    projections.get(filter.alias)?.add(filter.clause.column);
  }

  for (const join of parsed.joinEdges) {
    projections.get(join.leftAlias)?.add(join.leftColumn);
    projections.get(join.rightAlias)?.add(join.rightColumn);
  }

  for (const [alias, cols] of projections) {
    if (cols.size === 0) {
      const binding = parsed.bindings.find((candidate) => candidate.alias === alias);
      if (binding) {
        const firstColumn = Object.keys(getTable(schema, binding.table).columns)[0];
        if (!firstColumn) {
          throw new Error(`Table ${binding.table} has no columns.`);
        }
        cols.add(firstColumn);
      }
    }
  }

  return projections;
}

function collectWhereNodeColumns(
  node: WhereNode,
  projections: Map<string, Set<string>>,
): void {
  switch (node.kind) {
    case "filter":
      projections.get(node.alias)?.add(node.clause.column);
      break;
    case "and":
    case "or":
      for (const part of node.parts) {
        collectWhereNodeColumns(part, projections);
      }
      break;
    case "not":
      collectWhereNodeColumns(node.inner, projections);
      break;
  }
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
  joinEdges: JoinCondition[],
  rowsByAlias: Map<string, QueryRow[]>,
): ScanFilterClause[] {
  const clauses: ScanFilterClause[] = [];
  for (const edge of joinEdges) {
    if (edge.leftAlias === alias && rowsByAlias.has(edge.rightAlias)) {
      clauses.push({
        op: "in",
        column: edge.leftColumn,
        values: uniqueValues(rowsByAlias.get(edge.rightAlias) ?? [], edge.rightColumn),
      });
      continue;
    }

    if (edge.rightAlias === alias && rowsByAlias.has(edge.leftAlias)) {
      clauses.push({
        op: "in",
        column: edge.rightColumn,
        values: uniqueValues(rowsByAlias.get(edge.leftAlias) ?? [], edge.leftColumn),
      });
    }
  }

  return dedupeInClauses(clauses);
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
  const isJoinAliasLeft = join.condition.leftAlias === join.alias;
  const joinAliasColumn = isJoinAliasLeft ? join.condition.leftColumn : join.condition.rightColumn;
  const existingAlias = isJoinAliasLeft ? join.condition.rightAlias : join.condition.leftAlias;
  const existingColumn = isJoinAliasLeft ? join.condition.rightColumn : join.condition.leftColumn;

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
      continue;
    }

    const key = leftRow[existingColumn];
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
  const isJoinAliasLeft = join.condition.leftAlias === join.alias;
  const joinAliasColumn = isJoinAliasLeft ? join.condition.leftColumn : join.condition.rightColumn;
  const existingAlias = isJoinAliasLeft ? join.condition.rightAlias : join.condition.leftAlias;
  const existingColumn = isJoinAliasLeft ? join.condition.rightColumn : join.condition.leftColumn;

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
      joined.push({ ...bundle, [join.alias]: {} });
      continue;
    }

    const key = leftRow[existingColumn];
    const matches = index.get(key) ?? [];

    if (matches.length === 0) {
      // Left join: preserve the left row even when there is no right-side match
      joined.push({ ...bundle, [join.alias]: {} });
    } else {
      for (const match of matches) {
        joined.push({ ...bundle, [join.alias]: match });
      }
    }
  }

  return joined;
}

function applyFinalSort(
  rows: Array<Record<string, QueryRow>>,
  orderBy: OrderColumn[],
): Array<Record<string, QueryRow>> {
  const sorted = [...rows];
  sorted.sort((left, right) => {
    for (const term of orderBy) {
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

function projectResultRows(
  rows: Array<Record<string, QueryRow>>,
  parsed: ParsedSelectQuery,
): QueryRow[] {
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

  return rows.map((bundle) => {
    const out: QueryRow = {};
    for (const item of parsed.selectColumns) {
      const rowVal = bundle[item.alias]?.[item.column];
      out[item.output] = rowVal !== undefined ? rowVal : null;
    }
    return out;
  });
}

function applyDistinct(rows: QueryRow[]): QueryRow[] {
  const seen = new Set<string>();
  const out: QueryRow[] = [];

  for (const row of rows) {
    const key = JSON.stringify(row, (_, v) => (v === undefined ? null : v));
    if (!seen.has(key)) {
      seen.add(key);
      out.push(row);
    }
  }

  return out;
}

// ---------------------------------------------------------------------------
// SQL AST parsing helpers
// ---------------------------------------------------------------------------

function parseJoinCondition(raw: unknown): JoinCondition {
  const expr = raw as {
    type?: unknown;
    operator?: unknown;
    left?: unknown;
    right?: unknown;
  };

  if (expr?.type !== "binary_expr" || expr.operator !== "=") {
    throw new Error("Only equality join conditions are currently supported.");
  }

  const left = toColumnRef(expr.left);
  const right = toColumnRef(expr.right);
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

function normalizeBinaryOperator(raw: unknown): string {
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
    default:
      throw new Error(`Unsupported operator: ${String(raw)}`);
  }
}

function invertOperator(
  op: Exclude<ScalarFilterClause["op"], "like" | "not_like">,
): Exclude<ScalarFilterClause["op"], "like" | "not_like"> {
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

function toColumnRef(raw: unknown): { alias: string; column: string } | undefined {
  const expr = raw as { type?: unknown; table?: unknown; column?: unknown };
  if (expr?.type !== "column_ref") {
    return undefined;
  }

  if (typeof expr.column !== "string" || expr.column.length === 0) {
    return undefined;
  }

  if (typeof expr.table !== "string" || expr.table.length === 0) {
    throw new Error(`Ambiguous unqualified column reference: ${expr.column}`);
  }

  return {
    alias: expr.table,
    column: expr.column,
  };
}

function isStarColumn(raw: { expr?: unknown }): boolean {
  const expr = raw.expr as { type?: unknown; column?: unknown } | undefined;
  return expr?.type === "column_ref" && expr.column === "*";
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

function parseExpressionList(raw: unknown): unknown[] {
  const expr = raw as { type?: unknown; value?: unknown };
  if (expr?.type !== "expr_list" || !Array.isArray(expr.value)) {
    throw new Error("IN predicates must use literal lists.");
  }

  const values = expr.value.map((entry) => parseLiteral(entry));
  if (values.some((value) => value === undefined)) {
    throw new Error("IN predicates must contain only literal values.");
  }

  return values;
}

function parseBetweenBounds(raw: unknown): [unknown, unknown] {
  const expr = raw as { type?: unknown; value?: unknown };
  if (expr?.type !== "expr_list" || !Array.isArray(expr.value) || expr.value.length < 2) {
    throw new Error("BETWEEN requires two bound values.");
  }
  const low = parseLiteral(expr.value[0]);
  const high = parseLiteral(expr.value[1]);
  if (low === undefined || high === undefined) {
    throw new Error("BETWEEN bounds must be literal values.");
  }
  return [low, high];
}

function parseAggregateFunction(raw: unknown): AggregateFunction {
  const name = String(raw ?? "").toUpperCase();
  switch (name) {
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
      throw new Error(`Unsupported aggregate function: ${name}`);
  }
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

    const setClause = clause as SetFilterClause;
    const key = `${setClause.column}:${JSON.stringify(setClause.values)}`;
    if (seen.has(key)) {
      continue;
    }

    seen.add(key);
    out.push(clause);
  }

  return out;
}
