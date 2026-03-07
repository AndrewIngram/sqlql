import {
  normalizeCapability,
  resolveTableProvider,
  type ProviderFragment,
  type ProvidersMap,
} from "./provider";
import {
  isRelProjectColumnMapping,
  type RelExpr,
  type RelJoinNode,
  type RelNode,
  type RelProjectNode,
  type RelScanNode,
} from "./rel";
import {
  getNormalizedColumnSourceMap,
  mapProviderRowsToLogical,
  resolveNormalizedColumnSource,
  getNormalizedTableBinding,
  type NormalizedPhysicalTableBinding,
  type QueryRow,
  type ScanFilterClause,
  type SchemaDefinition,
  type SchemaViewRelNode,
  type TableScanRequest,
} from "./schema";

export interface RelExecutionGuardrails {
  maxExecutionRows: number;
  maxLookupKeysPerBatch: number;
  maxLookupBatches: number;
}

export class UnsupportedRelExecutionError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "UnsupportedRelExecutionError";
  }
}

type InternalRow = Record<string, unknown>;

interface RelExecutionContext<TContext> {
  schema: SchemaDefinition;
  providers: ProvidersMap<TContext>;
  context: TContext;
  guardrails: RelExecutionGuardrails;
  lookupBatches: number;
  cteRows: Map<string, QueryRow[]>;
}

export async function executeRelWithProviders<TContext>(
  rel: RelNode,
  schema: SchemaDefinition,
  providers: ProvidersMap<TContext>,
  context: TContext,
  guardrails: RelExecutionGuardrails,
): Promise<QueryRow[]> {
  const executionContext: RelExecutionContext<TContext> = {
    schema,
    providers,
    context,
    guardrails,
    lookupBatches: 0,
    cteRows: new Map<string, QueryRow[]>(),
  };

  const rows = await executeRelNode(rel, executionContext);
  if (rows.length > guardrails.maxExecutionRows) {
    throw new Error(
      `Query exceeded maxExecutionRows guardrail (${guardrails.maxExecutionRows}). Received ${rows.length} rows.`,
    );
  }

  return rows;
}

async function executeRelNode<TContext>(
  node: RelNode,
  context: RelExecutionContext<TContext>,
): Promise<QueryRow[]> {
  switch (node.kind) {
    case "scan":
      return executeScan(node, context);
    case "join":
      return executeJoin(node, context);
    case "filter":
      return executeFilter(node, context);
    case "project":
      return executeProject(node, context);
    case "aggregate":
      return executeAggregate(node, context);
    case "window":
      return executeWindow(node, context);
    case "sort":
      return executeSort(node, context);
    case "limit_offset":
      return executeLimitOffset(node, context);
    case "set_op":
      return executeSetOp(node, context);
    case "with":
      return executeWith(node, context);
    case "sql":
      throw new UnsupportedRelExecutionError(
        "SQL-shaped rel nodes are not executable in the current provider runtime.",
      );
  }
}

async function executeScan<TContext>(
  scan: RelScanNode,
  context: RelExecutionContext<TContext>,
): Promise<InternalRow[]> {
  const normalizedBinding = getNormalizedTableBinding(context.schema, scan.table);
  if (normalizedBinding?.kind === "view") {
    const rel = compileViewRelToExecutable(scan.table, normalizedBinding.rel(context.context), context.schema);
    const viewRows = await executeRelNode(rel, context);
    const scanned = scanLocalRows(viewRows, {
      table: scan.table,
      ...(scan.alias ? { alias: scan.alias } : {}),
      select: scan.select,
      ...(scan.where ? { where: scan.where } : {}),
      ...(scan.orderBy ? { orderBy: scan.orderBy } : {}),
      ...(scan.limit != null ? { limit: scan.limit } : {}),
      ...(scan.offset != null ? { offset: scan.offset } : {}),
    });

    const alias = scan.alias ?? scan.table;
    return scanned.map((row) => prefixRow(row, alias));
  }

  const cteRows = context.cteRows.get(scan.table);
  if (cteRows) {
    const scanned = scanLocalRows(cteRows, {
      table: scan.table,
      ...(scan.alias ? { alias: scan.alias } : {}),
      select: scan.select,
      ...(scan.where ? { where: scan.where } : {}),
      ...(scan.orderBy ? { orderBy: scan.orderBy } : {}),
      ...(scan.limit != null ? { limit: scan.limit } : {}),
      ...(scan.offset != null ? { offset: scan.offset } : {}),
    });

    const alias = scan.alias ?? scan.table;
    return scanned.map((row) => prefixRow(row, alias));
  }

  const providerName = resolveTableProvider(context.schema, scan.table);
  const provider = context.providers[providerName];
  if (!provider) {
    throw new Error(`Missing provider adapter: ${providerName}`);
  }

  const physicalBinding = normalizedBinding?.kind === "physical" ? normalizedBinding : null;
  const request: TableScanRequest = {
    table: physicalBinding?.entity ?? scan.table,
    ...(scan.alias ? { alias: scan.alias } : {}),
    select: mapLogicalColumnsToSource(scan.select, physicalBinding),
    ...(scan.where ? { where: mapWhereToSource(scan.where, physicalBinding) } : {}),
    ...(scan.orderBy ? { orderBy: mapOrderToSource(scan.orderBy, physicalBinding) } : {}),
    ...(scan.limit != null ? { limit: scan.limit } : {}),
    ...(scan.offset != null ? { offset: scan.offset } : {}),
  };

  const fragment: ProviderFragment = {
    kind: "scan",
    provider: providerName,
    table: request.table,
    request,
  };

  const capability = normalizeCapability(await provider.canExecute(fragment, context.context));
  if (!capability.supported) {
    throw new UnsupportedRelExecutionError(
      `Provider ${providerName} cannot execute scan for table ${scan.table}${
        capability.reason ? `: ${capability.reason}` : ""
      }`,
    );
  }

  const compiled = await provider.compile(fragment, context.context);
  const rows = await provider.execute(compiled, context.context);
  const projected = mapProviderRowsToLogical(
    rows,
    scan.select,
    physicalBinding,
    context.schema.tables[scan.table],
  );

  const alias = scan.alias ?? scan.table;
  return projected.map((row) => prefixRow(row, alias));
}

async function executeFilter<TContext>(
  filter: Extract<RelNode, { kind: "filter" }>,
  context: RelExecutionContext<TContext>,
): Promise<InternalRow[]> {
  const rows = (await executeRelNode(filter.input, context)) as InternalRow[];
  let out = [...rows];

  for (const clause of filter.where ?? []) {
    out = out.filter((row) => matchesClause(row, clause));
  }

  if (filter.expr) {
    out = out.filter((row) => Boolean(evaluateRelExpr(filter.expr!, row)));
  }

  return out;
}

async function executeJoin<TContext>(
  join: RelJoinNode,
  context: RelExecutionContext<TContext>,
): Promise<InternalRow[]> {
  const leftRows = (await executeRelNode(join.left, context)) as InternalRow[];

  const lookupResult = await maybeExecuteLookupJoin(join, leftRows, context);
  if (lookupResult) {
    return lookupResult;
  }

  const rightRows = (await executeRelNode(join.right, context)) as InternalRow[];
  return applyLocalHashJoin(join, leftRows, rightRows);
}

async function maybeExecuteLookupJoin<TContext>(
  join: RelJoinNode,
  leftRows: InternalRow[],
  context: RelExecutionContext<TContext>,
): Promise<InternalRow[] | null> {
  if (join.joinType !== "inner" && join.joinType !== "left") {
    return null;
  }

  const leftScan = findFirstScan(join.left);
  const rightScan = findFirstScan(join.right);
  if (!leftScan || !rightScan) {
    return null;
  }

  const leftBinding = getNormalizedTableBinding(context.schema, leftScan.table);
  const rightBinding = getNormalizedTableBinding(context.schema, rightScan.table);
  if (leftBinding?.kind === "view" || rightBinding?.kind === "view") {
    return null;
  }

  const leftProviderName = resolveTableProvider(context.schema, leftScan.table);
  const rightProviderName = resolveTableProvider(context.schema, rightScan.table);
  if (leftProviderName === rightProviderName) {
    return null;
  }

  const rightProvider = context.providers[rightProviderName];
  if (!rightProvider?.lookupMany) {
    return null;
  }

  const leftKey = `${join.leftKey.alias}.${join.leftKey.column}`;
  const rightKey = rightBinding?.kind === "physical"
    ? resolveNormalizedColumnSource(rightBinding, join.rightKey.column)
    : join.rightKey.column;
  const dedupedKeys = [...new Set(leftRows.map((row) => row[leftKey]).filter((value) => value != null))];

  const rightRows: InternalRow[] = [];
  for (
    let startIndex = 0;
    startIndex < dedupedKeys.length;
    startIndex += context.guardrails.maxLookupKeysPerBatch
  ) {
    context.lookupBatches += 1;
    if (context.lookupBatches > context.guardrails.maxLookupBatches) {
      throw new Error(
        `Query exceeded maxLookupBatches guardrail (${context.guardrails.maxLookupBatches}).`,
      );
    }

    const batch = dedupedKeys.slice(
      startIndex,
      startIndex + context.guardrails.maxLookupKeysPerBatch,
    );

    const lookedUp = await rightProvider.lookupMany(
      {
        table: rightBinding?.kind === "physical" ? rightBinding.entity : rightScan.table,
        key: rightKey,
        keys: batch,
        select: mapLogicalColumnsToSource(rightScan.select, rightBinding?.kind === "physical" ? rightBinding : null),
        ...(rightScan.where
          ? { where: mapWhereToSource(rightScan.where, rightBinding?.kind === "physical" ? rightBinding : null) }
          : {}),
      },
      context.context,
    );

    const rightAlias = rightScan.alias ?? rightScan.table;
    for (const row of mapProviderRowsToLogical(
      lookedUp,
      rightScan.select,
      rightBinding?.kind === "physical" ? rightBinding : null,
      context.schema.tables[rightScan.table],
    )) {
      rightRows.push(prefixRow(row, rightAlias));
    }
  }

  return applyLocalHashJoin(join, leftRows, rightRows);
}

function applyLocalHashJoin(
  join: RelJoinNode,
  leftRows: InternalRow[],
  rightRows: InternalRow[],
): InternalRow[] {
  const leftKey = toColumnKey(join.leftKey);
  const rightKey = toColumnKey(join.rightKey);

  const rightIndex = new Map<unknown, InternalRow[]>();
  rightRows.forEach((row) => {
    const key = row[rightKey];
    if (key == null) {
      return;
    }

    const bucket = rightIndex.get(key) ?? [];
    bucket.push(row);
    rightIndex.set(key, bucket);
  });

  const joined: InternalRow[] = [];
  const matchedRightRows = new Set<InternalRow>();

  for (const leftRow of leftRows) {
    const key = leftRow[leftKey];
    const matches = key == null ? [] : (rightIndex.get(key) ?? []);

    if (join.joinType === "semi") {
      if (matches.length > 0) {
        joined.push({ ...leftRow });
      }
      continue;
    }

    if (matches.length === 0) {
      if (join.joinType === "left" || join.joinType === "full") {
        joined.push({ ...leftRow });
      }
      continue;
    }

    for (const match of matches) {
      matchedRightRows.add(match);
      joined.push({
        ...leftRow,
        ...match,
      });
    }
  }

  if (join.joinType === "right" || join.joinType === "full") {
    for (const rightRow of rightRows) {
      if (matchedRightRows.has(rightRow)) {
        continue;
      }
      joined.push({ ...rightRow });
    }
  }

  return joined;
}

async function executeWindow<TContext>(
  windowNode: Extract<RelNode, { kind: "window" }>,
  context: RelExecutionContext<TContext>,
): Promise<InternalRow[]> {
  const rows = (await executeRelNode(windowNode.input, context)) as InternalRow[];
  if (windowNode.functions.length === 0) {
    return rows;
  }

  let current = rows.map((row) => ({ ...row }));
  for (const fn of windowNode.functions) {
    current = applyWindowFunction(current, fn);
  }
  return current;
}

function applyWindowFunction(
  rows: InternalRow[],
  fn: Extract<RelNode, { kind: "window" }>["functions"][number],
): InternalRow[] {
  const partitioned = new Map<string, Array<{ row: InternalRow; index: number }>>();
  for (let index = 0; index < rows.length; index += 1) {
    const row = rows[index];
    if (!row) {
      continue;
    }
    const key = JSON.stringify(
      fn.partitionBy.map((ref) => readRowValue(row, toColumnKey(ref)) ?? null),
    );
    const bucket = partitioned.get(key) ?? [];
    bucket.push({ row, index });
    partitioned.set(key, bucket);
  }

  const out = rows.map((row) => ({ ...row }));

  for (const entries of partitioned.values()) {
    entries.sort((left, right) => compareWindowEntries(left.row, right.row, fn.orderBy));

    let denseRank = 0;
    let rank = 1;

    for (let idx = 0; idx < entries.length; idx += 1) {
      const entry = entries[idx];
      if (!entry) {
        continue;
      }
      const row = out[entry.index];
      if (!row) {
        continue;
      }

      if (fn.fn === "row_number") {
        row[fn.as] = idx + 1;
        continue;
      }

      const prev = idx > 0 ? entries[idx - 1] : undefined;
      const isPeer = prev
        ? compareWindowEntries(prev.row, entry.row, fn.orderBy) === 0
        : false;
      if (!isPeer) {
        denseRank += 1;
        rank = idx + 1;
      }

      row[fn.as] = fn.fn === "dense_rank" ? denseRank : rank;
    }
  }

  return out;
}

function compareWindowEntries(
  left: InternalRow,
  right: InternalRow,
  orderBy: Extract<RelNode, { kind: "window" }>["functions"][number]["orderBy"],
): number {
  for (const term of orderBy) {
    const comparison = compareNullableValues(
      readRowValue(left, toColumnKey(term.source)),
      readRowValue(right, toColumnKey(term.source)),
    );
    if (comparison !== 0) {
      return term.direction === "asc" ? comparison : -comparison;
    }
  }
  return 0;
}

async function executeProject<TContext>(
  project: RelProjectNode,
  context: RelExecutionContext<TContext>,
): Promise<QueryRow[]> {
  const rows = (await executeRelNode(project.input, context)) as InternalRow[];

  return rows.map((row) => {
    const out: QueryRow = {};
    for (const mapping of project.columns) {
      out[mapping.output] = isRelProjectColumnMapping(mapping)
        ? (readRowValue(row, toColumnKey(mapping.source)) ?? null)
        : evaluateRelExpr(mapping.expr, row);
    }
    return out;
  });
}

async function executeAggregate<TContext>(
  aggregate: Extract<RelNode, { kind: "aggregate" }>,
  context: RelExecutionContext<TContext>,
): Promise<QueryRow[]> {
  const rows = (await executeRelNode(aggregate.input, context)) as InternalRow[];
  const groups = new Map<string, InternalRow[]>();

  for (const row of rows) {
    const key = JSON.stringify(
      aggregate.groupBy.map((ref) => readRowValue(row, toColumnKey(ref)) ?? null),
    );
    const bucket = groups.get(key) ?? [];
    bucket.push(row);
    groups.set(key, bucket);
  }

  if (groups.size === 0 && aggregate.groupBy.length === 0) {
    groups.set("__all__", []);
  }

  const out: QueryRow[] = [];

  for (const [groupKey, bucket] of groups.entries()) {
    const row: QueryRow = {};

    if (aggregate.groupBy.length > 0) {
      const values = JSON.parse(groupKey) as unknown[];
      aggregate.groupBy.forEach((ref, index) => {
        row[ref.column] = values[index] ?? null;
      });
    }

    for (const metric of aggregate.metrics) {
      const values = metric.column
        ? bucket.map((entry) => readRowValue(entry, toColumnKey(metric.column!)) ?? null)
        : bucket.map(() => 1);
      const metricValues = metric.distinct
        ? [...new Map(values.map((value) => [JSON.stringify(value), value])).values()]
        : values;

      row[metric.as] = evaluateAggregateMetric(metric.fn, metricValues, bucket.length, metric.column != null);
    }

    out.push(row);
  }

  return out;
}

async function executeSort<TContext>(
  sort: Extract<RelNode, { kind: "sort" }>,
  context: RelExecutionContext<TContext>,
): Promise<InternalRow[]> {
  const rows = (await executeRelNode(sort.input, context)) as InternalRow[];
  const sorted = [...rows];

  sorted.sort((left, right) => {
    for (const term of sort.orderBy) {
      const comparison = compareNullableValues(
        readRowValue(left, toColumnKey(term.source)),
        readRowValue(right, toColumnKey(term.source)),
      );
      if (comparison !== 0) {
        return term.direction === "asc" ? comparison : -comparison;
      }
    }
    return 0;
  });

  return sorted;
}

async function executeLimitOffset<TContext>(
  limitOffset: Extract<RelNode, { kind: "limit_offset" }>,
  context: RelExecutionContext<TContext>,
): Promise<QueryRow[]> {
  let rows = await executeRelNode(limitOffset.input, context);

  if (limitOffset.offset != null) {
    rows = rows.slice(limitOffset.offset);
  }

  if (limitOffset.limit != null) {
    rows = rows.slice(0, limitOffset.limit);
  }

  return rows;
}

async function executeSetOp<TContext>(
  setOp: Extract<RelNode, { kind: "set_op" }>,
  context: RelExecutionContext<TContext>,
): Promise<QueryRow[]> {
  const leftRows = await executeRelNode(setOp.left, context);
  const rightRows = await executeRelNode(setOp.right, context);

  switch (setOp.op) {
    case "union_all":
      return [...leftRows, ...rightRows];
    case "union":
      return dedupeRows([...leftRows, ...rightRows]);
    case "intersect": {
      const rightKeys = new Set(rightRows.map((row) => stableRowKey(row)));
      return dedupeRows(leftRows.filter((row) => rightKeys.has(stableRowKey(row))));
    }
    case "except": {
      const rightKeys = new Set(rightRows.map((row) => stableRowKey(row)));
      return dedupeRows(leftRows.filter((row) => !rightKeys.has(stableRowKey(row))));
    }
  }
}

async function executeWith<TContext>(
  withNode: Extract<RelNode, { kind: "with" }>,
  context: RelExecutionContext<TContext>,
): Promise<QueryRow[]> {
  const cteRows = new Map(context.cteRows);
  const nested: RelExecutionContext<TContext> = {
    ...context,
    cteRows,
  };

  for (const cte of withNode.ctes) {
    const rows = await executeRelNode(cte.query, nested);
    cteRows.set(cte.name, rows);
  }

  return executeRelNode(withNode.body, nested);
}

function compileViewRelToExecutable(
  viewName: string,
  definition: SchemaViewRelNode | unknown,
  schema: SchemaDefinition,
): RelNode {
  if (isRelNode(definition)) {
    return definition;
  }

  if (!definition || typeof definition !== "object" || typeof (definition as { kind?: unknown }).kind !== "string") {
    throw new Error(`View ${viewName} returned an unsupported rel definition.`);
  }

  const rel = compileSchemaViewRelNode(definition as SchemaViewRelNode, schema);
  const binding = getNormalizedTableBinding(schema, viewName);
  if (!binding || binding.kind !== "view") {
    return rel;
  }

  const columns = Object.entries(getNormalizedColumnSourceMap(binding));
  return {
    id: nextSyntheticRelId("view_project"),
    kind: "project",
    convention: "local",
    input: rel,
    columns: columns.map(([output, source]) => ({
      kind: "column" as const,
      source: parseRef(source),
      output,
    })),
    output: columns.map(([name]) => ({ name })),
  };
}

function compileSchemaViewRelNode(node: SchemaViewRelNode, schema: SchemaDefinition): RelNode {
  switch (node.kind) {
    case "scan": {
      const table = schema.tables[node.table];
      if (!table) {
        throw new Error(`Unknown table in view rel scan: ${node.table}`);
      }
      const select = Object.keys(table.columns);
      return {
        id: nextSyntheticRelId("view_scan"),
        kind: "scan",
        convention: "local",
        table: node.table,
        alias: node.table,
        select,
        output: select.map((column) => ({ name: `${node.table}.${column}` })),
      };
    }
    case "join": {
      const left = compileSchemaViewRelNode(node.left, schema);
      const right = compileSchemaViewRelNode(node.right, schema);
      return {
        id: nextSyntheticRelId("view_join"),
        kind: "join",
        convention: "local",
        joinType: node.type,
        left,
        right,
        leftKey: parseRef(resolveSchemaColRef(node.on.left)),
        rightKey: parseRef(resolveSchemaColRef(node.on.right)),
        output: [...left.output, ...right.output],
      };
    }
    case "aggregate": {
      const input = compileSchemaViewRelNode(node.from, schema);
      const groupBy = Object.entries(node.groupBy).map(([name, column]) => ({
        name,
        ref: parseRef(resolveSchemaColRef(column)),
      }));
      const metrics = Object.entries(node.measures).map(([output, metric]) => ({
        fn: metric.fn,
        as: output,
        ...(metric.column ? { column: parseRef(resolveSchemaColRef(metric.column)) } : {}),
      }));

      return {
        id: nextSyntheticRelId("view_aggregate"),
        kind: "aggregate",
        convention: "local",
        input,
        groupBy: groupBy.map((entry) => entry.ref),
        metrics,
        output: [
          ...groupBy.map((column) => ({ name: column.name })),
          ...metrics.map((metric) => ({ name: metric.as })),
        ],
      };
    }
  }
}

function resolveSchemaColRef(ref: { ref?: string }): string {
  if (!ref.ref) {
    throw new Error("View rel column reference was not normalized to a string reference.");
  }
  return ref.ref;
}

function isRelNode(value: unknown): value is RelNode {
  return (
    !!value &&
    typeof value === "object" &&
    typeof (value as { kind?: unknown }).kind === "string" &&
    typeof (value as { convention?: unknown }).convention === "string"
  );
}

function parseRef(ref: string): { alias?: string; table?: string; column: string } {
  const parts = ref.split(".");
  if (parts.length === 1) {
    return {
      column: parts[0] ?? ref,
    };
  }
  const column = parts[parts.length - 1] ?? ref;
  const alias = parts.slice(0, -1).join(".");
  return {
    alias,
    column,
  };
}

let syntheticRelIdCounter = 0;

function nextSyntheticRelId(prefix: string): string {
  syntheticRelIdCounter += 1;
  return `${prefix}_${syntheticRelIdCounter}`;
}

function mapLogicalColumnsToSource(
  columns: string[],
  binding: NormalizedPhysicalTableBinding | null,
): string[] {
  if (!binding) {
    return columns;
  }
  return columns.map((column) => resolveNormalizedColumnSource(binding, column));
}

function mapWhereToSource(
  where: ScanFilterClause[],
  binding: NormalizedPhysicalTableBinding | null,
): ScanFilterClause[] {
  if (!binding) {
    return where;
  }

  return where.map((clause) => ({
    ...clause,
    column: resolveNormalizedColumnSource(binding, clause.column),
  }));
}

function mapOrderToSource(
  orderBy: NonNullable<TableScanRequest["orderBy"]>,
  binding: NormalizedPhysicalTableBinding | null,
): NonNullable<TableScanRequest["orderBy"]> {
  if (!binding) {
    return orderBy;
  }

  return orderBy.map((term) => ({
    ...term,
    column: resolveNormalizedColumnSource(binding, term.column),
  }));
}

function findFirstScan(node: RelNode): RelScanNode | null {
  switch (node.kind) {
    case "scan":
      return node;
    case "filter":
    case "project":
    case "aggregate":
    case "window":
    case "sort":
    case "limit_offset":
      return findFirstScan(node.input);
    case "join":
    case "set_op":
      return findFirstScan(node.left) ?? findFirstScan(node.right);
    case "with":
      return findFirstScan(node.body);
    case "sql":
      return null;
  }
}

function prefixRow(row: QueryRow, alias: string): InternalRow {
  const out: InternalRow = {};
  for (const [column, value] of Object.entries(row)) {
    out[`${alias}.${column}`] = value;
  }
  return out;
}

function scanLocalRows(rows: QueryRow[], request: TableScanRequest): QueryRow[] {
  let out = [...rows];
  for (const clause of request.where ?? []) {
    out = out.filter((row) => matchesClause(row, clause));
  }

  if (request.orderBy && request.orderBy.length > 0) {
    out = [...out].sort((left, right) => {
      for (const term of request.orderBy ?? []) {
        const comparison = compareNullableValues(left[term.column], right[term.column]);
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

function matchesClause(row: Record<string, unknown>, clause: ScanFilterClause): boolean {
  const value = readRowValue(row, clause.column);

  switch (clause.op) {
    case "eq":
      return value != null && value === clause.value;
    case "neq":
      return value != null && value !== clause.value;
    case "gt":
      return value != null && clause.value != null && compareNonNull(value, clause.value) > 0;
    case "gte":
      return value != null && clause.value != null && compareNonNull(value, clause.value) >= 0;
    case "lt":
      return value != null && clause.value != null && compareNonNull(value, clause.value) < 0;
    case "lte":
      return value != null && clause.value != null && compareNonNull(value, clause.value) <= 0;
    case "in": {
      const set = new Set(clause.values.filter((entry) => entry != null));
      return value != null && set.has(value);
    }
    case "not_in": {
      const set = new Set(clause.values.filter((entry) => entry != null));
      return value != null && !set.has(value);
    }
    case "like":
      return typeof value === "string" && typeof clause.value === "string"
        ? testSqlLikePattern(value, clause.value)
        : false;
    case "not_like":
      return typeof value === "string" && typeof clause.value === "string"
        ? !testSqlLikePattern(value, clause.value)
        : false;
    case "is_distinct_from":
      return value !== clause.value;
    case "is_not_distinct_from":
      return value === clause.value;
    case "is_null":
      return value == null;
    case "is_not_null":
      return value != null;
    default:
      return false;
  }
}

function evaluateRelExpr(expr: RelExpr, row: InternalRow): unknown {
  switch (expr.kind) {
    case "literal":
      return expr.value;
    case "column":
      return readRowValue(row, toColumnKey(expr.ref)) ?? null;
    case "function": {
      const args = expr.args.map((arg) => evaluateRelExpr(arg, row));
      switch (expr.name) {
        case "eq":
          return args[0] != null && args[0] === args[1];
        case "neq":
          return args[0] != null && args[0] !== args[1];
        case "gt":
          return args[0] != null && args[1] != null && compareNonNull(args[0], args[1]) > 0;
        case "gte":
          return args[0] != null && args[1] != null && compareNonNull(args[0], args[1]) >= 0;
        case "lt":
          return args[0] != null && args[1] != null && compareNonNull(args[0], args[1]) < 0;
        case "lte":
          return args[0] != null && args[1] != null && compareNonNull(args[0], args[1]) <= 0;
        case "and":
          return args.every(Boolean);
        case "or":
          return args.some(Boolean);
        case "not":
          return !args[0];
        case "add":
          return toFiniteNumber(args[0], "ADD") + toFiniteNumber(args[1], "ADD");
        case "subtract":
          return toFiniteNumber(args[0], "SUBTRACT") - toFiniteNumber(args[1], "SUBTRACT");
        case "multiply":
          return toFiniteNumber(args[0], "MULTIPLY") * toFiniteNumber(args[1], "MULTIPLY");
        case "divide":
          return toFiniteNumber(args[0], "DIVIDE") / toFiniteNumber(args[1], "DIVIDE");
        case "mod":
          return toFiniteNumber(args[0], "MOD") % toFiniteNumber(args[1], "MOD");
        case "concat":
          return args.map((arg) => (arg == null ? "" : String(arg))).join("");
        case "like":
          return typeof args[0] === "string" && typeof args[1] === "string"
            ? testSqlLikePattern(args[0], args[1])
            : false;
        case "not_like":
          return typeof args[0] === "string" && typeof args[1] === "string"
            ? !testSqlLikePattern(args[0], args[1])
            : false;
        case "in":
          return args[0] != null && args.slice(1).some((arg) => arg === args[0]);
        case "not_in":
          return args[0] != null && args.slice(1).every((arg) => arg !== args[0]);
        case "is_null":
          return args[0] == null;
        case "is_not_null":
          return args[0] != null;
        case "is_distinct_from":
          return args[0] !== args[1];
        case "is_not_distinct_from":
          return args[0] === args[1];
        case "between":
          return args[0] != null && args[1] != null && args[2] != null
            ? compareNonNull(args[0], args[1]) >= 0 && compareNonNull(args[0], args[2]) <= 0
            : false;
        case "lower":
          return args[0] == null ? null : String(args[0]).toLowerCase();
        case "upper":
          return args[0] == null ? null : String(args[0]).toUpperCase();
        case "trim":
          return args[0] == null ? null : String(args[0]).trim();
        case "length":
          return args[0] == null ? null : String(args[0]).length;
        case "substr": {
          if (args[0] == null || args[1] == null) {
            return null;
          }
          const input = String(args[0]);
          const start = Math.trunc(toFiniteNumber(args[1], "SUBSTR"));
          const begin = start >= 0 ? Math.max(0, start - 1) : Math.max(input.length + start, 0);
          if (args[2] == null) {
            return input.slice(begin);
          }
          const length = Math.max(0, Math.trunc(toFiniteNumber(args[2], "SUBSTR")));
          return input.slice(begin, begin + length);
        }
        case "coalesce":
          return args.find((arg) => arg != null) ?? null;
        case "nullif":
          return args[0] === args[1] ? null : (args[0] ?? null);
        case "abs":
          return args[0] == null ? null : Math.abs(toFiniteNumber(args[0], "ABS"));
        case "round": {
          if (args[0] == null) {
            return null;
          }
          const value = toFiniteNumber(args[0], "ROUND");
          const precision = args[1] == null ? 0 : Math.trunc(toFiniteNumber(args[1], "ROUND"));
          const scale = 10 ** precision;
          return Math.round(value * scale) / scale;
        }
        case "cast":
          return castValue(args[0], args[1]);
        case "case": {
          const lastIndex = args.length - 1;
          const hasElse = args.length % 2 === 1;
          for (let index = 0; index < (hasElse ? lastIndex : args.length); index += 2) {
            if (args[index]) {
              return args[index + 1] ?? null;
            }
          }
          return hasElse ? (args[lastIndex] ?? null) : null;
        }
        default:
          throw new UnsupportedRelExecutionError(`Unsupported computed expression function: ${expr.name}`);
      }
    }
  }
}

function testSqlLikePattern(value: string, pattern: string): boolean {
  const regex = new RegExp(
    `^${pattern.replace(/[.*+?^${}()|[\]\\]/g, "\\$&").replace(/%/g, ".*").replace(/_/g, ".")}$`,
    "su",
  );
  return regex.test(value);
}

function castValue(value: unknown, target: unknown): unknown {
  if (value == null) {
    return null;
  }
  const normalized = String(target ?? "").trim().toLowerCase();
  switch (normalized) {
    case "text":
      return String(value);
    case "integer":
    case "int":
      return Math.trunc(Number(value));
    case "real":
    case "numeric":
    case "float":
      return Number(value);
    case "boolean":
      return Boolean(value);
    default:
      throw new UnsupportedRelExecutionError(`Unsupported CAST target type: ${String(target)}`);
  }
}

function evaluateAggregateMetric(
  fn: "count" | "sum" | "avg" | "min" | "max",
  values: unknown[],
  bucketSize: number,
  hasColumn: boolean,
): unknown {
  switch (fn) {
    case "count":
      return hasColumn ? values.filter((value) => value != null).length : bucketSize;
    case "sum": {
      const numeric = values.filter((value) => value != null).map((value) => toFiniteNumber(value, "SUM"));
      return numeric.length > 0 ? numeric.reduce((sum, value) => sum + value, 0) : null;
    }
    case "avg": {
      const numeric = values.filter((value) => value != null).map((value) => toFiniteNumber(value, "AVG"));
      return numeric.length > 0
        ? numeric.reduce((sum, value) => sum + value, 0) / numeric.length
        : null;
    }
    case "min": {
      const candidates = values.filter((value) => value != null);
      return candidates.length > 0
        ? candidates.reduce((left, right) =>
            compareNullableValues(left, right) <= 0 ? left : right,
          )
        : null;
    }
    case "max": {
      const candidates = values.filter((value) => value != null);
      return candidates.length > 0
        ? candidates.reduce((left, right) =>
            compareNullableValues(left, right) >= 0 ? left : right,
          )
        : null;
    }
  }
}

function dedupeRows(rows: QueryRow[]): QueryRow[] {
  const byKey = new Map<string, QueryRow>();
  for (const row of rows) {
    byKey.set(stableRowKey(row), row);
  }
  return [...byKey.values()];
}

function stableRowKey(row: QueryRow): string {
  const entries = Object.entries(row).sort(([left], [right]) => left.localeCompare(right));
  return JSON.stringify(entries);
}

function readRowValue(row: Record<string, unknown>, column: string): unknown {
  if (column in row) {
    return row[column];
  }

  const suffix = `.${column}`;
  const candidates = Object.entries(row).filter(([key]) => key.endsWith(suffix));
  if (candidates.length === 1) {
    return candidates[0]?.[1];
  }

  return undefined;
}

function toColumnKey(ref: { alias?: string; table?: string; column: string }): string {
  const prefix = ref.alias ?? ref.table;
  return prefix ? `${prefix}.${ref.column}` : ref.column;
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

function compareNonNull(left: unknown, right: unknown): number {
  if (typeof left === "number" && typeof right === "number") {
    return left === right ? 0 : left < right ? -1 : 1;
  }

  if (typeof left === "boolean" && typeof right === "boolean") {
    const leftNumber = Number(left);
    const rightNumber = Number(right);
    return leftNumber === rightNumber ? 0 : leftNumber < rightNumber ? -1 : 1;
  }

  const leftString = String(left);
  const rightString = String(right);
  if (leftString === rightString) {
    return 0;
  }
  return leftString < rightString ? -1 : 1;
}

function toFiniteNumber(
  value: unknown,
  functionName:
    | "SUM"
    | "AVG"
    | "ADD"
    | "SUBTRACT"
    | "MULTIPLY"
    | "DIVIDE"
    | "MOD"
    | "SUBSTR"
    | "ABS"
    | "ROUND",
): number {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    throw new Error(`${functionName} expects numeric values.`);
  }
  return parsed;
}
