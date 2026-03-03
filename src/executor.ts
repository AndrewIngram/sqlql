import {
  normalizeCapability,
  resolveTableProvider,
  type ProviderFragment,
  type ProvidersMap,
} from "./provider";
import type { RelJoinNode, RelNode, RelProjectNode, RelScanNode } from "./rel";
import type { QueryRow, ScanFilterClause, SchemaDefinition, TableScanRequest } from "./schema";

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
    case "sort":
      return executeSort(node, context);
    case "limit_offset":
      return executeLimitOffset(node, context);
    case "set_op":
      return executeSetOp(node, context);
    case "with":
      return executeWith(node, context);
    case "sql":
      throw new UnsupportedRelExecutionError("SQL fallback rel nodes require provider sql_query execution.");
  }
}

async function executeScan<TContext>(
  scan: RelScanNode,
  context: RelExecutionContext<TContext>,
): Promise<InternalRow[]> {
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

  const request: TableScanRequest = {
    table: scan.table,
    ...(scan.alias ? { alias: scan.alias } : {}),
    select: scan.select,
    ...(scan.where ? { where: scan.where } : {}),
    ...(scan.orderBy ? { orderBy: scan.orderBy } : {}),
    ...(scan.limit != null ? { limit: scan.limit } : {}),
    ...(scan.offset != null ? { offset: scan.offset } : {}),
  };

  const fragment: ProviderFragment = {
    kind: "scan",
    provider: providerName,
    table: scan.table,
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

  const alias = scan.alias ?? scan.table;
  return rows.map((row) => prefixRow(row, alias));
}

async function executeFilter<TContext>(
  filter: Extract<RelNode, { kind: "filter" }>,
  context: RelExecutionContext<TContext>,
): Promise<InternalRow[]> {
  const rows = (await executeRelNode(filter.input, context)) as InternalRow[];
  let out = [...rows];

  for (const clause of filter.where) {
    out = out.filter((row) => matchesClause(row, clause));
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
  const rightKey = join.rightKey.column;
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
        table: rightScan.table,
        key: rightKey,
        keys: batch,
        select: rightScan.select,
        ...(rightScan.where ? { where: rightScan.where } : {}),
      },
      context.context,
    );

    const rightAlias = rightScan.alias ?? rightScan.table;
    for (const row of lookedUp) {
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
  const leftKey = `${join.leftKey.alias}.${join.leftKey.column}`;
  const rightKey = `${join.rightKey.alias}.${join.rightKey.column}`;

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

async function executeProject<TContext>(
  project: RelProjectNode,
  context: RelExecutionContext<TContext>,
): Promise<QueryRow[]> {
  const rows = (await executeRelNode(project.input, context)) as InternalRow[];

  return rows.map((row) => {
    const out: QueryRow = {};
    for (const mapping of project.columns) {
      const alias = mapping.source.alias;
      const key = `${alias}.${mapping.source.column}`;
      out[mapping.output] = row[key] ?? null;
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
      const key = `${term.source.alias}.${term.source.column}`;
      const comparison = compareNullableValues(left[key], right[key]);
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

function findFirstScan(node: RelNode): RelScanNode | null {
  switch (node.kind) {
    case "scan":
      return node;
    case "filter":
    case "project":
    case "aggregate":
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
    case "is_null":
      return value == null;
    case "is_not_null":
      return value != null;
    default:
      return false;
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

function toFiniteNumber(value: unknown, functionName: "SUM" | "AVG"): number {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    throw new Error(`${functionName} expects numeric values.`);
  }
  return parsed;
}
