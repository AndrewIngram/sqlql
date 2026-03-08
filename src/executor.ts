import { Result, type Result as BetterResult } from "better-result";

import {
  type ConstraintValidationOptions,
  validateTableConstraintRows,
} from "./constraints";
import {
  SqlqlExecutionError,
  SqlqlGuardrailError,
  type SqlqlError,
  type SqlqlResult,
} from "./errors";
import {
  getDataEntityAdapter,
  normalizeCapability,
  resolveTableProvider,
  unwrapProviderOperationResult,
  type ProviderAdapter,
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
import { buildProviderFragmentForRelResult } from "./planning";
import {
  createPhysicalBindingFromEntity,
  createTableDefinitionFromEntity,
  getNormalizedColumnBindings,
  isNormalizedSourceColumnBinding,
  mapProviderRowsToLogical,
  mapProviderRowsToRelOutput,
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

type InternalRow = Record<string, unknown>;

interface RelExecutionContext<TContext> {
  schema: SchemaDefinition;
  providers: ProvidersMap<TContext>;
  context: TContext;
  guardrails: RelExecutionGuardrails;
  constraintValidation?: ConstraintValidationOptions;
  lookupBatches: number;
  cteRows: Map<string, QueryRow[]>;
  subqueryResults: Map<string, unknown>;
}

function toSqlqlExecutionError(error: unknown, operation: string): SqlqlError {
  if (SqlqlExecutionError.is(error) || SqlqlGuardrailError.is(error)) {
    return error;
  }

  return new SqlqlExecutionError({
    operation,
    message: error instanceof Error ? error.message : String(error),
    cause: error,
  });
}

function unwrapExecutionResult<T, E>(result: BetterResult<T, E>): T {
  if (Result.isError(result)) {
    throw result.error;
  }
  return result.value;
}

function tryExecutionStep<T>(operation: string, fn: () => T): SqlqlResult<T> {
  return Result.try({
    try: () => fn() as Awaited<T>,
    catch: (error) => toSqlqlExecutionError(error, operation),
  }) as SqlqlResult<T>;
}

async function tryExecutionStepAsync<T>(
  operation: string,
  fn: () => Promise<T>,
): Promise<SqlqlResult<T>> {
  return Result.tryPromise({
    try: fn,
    catch: (error) => toSqlqlExecutionError(error, operation),
  });
}

export async function executeRelWithProviders<TContext>(
  rel: RelNode,
  schema: SchemaDefinition,
  providers: ProvidersMap<TContext>,
  context: TContext,
  guardrails: RelExecutionGuardrails,
): Promise<QueryRow[]> {
  return unwrapExecutionResult(
    await executeRelWithProvidersResult(rel, schema, providers, context, guardrails),
  );
}

export async function executeRelWithProvidersResult<TContext>(
  rel: RelNode,
  schema: SchemaDefinition,
  providers: ProvidersMap<TContext>,
  context: TContext,
  guardrails: RelExecutionGuardrails,
  options: {
    constraintValidation?: ConstraintValidationOptions;
  } = {},
): Promise<SqlqlResult<QueryRow[]>> {
  const executionContext: RelExecutionContext<TContext> = {
    schema,
    providers,
    context,
    guardrails,
    ...(options.constraintValidation ? { constraintValidation: options.constraintValidation } : {}),
    lookupBatches: 0,
    cteRows: new Map<string, QueryRow[]>(),
    subqueryResults: new Map<string, unknown>(),
  };

  const subqueryPrepResult = await prepareSubqueryResultsResult(rel, executionContext);
  if (Result.isError(subqueryPrepResult)) {
    return subqueryPrepResult;
  }

  const rowsResult = await executeRelNodeResult(rel, executionContext);
  if (Result.isError(rowsResult)) {
    return rowsResult;
  }

  const rows = rowsResult.value;
  if (rows.length > guardrails.maxExecutionRows) {
    return Result.err(
      new SqlqlGuardrailError({
        guardrail: "maxExecutionRows",
        limit: guardrails.maxExecutionRows,
        actual: rows.length,
        message: `Query exceeded maxExecutionRows guardrail (${guardrails.maxExecutionRows}). Received ${rows.length} rows.`,
      }),
    );
  }

  return Result.ok(rows);
}

async function executeRelNodeResult<TContext>(
  node: RelNode,
  context: RelExecutionContext<TContext>,
): Promise<SqlqlResult<QueryRow[]>> {
  const remoteRowsResult = await tryExecuteRemoteSubtreeResult(node, context);
  if (Result.isError(remoteRowsResult)) {
    return remoteRowsResult;
  }
  if (remoteRowsResult.value) {
    return Result.ok(remoteRowsResult.value);
  }

  switch (node.kind) {
    case "scan":
      return executeScanResult(node, context);
    case "join":
      return executeJoinResult(node, context);
    case "filter":
      return executeFilterResult(node, context);
    case "project":
      return executeProjectResult(node, context);
    case "aggregate":
      return executeAggregateResult(node, context);
    case "window":
      return executeWindowResult(node, context);
    case "sort":
      return executeSortResult(node, context);
    case "limit_offset":
      return executeLimitOffsetResult(node, context);
    case "set_op":
      return executeSetOpResult(node, context);
    case "with":
      return executeWithResult(node, context);
    case "sql":
      return Result.err(
        new SqlqlExecutionError({
          operation: "execute relational node",
          message: "SQL-shaped rel nodes are not executable in the current provider runtime.",
        }),
      );
  }
}

async function tryExecuteRemoteSubtreeResult<TContext>(
  node: RelNode,
  context: RelExecutionContext<TContext>,
): Promise<SqlqlResult<QueryRow[] | null>> {
  if (node.kind === "sql") {
    return Result.ok(null);
  }

  const normalizedBinding =
    node.kind === "scan" ? getNormalizedTableBinding(context.schema, node.table) : null;
  if (node.kind === "scan" && normalizedBinding?.kind !== "view") {
    return Result.ok(null);
  }

  const fragmentResult = buildProviderFragmentForRelResult(node, context.schema, context.context);
  if (Result.isError(fragmentResult)) {
    if (node.kind === "scan" && normalizedBinding?.kind === "view") {
      return Result.ok(null);
    }
    return fragmentResult;
  }
  const fragment = fragmentResult.value;
  if (!fragment) {
    return Result.ok(null);
  }

  const provider = resolveProviderForNode(node, fragment.provider, context);
  if (!provider) {
    return Result.err(
      new SqlqlExecutionError({
        operation: "execute relational node",
        message: `Missing provider adapter: ${fragment.provider}`,
      }),
    );
  }

  const capabilityResult = await tryExecutionStepAsync("check subtree provider capability", () =>
    Promise.resolve(provider.canExecute(fragment, context.context)),
  );
  if (Result.isError(capabilityResult)) {
    return capabilityResult;
  }

  const capability = normalizeCapability(capabilityResult.value);
  if (!capability.supported) {
    return Result.ok(null);
  }

  const compiledResult = await tryExecutionStepAsync("compile subtree provider fragment", () =>
    Promise.resolve(provider.compile(fragment, context.context)).then(
      unwrapProviderOperationResult,
    ),
  );
  if (Result.isError(compiledResult)) {
    return compiledResult;
  }

  const rowsResult = await tryExecutionStepAsync("execute subtree provider fragment", () =>
    Promise.resolve(provider.execute(compiledResult.value, context.context)).then(
      unwrapProviderOperationResult,
    ),
  );
  if (Result.isError(rowsResult)) {
    return rowsResult;
  }

  if (fragment.kind === "rel") {
    return tryExecutionStep("map provider rows to logical rel output rows", () =>
      mapProviderRowsToRelOutput(rowsResult.value, fragment.rel, context.schema),
    );
  }

  if (node.kind === "scan") {
    const physicalBinding =
      normalizedBinding?.kind === "physical"
        ? normalizedBinding
        : node.entity
          ? createPhysicalBindingFromEntity(node.entity)
          : null;
    const tableDefinition =
      context.schema.tables[node.table] ??
      (node.entity ? createTableDefinitionFromEntity(node.entity) : undefined);
    const projectedResult = tryExecutionStep("map provider rows to logical rows", () =>
      mapProviderRowsToLogical(
        rowsResult.value as QueryRow[],
        node.select,
        physicalBinding,
        tableDefinition,
        {
          enforceNotNull: !context.constraintValidation || context.constraintValidation.mode === "off",
          enforceEnum: !context.constraintValidation || context.constraintValidation.mode === "off",
        },
      ),
    );
    if (Result.isError(projectedResult)) {
      return projectedResult;
    }
    const validatedResult = tryExecutionStep("validate scan result constraints", () => {
      validateTableConstraintRows({
        schema: context.schema,
        tableName: node.table,
        rows: projectedResult.value,
        ...(context.constraintValidation ? { options: context.constraintValidation } : {}),
      });
    });
    if (Result.isError(validatedResult)) {
      return validatedResult;
    }

    const alias = node.alias ?? node.table;
    return Result.ok(projectedResult.value.map((row) => prefixRow(row, alias)));
  }

  return Result.ok(rowsResult.value);
}

function resolveProviderForNode<TContext>(
  node: RelNode,
  providerName: string,
  context: RelExecutionContext<TContext>,
): ProviderAdapter<TContext> | undefined {
  return context.providers[providerName] ?? findNodeProviderAdapter(node, providerName);
}

function findNodeProviderAdapter<TContext>(
  node: RelNode,
  providerName: string,
): ProviderAdapter<TContext> | undefined {
  switch (node.kind) {
    case "scan": {
      if (!node.entity || node.entity.provider !== providerName) {
        return undefined;
      }
      return getDataEntityAdapter(node.entity) as ProviderAdapter<TContext> | undefined;
    }
    case "filter":
    case "project":
    case "aggregate":
    case "window":
    case "sort":
    case "limit_offset":
      return findNodeProviderAdapter(node.input, providerName);
    case "join":
    case "set_op":
      return (
        findNodeProviderAdapter(node.left, providerName) ??
        findNodeProviderAdapter(node.right, providerName)
      );
    case "with":
      return (
        node.ctes.map((cte) => findNodeProviderAdapter(cte.query, providerName)).find(Boolean) ??
        findNodeProviderAdapter(node.body, providerName)
      );
    case "sql":
      return undefined;
  }
}

async function executeScanResult<TContext>(
  scan: RelScanNode,
  context: RelExecutionContext<TContext>,
): Promise<SqlqlResult<InternalRow[]>> {
  const normalizedBinding = getNormalizedTableBinding(context.schema, scan.table);
  if (normalizedBinding?.kind === "view") {
    const relResult = compileViewRelToExecutableResult(
      scan.table,
      normalizedBinding.rel(context.context),
      context.schema,
    );
    if (Result.isError(relResult)) {
      return relResult;
    }
    const viewRowsResult = await executeRelNodeResult(relResult.value, context);
    if (Result.isError(viewRowsResult)) {
      return viewRowsResult;
    }
    const scannedRows = scanLocalRows(viewRowsResult.value, {
      table: scan.table,
      ...(scan.alias ? { alias: scan.alias } : {}),
      select: scan.select,
      ...(scan.where ? { where: scan.where } : {}),
      ...(scan.orderBy ? { orderBy: scan.orderBy } : {}),
      ...(scan.limit != null ? { limit: scan.limit } : {}),
      ...(scan.offset != null ? { offset: scan.offset } : {}),
    });

    const alias = scan.alias ?? scan.table;
    return Result.ok(scannedRows.map((row) => prefixRow(row, alias)));
  }

  const cteRows = context.cteRows.get(scan.table);
  if (cteRows) {
    const scannedRows = scanLocalRows(cteRows, {
      table: scan.table,
      ...(scan.alias ? { alias: scan.alias } : {}),
      select: scan.select,
      ...(scan.where ? { where: scan.where } : {}),
      ...(scan.orderBy ? { orderBy: scan.orderBy } : {}),
      ...(scan.limit != null ? { limit: scan.limit } : {}),
      ...(scan.offset != null ? { offset: scan.offset } : {}),
    });

    const alias = scan.alias ?? scan.table;
    return Result.ok(scannedRows.map((row) => prefixRow(row, alias)));
  }

  const providerNameResult = tryExecutionStep(
    "resolve scan provider",
    () => scan.entity?.provider ?? resolveTableProvider(context.schema, scan.table),
  );
  if (Result.isError(providerNameResult)) {
    return providerNameResult;
  }
  const providerName = providerNameResult.value;
  const provider =
    context.providers[providerName] ??
    (scan.entity
      ? (getDataEntityAdapter(scan.entity) as ProviderAdapter<TContext> | undefined)
      : undefined);
  if (!provider) {
    return Result.err(
      new SqlqlExecutionError({
        operation: "execute scan",
        message: `Missing provider adapter: ${providerName}`,
      }),
    );
  }

  const physicalBinding =
    normalizedBinding?.kind === "physical"
      ? normalizedBinding
      : scan.entity
        ? createPhysicalBindingFromEntity(scan.entity)
        : null;
  const tableDefinition =
    context.schema.tables[scan.table] ??
    (scan.entity ? createTableDefinitionFromEntity(scan.entity) : undefined);
  const requestResult = tryExecutionStep(
    "build provider scan request",
    () =>
      ({
        table: physicalBinding?.entity ?? scan.table,
        ...(scan.alias ? { alias: scan.alias } : {}),
        select: mapLogicalColumnsToSource(scan.select, physicalBinding),
        ...(scan.where ? { where: mapWhereToSource(scan.where, physicalBinding) } : {}),
        ...(scan.orderBy ? { orderBy: mapOrderToSource(scan.orderBy, physicalBinding) } : {}),
        ...(scan.limit != null ? { limit: scan.limit } : {}),
        ...(scan.offset != null ? { offset: scan.offset } : {}),
      }) satisfies TableScanRequest,
  );
  if (Result.isError(requestResult)) {
    return requestResult;
  }
  const request = requestResult.value;

  const fragment: ProviderFragment = {
    kind: "scan",
    provider: providerName,
    table: request.table,
    request,
  };

  const capabilityResult = await tryExecutionStepAsync("check scan provider capability", () =>
    Promise.resolve(provider.canExecute(fragment, context.context)),
  );
  if (Result.isError(capabilityResult)) {
    return capabilityResult;
  }
  const capability = normalizeCapability(capabilityResult.value);
  if (!capability.supported) {
    return Result.err(
      new SqlqlExecutionError({
        operation: "execute scan",
        message: `Provider ${providerName} cannot execute scan for table ${scan.table}${
          capability.reason ? `: ${capability.reason}` : ""
        }`,
      }),
    );
  }

  const compiledResult = await tryExecutionStepAsync("compile scan provider fragment", () =>
    Promise.resolve(provider.compile(fragment, context.context)).then(
      unwrapProviderOperationResult,
    ),
  );
  if (Result.isError(compiledResult)) {
    return compiledResult;
  }
  const rowsResult = await tryExecutionStepAsync("execute scan provider fragment", () =>
    Promise.resolve(provider.execute(compiledResult.value, context.context)).then(
      unwrapProviderOperationResult,
    ),
  );
  if (Result.isError(rowsResult)) {
    return rowsResult;
  }
  const projectedResult = tryExecutionStep("map provider rows to logical rows", () =>
    mapProviderRowsToLogical(
      rowsResult.value as QueryRow[],
      scan.select,
      physicalBinding,
      tableDefinition,
      {
        enforceNotNull: !context.constraintValidation || context.constraintValidation.mode === "off",
        enforceEnum: !context.constraintValidation || context.constraintValidation.mode === "off",
      },
    ),
  );
  if (Result.isError(projectedResult)) {
    return projectedResult;
  }
  const validatedResult = tryExecutionStep("validate scan result constraints", () => {
    validateTableConstraintRows({
      schema: context.schema,
      tableName: scan.table,
      rows: projectedResult.value,
      ...(context.constraintValidation ? { options: context.constraintValidation } : {}),
    });
  });
  if (Result.isError(validatedResult)) {
    return validatedResult;
  }

  const alias = scan.alias ?? scan.table;
  return Result.ok(projectedResult.value.map((row) => prefixRow(row, alias)));
}

async function executeFilterResult<TContext>(
  filter: Extract<RelNode, { kind: "filter" }>,
  context: RelExecutionContext<TContext>,
): Promise<SqlqlResult<InternalRow[]>> {
  const rowsResult = await executeRelNodeResult(filter.input, context);
  if (Result.isError(rowsResult)) {
    return rowsResult;
  }

  let out = [...(rowsResult.value as InternalRow[])];
  for (const clause of filter.where ?? []) {
    out = out.filter((row) => matchesClause(row, clause));
  }

  if (!filter.expr) {
    return Result.ok(out);
  }

  const filtered: InternalRow[] = [];
  for (const row of out) {
    const exprResult = evaluateRelExprResult(filter.expr, row, context.subqueryResults);
    if (Result.isError(exprResult)) {
      return exprResult;
    }
    if (exprResult.value) {
      filtered.push(row);
    }
  }

  return Result.ok(filtered);
}

async function executeJoinResult<TContext>(
  join: RelJoinNode,
  context: RelExecutionContext<TContext>,
): Promise<SqlqlResult<InternalRow[]>> {
  const leftRowsResult = await executeRelNodeResult(join.left, context);
  if (Result.isError(leftRowsResult)) {
    return leftRowsResult;
  }

  const lookupResult = await maybeExecuteLookupJoinResult(
    join,
    leftRowsResult.value as InternalRow[],
    context,
  );
  if (Result.isError(lookupResult)) {
    return lookupResult;
  }
  if (lookupResult.value) {
    return Result.ok(lookupResult.value);
  }

  const rightRowsResult = await executeRelNodeResult(join.right, context);
  if (Result.isError(rightRowsResult)) {
    return rightRowsResult;
  }

  return Result.ok(
    applyLocalHashJoin(
      join,
      leftRowsResult.value as InternalRow[],
      rightRowsResult.value as InternalRow[],
    ),
  );
}

async function maybeExecuteLookupJoinResult<TContext>(
  join: RelJoinNode,
  leftRows: InternalRow[],
  context: RelExecutionContext<TContext>,
): Promise<SqlqlResult<InternalRow[] | null>> {
  if (join.joinType !== "inner" && join.joinType !== "left") {
    return Result.ok(null);
  }

  const leftScan = findFirstScan(join.left);
  const rightScan = findFirstScan(join.right);
  if (!leftScan || !rightScan) {
    return Result.ok(null);
  }

  const leftBinding = getNormalizedTableBinding(context.schema, leftScan.table);
  const rightBinding = getNormalizedTableBinding(context.schema, rightScan.table);
  if (leftBinding?.kind === "view" || rightBinding?.kind === "view") {
    return Result.ok(null);
  }

  const rightProviderName =
    rightScan.entity?.provider ?? resolveTableProvider(context.schema, rightScan.table);

  const rightProvider =
    context.providers[rightProviderName] ??
    (rightScan.entity
      ? (getDataEntityAdapter(rightScan.entity) as ProviderAdapter<TContext> | undefined)
      : undefined);
  if (!rightProvider?.lookupMany) {
    return Result.ok(null);
  }
  const lookupMany = rightProvider.lookupMany;

  const leftKey = `${join.leftKey.alias}.${join.leftKey.column}`;
  const rightPhysicalBinding =
    rightBinding?.kind === "physical"
      ? rightBinding
      : rightScan.entity
        ? createPhysicalBindingFromEntity(rightScan.entity)
        : null;
  const rightKey = rightPhysicalBinding
    ? resolveNormalizedColumnSource(rightPhysicalBinding, join.rightKey.column)
    : join.rightKey.column;
  const dedupedKeys = [
    ...new Set(leftRows.map((row) => row[leftKey]).filter((value) => value != null)),
  ];

  const rightRows: InternalRow[] = [];
  for (
    let startIndex = 0;
    startIndex < dedupedKeys.length;
    startIndex += context.guardrails.maxLookupKeysPerBatch
  ) {
    context.lookupBatches += 1;
    if (context.lookupBatches > context.guardrails.maxLookupBatches) {
      return Result.err(
        new SqlqlGuardrailError({
          guardrail: "maxLookupBatches",
          limit: context.guardrails.maxLookupBatches,
          actual: context.lookupBatches,
          message: `Query exceeded maxLookupBatches guardrail (${context.guardrails.maxLookupBatches}).`,
        }),
      );
    }

    const batch = dedupedKeys.slice(
      startIndex,
      startIndex + context.guardrails.maxLookupKeysPerBatch,
    );

    const lookedUpResult = await tryExecutionStepAsync("execute lookup join batch", async () =>
      unwrapProviderOperationResult(
        await lookupMany(
          {
            table: rightPhysicalBinding?.entity ?? rightScan.table,
            ...(rightScan.alias ? { alias: rightScan.alias } : {}),
            key: rightKey,
            keys: batch,
            select: mapLogicalColumnsToSource(rightScan.select, rightPhysicalBinding),
            ...(rightScan.where
              ? { where: mapWhereToSource(rightScan.where, rightPhysicalBinding) }
              : {}),
          },
          context.context,
        ),
      ),
    );
    if (Result.isError(lookedUpResult)) {
      return lookedUpResult;
    }

    const rightAlias = rightScan.alias ?? rightScan.table;
    const mappedRowsResult = tryExecutionStep("map lookup join rows to logical rows", () =>
      mapProviderRowsToLogical(
        lookedUpResult.value,
        rightScan.select,
        rightPhysicalBinding,
        context.schema.tables[rightScan.table] ??
          (rightScan.entity ? createTableDefinitionFromEntity(rightScan.entity) : undefined),
        {
          enforceNotNull: !context.constraintValidation || context.constraintValidation.mode === "off",
          enforceEnum: !context.constraintValidation || context.constraintValidation.mode === "off",
        },
      ),
    );
    if (Result.isError(mappedRowsResult)) {
      return mappedRowsResult;
    }
    for (const row of mappedRowsResult.value) {
      rightRows.push(prefixRow(row, rightAlias));
    }
  }

  return Result.ok(applyLocalHashJoin(join, leftRows, rightRows));
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

async function executeWindowResult<TContext>(
  windowNode: Extract<RelNode, { kind: "window" }>,
  context: RelExecutionContext<TContext>,
): Promise<SqlqlResult<InternalRow[]>> {
  const rowsResult = await executeRelNodeResult(windowNode.input, context);
  if (Result.isError(rowsResult)) {
    return rowsResult;
  }

  const rows = rowsResult.value as InternalRow[];
  if (windowNode.functions.length === 0) {
    return Result.ok(rows);
  }

  let current = rows.map((row) => ({ ...row }));
  for (const fn of windowNode.functions) {
    current = applyWindowFunction(current, fn);
  }
  return Result.ok(current);
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

      if (fn.fn === "rank" || fn.fn === "dense_rank") {
        const prev = idx > 0 ? entries[idx - 1] : undefined;
        const isPeer = prev ? compareWindowEntries(prev.row, entry.row, fn.orderBy) === 0 : false;
        if (!isPeer) {
          denseRank += 1;
          rank = idx + 1;
        }

        row[fn.as] = fn.fn === "dense_rank" ? denseRank : rank;
        continue;
      }

      const aggregateFn = fn as Extract<typeof fn, { fn: "count" | "sum" | "avg" | "min" | "max" }>;
      const frameEntries = aggregateFn.orderBy.length > 0 ? entries.slice(0, idx + 1) : entries;
      const values = aggregateFn.column
        ? frameEntries.map((current) => readRowValue(current.row, toColumnKey(aggregateFn.column!)) ?? null)
        : frameEntries.map(() => 1);
      const metricValues = aggregateFn.distinct
        ? [...new Map(values.map((value) => [JSON.stringify(value), value])).values()]
        : values;

      const metricResult = evaluateAggregateMetricResult(
        aggregateFn.fn,
        metricValues,
        frameEntries.length,
        aggregateFn.column != null,
      );
      if (Result.isError(metricResult)) {
        throw metricResult.error;
      }
      row[fn.as] = metricResult.value;
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

async function executeProjectResult<TContext>(
  project: RelProjectNode,
  context: RelExecutionContext<TContext>,
): Promise<SqlqlResult<QueryRow[]>> {
  const rowsResult = await executeRelNodeResult(project.input, context);
  if (Result.isError(rowsResult)) {
    return rowsResult;
  }

  const out: QueryRow[] = [];
  for (const row of rowsResult.value as InternalRow[]) {
    const projected: QueryRow = {};
    for (const mapping of project.columns) {
      if (isRelProjectColumnMapping(mapping)) {
        projected[mapping.output] = readRowValue(row, toColumnKey(mapping.source)) ?? null;
        continue;
      }

      const exprResult = evaluateRelExprResult(mapping.expr, row, context.subqueryResults);
      if (Result.isError(exprResult)) {
        return exprResult;
      }
      projected[mapping.output] = exprResult.value;
    }
    out.push(projected);
  }

  return Result.ok(out);
}

async function executeAggregateResult<TContext>(
  aggregate: Extract<RelNode, { kind: "aggregate" }>,
  context: RelExecutionContext<TContext>,
): Promise<SqlqlResult<QueryRow[]>> {
  const rowsResult = await executeRelNodeResult(aggregate.input, context);
  if (Result.isError(rowsResult)) {
    return rowsResult;
  }

  const rows = rowsResult.value as InternalRow[];
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

      const metricResult = evaluateAggregateMetricResult(
        metric.fn,
        metricValues,
        bucket.length,
        metric.column != null,
      );
      if (Result.isError(metricResult)) {
        return metricResult;
      }
      row[metric.as] = metricResult.value;
    }

    out.push(row);
  }

  return Result.ok(out);
}

async function executeSortResult<TContext>(
  sort: Extract<RelNode, { kind: "sort" }>,
  context: RelExecutionContext<TContext>,
): Promise<SqlqlResult<InternalRow[]>> {
  const rowsResult = await executeRelNodeResult(sort.input, context);
  if (Result.isError(rowsResult)) {
    return rowsResult;
  }

  const sorted = [...(rowsResult.value as InternalRow[])];

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

  return Result.ok(sorted);
}

async function executeLimitOffsetResult<TContext>(
  limitOffset: Extract<RelNode, { kind: "limit_offset" }>,
  context: RelExecutionContext<TContext>,
): Promise<SqlqlResult<QueryRow[]>> {
  const rowsResult = await executeRelNodeResult(limitOffset.input, context);
  if (Result.isError(rowsResult)) {
    return rowsResult;
  }

  let rows = rowsResult.value;

  if (limitOffset.offset != null) {
    rows = rows.slice(limitOffset.offset);
  }

  if (limitOffset.limit != null) {
    rows = rows.slice(0, limitOffset.limit);
  }

  return Result.ok(rows);
}

async function executeSetOpResult<TContext>(
  setOp: Extract<RelNode, { kind: "set_op" }>,
  context: RelExecutionContext<TContext>,
): Promise<SqlqlResult<QueryRow[]>> {
  const leftRowsResult = await executeRelNodeResult(setOp.left, context);
  if (Result.isError(leftRowsResult)) {
    return leftRowsResult;
  }
  const rightRowsResult = await executeRelNodeResult(setOp.right, context);
  if (Result.isError(rightRowsResult)) {
    return rightRowsResult;
  }

  const leftRows = leftRowsResult.value;
  const rightRows = rightRowsResult.value;

  switch (setOp.op) {
    case "union_all":
      return Result.ok([...leftRows, ...rightRows]);
    case "union":
      return Result.ok(dedupeRows([...leftRows, ...rightRows]));
    case "intersect": {
      const rightKeys = new Set(rightRows.map((row) => stableRowKey(row)));
      return Result.ok(dedupeRows(leftRows.filter((row) => rightKeys.has(stableRowKey(row)))));
    }
    case "except": {
      const rightKeys = new Set(rightRows.map((row) => stableRowKey(row)));
      return Result.ok(dedupeRows(leftRows.filter((row) => !rightKeys.has(stableRowKey(row)))));
    }
  }
}

async function executeWithResult<TContext>(
  withNode: Extract<RelNode, { kind: "with" }>,
  context: RelExecutionContext<TContext>,
): Promise<SqlqlResult<QueryRow[]>> {
  const cteRows = new Map(context.cteRows);
  const nested: RelExecutionContext<TContext> = {
    ...context,
    cteRows,
  };

  for (const cte of withNode.ctes) {
    const rowsResult = await executeRelNodeResult(cte.query, nested);
    if (Result.isError(rowsResult)) {
      return rowsResult;
    }
    cteRows.set(cte.name, rowsResult.value);
  }

  return executeRelNodeResult(withNode.body, nested);
}

function compileViewRelToExecutableResult(
  viewName: string,
  definition: SchemaViewRelNode | unknown,
  schema: SchemaDefinition,
): SqlqlResult<RelNode> {
  if (isRelNode(definition)) {
    return Result.ok(definition);
  }

  if (
    !definition ||
    typeof definition !== "object" ||
    typeof (definition as { kind?: unknown }).kind !== "string"
  ) {
    return Result.err(
      new SqlqlExecutionError({
        operation: "compile executable view rel",
        message: `View ${viewName} returned an unsupported rel definition.`,
      }),
    );
  }

  const relResult = compileSchemaViewRelNodeResult(definition as SchemaViewRelNode, schema);
  if (Result.isError(relResult)) {
    return relResult;
  }
  const rel = relResult.value;
  const binding = getNormalizedTableBinding(schema, viewName);
  if (!binding || binding.kind !== "view") {
    return Result.ok(rel);
  }

  const columns = Object.entries(getNormalizedColumnBindings(binding));
  return Result.ok({
    id: nextSyntheticRelId("view_project"),
    kind: "project",
    convention: "local",
    input: rel,
    columns: columns.map(([output, columnBinding]) =>
      isNormalizedSourceColumnBinding(columnBinding)
        ? {
            kind: "column" as const,
            source: parseRef(columnBinding.source),
            output,
          }
        : {
            kind: "expr" as const,
            expr: rewriteViewBindingExprForExecution(
              columnBinding.expr,
              getNormalizedColumnBindings(binding),
            ),
            output,
          },
    ),
    output: columns.map(([name]) => ({ name })),
  });
}

function rewriteViewBindingExprForExecution(
  expr: RelExpr,
  columnBindings: ReturnType<typeof getNormalizedColumnBindings>,
): RelExpr {
  switch (expr.kind) {
    case "literal":
      return expr;
    case "function":
      return {
        kind: "function",
        name: expr.name,
        args: expr.args.map((arg) => rewriteViewBindingExprForExecution(arg, columnBindings)),
      };
    case "column": {
      if (!expr.ref.table && !expr.ref.alias) {
        const binding = columnBindings[expr.ref.column];
        if (binding && isNormalizedSourceColumnBinding(binding)) {
          return {
            kind: "column",
            ref: parseRef(binding.source),
          };
        }
      }
      return expr;
    }
    case "subquery":
      return expr;
  }
}

function compileSchemaViewRelNodeResult(
  node: SchemaViewRelNode,
  schema: SchemaDefinition,
): SqlqlResult<RelNode> {
  switch (node.kind) {
    case "scan": {
      const table =
        schema.tables[node.table] ??
        (node.entity ? createTableDefinitionFromEntity(node.entity) : undefined);
      if (!table || (node.entity && Object.keys(table.columns).length === 0)) {
        return Result.err(
          new SqlqlExecutionError({
            operation: "compile executable view rel",
            message: `Unknown table in view rel scan: ${node.table}`,
          }),
        );
      }
      const select = Object.keys(table.columns);
      return Result.ok({
        id: nextSyntheticRelId("view_scan"),
        kind: "scan",
        convention: "local",
        table: node.table,
        ...(node.entity ? { entity: node.entity } : {}),
        alias: node.table,
        select,
        output: select.map((column) => ({ name: `${node.table}.${column}` })),
      });
    }
    case "join": {
      const leftResult = compileSchemaViewRelNodeResult(node.left, schema);
      if (Result.isError(leftResult)) {
        return leftResult;
      }
      const rightResult = compileSchemaViewRelNodeResult(node.right, schema);
      if (Result.isError(rightResult)) {
        return rightResult;
      }
      const leftRefResult = resolveSchemaColRefResult(node.on.left);
      if (Result.isError(leftRefResult)) {
        return leftRefResult;
      }
      const rightRefResult = resolveSchemaColRefResult(node.on.right);
      if (Result.isError(rightRefResult)) {
        return rightRefResult;
      }
      return Result.ok({
        id: nextSyntheticRelId("view_join"),
        kind: "join",
        convention: "local",
        joinType: node.type,
        left: leftResult.value,
        right: rightResult.value,
        leftKey: parseRef(leftRefResult.value),
        rightKey: parseRef(rightRefResult.value),
        output: [...leftResult.value.output, ...rightResult.value.output],
      });
    }
    case "aggregate": {
      const inputResult = compileSchemaViewRelNodeResult(node.from, schema);
      if (Result.isError(inputResult)) {
        return inputResult;
      }
      const groupBy: Array<{
        name: string;
        ref: { alias?: string; table?: string; column: string };
      }> = [];
      for (const [name, column] of Object.entries(node.groupBy)) {
        const refResult = resolveSchemaColRefResult(column);
        if (Result.isError(refResult)) {
          return refResult;
        }
        groupBy.push({
          name,
          ref: parseRef(refResult.value),
        });
      }
      const metrics: Array<{
        fn: "count" | "sum" | "avg" | "min" | "max";
        as: string;
        column?: { alias?: string; table?: string; column: string };
      }> = [];
      for (const [output, metric] of Object.entries(node.measures)) {
        if (metric.column) {
          const columnResult = resolveSchemaColRefResult(metric.column);
          if (Result.isError(columnResult)) {
            return columnResult;
          }
          metrics.push({
            fn: metric.fn,
            as: output,
            column: parseRef(columnResult.value),
          });
          continue;
        }
        metrics.push({
          fn: metric.fn,
          as: output,
        });
      }

      const aggregateNode: RelNode = {
        id: nextSyntheticRelId("view_aggregate"),
        kind: "aggregate",
        convention: "local",
        input: inputResult.value,
        groupBy: groupBy.map((entry) => entry.ref),
        metrics,
        output: [
          ...groupBy.map((column) => ({ name: column.name })),
          ...metrics.map((metric) => ({ name: metric.as })),
        ],
      };

      const expectedOutputs = [
        ...groupBy.map((column) => column.name),
        ...metrics.map((metric) => metric.as),
      ];
      const actualOutputs = [
        ...groupBy.map((column) => column.ref.column),
        ...metrics.map((metric) => metric.as),
      ];

      if (expectedOutputs.every((name, index) => name === actualOutputs[index])) {
        return Result.ok(aggregateNode);
      }

      return Result.ok({
        id: nextSyntheticRelId("view_project"),
        kind: "project",
        convention: "local",
        input: aggregateNode,
        columns: expectedOutputs.map((output: string, index: number) => ({
          kind: "column" as const,
          source: { column: actualOutputs[index] ?? output },
          output,
        })),
        output: expectedOutputs.map((output: string) => ({ name: output })),
      });
    }
  }
}

function resolveSchemaColRefResult(ref: { ref?: string }): SqlqlResult<string> {
  if (!ref.ref) {
    return Result.err(
      new SqlqlExecutionError({
        operation: "compile executable view rel",
        message: "View rel column reference was not normalized to a string reference.",
      }),
    );
  }
  return Result.ok(ref.ref);
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

function evaluateRelExprResult(
  expr: RelExpr,
  row: InternalRow,
  subqueryResults: Map<string, unknown>,
): SqlqlResult<unknown> {
  switch (expr.kind) {
    case "literal":
      return Result.ok(expr.value);
    case "column":
      return Result.ok(readRowValue(row, toColumnKey(expr.ref)) ?? null);
    case "subquery":
      return Result.ok(subqueryResults.get(expr.id) ?? null);
    case "function": {
      const args: unknown[] = [];
      for (const arg of expr.args) {
        const argResult = evaluateRelExprResult(arg, row, subqueryResults);
        if (Result.isError(argResult)) {
          return argResult;
        }
        args.push(argResult.value);
      }
      switch (expr.name) {
        case "eq":
          return Result.ok(args[0] != null && args[0] === args[1]);
        case "neq":
          return Result.ok(args[0] != null && args[0] !== args[1]);
        case "gt":
          return Result.ok(
            args[0] != null && args[1] != null && compareNonNull(args[0], args[1]) > 0,
          );
        case "gte":
          return Result.ok(
            args[0] != null && args[1] != null && compareNonNull(args[0], args[1]) >= 0,
          );
        case "lt":
          return Result.ok(
            args[0] != null && args[1] != null && compareNonNull(args[0], args[1]) < 0,
          );
        case "lte":
          return Result.ok(
            args[0] != null && args[1] != null && compareNonNull(args[0], args[1]) <= 0,
          );
        case "and":
          return Result.ok(args.every(Boolean));
        case "or":
          return Result.ok(args.some(Boolean));
        case "not":
          return Result.ok(!args[0]);
        case "add": {
          const leftResult = toFiniteNumberResult(args[0], "ADD");
          if (Result.isError(leftResult)) {
            return leftResult;
          }
          const rightResult = toFiniteNumberResult(args[1], "ADD");
          if (Result.isError(rightResult)) {
            return rightResult;
          }
          return Result.ok(leftResult.value + rightResult.value);
        }
        case "subtract": {
          const leftResult = toFiniteNumberResult(args[0], "SUBTRACT");
          if (Result.isError(leftResult)) {
            return leftResult;
          }
          const rightResult = toFiniteNumberResult(args[1], "SUBTRACT");
          if (Result.isError(rightResult)) {
            return rightResult;
          }
          return Result.ok(leftResult.value - rightResult.value);
        }
        case "multiply": {
          const leftResult = toFiniteNumberResult(args[0], "MULTIPLY");
          if (Result.isError(leftResult)) {
            return leftResult;
          }
          const rightResult = toFiniteNumberResult(args[1], "MULTIPLY");
          if (Result.isError(rightResult)) {
            return rightResult;
          }
          return Result.ok(leftResult.value * rightResult.value);
        }
        case "divide": {
          const leftResult = toFiniteNumberResult(args[0], "DIVIDE");
          if (Result.isError(leftResult)) {
            return leftResult;
          }
          const rightResult = toFiniteNumberResult(args[1], "DIVIDE");
          if (Result.isError(rightResult)) {
            return rightResult;
          }
          return Result.ok(leftResult.value / rightResult.value);
        }
        case "mod": {
          const leftResult = toFiniteNumberResult(args[0], "MOD");
          if (Result.isError(leftResult)) {
            return leftResult;
          }
          const rightResult = toFiniteNumberResult(args[1], "MOD");
          if (Result.isError(rightResult)) {
            return rightResult;
          }
          return Result.ok(leftResult.value % rightResult.value);
        }
        case "concat":
          return Result.ok(args.map((arg) => (arg == null ? "" : String(arg))).join(""));
        case "like":
          return Result.ok(
            typeof args[0] === "string" && typeof args[1] === "string"
              ? testSqlLikePattern(args[0], args[1])
              : false,
          );
        case "not_like":
          return Result.ok(
            typeof args[0] === "string" && typeof args[1] === "string"
              ? !testSqlLikePattern(args[0], args[1])
              : false,
          );
        case "in":
          return Result.ok(args[0] != null && args.slice(1).some((arg) => arg === args[0]));
        case "not_in":
          return Result.ok(args[0] != null && args.slice(1).every((arg) => arg !== args[0]));
        case "is_null":
          return Result.ok(args[0] == null);
        case "is_not_null":
          return Result.ok(args[0] != null);
        case "is_distinct_from":
          return Result.ok(args[0] !== args[1]);
        case "is_not_distinct_from":
          return Result.ok(args[0] === args[1]);
        case "between":
          return Result.ok(
            args[0] != null && args[1] != null && args[2] != null
              ? compareNonNull(args[0], args[1]) >= 0 && compareNonNull(args[0], args[2]) <= 0
              : false,
          );
        case "lower":
          return Result.ok(args[0] == null ? null : String(args[0]).toLowerCase());
        case "upper":
          return Result.ok(args[0] == null ? null : String(args[0]).toUpperCase());
        case "trim":
          return Result.ok(args[0] == null ? null : String(args[0]).trim());
        case "length":
          return Result.ok(args[0] == null ? null : String(args[0]).length);
        case "substr": {
          if (args[0] == null || args[1] == null) {
            return Result.ok(null);
          }
          const input = String(args[0]);
          const startResult = toFiniteNumberResult(args[1], "SUBSTR");
          if (Result.isError(startResult)) {
            return startResult;
          }
          const start = Math.trunc(startResult.value);
          const begin = start >= 0 ? Math.max(0, start - 1) : Math.max(input.length + start, 0);
          if (args[2] == null) {
            return Result.ok(input.slice(begin));
          }
          const lengthResult = toFiniteNumberResult(args[2], "SUBSTR");
          if (Result.isError(lengthResult)) {
            return lengthResult;
          }
          const length = Math.max(0, Math.trunc(lengthResult.value));
          return Result.ok(input.slice(begin, begin + length));
        }
        case "coalesce":
          return Result.ok(args.find((arg) => arg != null) ?? null);
        case "nullif":
          return Result.ok(args[0] === args[1] ? null : (args[0] ?? null));
        case "abs": {
          if (args[0] == null) {
            return Result.ok(null);
          }
          const valueResult = toFiniteNumberResult(args[0], "ABS");
          if (Result.isError(valueResult)) {
            return valueResult;
          }
          return Result.ok(Math.abs(valueResult.value));
        }
        case "round": {
          if (args[0] == null) {
            return Result.ok(null);
          }
          const valueResult = toFiniteNumberResult(args[0], "ROUND");
          if (Result.isError(valueResult)) {
            return valueResult;
          }
          let precision = 0;
          if (args[1] != null) {
            const precisionResult = toFiniteNumberResult(args[1], "ROUND");
            if (Result.isError(precisionResult)) {
              return precisionResult;
            }
            precision = Math.trunc(precisionResult.value);
          }
          const scale = 10 ** precision;
          return Result.ok(Math.round(valueResult.value * scale) / scale);
        }
        case "cast":
          return castValueResult(args[0], args[1]);
        case "case": {
          const lastIndex = args.length - 1;
          const hasElse = args.length % 2 === 1;
          for (let index = 0; index < (hasElse ? lastIndex : args.length); index += 2) {
            if (args[index]) {
              return Result.ok(args[index + 1] ?? null);
            }
          }
          return Result.ok(hasElse ? (args[lastIndex] ?? null) : null);
        }
        default:
          return Result.err(
            new SqlqlExecutionError({
              operation: "evaluate relational expression",
              message: `Unsupported computed expression function: ${expr.name}`,
            }),
          );
      }
    }
  }
}

async function prepareSubqueryResultsResult<TContext>(
  node: RelNode,
  context: RelExecutionContext<TContext>,
): Promise<SqlqlResult<void>> {
  const visited = new Set<string>();

  const prepareExpr = async (expr: RelExpr): Promise<SqlqlResult<void>> => {
    switch (expr.kind) {
      case "literal":
      case "column":
        return Result.ok(undefined);
      case "function":
        for (const arg of expr.args) {
          const argResult = await prepareExpr(arg);
          if (Result.isError(argResult)) {
            return argResult;
          }
        }
        return Result.ok(undefined);
      case "subquery": {
        if (visited.has(expr.id)) {
          return Result.ok(undefined);
        }
        visited.add(expr.id);

        const nestedResult = await prepareSubqueryResultsResult(expr.rel, context);
        if (Result.isError(nestedResult)) {
          return nestedResult;
        }

        const rowsResult = await executeRelNodeResult(expr.rel, context);
        if (Result.isError(rowsResult)) {
          return rowsResult;
        }

        const rows = rowsResult.value;
        const value =
          expr.mode === "exists"
            ? rows.length > 0
            : rows.length === 0
              ? null
              : rows[0]?.[expr.outputColumn ?? "" ] ?? null;
        context.subqueryResults.set(expr.id, value);
        return Result.ok(undefined);
      }
    }
  };

  switch (node.kind) {
    case "scan":
    case "sql":
      return Result.ok(undefined);
    case "filter": {
      const inputResult = await prepareSubqueryResultsResult(node.input, context);
      if (Result.isError(inputResult)) {
        return inputResult;
      }
      return node.expr ? prepareExpr(node.expr) : Result.ok(undefined);
    }
    case "project": {
      const inputResult = await prepareSubqueryResultsResult(node.input, context);
      if (Result.isError(inputResult)) {
        return inputResult;
      }
      for (const column of node.columns) {
        if (!("expr" in column)) {
          continue;
        }
        const exprResult = await prepareExpr(column.expr);
        if (Result.isError(exprResult)) {
          return exprResult;
        }
      }
      return Result.ok(undefined);
    }
    case "aggregate":
    case "window":
    case "sort":
    case "limit_offset":
      return prepareSubqueryResultsResult(node.input, context);
    case "join": {
      const leftResult = await prepareSubqueryResultsResult(node.left, context);
      if (Result.isError(leftResult)) {
        return leftResult;
      }
      return prepareSubqueryResultsResult(node.right, context);
    }
    case "set_op": {
      const leftResult = await prepareSubqueryResultsResult(node.left, context);
      if (Result.isError(leftResult)) {
        return leftResult;
      }
      return prepareSubqueryResultsResult(node.right, context);
    }
    case "with": {
      for (const cte of node.ctes) {
        const cteResult = await prepareSubqueryResultsResult(cte.query, context);
        if (Result.isError(cteResult)) {
          return cteResult;
        }
      }
      return prepareSubqueryResultsResult(node.body, context);
    }
  }
}

function testSqlLikePattern(value: string, pattern: string): boolean {
  const regex = new RegExp(
    `^${pattern
      .replace(/[.*+?^${}()|[\]\\]/g, "\\$&")
      .replace(/%/g, ".*")
      .replace(/_/g, ".")}$`,
    "su",
  );
  return regex.test(value);
}

function castValueResult(value: unknown, target: unknown): SqlqlResult<unknown> {
  if (value == null) {
    return Result.ok(null);
  }
  const normalized = String(target ?? "")
    .trim()
    .toLowerCase();
  switch (normalized) {
    case "text":
      return Result.ok(String(value));
    case "integer":
    case "int":
      return Result.ok(Math.trunc(Number(value)));
    case "real":
    case "numeric":
    case "float":
      return Result.ok(Number(value));
    case "boolean":
      return Result.ok(Boolean(value));
    default:
      return Result.err(
        new SqlqlExecutionError({
          operation: "evaluate relational expression",
          message: `Unsupported CAST target type: ${String(target)}`,
        }),
      );
  }
}

function evaluateAggregateMetricResult(
  fn: "count" | "sum" | "avg" | "min" | "max",
  values: unknown[],
  bucketSize: number,
  hasColumn: boolean,
): SqlqlResult<unknown> {
  switch (fn) {
    case "count":
      return Result.ok(hasColumn ? values.filter((value) => value != null).length : bucketSize);
    case "sum": {
      const numeric: number[] = [];
      for (const value of values.filter((entry) => entry != null)) {
        const numericResult = toFiniteNumberResult(value, "SUM");
        if (Result.isError(numericResult)) {
          return numericResult;
        }
        numeric.push(numericResult.value);
      }
      return Result.ok(numeric.length > 0 ? numeric.reduce((sum, value) => sum + value, 0) : null);
    }
    case "avg": {
      const numeric: number[] = [];
      for (const value of values.filter((entry) => entry != null)) {
        const numericResult = toFiniteNumberResult(value, "AVG");
        if (Result.isError(numericResult)) {
          return numericResult;
        }
        numeric.push(numericResult.value);
      }
      return Result.ok(
        numeric.length > 0 ? numeric.reduce((sum, value) => sum + value, 0) / numeric.length : null,
      );
    }
    case "min": {
      const candidates = values.filter((value) => value != null);
      return Result.ok(
        candidates.length > 0
          ? candidates.reduce((left, right) =>
              compareNullableValues(left, right) <= 0 ? left : right,
            )
          : null,
      );
    }
    case "max": {
      const candidates = values.filter((value) => value != null);
      return Result.ok(
        candidates.length > 0
          ? candidates.reduce((left, right) =>
              compareNullableValues(left, right) >= 0 ? left : right,
            )
          : null,
      );
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

function toFiniteNumberResult(
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
): SqlqlResult<number> {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return Result.err(
      new SqlqlExecutionError({
        operation: "evaluate relational expression",
        message: `${functionName} expects numeric values.`,
      }),
    );
  }
  return Result.ok(parsed);
}
