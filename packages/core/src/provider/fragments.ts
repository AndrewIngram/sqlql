import { Result, type Result as BetterResult } from "better-result";

import {
  TuplDiagnosticError,
  TuplExecutionError,
  TuplPlanningError,
  type TuplError,
} from "../model/errors";
import {
  isRelProjectColumnMapping,
  type RelColumnRef,
  type RelExpr,
  type RelNode,
  type RelProjectNode,
  type RelScanNode,
} from "../model/rel";
import { expandRelViewsResult } from "../planner/planning";
import {
  normalizeCapability,
  unwrapProviderOperationResult,
  type ProviderAdapter,
  type ProviderCapabilityReport,
  type ProviderCompiledPlan,
  type ProviderFragment,
  type ProvidersMap,
  type QueryFallbackPolicy,
  type TuplDiagnostic,
} from "../provider";
import {
  createPhysicalBindingFromEntity,
  createTableDefinitionFromEntity,
  getNormalizedTableBinding,
  mapProviderRowsToLogical,
  mapProviderRowsToRelOutput,
  resolveColumnDefinition,
  resolveNormalizedColumnSource,
  resolveTableProvider,
  type QueryRow,
  type ScanFilterClause,
  type SchemaDefinition,
  type TableColumnDefinition,
} from "../schema";
import type { ConstraintValidationOptions } from "../runtime/constraints";

function toTuplPlanningError(error: unknown, operation: string) {
  if (TuplPlanningError.is(error)) {
    return error;
  }

  return new TuplPlanningError({
    operation,
    message: error instanceof Error ? error.message : String(error),
    cause: error,
  });
}

function toTuplExecutionError(error: unknown, operation: string) {
  if (TuplExecutionError.is(error) || TuplDiagnosticError.is(error)) {
    return error;
  }

  return new TuplExecutionError({
    operation,
    message: error instanceof Error ? error.message : String(error),
    cause: error,
  });
}

function tryExecutionStep<T>(operation: string, fn: () => T) {
  return Result.try({
    try: () => fn() as Awaited<T>,
    catch: (error) => toTuplExecutionError(error, operation),
  });
}

async function tryExecutionStepAsync<T>(operation: string, fn: () => Promise<T>) {
  return Result.tryPromise({
    try: fn,
    catch: (error) => toTuplExecutionError(error, operation),
  });
}

export interface ProviderFragmentResolution<TContext> {
  fragment: ProviderFragment | null;
  provider: ProviderAdapter<TContext> | null;
  report: ProviderCapabilityReport | null;
  diagnostics: TuplDiagnostic[];
}

export interface ProviderCapabilityInspection {
  report: ProviderCapabilityReport;
  diagnostics: TuplDiagnostic[];
}

export function summarizeCapabilityReason(report: ProviderCapabilityReport | null): string {
  if (!report) {
    return "Provider cannot execute the requested fragment.";
  }
  if (report.reason && report.reason.length > 0) {
    return report.reason;
  }
  if (report.missingAtoms && report.missingAtoms.length > 0) {
    return `Provider is missing required capabilities: ${report.missingAtoms.join(", ")}`;
  }
  return report.supported
    ? "Provider can execute the requested fragment."
    : "Provider cannot execute the requested fragment.";
}

function buildCapabilityDiagnostics<TContext>(
  provider: ProviderAdapter<TContext>,
  fragment: ProviderFragment,
  report: ProviderCapabilityReport,
  policy?: QueryFallbackPolicy,
): TuplDiagnostic[] {
  if (report.diagnostics && report.diagnostics.length > 0) {
    return report.diagnostics;
  }

  if (report.supported) {
    return [];
  }

  const details: Record<string, unknown> = {
    provider: provider.name,
    fragment: fragment.kind,
  };
  if (report.missingAtoms?.length) {
    details.missingAtoms = report.missingAtoms;
  }
  if (report.requiredAtoms?.length) {
    details.requiredAtoms = report.requiredAtoms;
  }
  if (report.estimatedRows != null) {
    details.estimatedRows = report.estimatedRows;
  }
  if (report.estimatedCost != null) {
    details.estimatedCost = report.estimatedCost;
  }

  return [
    {
      code: policy?.allowFallback === false ? "TUPL_ERR_FALLBACK" : "TUPL_WARN_FALLBACK",
      class: policy?.allowFallback === false ? "42000" : "0A000",
      severity: policy?.allowFallback === false ? "error" : "warning",
      message: summarizeCapabilityReason(report),
      details,
    } satisfies TuplDiagnostic,
  ];
}

export async function inspectProviderFragmentCapabilityResult<TContext>(input: {
  provider: ProviderAdapter<TContext>;
  fragment: ProviderFragment;
  context: TContext;
  fallbackPolicy?: QueryFallbackPolicy;
}): Promise<BetterResult<ProviderCapabilityInspection, TuplError>> {
  const capabilityResult = await tryExecutionStepAsync("resolve provider capability", () =>
    Promise.resolve(input.provider.canExecute(input.fragment, input.context)),
  );
  if (Result.isError(capabilityResult)) {
    return capabilityResult;
  }

  const report = normalizeCapability(capabilityResult.value);
  return Result.ok({
    report,
    diagnostics: buildCapabilityDiagnostics(
      input.provider,
      input.fragment,
      report,
      input.fallbackPolicy,
    ),
  });
}

export function inspectSyncProviderFragmentCapabilityResult<TContext>(input: {
  provider: ProviderAdapter<TContext>;
  fragment: ProviderFragment;
  context: TContext;
  fallbackPolicy?: QueryFallbackPolicy;
}): BetterResult<ProviderCapabilityInspection | null, TuplError> {
  const capabilityResult = tryExecutionStep("resolve provider capability", () =>
    input.provider.canExecute(input.fragment, input.context),
  );
  if (Result.isError(capabilityResult)) {
    return capabilityResult;
  }

  const capability = capabilityResult.value;
  if (isPromiseLike(capability)) {
    return Result.ok(null);
  }

  const report = normalizeCapability(capability);
  return Result.ok({
    report,
    diagnostics: buildCapabilityDiagnostics(
      input.provider,
      input.fragment,
      report,
      input.fallbackPolicy,
    ),
  });
}

function maybeRejectFallbackResult<TContext>(
  policy: QueryFallbackPolicy | undefined,
  resolution: ProviderFragmentResolution<TContext>,
): BetterResult<ProviderFragmentResolution<TContext>, TuplDiagnosticError> {
  if (!resolution.provider || !resolution.report || resolution.report.supported) {
    return Result.ok(resolution);
  }

  const effectivePolicy: Required<QueryFallbackPolicy> = {
    allowFallback: policy?.allowFallback ?? true,
    warnOnFallback: policy?.warnOnFallback ?? true,
    rejectOnMissingAtom: policy?.rejectOnMissingAtom ?? false,
    rejectOnEstimatedCost: policy?.rejectOnEstimatedCost ?? false,
    maxLocalRows: policy?.maxLocalRows ?? Number.POSITIVE_INFINITY,
    maxLookupFanout: policy?.maxLookupFanout ?? Number.POSITIVE_INFINITY,
    maxJoinExpansionRisk: policy?.maxJoinExpansionRisk ?? Number.POSITIVE_INFINITY,
  };
  const exceedsEstimatedCost =
    effectivePolicy.rejectOnEstimatedCost &&
    resolution.report.estimatedCost != null &&
    Number.isFinite(effectivePolicy.maxJoinExpansionRisk) &&
    resolution.report.estimatedCost > effectivePolicy.maxJoinExpansionRisk;

  if (
    !effectivePolicy.allowFallback ||
    effectivePolicy.rejectOnMissingAtom ||
    exceedsEstimatedCost
  ) {
    const diagnostics =
      resolution.diagnostics.length > 0
        ? resolution.diagnostics
        : [
            {
              code: "TUPL_ERR_FALLBACK",
              class: "42000",
              severity: "error",
              message: summarizeCapabilityReason(resolution.report),
              details: {
                provider: resolution.provider.name,
                fragment: resolution.fragment?.kind,
                missingAtoms: resolution.report.missingAtoms,
              },
            } satisfies TuplDiagnostic,
          ];

    return Result.err(
      new TuplDiagnosticError({
        message: summarizeCapabilityReason(resolution.report),
        diagnostics,
      }),
    );
  }

  return Result.ok(resolution);
}

export async function resolveProviderCapabilityForRelResult<TContext>(
  input: {
    schema: SchemaDefinition;
    providers: ProvidersMap<TContext>;
    context: TContext;
    fallbackPolicy?: QueryFallbackPolicy;
  },
  rel: RelNode,
): Promise<BetterResult<ProviderFragmentResolution<TContext>, TuplError>> {
  const fragmentResult = buildProviderFragmentForRelResult(rel, input.schema, input.context);
  if (Result.isError(fragmentResult)) {
    return fragmentResult;
  }

  const fragment = fragmentResult.value;
  if (!fragment) {
    return Result.ok({
      fragment: null,
      provider: null,
      report: null,
      diagnostics: [],
    });
  }

  const provider = input.providers[fragment.provider] ?? null;
  if (!provider) {
    return Result.ok({
      fragment,
      provider: null,
      report: null,
      diagnostics: [],
    });
  }

  const capabilityResult = await inspectProviderFragmentCapabilityResult({
    provider,
    fragment,
    context: input.context,
    ...(input.fallbackPolicy ? { fallbackPolicy: input.fallbackPolicy } : {}),
  });
  if (Result.isError(capabilityResult)) {
    return capabilityResult;
  }

  return Result.ok({
    fragment,
    provider,
    report: capabilityResult.value.report,
    diagnostics: capabilityResult.value.diagnostics,
  });
}

export function resolveSyncProviderCapabilityForRelResult<TContext>(
  input: {
    schema: SchemaDefinition;
    providers: ProvidersMap<TContext>;
    context: TContext;
    fallbackPolicy?: QueryFallbackPolicy;
  },
  rel: RelNode,
): BetterResult<ProviderFragmentResolution<TContext> | null, TuplError> {
  const fragmentResult = buildProviderFragmentForRelResult(rel, input.schema, input.context);
  if (Result.isError(fragmentResult)) {
    return fragmentResult;
  }

  const fragment = fragmentResult.value;
  if (!fragment) {
    return Result.ok({
      fragment: null,
      provider: null,
      report: null,
      diagnostics: [],
    });
  }

  const provider = input.providers[fragment.provider] ?? null;
  if (!provider) {
    return Result.ok({
      fragment,
      provider: null,
      report: null,
      diagnostics: [],
    });
  }

  const capabilityResult = inspectSyncProviderFragmentCapabilityResult({
    provider,
    fragment,
    context: input.context,
    ...(input.fallbackPolicy ? { fallbackPolicy: input.fallbackPolicy } : {}),
  });
  if (Result.isError(capabilityResult)) {
    return capabilityResult;
  }
  if (!capabilityResult.value) {
    return Result.ok(null);
  }

  const resolution = {
    fragment,
    provider,
    report: capabilityResult.value.report,
    diagnostics: capabilityResult.value.diagnostics,
  } satisfies ProviderFragmentResolution<TContext>;
  const fallbackResult = maybeRejectFallbackResult(input.fallbackPolicy, resolution);
  if (Result.isError(fallbackResult)) {
    return fallbackResult;
  }
  return Result.ok(resolution);
}

export async function maybeExecuteProviderFragmentResult<TContext>(
  input: {
    schema: SchemaDefinition;
    providers: ProvidersMap<TContext>;
    context: TContext;
    rel: RelNode;
    fallbackPolicy?: QueryFallbackPolicy;
    constraintValidation?: ConstraintValidationOptions;
  },
  options: {
    enforceFallbackPolicy: boolean;
  },
): Promise<BetterResult<QueryRow[] | null, TuplError>> {
  const resolutionResult = await resolveProviderCapabilityForRelResult(input, input.rel);
  if (Result.isError(resolutionResult)) {
    return resolutionResult;
  }

  const resolution = resolutionResult.value;
  if (!resolution.fragment || !resolution.provider || !resolution.report) {
    return Result.ok(null);
  }

  if (!resolution.report.supported) {
    if (options.enforceFallbackPolicy) {
      const fallbackResult = maybeRejectFallbackResult(input.fallbackPolicy, resolution);
      if (Result.isError(fallbackResult)) {
        return fallbackResult;
      }
    }

    return Result.ok(null);
  }

  return executeProviderFragmentResult({
    schema: input.schema,
    context: input.context,
    rel: input.rel,
    provider: resolution.provider,
    fragment: resolution.fragment,
    ...(input.constraintValidation ? { constraintValidation: input.constraintValidation } : {}),
  });
}

export async function executeProviderFragmentResult<TContext>(input: {
  schema: SchemaDefinition;
  context: TContext;
  rel: RelNode;
  provider: ProviderAdapter<TContext>;
  fragment: ProviderFragment;
  constraintValidation?: ConstraintValidationOptions;
}): Promise<
  BetterResult<QueryRow[], TuplPlanningError | TuplExecutionError | TuplDiagnosticError>
> {
  const compiledResult = await tryExecutionStepAsync(
    "compile provider fragment",
    () =>
      Promise.resolve(input.provider.compile(input.fragment, input.context)).then(
        unwrapProviderOperationResult,
      ) as Promise<ProviderCompiledPlan>,
  );
  if (Result.isError(compiledResult)) {
    return compiledResult;
  }

  const rowsResult = await tryExecutionStepAsync("execute provider fragment", () =>
    Promise.resolve(input.provider.execute(compiledResult.value, input.context)).then(
      unwrapProviderOperationResult,
    ),
  );
  if (Result.isError(rowsResult)) {
    return rowsResult;
  }

  if (input.fragment.kind === "rel") {
    return tryExecutionStep("map provider rows to rel output", () =>
      mapProviderRowsToRelOutput(rowsResult.value, input.rel, input.schema),
    );
  }

  if (input.fragment.kind === "scan" && input.rel.kind === "scan") {
    const scanRel = input.rel;
    return tryExecutionStep("map provider rows to logical output", () => {
      const normalizedBinding = getNormalizedTableBinding(input.schema, scanRel.table);
      const physicalBinding =
        normalizedBinding?.kind === "physical"
          ? normalizedBinding
          : scanRel.entity
            ? createPhysicalBindingFromEntity(scanRel.entity)
            : null;
      const tableDefinition =
        input.schema.tables[scanRel.table] ??
        (scanRel.entity ? createTableDefinitionFromEntity(scanRel.entity) : undefined);

      return mapProviderRowsToLogical(
        rowsResult.value,
        scanRel.select,
        physicalBinding,
        tableDefinition,
        {
          enforceNotNull: !input.constraintValidation || input.constraintValidation.mode === "off",
          enforceEnum: !input.constraintValidation || input.constraintValidation.mode === "off",
        },
      );
    });
  }

  return Result.ok(rowsResult.value);
}

export function buildProviderFragmentForRel<TContext = unknown>(
  node: RelNode,
  schema: SchemaDefinition,
  context?: TContext,
): ProviderFragment | null {
  const result = buildProviderFragmentForRelResult(node, schema, context);
  if (Result.isError(result)) {
    throw result.error;
  }
  return result.value;
}

export function buildProviderFragmentForRelResult<TContext = unknown>(
  node: RelNode,
  schema: SchemaDefinition,
  context?: TContext,
) {
  return Result.gen(function* () {
    const expanded = yield* expandRelViewsResult(node, schema, context);
    const provider = resolveSingleProvider(expanded, schema);
    if (!provider) {
      return Result.ok(null);
    }

    return buildProviderFragmentForNodeResult(expanded, schema, provider);
  });
}

function buildProviderFragmentForNodeResult(
  node: RelNode,
  schema: SchemaDefinition,
  provider: string,
) {
  return Result.try({
    try: () => buildProviderFragmentForNode(node, schema, provider),
    catch: (error) => toTuplPlanningError(error, "build provider fragment"),
  });
}

function buildProviderFragmentForNode(
  node: RelNode,
  schema: SchemaDefinition,
  provider: string,
): ProviderFragment {
  if (node.kind === "scan") {
    const normalizedScan = normalizeScanForProvider(node, schema);
    return {
      kind: "scan",
      provider,
      table: normalizedScan.table,
      request: {
        table: normalizedScan.table,
        ...(normalizedScan.alias ? { alias: normalizedScan.alias } : {}),
        select: normalizedScan.select,
        ...(normalizedScan.where ? { where: normalizedScan.where } : {}),
        ...(normalizedScan.orderBy ? { orderBy: normalizedScan.orderBy } : {}),
        ...(normalizedScan.limit != null ? { limit: normalizedScan.limit } : {}),
        ...(normalizedScan.offset != null ? { offset: normalizedScan.offset } : {}),
      },
    };
  }

  if (node.kind === "aggregate") {
    const aggregateFragment = buildAggregateProviderFragment(node, schema, provider);
    if (aggregateFragment) {
      return aggregateFragment;
    }
  }

  return {
    kind: "rel",
    provider,
    rel: normalizeRelForProvider(node, schema),
  };
}

function buildAggregateProviderFragment(
  node: Extract<RelNode, { kind: "aggregate" }>,
  schema: SchemaDefinition,
  provider: string,
): ProviderFragment | null {
  const extracted = extractAggregateProviderInput(node.input);
  if (!extracted) {
    return null;
  }

  const mergedScan: RelScanNode = {
    ...extracted.scan,
    ...(extracted.where.length > 0
      ? {
          where: [...(extracted.scan.where ?? []), ...extracted.where],
        }
      : {}),
  };
  const normalizedScan = normalizeScanForProvider(mergedScan, schema);
  const aliasToSource = collectAliasToSourceMappings(mergedScan, schema);

  return {
    kind: "aggregate",
    provider,
    table: normalizedScan.table,
    request: {
      table: normalizedScan.table,
      ...(normalizedScan.alias ? { alias: normalizedScan.alias } : {}),
      ...(normalizedScan.where?.length ? { where: normalizedScan.where } : {}),
      ...(node.groupBy.length
        ? {
            groupBy: node.groupBy.map(
              (column) => mapColumnRefForAlias(column, aliasToSource).column,
            ),
          }
        : {}),
      metrics: node.metrics.map((metric) => ({
        fn: metric.fn,
        as: metric.as,
        ...(metric.distinct ? { distinct: true } : {}),
        ...(metric.column
          ? {
              column: mapColumnRefForAlias(metric.column, aliasToSource).column,
            }
          : {}),
      })),
    },
  };
}

function extractAggregateProviderInput(node: RelNode): {
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

type AliasToSourceMap = Map<string, Record<string, string>>;

function normalizeRelForProvider(node: RelNode, schema: SchemaDefinition): RelNode {
  const aliasToSource = collectAliasToSourceMappings(node, schema);

  const visit = (current: RelNode): RelNode => {
    switch (current.kind) {
      case "scan":
        return normalizeScanForProvider(current, schema);
      case "filter":
        return {
          ...current,
          input: visit(current.input),
          ...(current.where
            ? {
                where: current.where.map((clause) => ({
                  ...clause,
                  column: mapColumnNameForAlias(clause.column, aliasToSource),
                })),
              }
            : {}),
          ...(current.expr
            ? {
                expr: mapRelExprRefsForAliasSource(current.expr, aliasToSource, schema),
              }
            : {}),
        };
      case "project":
        return {
          ...current,
          input: visit(current.input),
          columns: current.columns.map((column) =>
            isRelProjectColumnMapping(column)
              ? {
                  ...column,
                  source: mapColumnRefForAlias(column.source, aliasToSource),
                }
              : {
                  ...column,
                  expr: mapRelExprRefsForAliasSource(column.expr, aliasToSource, schema),
                },
          ),
        };
      case "join":
        return {
          ...current,
          left: visit(current.left),
          right: visit(current.right),
          leftKey: mapColumnRefForAlias(current.leftKey, aliasToSource),
          rightKey: mapColumnRefForAlias(current.rightKey, aliasToSource),
        };
      case "aggregate":
        return {
          ...current,
          input: visit(current.input),
          groupBy: current.groupBy.map((column) => mapColumnRefForAlias(column, aliasToSource)),
          metrics: current.metrics.map((metric) => ({
            ...metric,
            ...(metric.column
              ? { column: mapColumnRefForAlias(metric.column, aliasToSource) }
              : {}),
          })),
        };
      case "window":
        return {
          ...current,
          input: visit(current.input),
          functions: current.functions.map((fn) => ({
            ...fn,
            partitionBy: fn.partitionBy.map((column) =>
              mapColumnRefForAlias(column, aliasToSource),
            ),
            orderBy: fn.orderBy.map((term) => ({
              ...term,
              source: mapColumnRefForAlias(term.source, aliasToSource),
            })),
            ...("column" in fn && fn.column
              ? { column: mapColumnRefForAlias(fn.column, aliasToSource) }
              : {}),
          })),
        };
      case "sort":
        return {
          ...current,
          input: visit(current.input),
          orderBy: current.orderBy.map((term) => ({
            ...term,
            source: mapColumnRefForAlias(term.source, aliasToSource),
          })),
        };
      case "limit_offset":
        return {
          ...current,
          input: visit(current.input),
        };
      case "set_op":
        return {
          ...current,
          left: visit(current.left),
          right: visit(current.right),
        };
      case "with":
        return {
          ...current,
          ctes: current.ctes.map((cte) => ({
            ...cte,
            query: visit(cte.query),
          })),
          body: visit(current.body),
        };
      case "sql":
        return current;
    }
  };

  return simplifyProviderProjects(visit(node));
}

function simplifyProviderProjects(node: RelNode): RelNode {
  switch (node.kind) {
    case "scan":
    case "sql":
      return node;
    case "filter":
      return {
        ...node,
        input: simplifyProviderProjects(node.input),
      };
    case "project": {
      const simplified = {
        ...node,
        input: simplifyProviderProjects(node.input),
      };
      return hoistProjectAcrossUnaryChain(simplified);
    }
    case "join":
      return {
        ...node,
        left: simplifyProviderProjects(node.left),
        right: simplifyProviderProjects(node.right),
      };
    case "aggregate":
    case "window":
    case "sort":
    case "limit_offset":
      return {
        ...node,
        input: simplifyProviderProjects(node.input),
      };
    case "set_op":
      return {
        ...node,
        left: simplifyProviderProjects(node.left),
        right: simplifyProviderProjects(node.right),
      };
    case "with":
      return {
        ...node,
        ctes: node.ctes.map((cte) => ({
          ...cte,
          query: simplifyProviderProjects(cte.query),
        })),
        body: simplifyProviderProjects(node.body),
      };
  }
}

function hoistProjectAcrossUnaryChain(project: RelProjectNode): RelNode {
  let current = project.input;

  while (current.kind === "filter") {
    current = current.input;
  }

  if (current.kind !== "project") {
    return project;
  }

  const mergedColumns = composeProjectMappings(project.columns, current.columns);
  return {
    ...project,
    columns: mergedColumns,
    input: current.input,
  };
}

function composeProjectMappings(
  outer: RelProjectNode["columns"],
  inner: RelProjectNode["columns"],
): RelProjectNode["columns"] {
  const innerByOutput = new Map(inner.map((column) => [column.output, column]));
  return outer.map((column) => {
    if (!isRelProjectColumnMapping(column)) {
      return column;
    }

    const source = innerByOutput.get(column.source.column);
    if (!source || !isRelProjectColumnMapping(source)) {
      return column;
    }

    return {
      source: source.source,
      output: column.output,
    };
  });
}

function normalizeScanForProvider(node: RelScanNode, schema: SchemaDefinition): RelScanNode {
  const normalized = getNormalizedTableBinding(schema, node.table);
  const physicalBinding =
    normalized?.kind === "physical"
      ? normalized
      : node.entity
        ? createPhysicalBindingFromEntity(node.entity)
        : null;

  const table =
    schema.tables[node.table] ??
    (node.entity ? createTableDefinitionFromEntity(node.entity) : null);
  if (!physicalBinding || !table) {
    return node;
  }

  return {
    ...node,
    table: physicalBinding.entity,
    select: node.select.map((column) => resolveNormalizedColumnSource(physicalBinding, column)),
    ...(node.where
      ? {
          where: node.where.map((clause) => {
            const sourceColumn = resolveNormalizedColumnSource(physicalBinding, clause.column);
            const definition = table.columns[clause.column];
            return definition
              ? mapEnumFilterForProvider(definition, {
                  ...clause,
                  column: sourceColumn,
                })
              : { ...clause, column: sourceColumn };
          }),
        }
      : {}),
    ...(node.orderBy
      ? {
          orderBy: node.orderBy.map((term) => ({
            ...term,
            column: resolveNormalizedColumnSource(physicalBinding, term.column),
          })),
        }
      : {}),
  };
}

function mapEnumFilterForProvider(
  definition: TableColumnDefinition,
  clause: ScanFilterClause,
): ScanFilterClause {
  const resolved = resolveColumnDefinition(definition);
  if (!resolved.enumMap) {
    return clause;
  }

  if ("value" in clause && typeof clause.value === "string") {
    const mapped = Object.entries(resolved.enumMap)
      .filter(([, value]) => value === clause.value)
      .map(([key]) => key);
    if (mapped.length === 0) {
      return clause;
    }
    if (mapped.length === 1) {
      return { ...clause, value: mapped[0] };
    }
    return {
      ...clause,
      op: clause.op === "neq" ? "not_in" : "in",
      values: mapped,
    };
  }

  if ("values" in clause) {
    return {
      ...clause,
      values: clause.values.map((entry) =>
        typeof entry === "string"
          ? (Object.entries(resolved.enumMap!).find(([, value]) => value === entry)?.[0] ?? entry)
          : entry,
      ),
    };
  }

  return clause;
}

function collectAliasToSourceMappings(node: RelNode, schema: SchemaDefinition): AliasToSourceMap {
  const aliasToSource: AliasToSourceMap = new Map();

  const visit = (current: RelNode): void => {
    switch (current.kind) {
      case "scan": {
        const binding = getNormalizedTableBinding(schema, current.table);
        if (binding?.kind === "physical") {
          aliasToSource.set(current.alias ?? current.table, binding.columnToSource);
        } else if (current.entity) {
          aliasToSource.set(
            current.alias ?? current.table,
            createPhysicalBindingFromEntity(current.entity).columnToSource,
          );
        }
        return;
      }
      case "filter":
      case "project":
      case "aggregate":
      case "window":
      case "sort":
      case "limit_offset":
        visit(current.input);
        return;
      case "join":
      case "set_op":
        visit(current.left);
        visit(current.right);
        return;
      case "with":
        current.ctes.forEach((cte) => visit(cte.query));
        visit(current.body);
        return;
      case "sql":
        return;
    }
  };

  visit(node);
  return aliasToSource;
}

function mapRelExprRefsForAliasSource(
  expr: RelExpr,
  aliasToSource: AliasToSourceMap,
  schema: SchemaDefinition,
): RelExpr {
  switch (expr.kind) {
    case "literal":
      return expr;
    case "column":
      return {
        ...expr,
        ref: mapColumnRefForAlias(expr.ref, aliasToSource),
      };
    case "function":
      return {
        ...expr,
        args: expr.args.map((arg) => mapRelExprRefsForAliasSource(arg, aliasToSource, schema)),
      };
    case "subquery":
      return {
        ...expr,
        rel: normalizeRelForProvider(expr.rel, schema),
      };
  }
}

function mapColumnRefForAlias(ref: RelColumnRef, aliasToSource: AliasToSourceMap): RelColumnRef {
  return {
    ...ref,
    column: mapColumnNameForAlias(ref.column, aliasToSource),
  };
}

function mapColumnNameForAlias(column: string, aliasToSource: AliasToSourceMap): string {
  let mappedColumn: string | undefined;
  for (const mapping of aliasToSource.values()) {
    const candidate = mapping[column];
    if (!candidate) {
      continue;
    }
    mappedColumn = candidate;
  }

  return mappedColumn ?? column;
}

export function resolveSingleProvider(
  node: RelNode,
  schema: SchemaDefinition,
  cteNames: Set<string> = new Set<string>(),
): string | null {
  const providers = new Set<string>();

  const visit = (current: RelNode, scopedCteNames: Set<string>): boolean => {
    switch (current.kind) {
      case "scan": {
        if (scopedCteNames.has(current.table)) {
          return true;
        }
        if (!schema.tables[current.table] && !current.entity) {
          return true;
        }
        const normalized = getNormalizedTableBinding(schema, current.table);
        if (normalized?.kind === "view") {
          return false;
        }
        providers.add(current.entity?.provider ?? resolveTableProvider(schema, current.table));
        return true;
      }
      case "sql": {
        for (const table of current.tables) {
          if (scopedCteNames.has(table) || !schema.tables[table]) {
            continue;
          }
          const normalized = getNormalizedTableBinding(schema, table);
          if (normalized?.kind === "view") {
            return false;
          }
          providers.add(resolveTableProvider(schema, table));
        }
        return true;
      }
      case "filter":
      case "project":
      case "aggregate":
      case "window":
      case "sort":
      case "limit_offset":
        return visit(current.input, scopedCteNames);
      case "join":
      case "set_op":
        return visit(current.left, scopedCteNames) && visit(current.right, scopedCteNames);
      case "with": {
        const nextScopedCteNames = new Set(scopedCteNames);
        for (const cte of current.ctes) {
          nextScopedCteNames.add(cte.name);
        }
        for (const cte of current.ctes) {
          if (!visit(cte.query, nextScopedCteNames)) {
            return false;
          }
        }
        return visit(current.body, nextScopedCteNames);
      }
    }
  };

  if (!visit(node, cteNames)) {
    return null;
  }

  if (providers.size !== 1) {
    return null;
  }
  return [...providers][0] ?? null;
}

function isPromiseLike<T>(value: unknown): value is Promise<T> {
  return !!value && typeof (value as { then?: unknown }).then === "function";
}
