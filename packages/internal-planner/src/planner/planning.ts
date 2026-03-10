import { Result, type Result as BetterResult } from "better-result";

import {
  TuplPlanningError,
  collectRelTables,
  createSqlRel,
  isRelProjectColumnMapping,
  stringifyUnknownValue,
  type RelColumnRef,
  type RelExpr,
  type RelJoinNode,
  type RelNode,
  type RelProjectExprMapping,
  type RelProjectNode,
  type RelScanNode,
  type RelWindowFunction,
} from "@tupl-internal/foundation";
import type {
  FromEntryAst,
  OrderByTermAst,
  SelectAst,
  SelectColumnAst,
  WindowClauseEntryAst,
  WindowSpecificationAst,
} from "./sqlite-parser/ast";
import { parseSqliteSelectAstResult } from "./sqlite-parser/parser";
import type { PhysicalPlan, PhysicalStep } from "./physical";
import {
  normalizeCapability,
  supportsLookupMany,
  type ProviderFragment,
  type ProvidersMap,
} from "@tupl-internal/provider";
import type { ScanFilterClause, SchemaDefinition, SchemaViewRelNode } from "@tupl-internal/schema";
import {
  createPhysicalBindingFromEntity,
  createTableDefinitionFromEntity,
  getNormalizedColumnBindings,
  getNormalizedColumnSourceMap,
  getNormalizedTableBinding,
  isNormalizedSourceColumnBinding,
  resolveTableProvider,
  resolveColumnDefinition,
  resolveNormalizedColumnSource,
  validateRelAgainstSchema,
  type ColumnDefinition,
  type NormalizedColumnBinding,
  type NormalizedPhysicalTableBinding,
} from "@tupl-internal/schema";

export interface RelLoweringResult {
  rel: RelNode;
  tables: string[];
}

interface Binding {
  table: string;
  alias: string;
  index: number;
}

interface ParsedJoin {
  alias: string;
  joinType: "inner" | "left" | "right" | "full";
  leftAlias: string;
  leftColumn: string;
  rightAlias: string;
  rightColumn: string;
}

interface SelectColumnProjection {
  kind: "column";
  source: RelColumnRef;
  output: string;
}

interface SelectWindowProjection {
  kind: "window";
  output: string;
  function: Extract<RelNode, { kind: "window" }>["functions"][number];
}

interface SelectExprProjection {
  kind: "expr";
  output: string;
  expr: RelExpr;
  source?: RelColumnRef;
}

type SelectProjection = SelectColumnProjection | SelectWindowProjection | SelectExprProjection;

interface ResolvedOrderTerm {
  source: {
    alias?: string;
    column: string;
  };
  direction: "asc" | "desc";
}

interface ParsedAggregateGroupProjection {
  kind: "group";
  output: string;
  source?: RelColumnRef;
  expr?: RelExpr;
}

interface ParsedAggregateMetricProjection {
  kind: "metric";
  output: string;
  metric: {
    fn: "count" | "sum" | "avg" | "min" | "max";
    as: string;
    column?: RelColumnRef;
    distinct?: boolean;
  };
}

type ParsedAggregateProjection = ParsedAggregateGroupProjection | ParsedAggregateMetricProjection;

interface ParsedGroupByRefTerm {
  kind: "ref";
  ref: RelColumnRef;
}

interface ParsedGroupByOrdinalTerm {
  kind: "ordinal";
  position: number;
}

type ParsedGroupByTerm = ParsedGroupByRefTerm | ParsedGroupByOrdinalTerm;

interface ParsedOrderByRefTerm {
  kind: "ref";
  source: ResolvedOrderTerm["source"];
  direction: "asc" | "desc";
}

interface ParsedOrderByOutputTerm {
  kind: "output";
  output: string;
  direction: "asc" | "desc";
}

interface ParsedOrderByOrdinalTerm {
  kind: "ordinal";
  position: number;
  direction: "asc" | "desc";
}

type ParsedOrderByTerm = ParsedOrderByRefTerm | ParsedOrderByOutputTerm | ParsedOrderByOrdinalTerm;

interface LiteralFilter {
  alias: string;
  clause: ScanFilterClause;
}

interface InSubqueryFilter {
  alias: string;
  column: string;
  subquery: SelectAst;
}

interface ParsedWhereFilters {
  literals: LiteralFilter[];
  inSubqueries: InSubqueryFilter[];
  residualExpr?: RelExpr;
}

function validateEnumLiteralFilters(
  filters: LiteralFilter[],
  bindings: Binding[],
  schema: SchemaDefinition,
): void {
  const tableByAlias = new Map(bindings.map((binding) => [binding.alias, binding.table]));

  for (const filter of filters) {
    const tableName = tableByAlias.get(filter.alias);
    if (!tableName) {
      continue;
    }
    const definition = schema.tables[tableName]?.columns[filter.clause.column];
    if (!definition || typeof definition === "string") {
      continue;
    }
    const resolved = resolveColumnDefinition(definition);
    if (!resolved.enum) {
      continue;
    }

    if (filter.clause.op === "eq") {
      if (typeof filter.clause.value === "string" && !resolved.enum.includes(filter.clause.value)) {
        throw new Error(`Invalid enum value for ${tableName}.${filter.clause.column}`);
      }
      continue;
    }

    if (filter.clause.op === "in") {
      for (const value of filter.clause.values) {
        if (value == null) {
          continue;
        }
        if (typeof value !== "string" || !resolved.enum.includes(value)) {
          throw new Error(`Invalid enum value for ${tableName}.${filter.clause.column}`);
        }
      }
    }
  }
}

function combineAndExprs(exprs: RelExpr[]): RelExpr | undefined {
  return exprs.reduce<RelExpr | undefined>(
    (acc, current) =>
      acc
        ? {
            kind: "function",
            name: "and",
            args: [acc, current],
          }
        : current,
    undefined,
  );
}

function toRelLiteralValue(value: unknown): string | number | boolean | null {
  if (
    value === null ||
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "boolean"
  ) {
    return value;
  }
  throw new Error(`Unsupported literal filter value: ${stringifyUnknownValue(value)}`);
}

function toParsedOrderSource(
  ref: RelColumnRef | null | undefined,
  fallbackColumn: string,
): ResolvedOrderTerm["source"] {
  if (!ref) {
    return {
      column: fallbackColumn,
    };
  }
  return ref.alias
    ? {
        alias: ref.alias,
        column: ref.column,
      }
    : {
        column: ref.column,
      };
}

function literalFilterToRelExpr(filter: LiteralFilter): RelExpr {
  const source: RelExpr = {
    kind: "column",
    ref: {
      alias: filter.alias,
      column: filter.clause.column,
    },
  };

  switch (filter.clause.op) {
    case "eq":
    case "neq":
    case "gt":
    case "gte":
    case "lt":
    case "lte":
    case "like":
    case "not_like":
    case "is_distinct_from":
    case "is_not_distinct_from":
      return {
        kind: "function",
        name: filter.clause.op,
        args: [source, { kind: "literal", value: toRelLiteralValue(filter.clause.value) }],
      };
    case "in":
    case "not_in":
      return {
        kind: "function",
        name: filter.clause.op,
        args: [
          source,
          ...filter.clause.values.map((value) => ({
            kind: "literal" as const,
            value: toRelLiteralValue(value),
          })),
        ],
      };
    case "is_null":
    case "is_not_null":
      return {
        kind: "function",
        name: filter.clause.op,
        args: [source],
      };
  }
}

function getPushableWhereAliases(rootAlias: string, joins: ParsedJoin[]): Set<string> {
  const pushable = new Set<string>([rootAlias]);

  for (const join of joins) {
    switch (join.joinType) {
      case "inner":
        pushable.add(join.alias);
        break;
      case "left":
        break;
      case "right":
        pushable.clear();
        pushable.add(join.alias);
        break;
      case "full":
        pushable.clear();
        break;
    }
  }

  return pushable;
}

let physicalStepIdCounter = 0;
let relIdCounter = 0;
let syntheticColumnCounter = 0;

type ViewAliasColumnMap = Record<string, RelColumnRef>;

interface ViewExpansionResult {
  node: RelNode;
  aliases: Map<string, ViewAliasColumnMap>;
}

function nextPhysicalStepId(prefix: string): string {
  physicalStepIdCounter += 1;
  return `${prefix}_${physicalStepIdCounter}`;
}

function nextRelId(prefix: string): string {
  relIdCounter += 1;
  return `${prefix}_${relIdCounter}`;
}

function nextSyntheticColumnName(prefix: string): string {
  syntheticColumnCounter += 1;
  return `__${prefix}_${syntheticColumnCounter}`;
}

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

export function lowerSqlToRel(sql: string, schema: SchemaDefinition): RelLoweringResult {
  const result = lowerSqlToRelResult(sql, schema);
  if (Result.isError(result)) {
    throw result.error;
  }
  return result.value;
}

export function lowerSqlToRelResult(sql: string, schema: SchemaDefinition) {
  return Result.gen(function* () {
    const ast = yield* parseSqliteSelectAstResult(sql);
    return Result.try({
      try: () => lowerSqlAstToRel(ast, sql, schema),
      catch: (error) => toTuplPlanningError(error, "lower SQL to relational plan"),
    });
  });
}

function lowerSqlAstToRel(
  ast: SelectAst,
  sql: string,
  schema: SchemaDefinition,
): RelLoweringResult {
  assertNoUnsupportedQueryShapes(ast);

  const structured = tryLowerStructuredSelect(ast, schema, new Set<string>());
  if (structured) {
    validateRelAgainstSchema(structured, schema);
    return {
      rel: structured,
      tables: collectRelTables(structured),
    };
  }

  const tables = collectTablesFromSelectAst(ast);
  const rel = createSqlRel(sql, tables);
  validateRelAgainstSchema(rel, schema);
  return {
    rel,
    tables,
  };
}

function assertNoUnsupportedQueryShapes(ast: SelectAst): void {
  const reason = findUnsupportedQueryShape(ast, new Set<string>());
  if (reason) {
    throw new Error(reason);
  }
}

function findUnsupportedQueryShape(ast: SelectAst, cteNames: Set<string>): string | null {
  const windowReason = findUnsupportedWindowShape(ast);
  if (windowReason) {
    return windowReason;
  }

  const withClauses = Array.isArray(ast.with) ? ast.with : [];
  if (withClauses.some((clause) => clause.recursive)) {
    return "Recursive CTEs are not yet supported.";
  }

  const scopedCteNames = new Set(cteNames);
  for (const clause of withClauses) {
    const rawName = clause.name;
    const cteName =
      typeof rawName === "string"
        ? rawName
        : rawName && typeof rawName === "object" && typeof rawName.value === "string"
          ? rawName.value
          : null;
    if (cteName) {
      scopedCteNames.add(cteName);
    }
  }

  for (const clause of withClauses) {
    const nested = clause.stmt?.ast;
    if (nested) {
      const reason = findUnsupportedQueryShape(nested, scopedCteNames);
      if (reason) {
        return reason;
      }
    }
  }

  const from = Array.isArray(ast.from) ? ast.from : ast.from ? [ast.from] : [];
  if (from.some((entry) => !!entry.stmt)) {
    return "Unsupported FROM clause entry.";
  }

  const outerAliases = new Set<string>(
    from.flatMap((entry) => {
      const alias = typeof entry.as === "string" && entry.as ? entry.as : entry.table;
      return typeof alias === "string" ? [alias] : [];
    }),
  );

  const expressionFields = [
    ast.columns,
    ast.where,
    ast.groupby,
    ast.having,
    ast.orderby,
    ast.limit,
    ast.window,
  ];
  for (const field of expressionFields) {
    const reason = findUnsupportedSubqueryShape(field, outerAliases, scopedCteNames);
    if (reason) {
      return reason;
    }
  }

  if (ast._next) {
    return findUnsupportedQueryShape(ast._next, scopedCteNames);
  }

  return null;
}

function findUnsupportedWindowShape(ast: SelectAst): string | null {
  const hasWindow = hasWindowExpression(ast.columns);
  if (hasWindow && (ast.groupby || ast.having)) {
    return "Window functions cannot be mixed with GROUP BY/HAVING.";
  }

  if (ast.window && ast.window.length > 0) {
    return "Named WINDOW clauses are not yet supported.";
  }

  return findUnsupportedWindowShapeInValue(ast.columns);
}

function findUnsupportedWindowShapeInValue(value: unknown): string | null {
  if (!value || typeof value !== "object") {
    return null;
  }

  if (Array.isArray(value)) {
    for (const item of value) {
      const reason = findUnsupportedWindowShapeInValue(item);
      if (reason) {
        return reason;
      }
    }
    return null;
  }

  const record = value as Record<string, unknown>;
  const exprType = record.type;
  if (exprType === "function" || exprType === "aggr_func") {
    const overReason = validateSupportedWindowOver(record);
    if (overReason) {
      return overReason;
    }
  }

  for (const nested of Object.values(record)) {
    const reason = findUnsupportedWindowShapeInValue(nested);
    if (reason) {
      return reason;
    }
  }

  return null;
}

function validateSupportedWindowOver(expr: Record<string, unknown>): string | null {
  if (!expr.over || typeof expr.over !== "object") {
    return null;
  }

  const rawName =
    expr.type === "aggr_func"
      ? typeof expr.name === "string"
        ? expr.name
        : null
      : (expr.name as { name?: Array<{ value?: unknown }> } | undefined)?.name?.[0]?.value;
  if (typeof rawName !== "string") {
    return "Unsupported window function.";
  }

  const normalized = rawName.toLowerCase();
  if (
    normalized !== "row_number" &&
    normalized !== "rank" &&
    normalized !== "dense_rank" &&
    normalized !== "count" &&
    normalized !== "sum" &&
    normalized !== "avg" &&
    normalized !== "min" &&
    normalized !== "max"
  ) {
    return `Unsupported window function: ${rawName.toUpperCase()}`;
  }

  const rawSpec = (expr.over as { as_window_specification?: unknown }).as_window_specification;
  if (typeof rawSpec === "string") {
    return "Named WINDOW clauses are not yet supported.";
  }
  if (!rawSpec || typeof rawSpec !== "object") {
    return null;
  }

  const spec = (rawSpec as { window_specification?: unknown }).window_specification;
  if (!spec || typeof spec !== "object") {
    return null;
  }

  if ((spec as WindowSpecificationAst).window_frame_clause) {
    return "Explicit window frame clauses are not yet supported.";
  }

  return null;
}

function hasWindowExpression(rawColumns: unknown): boolean {
  if (rawColumns === "*") {
    return false;
  }

  const columns = Array.isArray(rawColumns) ? (rawColumns as SelectColumnAst[]) : [];
  return columns.some((entry) => {
    const expr = entry.expr as { over?: unknown };
    return !!expr.over;
  });
}

function findUnsupportedSubqueryShape(
  value: unknown,
  outerAliases: Set<string>,
  cteNames: Set<string>,
): string | null {
  const subquery = parseSubqueryAst(value);
  if (subquery) {
    if (isCorrelatedSubquery(subquery, outerAliases)) {
      return "Correlated subqueries are not yet supported.";
    }
    return findUnsupportedQueryShape(subquery, cteNames);
  }

  if (!value || typeof value !== "object") {
    return null;
  }

  if (Array.isArray(value)) {
    for (const item of value) {
      const reason = findUnsupportedSubqueryShape(item, outerAliases, cteNames);
      if (reason) {
        return reason;
      }
    }
    return null;
  }

  for (const nested of Object.values(value)) {
    const reason = findUnsupportedSubqueryShape(nested, outerAliases, cteNames);
    if (reason) {
      return reason;
    }
  }

  return null;
}

export function expandRelViews<TContext>(
  rel: RelNode,
  schema: SchemaDefinition,
  context?: TContext,
): RelNode {
  const result = expandRelViewsResult(rel, schema, context);
  if (Result.isError(result)) {
    throw result.error;
  }
  return result.value;
}

export function expandRelViewsResult<TContext>(
  rel: RelNode,
  schema: SchemaDefinition,
  context?: TContext,
) {
  return Result.try({
    try: () => expandRelViewsInternal(rel, schema, context).node,
    catch: (error) => toTuplPlanningError(error, "expand relational views"),
  });
}

export async function planPhysicalQuery<TContext>(
  rel: RelNode,
  schema: SchemaDefinition,
  providers: ProvidersMap<TContext>,
  context: TContext,
  _sql: string,
): Promise<PhysicalPlan> {
  const result = await planPhysicalQueryResult(rel, schema, providers, context, _sql);
  if (Result.isError(result)) {
    throw result.error;
  }
  return result.value;
}

export async function planPhysicalQueryResult<TContext>(
  rel: RelNode,
  schema: SchemaDefinition,
  providers: ProvidersMap<TContext>,
  context: TContext,
  _sql: string,
) {
  return Result.gen(async function* () {
    const expandedRel = yield* expandRelViewsResult(rel, schema, context);
    const plannedRel = assignConventions(expandedRel, schema);
    const state: { steps: PhysicalStep[] } = { steps: [] };

    const rootStepId = yield* Result.await(
      planPhysicalNodeResult(plannedRel, schema, providers, context, state),
    );

    return Result.ok({
      rel: plannedRel,
      rootStepId,
      steps: state.steps,
    });
  });
}

async function planPhysicalNodeResult<TContext>(
  node: RelNode,
  schema: SchemaDefinition,
  providers: ProvidersMap<TContext>,
  context: TContext,
  state: { steps: PhysicalStep[] },
): Promise<BetterResult<string, TuplPlanningError>> {
  return Result.gen(async function* () {
    const remoteStepId = yield* Result.await(
      tryPlanRemoteFragmentResult(node, schema, providers, context, state),
    );
    if (remoteStepId) {
      return Result.ok(remoteStepId);
    }

    switch (node.kind) {
      case "scan": {
        const step: PhysicalStep = {
          id: nextPhysicalStepId("local_project"),
          kind: "local_project",
          dependsOn: [],
          summary: `Local fallback scan for ${node.table}`,
        };
        state.steps.push(step);
        return Result.ok(step.id);
      }
      case "filter":
      case "project":
      case "aggregate":
      case "sort":
      case "limit_offset": {
        const input = yield* Result.await(
          planPhysicalNodeResult(node.input, schema, providers, context, state),
        );
        const kind =
          node.kind === "filter"
            ? "local_filter"
            : node.kind === "project"
              ? "local_project"
              : node.kind === "aggregate"
                ? "local_aggregate"
                : node.kind === "sort"
                  ? "local_sort"
                  : "local_limit_offset";

        const step: PhysicalStep = {
          id: nextPhysicalStepId(kind),
          kind,
          dependsOn: [input],
          summary: `Local ${node.kind} execution`,
        };
        state.steps.push(step);
        return Result.ok(step.id);
      }
      case "join": {
        const lookup = resolveLookupJoinCandidate(node, schema, providers);
        if (lookup) {
          const left = yield* Result.await(
            planPhysicalNodeResult(node.left, schema, providers, context, state),
          );
          const step: PhysicalStep = {
            id: nextPhysicalStepId("lookup_join"),
            kind: "lookup_join",
            dependsOn: [left],
            summary: `Lookup join ${lookup.leftScan.table}.${lookup.leftKey} -> ${lookup.rightScan.table}.${lookup.rightKey}`,
            leftProvider: lookup.leftProvider,
            rightProvider: lookup.rightProvider,
            leftTable: lookup.leftScan.table,
            rightTable: lookup.rightScan.table,
            leftKey: lookup.leftKey,
            rightKey: lookup.rightKey,
            joinType: lookup.joinType,
          };
          state.steps.push(step);
          return Result.ok(step.id);
        }

        const left = yield* Result.await(
          planPhysicalNodeResult(node.left, schema, providers, context, state),
        );
        const right = yield* Result.await(
          planPhysicalNodeResult(node.right, schema, providers, context, state),
        );
        const step: PhysicalStep = {
          id: nextPhysicalStepId("local_hash_join"),
          kind: "local_hash_join",
          dependsOn: [left, right],
          summary: `Local ${node.joinType} join execution`,
        };
        state.steps.push(step);
        return Result.ok(step.id);
      }
      case "window": {
        const input = yield* Result.await(
          planPhysicalNodeResult(node.input, schema, providers, context, state),
        );
        const step: PhysicalStep = {
          id: nextPhysicalStepId("local_window"),
          kind: "local_window",
          dependsOn: [input],
          summary: "Local window execution",
        };
        state.steps.push(step);
        return Result.ok(step.id);
      }
      case "set_op": {
        const left = yield* Result.await(
          planPhysicalNodeResult(node.left, schema, providers, context, state),
        );
        const right = yield* Result.await(
          planPhysicalNodeResult(node.right, schema, providers, context, state),
        );
        const step: PhysicalStep = {
          id: nextPhysicalStepId("local_set_op"),
          kind: "local_set_op",
          dependsOn: [left, right],
          summary: `Local ${node.op} execution`,
        };
        state.steps.push(step);
        return Result.ok(step.id);
      }
      case "with": {
        const dependencies: string[] = [];
        for (const cte of node.ctes) {
          dependencies.push(
            yield* Result.await(
              planPhysicalNodeResult(cte.query, schema, providers, context, state),
            ),
          );
        }
        dependencies.push(
          yield* Result.await(planPhysicalNodeResult(node.body, schema, providers, context, state)),
        );

        const step: PhysicalStep = {
          id: nextPhysicalStepId("local_with"),
          kind: "local_with",
          dependsOn: dependencies,
          summary: "Local WITH materialization",
        };
        state.steps.push(step);
        return Result.ok(step.id);
      }
      case "sql": {
        const step: PhysicalStep = {
          id: nextPhysicalStepId("local_project"),
          kind: "local_project",
          dependsOn: [],
          summary: "Local SQL fallback execution",
        };
        state.steps.push(step);
        return Result.ok(step.id);
      }
    }
  });
}

function expandRelViewsInternal<TContext>(
  node: RelNode,
  schema: SchemaDefinition,
  context?: TContext,
): ViewExpansionResult {
  switch (node.kind) {
    case "scan": {
      const binding = getNormalizedTableBinding(schema, node.table);
      if (binding?.kind === "physical" && hasCalculatedColumns(binding)) {
        const expanded = expandCalculatedScan(node, binding);
        if (expanded) {
          return expanded;
        }
      }

      if (!binding || binding.kind !== "view") {
        return {
          node,
          aliases: new Map(),
        };
      }

      const alias = node.alias ?? node.table;
      let current = compileViewRelForPlanner(node.table, binding.rel(context as unknown), schema);
      const expandedView = expandRelViewsInternal(current, schema, context);
      current = expandedView.node;

      const columnBindings = getNormalizedColumnBindings(binding);
      const hasCalculatedViewColumns = Object.values(columnBindings).some(
        (columnBinding) => !isNormalizedSourceColumnBinding(columnBinding),
      );

      let viewAliasMapping: ViewAliasColumnMap;
      if (hasCalculatedViewColumns) {
        current = buildPlannerViewProjection(alias, current, binding, expandedView.aliases);
        viewAliasMapping = Object.fromEntries(
          Object.keys(columnBindings).map((column) => [column, { alias, column }]),
        );
      } else {
        viewAliasMapping = {};
        for (const [logicalColumn, source] of Object.entries(
          getNormalizedColumnSourceMap(binding),
        )) {
          viewAliasMapping[logicalColumn] = resolveViewSourceRef(source, expandedView.aliases);
        }
      }

      if (node.where && node.where.length > 0) {
        current = {
          id: nextRelId("filter"),
          kind: "filter",
          convention: "local",
          input: current,
          where: node.where.map((clause) => ({
            ...clause,
            column: mapViewColumnName(clause.column, viewAliasMapping, expandedView.aliases),
          })),
          output: current.output,
        };
      }

      if (node.orderBy && node.orderBy.length > 0) {
        current = {
          id: nextRelId("sort"),
          kind: "sort",
          convention: "local",
          input: current,
          orderBy: node.orderBy.map((term) => ({
            source: parseRelColumnRef(
              mapViewColumnName(term.column, viewAliasMapping, expandedView.aliases),
            ),
            direction: term.direction,
          })),
          output: current.output,
        };
      }

      if (node.limit != null || node.offset != null) {
        current = {
          id: nextRelId("limit_offset"),
          kind: "limit_offset",
          convention: "local",
          input: current,
          ...(node.limit != null ? { limit: node.limit } : {}),
          ...(node.offset != null ? { offset: node.offset } : {}),
          output: current.output,
        };
      }

      const aliases = mergeAliasMaps(expandedView.aliases, new Map([[alias, viewAliasMapping]]));
      return {
        node: current,
        aliases,
      };
    }
    case "filter": {
      const input = expandRelViewsInternal(node.input, schema, context);
      return {
        node: {
          ...node,
          input: input.node,
          ...(node.where
            ? {
                where: node.where.map((clause) => ({
                  ...clause,
                  column: rewriteColumnNameWithAliases(clause.column, input.aliases),
                })),
              }
            : {}),
          ...(node.expr
            ? {
                expr: mapRelExprRefs(node.expr, input.aliases),
              }
            : {}),
        },
        aliases: input.aliases,
      };
    }
    case "project": {
      const input = expandRelViewsInternal(node.input, schema, context);
      return {
        node: {
          ...node,
          input: input.node,
          columns: node.columns.map((column) =>
            isRelProjectColumnMapping(column)
              ? {
                  ...column,
                  source: resolveMappedColumnRef(column.source, input.aliases),
                }
              : {
                  ...column,
                  expr: mapRelExprRefs(column.expr, input.aliases),
                },
          ),
        },
        aliases: input.aliases,
      };
    }
    case "aggregate": {
      const input = expandRelViewsInternal(node.input, schema, context);
      const groupBy = node.groupBy.map((column) => resolveMappedColumnRef(column, input.aliases));
      const metrics = node.metrics.map((metric) => ({
        ...metric,
        ...(metric.column ? { column: resolveMappedColumnRef(metric.column, input.aliases) } : {}),
      }));
      const aggregateNode: RelNode = {
        ...node,
        input: input.node,
        groupBy,
        metrics,
      };

      const expectedOutputs = node.output.map((column) => column.name);
      const actualOutputs = [
        ...groupBy.map((column) => column.column),
        ...metrics.map((metric) => metric.as),
      ];

      if (expectedOutputs.every((name, index) => name === actualOutputs[index])) {
        return {
          node: aggregateNode,
          aliases: input.aliases,
        };
      }

      return {
        node: {
          id: nextRelId("project"),
          kind: "project",
          convention: "local",
          input: aggregateNode,
          columns: expectedOutputs.map((output, index) => ({
            kind: "column" as const,
            source: { column: actualOutputs[index] ?? output },
            output,
          })),
          output: node.output,
        },
        aliases: input.aliases,
      };
    }
    case "window": {
      const input = expandRelViewsInternal(node.input, schema, context);
      return {
        node: {
          ...node,
          input: input.node,
          functions: node.functions.map((fn) => ({
            ...fn,
            partitionBy: fn.partitionBy.map((column) =>
              resolveMappedColumnRef(column, input.aliases),
            ),
            orderBy: fn.orderBy.map((term) => ({
              ...term,
              source: resolveMappedColumnRef(term.source, input.aliases),
            })),
          })),
        },
        aliases: input.aliases,
      };
    }
    case "sort": {
      const input = expandRelViewsInternal(node.input, schema, context);
      return {
        node: {
          ...node,
          input: input.node,
          orderBy: node.orderBy.map((term) => ({
            ...term,
            source: resolveMappedColumnRef(term.source, input.aliases),
          })),
        },
        aliases: input.aliases,
      };
    }
    case "limit_offset": {
      const input = expandRelViewsInternal(node.input, schema, context);
      return {
        node: {
          ...node,
          input: input.node,
        },
        aliases: input.aliases,
      };
    }
    case "join": {
      const left = expandRelViewsInternal(node.left, schema, context);
      const right = expandRelViewsInternal(node.right, schema, context);
      const aliases = mergeAliasMaps(left.aliases, right.aliases);
      return {
        node: {
          ...node,
          left: left.node,
          right: right.node,
          leftKey: resolveMappedColumnRef(node.leftKey, aliases),
          rightKey: resolveMappedColumnRef(node.rightKey, aliases),
        },
        aliases,
      };
    }
    case "set_op": {
      const left = expandRelViewsInternal(node.left, schema, context);
      const right = expandRelViewsInternal(node.right, schema, context);
      return {
        node: {
          ...node,
          left: left.node,
          right: right.node,
        },
        aliases: mergeAliasMaps(left.aliases, right.aliases),
      };
    }
    case "with": {
      const cteAliases: Array<Map<string, ViewAliasColumnMap>> = [];
      const ctes = node.ctes.map((cte) => {
        const expanded = expandRelViewsInternal(cte.query, schema, context);
        cteAliases.push(expanded.aliases);
        return {
          ...cte,
          query: expanded.node,
        };
      });
      const body = expandRelViewsInternal(node.body, schema, context);
      return {
        node: {
          ...node,
          ctes,
          body: body.node,
        },
        aliases: mergeAliasMaps(...cteAliases, body.aliases),
      };
    }
    case "sql":
      return {
        node,
        aliases: new Map(),
      };
  }
}

function mergeAliasMaps(
  ...maps: Array<Map<string, ViewAliasColumnMap>>
): Map<string, ViewAliasColumnMap> {
  const out = new Map<string, ViewAliasColumnMap>();
  for (const aliases of maps) {
    for (const [alias, mapping] of aliases.entries()) {
      out.set(alias, mapping);
    }
  }
  return out;
}

function hasCalculatedColumns(binding: NormalizedPhysicalTableBinding): boolean {
  return Object.values(getNormalizedColumnBindings(binding)).some(
    (columnBinding) => !isNormalizedSourceColumnBinding(columnBinding),
  );
}

function expandCalculatedScan(
  node: RelScanNode,
  binding: NormalizedPhysicalTableBinding,
): ViewExpansionResult | null {
  const columnBindings = getNormalizedColumnBindings(binding);
  const referencedColumns = new Set<string>(node.select);
  for (const clause of node.where ?? []) {
    referencedColumns.add(clause.column);
  }
  for (const term of node.orderBy ?? []) {
    referencedColumns.add(term.column);
  }

  const referencedCalculated = [...referencedColumns].filter((column) => {
    const columnBinding = columnBindings[column];
    return !!columnBinding && !isNormalizedSourceColumnBinding(columnBinding);
  });
  if (referencedCalculated.length === 0) {
    return null;
  }

  const requiredSourceColumns = new Set<string>();
  for (const column of referencedColumns) {
    const columnBinding = columnBindings[column];
    if (!columnBinding) {
      requiredSourceColumns.add(column);
      continue;
    }
    if (isNormalizedSourceColumnBinding(columnBinding)) {
      requiredSourceColumns.add(column);
      continue;
    }
    for (const dependency of collectExprColumns(columnBinding.expr)) {
      requiredSourceColumns.add(dependency);
    }
  }

  const alias = node.alias ?? node.table;
  let current: RelNode = {
    id: node.id,
    kind: "scan",
    convention: node.convention,
    table: node.table,
    ...(node.alias ? { alias: node.alias } : {}),
    select: [...requiredSourceColumns],
    output: [...requiredSourceColumns].map((column) => ({
      name: `${alias}.${column}`,
    })),
  };

  const projectedColumns = [...referencedColumns].map((column) => {
    const columnBinding = columnBindings[column];
    if (!columnBinding || isNormalizedSourceColumnBinding(columnBinding)) {
      return {
        kind: "column" as const,
        source: { alias, column },
        output: column,
      };
    }
    return {
      kind: "expr" as const,
      expr: qualifyExprColumns(columnBinding.expr, alias),
      output: column,
    };
  });

  current = {
    id: nextRelId("project"),
    kind: "project",
    convention: "local",
    input: current,
    columns: projectedColumns,
    output: [...referencedColumns].map((column) => ({ name: column })),
  };

  if (node.where && node.where.length > 0) {
    current = {
      id: nextRelId("filter"),
      kind: "filter",
      convention: "local",
      input: current,
      where: node.where,
      output: current.output,
    };
  }

  if (node.orderBy && node.orderBy.length > 0) {
    current = {
      id: nextRelId("sort"),
      kind: "sort",
      convention: "local",
      input: current,
      orderBy: node.orderBy.map((term) => ({
        source: { column: term.column },
        direction: term.direction,
      })),
      output: current.output,
    };
  }

  if (node.limit != null || node.offset != null) {
    current = {
      id: nextRelId("limit_offset"),
      kind: "limit_offset",
      convention: "local",
      input: current,
      ...(node.limit != null ? { limit: node.limit } : {}),
      ...(node.offset != null ? { offset: node.offset } : {}),
      output: current.output,
    };
  }

  const aliasMap: ViewAliasColumnMap = Object.fromEntries(
    [...referencedColumns].map((column) => [column, { column }]),
  );
  return {
    node: current,
    aliases: new Map([[alias, aliasMap]]),
  };
}

function buildPlannerViewProjection(
  alias: string,
  input: RelNode,
  binding: Parameters<typeof getNormalizedColumnBindings>[0],
  aliases: Map<string, ViewAliasColumnMap>,
): RelNode {
  const columnBindings = getNormalizedColumnBindings(binding);
  const columns = Object.entries(columnBindings).map(([output, columnBinding]) => {
    if (isNormalizedSourceColumnBinding(columnBinding)) {
      return {
        kind: "column" as const,
        source: resolveViewSourceRef(columnBinding.source, aliases),
        output: `${alias}.${output}`,
      };
    }

    return {
      kind: "expr" as const,
      expr: rewriteViewBindingExprForPlanner(columnBinding.expr, columnBindings, aliases),
      output: `${alias}.${output}`,
    };
  });

  return {
    id: nextRelId("view_project"),
    kind: "project",
    convention: "local",
    input,
    columns,
    output: Object.keys(columnBindings).map((column) => ({ name: `${alias}.${column}` })),
  };
}

function rewriteViewBindingExprForPlanner(
  expr: RelExpr,
  columnBindings: Record<string, NormalizedColumnBinding>,
  aliases: Map<string, ViewAliasColumnMap>,
): RelExpr {
  switch (expr.kind) {
    case "literal":
      return expr;
    case "function":
      return {
        kind: "function",
        name: expr.name,
        args: expr.args.map((arg) =>
          rewriteViewBindingExprForPlanner(arg, columnBindings, aliases),
        ),
      };
    case "column": {
      if (!expr.ref.table && !expr.ref.alias) {
        const binding = columnBindings[expr.ref.column];
        if (binding && isNormalizedSourceColumnBinding(binding)) {
          return {
            kind: "column",
            ref: resolveViewSourceRef(binding.source, aliases),
          };
        }
        return expr;
      }

      return {
        kind: "column",
        ref: resolveMappedColumnRef(expr.ref, aliases),
      };
    }
    case "subquery":
      return expr;
  }
}

function resolveViewSourceRef(
  source: string,
  aliases: Map<string, ViewAliasColumnMap>,
): RelColumnRef {
  const ref = parseRelColumnRef(source);
  return ref.alias || ref.table ? resolveMappedColumnRef(ref, aliases) : ref;
}

function mapViewColumnName(
  column: string,
  viewAliasMapping: ViewAliasColumnMap,
  aliases: Map<string, ViewAliasColumnMap>,
): string {
  const idx = column.lastIndexOf(".");
  if (idx > 0) {
    const name = column.slice(idx + 1);
    if (name in viewAliasMapping) {
      return toColumnName(
        resolveMappedColumnRef(viewAliasMapping[name] ?? parseRelColumnRef(name), aliases),
      );
    }
    return rewriteColumnNameWithAliases(column, aliases);
  }

  const mapped = viewAliasMapping[column];
  if (mapped) {
    return toColumnName(resolveMappedColumnRef(mapped, aliases));
  }
  return rewriteColumnNameWithAliases(column, aliases);
}

function rewriteColumnNameWithAliases(
  column: string,
  aliases: Map<string, ViewAliasColumnMap>,
): string {
  const ref = parseRelColumnRef(column);
  return toColumnName(resolveMappedColumnRef(ref, aliases));
}

function resolveMappedColumnRef(
  ref: RelColumnRef,
  aliases: Map<string, ViewAliasColumnMap>,
): RelColumnRef {
  const seen = new Set<string>();
  let current = ref;

  while (true) {
    const alias = current.alias ?? current.table;
    if (!alias) {
      let candidate: RelColumnRef | null = null;
      for (const mapping of aliases.values()) {
        const mapped = mapping[current.column];
        if (!mapped) {
          continue;
        }
        const resolved =
          mapped.alias || mapped.table ? resolveMappedColumnRef(mapped, aliases) : mapped;
        const key = toColumnName(resolved);
        if (!candidate) {
          candidate = resolved;
          continue;
        }
        if (toColumnName(candidate) !== key) {
          return current;
        }
      }
      return candidate ?? current;
    }

    const key = `${alias}.${current.column}`;
    if (seen.has(key)) {
      return current;
    }
    seen.add(key);

    const mapping = aliases.get(alias);
    if (!mapping) {
      return current;
    }
    const next = mapping[current.column];
    if (!next) {
      return {
        column: current.column,
      };
    }
    current = next;
  }
}

function mapRelExprRefs(expr: RelExpr, aliases: Map<string, ViewAliasColumnMap>): RelExpr {
  switch (expr.kind) {
    case "literal":
      return expr;
    case "column":
      return {
        kind: "column",
        ref: resolveMappedColumnRef(expr.ref, aliases),
      };
    case "function":
      return {
        kind: "function",
        name: expr.name,
        args: expr.args.map((arg) => mapRelExprRefs(arg, aliases)),
      };
    case "subquery":
      return expr;
  }
}

function mapRelExprRefsForAliasSource(expr: RelExpr, aliasToSource: AliasToSourceMap): RelExpr {
  switch (expr.kind) {
    case "literal":
      return expr;
    case "column":
      return {
        kind: "column",
        ref: mapColumnRefForAlias(expr.ref, aliasToSource),
      };
    case "function":
      return {
        kind: "function",
        name: expr.name,
        args: expr.args.map((arg) => mapRelExprRefsForAliasSource(arg, aliasToSource)),
      };
    case "subquery":
      return expr;
  }
}

function qualifyExprColumns(expr: RelExpr, alias: string): RelExpr {
  switch (expr.kind) {
    case "literal":
      return expr;
    case "column":
      return {
        kind: "column",
        ref: {
          alias: expr.ref.alias ?? expr.ref.table ?? alias,
          column: expr.ref.column,
        },
      };
    case "function":
      return {
        kind: "function",
        name: expr.name,
        args: expr.args.map((arg) => qualifyExprColumns(arg, alias)),
      };
    case "subquery":
      return expr;
  }
}

function collectExprColumns(expr: RelExpr): Set<string> {
  const columns = new Set<string>();

  const visit = (current: RelExpr): void => {
    switch (current.kind) {
      case "literal":
        return;
      case "column":
        columns.add(current.ref.column);
        return;
      case "function":
        for (const arg of current.args) {
          visit(arg);
        }
        return;
      case "subquery":
        return;
    }
  };

  visit(expr);
  return columns;
}

function collectRelExprRefs(expr: RelExpr): RelColumnRef[] {
  const refs: RelColumnRef[] = [];

  const visit = (current: RelExpr): void => {
    switch (current.kind) {
      case "literal":
        return;
      case "column":
        refs.push(current.ref);
        return;
      case "function":
        for (const arg of current.args) {
          visit(arg);
        }
        return;
      case "subquery":
        return;
    }
  };

  visit(expr);
  return refs;
}

function toColumnName(ref: RelColumnRef): string {
  const alias = ref.alias ?? ref.table;
  return alias ? `${alias}.${ref.column}` : ref.column;
}

function parseRelColumnRef(ref: string): RelColumnRef {
  const idx = ref.lastIndexOf(".");
  if (idx < 0) {
    return {
      column: ref,
    };
  }
  return {
    alias: ref.slice(0, idx),
    column: ref.slice(idx + 1),
  };
}

function compileViewRelForPlanner(
  _viewName: string,
  definition: unknown,
  schema: SchemaDefinition,
): RelNode {
  if (
    definition &&
    typeof definition === "object" &&
    typeof (definition as { kind?: unknown }).kind === "string" &&
    typeof (definition as { convention?: unknown }).convention === "string"
  ) {
    return definition as RelNode;
  }

  if (
    !definition ||
    typeof definition !== "object" ||
    typeof (definition as { kind?: unknown }).kind !== "string"
  ) {
    throw new Error("View returned an unsupported rel definition.");
  }

  return compileSchemaViewRelForPlanner(definition as SchemaViewRelNode, schema);
}

function compileSchemaViewRelForPlanner(
  node: SchemaViewRelNode,
  schema: SchemaDefinition,
): RelNode {
  switch (node.kind) {
    case "scan": {
      const table =
        schema.tables[node.table] ??
        (node.entity ? createTableDefinitionFromEntity(node.entity) : undefined);
      if (!table || (node.entity && Object.keys(table.columns).length === 0)) {
        throw new Error(`Unknown table in view rel scan: ${node.table}`);
      }
      const select = Object.keys(table.columns);
      return {
        id: nextRelId("view_scan"),
        kind: "scan",
        convention: "local",
        table: node.table,
        ...(node.entity ? { entity: node.entity } : {}),
        alias: node.table,
        select,
        output: select.map((column) => ({
          name: `${node.table}.${column}`,
        })),
      };
    }
    case "join": {
      const left = compileSchemaViewRelForPlanner(node.left, schema);
      const right = compileSchemaViewRelForPlanner(node.right, schema);
      return {
        id: nextRelId("view_join"),
        kind: "join",
        convention: "local",
        joinType: node.type,
        left,
        right,
        leftKey: parseRelColumnRef(resolveViewRelRef(node.on.left)),
        rightKey: parseRelColumnRef(resolveViewRelRef(node.on.right)),
        output: [...left.output, ...right.output],
      };
    }
    case "aggregate": {
      const input = compileSchemaViewRelForPlanner(node.from, schema);
      const groupBy = Object.entries(node.groupBy).map(([name, column]) => ({
        name,
        ref: parseRelColumnRef(resolveViewRelRef(column)),
      }));
      const metrics = Object.entries(node.measures).map(([output, metric]) => ({
        fn: metric.fn,
        as: output,
        ...(metric.column ? { column: parseRelColumnRef(resolveViewRelRef(metric.column)) } : {}),
      }));
      return {
        id: nextRelId("view_aggregate"),
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

function resolveViewRelRef(ref: { ref?: string }): string {
  if (!ref.ref) {
    throw new Error("View rel column reference was not normalized to a string reference.");
  }
  return ref.ref;
}

async function tryPlanRemoteFragmentResult<TContext>(
  node: RelNode,
  schema: SchemaDefinition,
  providers: ProvidersMap<TContext>,
  context: TContext,
  state: { steps: PhysicalStep[] },
): Promise<BetterResult<string | null, TuplPlanningError>> {
  const provider = resolveSingleProvider(node, schema);
  if (!provider) {
    return Result.ok(null);
  }

  const adapter = providers[provider];
  if (!adapter) {
    return Result.err(
      new TuplPlanningError({
        operation: "plan remote fragment",
        message: `Missing provider adapter: ${provider}`,
      }),
    );
  }

  const fragmentResult = buildProviderFragmentForNodeResult(node, schema, provider);
  if (Result.isError(fragmentResult)) {
    return fragmentResult;
  }

  const capabilityResult = await Result.tryPromise({
    try: () => Promise.resolve(adapter.canExecute(fragmentResult.value, context)),
    catch: (error) => toTuplPlanningError(error, "plan remote fragment"),
  });
  if (Result.isError(capabilityResult)) {
    return capabilityResult;
  }

  const capability = normalizeCapability(capabilityResult.value);
  if (!capability.supported) {
    return Result.ok(null);
  }

  const step: PhysicalStep = {
    id: nextPhysicalStepId("remote_fragment"),
    kind: "remote_fragment",
    dependsOn: [],
    summary: `Execute provider fragment (${provider})`,
    provider,
    fragment: fragmentResult.value,
  };

  state.steps.push(step);
  return Result.ok(step.id);
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
                expr: mapRelExprRefsForAliasSource(current.expr, aliasToSource),
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
                  expr: mapRelExprRefsForAliasSource(column.expr, aliasToSource),
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
      const simplified: RelProjectNode = {
        ...node,
        input: simplifyProviderProjects(node.input),
      };
      return hoistProjectAcrossUnaryChain(simplified);
    }
    case "aggregate":
      return {
        ...node,
        input: simplifyProviderProjects(node.input),
      };
    case "window":
      return {
        ...node,
        input: simplifyProviderProjects(node.input),
      };
    case "sort":
      return {
        ...node,
        input: simplifyProviderProjects(node.input),
      };
    case "limit_offset":
      return {
        ...node,
        input: simplifyProviderProjects(node.input),
      };
    case "join":
      return {
        ...node,
        left: simplifyProviderProjects(node.left),
        right: simplifyProviderProjects(node.right),
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
  const unaryChain: Array<Extract<RelNode, { kind: "filter" | "sort" | "limit_offset" }>> = [];
  let current = project.input;

  while (current.kind === "filter" || current.kind === "sort" || current.kind === "limit_offset") {
    unaryChain.push(current);
    current = current.input;
  }

  if (current.kind !== "project") {
    return project;
  }

  const mergedColumns = composeProjectMappings(project.columns, current.columns);
  if (!mergedColumns) {
    return project;
  }

  let rebuiltInput: RelNode = current.input;
  for (let index = unaryChain.length - 1; index >= 0; index -= 1) {
    const unary = unaryChain[index];
    if (!unary) {
      continue;
    }
    rebuiltInput = {
      ...unary,
      input: rebuiltInput,
    };
  }

  return {
    ...project,
    input: rebuiltInput,
    columns: mergedColumns,
  };
}

function composeProjectMappings(
  outer: RelProjectNode["columns"],
  inner: RelProjectNode["columns"],
): RelProjectNode["columns"] | null {
  const innerByOutput = new Map(inner.map((mapping) => [mapping.output, mapping] as const));
  const merged: RelProjectNode["columns"] = [];

  for (const mapping of outer) {
    if (!isRelProjectColumnMapping(mapping)) {
      return null;
    }

    if (mapping.source.alias || mapping.source.table) {
      return null;
    }

    const innerMapping = innerByOutput.get(mapping.source.column);
    if (!innerMapping) {
      return null;
    }

    if (isRelProjectColumnMapping(innerMapping)) {
      merged.push({
        kind: "column",
        source: innerMapping.source,
        output: mapping.output,
      });
      continue;
    }

    merged.push({
      kind: "expr",
      expr: innerMapping.expr,
      output: mapping.output,
    });
  }

  return merged;
}

function normalizeScanForProvider(node: RelScanNode, schema: SchemaDefinition): RelScanNode {
  const binding =
    getNormalizedTableBinding(schema, node.table) ??
    (node.entity ? createPhysicalBindingFromEntity(node.entity) : undefined);
  if (!binding || binding.kind !== "physical") {
    return node;
  }
  const table =
    schema.tables[node.table] ??
    (node.entity ? createTableDefinitionFromEntity(node.entity) : undefined);

  const mapColumn = (column: string): string => resolveNormalizedColumnSource(binding, column);
  const mapClause = (clause: ScanFilterClause): ScanFilterClause => {
    const mapped = mapEnumFilterForProvider(table?.columns[clause.column], clause);
    return {
      ...mapped,
      column: mapColumn(mapped.column),
    };
  };

  return {
    ...node,
    table: binding.entity,
    ...(node.entity ? { entity: node.entity } : {}),
    select: node.select.map(mapColumn),
    ...(node.where
      ? {
          where: node.where.map(mapClause),
        }
      : {}),
    ...(node.orderBy
      ? {
          orderBy: node.orderBy.map((term) => ({
            ...term,
            column: mapColumn(term.column),
          })),
        }
      : {}),
  };
}

function mapEnumFilterForProvider(definition: unknown, clause: ScanFilterClause): ScanFilterClause {
  if (!definition || typeof definition === "string") {
    return clause;
  }

  const column = definition as ColumnDefinition;
  if (!column.enumMap || Object.keys(column.enumMap).length === 0) {
    return clause;
  }

  const mapFacadeValueToSource = (value: unknown): string[] => {
    if (typeof value !== "string") {
      return [];
    }
    const out: string[] = [];
    for (const [sourceValue, facadeValue] of Object.entries(column.enumMap ?? {})) {
      if (facadeValue === value) {
        out.push(sourceValue);
      }
    }
    return out;
  };

  if (clause.op === "eq") {
    const mappedValues = mapFacadeValueToSource(clause.value);
    if (mappedValues.length === 0) {
      throw new Error(
        `No upstream enum mapping for value ${JSON.stringify(clause.value)} on ${clause.column}.`,
      );
    }
    if (mappedValues.length === 1) {
      return {
        ...clause,
        value: mappedValues[0],
      };
    }
    return {
      op: "in",
      column: clause.column,
      values: mappedValues,
    };
  }

  if (clause.op === "in") {
    const mapped = [...new Set(clause.values.flatMap((value) => mapFacadeValueToSource(value)))];
    if (mapped.length === 0) {
      throw new Error(`No upstream enum mappings for IN predicate on ${clause.column}.`);
    }
    return {
      ...clause,
      values: mapped,
    };
  }

  return clause;
}

function collectAliasToSourceMappings(node: RelNode, schema: SchemaDefinition): AliasToSourceMap {
  const mappings: AliasToSourceMap = new Map();

  const visit = (current: RelNode): void => {
    switch (current.kind) {
      case "scan": {
        const binding =
          getNormalizedTableBinding(schema, current.table) ??
          (current.entity ? createPhysicalBindingFromEntity(current.entity) : undefined);
        if (binding?.kind !== "physical") {
          return;
        }
        const alias = current.alias ?? current.table;
        mappings.set(alias, getNormalizedColumnSourceMap(binding));
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
        for (const cte of current.ctes) {
          visit(cte.query);
        }
        visit(current.body);
        return;
      case "sql":
        return;
    }
  };

  visit(node);
  return mappings;
}

function mapColumnRefForAlias(ref: RelColumnRef, aliasToSource: AliasToSourceMap): RelColumnRef {
  const alias = ref.alias ?? ref.table;
  if (alias) {
    const mapping = aliasToSource.get(alias);
    if (!mapping) {
      return ref;
    }
    return {
      ...ref,
      column: mapping[ref.column] ?? ref.column,
    };
  }

  return {
    ...ref,
    column: mapColumnNameForAlias(ref.column, aliasToSource),
  };
}

function mapColumnNameForAlias(column: string, aliasToSource: AliasToSourceMap): string {
  const idx = column.lastIndexOf(".");
  if (idx > 0) {
    const alias = column.slice(0, idx);
    const name = column.slice(idx + 1);
    const mapping = aliasToSource.get(alias);
    if (!mapping) {
      return column;
    }
    const mapped = mapping[name] ?? name;
    return `${alias}.${mapped}`;
  }

  let mappedColumn: string | null = null;
  for (const mapping of aliasToSource.values()) {
    if (!(column in mapping)) {
      continue;
    }
    const candidate = mapping[column] ?? column;
    if (mappedColumn && mappedColumn !== candidate) {
      return column;
    }
    mappedColumn = candidate;
  }

  return mappedColumn ?? column;
}

function resolveSingleProvider(
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

function assignConventions(
  node: RelNode,
  schema: SchemaDefinition,
  cteNames: Set<string> = new Set<string>(),
): RelNode {
  switch (node.kind) {
    case "scan": {
      if (cteNames.has(node.table)) {
        return {
          ...node,
          convention: "local",
        };
      }
      if (!schema.tables[node.table] && !node.entity) {
        return {
          ...node,
          convention: "local",
        };
      }
      const normalized = getNormalizedTableBinding(schema, node.table);
      if (normalized?.kind === "view") {
        return {
          ...node,
          convention: "local",
        };
      }
      const provider = node.entity?.provider ?? resolveTableProvider(schema, node.table);
      return {
        ...node,
        convention: `provider:${provider}`,
      };
    }
    case "filter":
    case "project":
    case "aggregate":
    case "window":
    case "sort":
    case "limit_offset": {
      const input = assignConventions(node.input, schema, cteNames);
      const provider = resolveSingleProvider(input, schema, cteNames);
      return {
        ...node,
        input,
        convention: provider ? (`provider:${provider}` as const) : "local",
      };
    }
    case "join":
    case "set_op": {
      const left = assignConventions(node.left, schema, cteNames);
      const right = assignConventions(node.right, schema, cteNames);
      const provider = resolveSingleProvider(
        {
          ...node,
          left,
          right,
        } as RelNode,
        schema,
        cteNames,
      );
      return {
        ...node,
        left,
        right,
        convention: provider ? (`provider:${provider}` as const) : "local",
      };
    }
    case "with": {
      const nextCteNames = new Set(cteNames);
      for (const cte of node.ctes) {
        nextCteNames.add(cte.name);
      }
      const ctes = node.ctes.map((cte) => ({
        ...cte,
        query: assignConventions(cte.query, schema, nextCteNames),
      }));
      const body = assignConventions(node.body, schema, nextCteNames);
      const provider = resolveSingleProvider(body, schema, nextCteNames);
      return {
        ...node,
        ctes,
        body,
        convention: provider ? (`provider:${provider}` as const) : "local",
      };
    }
    case "sql": {
      const provider = resolveSingleProvider(node, schema, cteNames);
      return {
        ...node,
        convention: provider ? (`provider:${provider}` as const) : "local",
      };
    }
  }
}

function tryLowerStructuredSelect(
  ast: SelectAst,
  schema: SchemaDefinition,
  cteNames: Set<string>,
): RelNode | null {
  const scopedCteNames = new Set(cteNames);
  const loweredCtes: Array<{ name: string; query: RelNode }> = [];
  const withClauses = Array.isArray(ast.with) ? ast.with : [];

  for (const clause of withClauses) {
    const rawName = (clause as { name?: unknown }).name;
    const cteName =
      typeof rawName === "string"
        ? rawName
        : rawName &&
            typeof rawName === "object" &&
            typeof (rawName as { value?: unknown }).value === "string"
          ? (rawName as { value: string }).value
          : null;
    if (!cteName) {
      return null;
    }
    scopedCteNames.add(cteName);
  }

  for (const clause of withClauses) {
    const rawName = (clause as { name?: unknown }).name;
    const cteName =
      typeof rawName === "string"
        ? rawName
        : rawName &&
            typeof rawName === "object" &&
            typeof (rawName as { value?: unknown }).value === "string"
          ? (rawName as { value: string }).value
          : null;
    const cteAst = (clause as { stmt?: { ast?: unknown } }).stmt?.ast;
    if (!cteName || !cteAst || typeof cteAst !== "object") {
      return null;
    }
    const loweredCte = tryLowerStructuredSelect(cteAst as SelectAst, schema, scopedCteNames);
    if (!loweredCte) {
      return null;
    }
    loweredCtes.push({
      name: cteName,
      query: loweredCte,
    });
  }

  const hasSetOp = typeof ast.set_op === "string" && !!ast._next;
  if (!hasSetOp) {
    const { with: _ignoredWith, ...withoutWith } = ast;
    const simple = tryLowerSimpleSelect(withoutWith as SelectAst, schema, scopedCteNames);
    if (!simple) {
      return null;
    }

    if (loweredCtes.length === 0) {
      return simple;
    }

    return {
      id: nextRelId("with"),
      kind: "with",
      convention: "local",
      ctes: loweredCtes,
      body: simple,
      output: simple.output,
    };
  }

  const { with: _ignoredWith, ...withoutWith } = ast;
  let currentAst: SelectAst = withoutWith as SelectAst;
  const { set_op: _ignoredSetOp, _next: _ignoredNext, ...currentBaseAst } = currentAst;
  let currentNode = tryLowerSimpleSelect(currentBaseAst as SelectAst, schema, scopedCteNames);
  if (!currentNode) {
    return null;
  }

  while (typeof currentAst.set_op === "string" && currentAst._next) {
    const op = parseSetOp(currentAst.set_op);
    if (!op) {
      return null;
    }

    const {
      with: _ignoredRightWith,
      set_op: _ignoredRightSetOp,
      _next: _ignoredRightNext,
      ...rightBaseAst
    } = currentAst._next;
    const rightBase = tryLowerSimpleSelect(rightBaseAst as SelectAst, schema, scopedCteNames);
    if (!rightBase) {
      return null;
    }

    currentNode = {
      id: nextRelId("set_op"),
      kind: "set_op",
      convention: "local",
      op,
      left: currentNode,
      right: rightBase,
      output: currentNode.output,
    };

    currentAst = currentAst._next;
  }

  if (loweredCtes.length === 0) {
    return currentNode;
  }

  return {
    id: nextRelId("with"),
    kind: "with",
    convention: "local",
    ctes: loweredCtes,
    body: currentNode,
    output: currentNode.output,
  };
}

function parseSetOp(raw: string): Extract<RelNode, { kind: "set_op" }>["op"] | null {
  const normalized = raw.trim().toUpperCase();
  switch (normalized) {
    case "UNION ALL":
      return "union_all";
    case "UNION":
      return "union";
    case "INTERSECT":
      return "intersect";
    case "EXCEPT":
      return "except";
    default:
      return null;
  }
}

function tryLowerSimpleSelect(
  ast: SelectAst,
  schema: SchemaDefinition,
  cteNames: Set<string>,
): RelNode | null {
  if (ast.type !== "select") {
    return null;
  }

  if (ast.with || ast.set_op || ast._next) {
    return null;
  }

  const from = Array.isArray(ast.from) ? ast.from : ast.from ? [ast.from] : [];
  if (from.length === 0) {
    return null;
  }

  if (
    from.some(
      (entry) => typeof (entry as FromEntryAst).table !== "string" || (entry as FromEntryAst).stmt,
    )
  ) {
    return null;
  }

  const bindings: Binding[] = from.map((entry, index) => {
    const table = (entry as FromEntryAst).table;
    if (typeof table !== "string" || (!schema.tables[table] && !cteNames.has(table))) {
      throw new Error(`Unknown table: ${String(table)}`);
    }

    const alias =
      typeof (entry as FromEntryAst).as === "string" && (entry as FromEntryAst).as
        ? ((entry as FromEntryAst).as as string)
        : table;

    return {
      table,
      alias,
      index,
    };
  });

  const aliasToBinding = new Map(bindings.map((binding) => [binding.alias, binding]));

  const joins = parseJoins(from, bindings, aliasToBinding);
  if (joins == null) {
    return null;
  }

  const whereFilters = parseWhereFilters(ast.where, bindings, aliasToBinding, schema, cteNames);
  if (!whereFilters) {
    return null;
  }
  validateEnumLiteralFilters(whereFilters.literals, bindings, schema);

  const distinctMode = ast.distinct === "DISTINCT";
  const aggregateMode =
    getGroupByColumns(ast.groupby).length > 0 ||
    hasAggregateProjection(ast.columns) ||
    distinctMode;

  const projections = aggregateMode
    ? null
    : parseProjection(
        ast.columns,
        bindings,
        aliasToBinding,
        parseNamedWindowSpecifications(ast.window),
        schema,
        cteNames,
      );
  if (!aggregateMode && projections == null) {
    return null;
  }

  const aggregateProjections = aggregateMode
    ? parseAggregateProjections(ast.columns, bindings, aliasToBinding, schema, cteNames)
    : null;
  if (aggregateMode && aggregateProjections == null) {
    return null;
  }

  const safeAggregateProjections = aggregateMode ? (aggregateProjections ?? []) : [];
  const safeProjections = aggregateMode ? [] : (projections ?? []);
  const groupByTerms = aggregateMode ? parseGroupBy(ast.groupby, bindings, aliasToBinding) : [];
  if (aggregateMode && groupByTerms == null) {
    return null;
  }
  const windowFunctions = safeProjections
    .filter((projection): projection is SelectWindowProjection => projection.kind === "window")
    .map((projection) => projection.function);
  const aggregateGroupByResolution = aggregateMode
    ? resolveAggregateGroupBy(groupByTerms ?? [], safeAggregateProjections)
    : { groupBy: [], materializations: [] };
  let effectiveGroupBy = aggregateGroupByResolution.groupBy;

  if (distinctMode && effectiveGroupBy.length === 0) {
    const distinctGroupBy: RelColumnRef[] = [];
    for (const projection of safeAggregateProjections) {
      if (projection.kind !== "group" || !projection.source) {
        // Defer DISTINCT+aggregate/function cases to the generic fallback path.
        return null;
      }
      distinctGroupBy.push(projection.source);
    }

    if (distinctGroupBy.length === 0) {
      return null;
    }
    effectiveGroupBy = distinctGroupBy;
  }

  if (
    aggregateMode &&
    !validateAggregateProjectionGroupBy(safeAggregateProjections, effectiveGroupBy)
  ) {
    return null;
  }

  const aggregateMetrics = safeAggregateProjections
    .filter(
      (projection): projection is ParsedAggregateMetricProjection => projection.kind === "metric",
    )
    .map((projection) => projection.metric);

  const aggregateMetricAliases = new Map<string, string>(
    aggregateMetrics.map((metric) => [getAggregateMetricSignature(metric), metric.as]),
  );
  const hiddenHavingMetrics: Extract<RelNode, { kind: "aggregate" }>["metrics"] = [];
  const havingExpr =
    aggregateMode && ast.having
      ? lowerHavingExpr(
          ast.having,
          bindings,
          aliasToBinding,
          aggregateMetricAliases,
          hiddenHavingMetrics,
        )
      : null;
  if (ast.having && (!aggregateMode || !havingExpr)) {
    return null;
  }
  const allAggregateMetrics = [...aggregateMetrics, ...hiddenHavingMetrics];

  const orderByTerms = parseOrderBy(
    ast.orderby,
    bindings,
    aliasToBinding,
    new Set(
      (aggregateMode ? safeAggregateProjections : safeProjections).map(
        (projection) => projection.output,
      ),
    ),
  );
  if (orderByTerms == null) {
    return null;
  }
  const { orderBy, materializations: orderByMaterializations } = aggregateMode
    ? {
        orderBy: resolveAggregateOrderBy(orderByTerms, safeAggregateProjections),
        materializations: [] as RelProjectExprMapping[],
      }
    : resolveNonAggregateOrderBy(orderByTerms, safeProjections);

  const rootBinding = bindings[0];
  if (!rootBinding) {
    return null;
  }
  const pushableWhereAliases = getPushableWhereAliases(rootBinding.alias, joins);
  const pushableLiteralFilters = whereFilters.literals.filter((filter) =>
    pushableWhereAliases.has(filter.alias),
  );
  const residualExpr = combineAndExprs([
    ...whereFilters.literals
      .filter((filter) => !pushableWhereAliases.has(filter.alias))
      .map(literalFilterToRelExpr),
    ...(whereFilters.residualExpr ? [whereFilters.residualExpr] : []),
  ]);

  const { limit, offset } = parseLimitAndOffset(ast.limit);

  const columnsByAlias = new Map<string, Set<string>>();
  for (const binding of bindings) {
    columnsByAlias.set(binding.alias, new Set<string>());
  }

  if (aggregateMode) {
    for (const projection of safeAggregateProjections) {
      if (projection.kind !== "group") {
        continue;
      }

      if (projection.source?.alias) {
        columnsByAlias.get(projection.source.alias)?.add(projection.source.column);
      }
      if (projection.expr) {
        for (const ref of collectRelExprRefs(projection.expr)) {
          if (ref.alias) {
            columnsByAlias.get(ref.alias)?.add(ref.column);
          }
        }
      }
    }

    for (const metric of allAggregateMetrics) {
      if (!metric.column) {
        continue;
      }
      const alias = metric.column.alias ?? metric.column.table;
      if (alias) {
        columnsByAlias.get(alias)?.add(metric.column.column);
      }
    }

    for (const ref of effectiveGroupBy) {
      if (ref.alias) {
        columnsByAlias.get(ref.alias)?.add(ref.column);
      }
    }
  } else {
    for (const projection of safeProjections) {
      if (projection.kind === "column") {
        if (projection.source.alias) {
          columnsByAlias.get(projection.source.alias)?.add(projection.source.column);
        }
        continue;
      }
      if (projection.kind === "expr") {
        for (const ref of collectRelExprRefs(projection.expr)) {
          if (ref.alias) {
            columnsByAlias.get(ref.alias)?.add(ref.column);
          }
        }
        continue;
      }
      for (const partition of projection.function.partitionBy) {
        if (partition.alias) {
          columnsByAlias.get(partition.alias)?.add(partition.column);
        }
      }
      if ("column" in projection.function && projection.function.column?.alias) {
        columnsByAlias
          .get(projection.function.column.alias)
          ?.add(projection.function.column.column);
      }
      for (const orderTerm of projection.function.orderBy) {
        if (orderTerm.source.alias) {
          columnsByAlias.get(orderTerm.source.alias)?.add(orderTerm.source.column);
        }
      }
    }
  }

  for (const join of joins) {
    columnsByAlias.get(join.leftAlias)?.add(join.leftColumn);
    columnsByAlias.get(join.rightAlias)?.add(join.rightColumn);
  }

  for (const filter of pushableLiteralFilters) {
    columnsByAlias.get(filter.alias)?.add(filter.clause.column);
  }
  for (const filter of whereFilters.inSubqueries) {
    columnsByAlias.get(filter.alias)?.add(filter.column);
  }
  if (residualExpr) {
    for (const ref of collectRelExprRefs(residualExpr)) {
      if (ref.alias) {
        columnsByAlias.get(ref.alias)?.add(ref.column);
      }
    }
  }

  for (const term of orderBy) {
    if (term.source.alias) {
      columnsByAlias.get(term.source.alias)?.add(term.source.column);
    }
  }

  for (const binding of bindings) {
    const columns = columnsByAlias.get(binding.alias);
    if (!columns || columns.size > 0) {
      continue;
    }

    if (schema.tables[binding.table]) {
      const schemaColumns = Object.keys(schema.tables[binding.table]?.columns ?? {});
      const first = schemaColumns[0];
      if (first) {
        columns.add(first);
      }
    }
  }

  const filtersByAlias = new Map<string, ScanFilterClause[]>();
  for (const filter of pushableLiteralFilters) {
    const current = filtersByAlias.get(filter.alias) ?? [];
    current.push(filter.clause);
    filtersByAlias.set(filter.alias, current);
  }

  const scansByAlias = new Map<string, RelScanNode>();
  for (const binding of bindings) {
    const select = [...(columnsByAlias.get(binding.alias) ?? new Set<string>())];
    const scanWhere = filtersByAlias.get(binding.alias);

    scansByAlias.set(binding.alias, {
      id: nextRelId("scan"),
      kind: "scan",
      convention: "local",
      table: binding.table,
      alias: binding.alias,
      select,
      ...(scanWhere && scanWhere.length > 0 ? { where: scanWhere } : {}),
      output: select.map((column) => ({
        name: `${binding.alias}.${column}`,
      })),
    });
  }

  const root = rootBinding;
  if (!root) {
    return null;
  }

  let current: RelNode = scansByAlias.get(root.alias)!;

  for (const join of joins) {
    const right = scansByAlias.get(join.alias);
    if (!right) {
      return null;
    }

    const joinLeftOnCurrent = appearsInRel(current, join.leftAlias);
    const leftKey: RelColumnRef = joinLeftOnCurrent
      ? {
          alias: join.leftAlias,
          column: join.leftColumn,
        }
      : {
          alias: join.rightAlias,
          column: join.rightColumn,
        };

    const rightKey: RelColumnRef = joinLeftOnCurrent
      ? {
          alias: join.rightAlias,
          column: join.rightColumn,
        }
      : {
          alias: join.leftAlias,
          column: join.leftColumn,
        };

    current = {
      id: nextRelId("join"),
      kind: "join",
      convention: "local",
      joinType: join.joinType,
      left: current,
      right,
      leftKey,
      rightKey,
      output: [...current.output, ...right.output],
    };
  }

  for (const inFilter of whereFilters.inSubqueries) {
    const outerAliases = new Set(bindings.map((binding) => binding.alias));
    if (isCorrelatedSubquery(inFilter.subquery, outerAliases)) {
      return null;
    }

    const subqueryRel = tryLowerStructuredSelect(inFilter.subquery, schema, cteNames);
    if (!subqueryRel || subqueryRel.output.length !== 1) {
      return null;
    }
    const rightOutput = subqueryRel.output[0]?.name;
    if (!rightOutput) {
      return null;
    }

    current = {
      id: nextRelId("join"),
      kind: "join",
      convention: "local",
      joinType: "semi",
      left: current,
      right: subqueryRel,
      leftKey: {
        alias: inFilter.alias,
        column: inFilter.column,
      },
      rightKey: parseRelColumnRef(rightOutput),
      output: current.output,
    };
  }

  if (residualExpr) {
    current = {
      id: nextRelId("filter"),
      kind: "filter",
      convention: "local",
      input: current,
      expr: residualExpr,
      output: current.output,
    };
  }

  if (aggregateMode && aggregateGroupByResolution.materializations.length > 0) {
    current = appendProjectExpressions(current, aggregateGroupByResolution.materializations);
  }

  if (aggregateMode) {
    current = {
      id: nextRelId("aggregate"),
      kind: "aggregate",
      convention: "local",
      input: current,
      groupBy: effectiveGroupBy,
      metrics: allAggregateMetrics,
      output: [
        ...effectiveGroupBy.map((ref) => ({
          name: ref.column,
        })),
        ...allAggregateMetrics.map((metric) => ({
          name: metric.as,
        })),
      ],
    };
  }

  if (havingExpr) {
    current = {
      id: nextRelId("filter"),
      kind: "filter",
      convention: "local",
      input: current,
      expr: havingExpr,
      output: current.output,
    };
  }

  if (!aggregateMode && windowFunctions.length > 0) {
    current = {
      id: nextRelId("window"),
      kind: "window",
      convention: "local",
      input: current,
      functions: windowFunctions,
      output: [...current.output, ...windowFunctions.map((fn) => ({ name: fn.as }))],
    };
  }

  if (!aggregateMode && orderByMaterializations.length > 0) {
    current = appendProjectExpressions(current, orderByMaterializations);
  }

  if (orderBy.length > 0) {
    current = {
      id: nextRelId("sort"),
      kind: "sort",
      convention: "local",
      input: current,
      orderBy: orderBy.map((term) => ({
        source: term.source.alias
          ? {
              alias: term.source.alias,
              column: term.source.column,
            }
          : {
              column: term.source.column,
            },
        direction: term.direction,
      })),
      output: current.output,
    };
  }

  if (limit != null || offset != null) {
    current = {
      id: nextRelId("limit_offset"),
      kind: "limit_offset",
      convention: "local",
      input: current,
      ...(limit != null ? { limit } : {}),
      ...(offset != null ? { offset } : {}),
      output: current.output,
    };
  }

  const projectNode: RelProjectNode = {
    id: nextRelId("project"),
    kind: "project",
    convention: "local",
    input: current,
    columns: aggregateMode
      ? safeAggregateProjections.map((projection) =>
          projection.kind === "group" && projection.source
            ? {
                kind: "column" as const,
                source: {
                  column: projection.source.column,
                },
                output: projection.output,
              }
            : projection.kind === "metric"
              ? {
                  kind: "column" as const,
                  source: {
                    column: projection.metric.as,
                  },
                  output: projection.output,
                }
              : {
                  kind: "expr" as const,
                  expr: projection.expr!,
                  output: projection.output,
                },
        )
      : safeProjections.map((projection) => ({
          ...(projection.kind === "expr" && !projection.source
            ? {
                kind: "expr" as const,
                expr: projection.expr,
              }
            : {
                kind: "column" as const,
                source:
                  projection.kind === "column"
                    ? {
                        ...(projection.source.alias ? { alias: projection.source.alias } : {}),
                        column: projection.source.column,
                      }
                    : projection.kind === "expr"
                      ? {
                          column: projection.source!.column,
                        }
                      : {
                          column: projection.function.as,
                        },
              }),
          output: projection.output,
        })),
    output: (aggregateMode ? safeAggregateProjections : safeProjections).map((projection) => ({
      name: projection.output,
    })),
  };

  return projectNode;
}

function parseJoins(
  from: FromEntryAst[],
  bindings: Binding[],
  aliasToBinding: Map<string, Binding>,
): ParsedJoin[] | null {
  const joins: ParsedJoin[] = [];

  for (let index = 1; index < from.length; index += 1) {
    const entry = from[index];
    if (!entry) {
      return null;
    }

    const joinRaw = typeof entry.join === "string" ? entry.join.toUpperCase() : "";
    const joinType =
      joinRaw === "JOIN" || joinRaw === "INNER JOIN"
        ? "inner"
        : joinRaw === "LEFT JOIN" || joinRaw === "LEFT OUTER JOIN"
          ? "left"
          : joinRaw === "RIGHT JOIN" || joinRaw === "RIGHT OUTER JOIN"
            ? "right"
            : joinRaw === "FULL JOIN" || joinRaw === "FULL OUTER JOIN"
              ? "full"
              : null;

    if (!joinType) {
      return null;
    }

    const binding = bindings[index];
    if (!binding || !entry.on) {
      return null;
    }

    const condition = parseJoinCondition(entry.on, bindings, aliasToBinding);
    if (!condition) {
      return null;
    }

    joins.push({
      alias: binding.alias,
      joinType,
      leftAlias: condition.leftAlias,
      leftColumn: condition.leftColumn,
      rightAlias: condition.rightAlias,
      rightColumn: condition.rightColumn,
    });
  }

  return joins;
}

function parseJoinCondition(
  raw: unknown,
  bindings: Binding[],
  aliasToBinding: Map<string, Binding>,
): {
  leftAlias: string;
  leftColumn: string;
  rightAlias: string;
  rightColumn: string;
} | null {
  const expr = raw as {
    type?: unknown;
    operator?: unknown;
    left?: unknown;
    right?: unknown;
  };

  if (expr?.type !== "binary_expr" || expr.operator !== "=") {
    return null;
  }

  const left = resolveColumnRef(expr.left, bindings, aliasToBinding);
  const right = resolveColumnRef(expr.right, bindings, aliasToBinding);
  if (!left || !right) {
    return null;
  }

  return {
    leftAlias: left.alias,
    leftColumn: left.column,
    rightAlias: right.alias,
    rightColumn: right.column,
  };
}

function parseProjection(
  rawColumns: unknown,
  bindings: Binding[],
  aliasToBinding: Map<string, Binding>,
  windowDefinitions: Map<string, WindowSpecificationAst>,
  schema: SchemaDefinition,
  cteNames: Set<string>,
): SelectProjection[] | null {
  if (rawColumns === "*") {
    return null;
  }

  const columns = Array.isArray(rawColumns) ? (rawColumns as SelectColumnAst[]) : [];
  if (columns.length === 0) {
    return null;
  }

  const out: SelectProjection[] = [];

  for (const entry of columns) {
    const column = resolveColumnRef(entry.expr, bindings, aliasToBinding);
    if (column) {
      out.push({
        kind: "column",
        source: {
          alias: column.alias,
          column: column.column,
        },
        output: typeof entry.as === "string" && entry.as.length > 0 ? entry.as : column.column,
      });
      continue;
    }

    const windowProjection = parseWindowProjection(
      entry,
      bindings,
      aliasToBinding,
      windowDefinitions,
    );
    if (windowProjection) {
      out.push(windowProjection);
      continue;
    }

    const expr = lowerSqlAstToRelExpr(entry.expr, bindings, aliasToBinding, schema, cteNames);
    if (!expr) {
      return null;
    }

    out.push({
      kind: "expr",
      expr,
      output: typeof entry.as === "string" && entry.as.length > 0 ? entry.as : "expr",
    });
  }

  return out;
}

function parseWindowProjection(
  entry: SelectColumnAst,
  bindings: Binding[],
  aliasToBinding: Map<string, Binding>,
  windowDefinitions: Map<string, WindowSpecificationAst>,
): SelectWindowProjection | null {
  const expr = entry.expr as {
    type?: unknown;
    name?: unknown;
    over?: unknown;
    args?: unknown;
  };
  if (expr.type !== "function" && expr.type !== "aggr_func") {
    return null;
  }

  const over = parseWindowOver(expr.over, windowDefinitions);
  if (!over) {
    return null;
  }

  const name = readWindowFunctionName(expr);
  if (!name) {
    return null;
  }

  const partitionBy: RelColumnRef[] = [];
  for (const term of over.partitionby ?? []) {
    const resolved = resolveColumnRef(term.expr, bindings, aliasToBinding);
    if (!resolved) {
      return null;
    }
    partitionBy.push({
      alias: resolved.alias,
      column: resolved.column,
    });
  }

  const orderBy: Array<{ source: RelColumnRef; direction: "asc" | "desc" }> = [];
  for (const term of over.orderby ?? []) {
    const resolved = resolveColumnRef(term.expr, bindings, aliasToBinding);
    if (!resolved) {
      return null;
    }
    orderBy.push({
      source: {
        alias: resolved.alias,
        column: resolved.column,
      },
      direction: term.type === "DESC" ? "desc" : "asc",
    });
  }

  const output = typeof entry.as === "string" && entry.as.length > 0 ? entry.as : name;

  if (name === "dense_rank" || name === "rank" || name === "row_number") {
    if (!supportsRankWindowArgs(expr.args)) {
      return null;
    }

    return {
      kind: "window",
      output,
      function: {
        fn: name,
        as: output,
        partitionBy,
        orderBy,
      },
    };
  }

  if (expr.type !== "aggr_func") {
    return null;
  }

  const metric = parseAggregateMetric(expr, output, bindings, aliasToBinding);
  if (!metric) {
    return null;
  }

  return {
    kind: "window",
    output,
    function: {
      fn: name,
      as: output,
      partitionBy,
      ...(metric.column ? { column: metric.column } : {}),
      ...(metric.distinct ? { distinct: true } : {}),
      orderBy,
    },
  };
}

function lowerSqlAstToRelExpr(
  raw: unknown,
  bindings: Binding[],
  aliasToBinding: Map<string, Binding>,
  schema: SchemaDefinition,
  cteNames: Set<string>,
): RelExpr | null {
  if (!raw || typeof raw !== "object") {
    return null;
  }

  const expr = raw as {
    type?: unknown;
    value?: unknown;
    table?: unknown;
    column?: unknown;
    operator?: unknown;
    left?: unknown;
    right?: unknown;
    name?: unknown;
    args?: { value?: unknown };
    ast?: unknown;
  };

  if ("ast" in expr) {
    return lowerScalarSubqueryExpr(raw, bindings, schema, cteNames);
  }

  switch (expr.type) {
    case "string":
      return { kind: "literal", value: typeof expr.value === "string" ? expr.value : "" };
    case "number":
      return typeof expr.value === "number" ? { kind: "literal", value: expr.value } : null;
    case "bool":
      return typeof expr.value === "boolean" ? { kind: "literal", value: expr.value } : null;
    case "null":
      return { kind: "literal", value: null };
    case "column_ref": {
      const resolved = resolveColumnRef(expr, bindings, aliasToBinding);
      if (!resolved) {
        return null;
      }
      return {
        kind: "column",
        ref: {
          alias: resolved.alias,
          column: resolved.column,
        },
      };
    }
    case "binary_expr":
      return lowerBinaryExprToRelExpr(expr, bindings, aliasToBinding, schema, cteNames);
    case "function":
      return lowerFunctionExprToRelExpr(expr, bindings, aliasToBinding, schema, cteNames);
    default:
      return null;
  }
}

function lowerBinaryExprToRelExpr(
  expr: {
    operator?: unknown;
    left?: unknown;
    right?: unknown;
  },
  bindings: Binding[],
  aliasToBinding: Map<string, Binding>,
  schema: SchemaDefinition,
  cteNames: Set<string>,
): RelExpr | null {
  const operator = typeof expr.operator === "string" ? expr.operator.toUpperCase() : null;
  if (!operator) {
    return null;
  }

  if (operator === "BETWEEN") {
    const range = expr.right as { type?: unknown; value?: unknown } | undefined;
    if (range?.type !== "expr_list" || !Array.isArray(range.value) || range.value.length !== 2) {
      return null;
    }
    const left = lowerSqlAstToRelExpr(expr.left, bindings, aliasToBinding, schema, cteNames);
    const low = lowerSqlAstToRelExpr(range.value[0], bindings, aliasToBinding, schema, cteNames);
    const high = lowerSqlAstToRelExpr(range.value[1], bindings, aliasToBinding, schema, cteNames);
    if (!left || !low || !high) {
      return null;
    }
    return {
      kind: "function",
      name: "between",
      args: [left, low, high],
    };
  }

  if (operator === "IN" || operator === "NOT IN") {
    const left = lowerSqlAstToRelExpr(expr.left, bindings, aliasToBinding, schema, cteNames);
    const values = parseExprListToRelExprArgs(
      expr.right,
      bindings,
      aliasToBinding,
      schema,
      cteNames,
    );
    if (!left || !values) {
      return null;
    }
    return {
      kind: "function",
      name: operator === "NOT IN" ? "not_in" : "in",
      args: [left, ...values],
    };
  }

  if (operator === "IS" || operator === "IS NOT") {
    const left = lowerSqlAstToRelExpr(expr.left, bindings, aliasToBinding, schema, cteNames);
    const rightLiteral = parseLiteral(expr.right);
    if (!left || rightLiteral !== null) {
      return null;
    }
    return {
      kind: "function",
      name: operator === "IS NOT" ? "is_not_null" : "is_null",
      args: [left],
    };
  }

  const left = lowerSqlAstToRelExpr(expr.left, bindings, aliasToBinding, schema, cteNames);
  const right = lowerSqlAstToRelExpr(expr.right, bindings, aliasToBinding, schema, cteNames);
  if (!left || !right) {
    return null;
  }

  const mapped = mapBinaryOperatorToRelFunction(operator);
  if (!mapped) {
    return null;
  }

  return {
    kind: "function",
    name: mapped,
    args: [left, right],
  };
}

function lowerFunctionExprToRelExpr(
  expr: {
    name?: unknown;
    args?: { value?: unknown };
    over?: unknown;
  },
  bindings: Binding[],
  aliasToBinding: Map<string, Binding>,
  schema: SchemaDefinition,
  cteNames: Set<string>,
): RelExpr | null {
  if (expr.over) {
    return null;
  }

  const rawName = (expr.name as { name?: Array<{ value?: unknown }> } | undefined)?.name?.[0]
    ?.value;
  if (typeof rawName !== "string") {
    return null;
  }

  const normalized = rawName.toLowerCase();
  if (normalized === "exists") {
    const values = Array.isArray(expr.args?.value) ? expr.args?.value : [expr.args?.value];
    if (values.length !== 1) {
      return null;
    }
    return lowerExistsSubqueryExpr(values[0], bindings, schema, cteNames);
  }

  const args = parseFunctionArgsToRelExpr(
    expr.args?.value,
    bindings,
    aliasToBinding,
    schema,
    cteNames,
  );
  if (!args) {
    return null;
  }

  if (normalized === "not") {
    return args.length === 1
      ? {
          kind: "function",
          name: "not",
          args,
        }
      : null;
  }

  if (
    normalized !== "lower" &&
    normalized !== "upper" &&
    normalized !== "trim" &&
    normalized !== "length" &&
    normalized !== "substr" &&
    normalized !== "substring" &&
    normalized !== "coalesce" &&
    normalized !== "nullif" &&
    normalized !== "abs" &&
    normalized !== "round" &&
    normalized !== "cast" &&
    normalized !== "case"
  ) {
    return null;
  }

  return {
    kind: "function",
    name: normalized === "substring" ? "substr" : normalized,
    args,
  };
}

function parseFunctionArgsToRelExpr(
  raw: unknown,
  bindings: Binding[],
  aliasToBinding: Map<string, Binding>,
  schema: SchemaDefinition,
  cteNames: Set<string>,
): RelExpr[] | null {
  if (raw == null) {
    return [];
  }
  const values = Array.isArray(raw) ? raw : [raw];
  const args: RelExpr[] = [];
  for (const value of values) {
    const arg = lowerSqlAstToRelExpr(value, bindings, aliasToBinding, schema, cteNames);
    if (!arg) {
      return null;
    }
    args.push(arg);
  }
  return args;
}

function parseExprListToRelExprArgs(
  raw: unknown,
  bindings: Binding[],
  aliasToBinding: Map<string, Binding>,
  schema: SchemaDefinition,
  cteNames: Set<string>,
): RelExpr[] | null {
  const expr = raw as { type?: unknown; value?: unknown };
  if (expr?.type !== "expr_list" || !Array.isArray(expr.value)) {
    return null;
  }
  return parseFunctionArgsToRelExpr(expr.value, bindings, aliasToBinding, schema, cteNames);
}

function lowerExistsSubqueryExpr(
  raw: unknown,
  bindings: Binding[],
  schema: SchemaDefinition,
  cteNames: Set<string>,
): RelExpr | null {
  const subquery = parseSubqueryAst(raw);
  if (!subquery) {
    return null;
  }

  const outerAliases = new Set(bindings.map((binding) => binding.alias));
  if (isCorrelatedSubquery(subquery, outerAliases)) {
    return null;
  }

  const rel = tryLowerStructuredSelect(subquery, schema, cteNames);
  if (!rel) {
    return null;
  }

  return {
    kind: "subquery",
    id: nextRelId("subquery_expr"),
    mode: "exists",
    rel,
  };
}

function lowerScalarSubqueryExpr(
  raw: unknown,
  bindings: Binding[],
  schema: SchemaDefinition,
  cteNames: Set<string>,
): RelExpr | null {
  const subquery = parseSubqueryAst(raw);
  if (!subquery) {
    return null;
  }

  const outerAliases = new Set(bindings.map((binding) => binding.alias));
  if (isCorrelatedSubquery(subquery, outerAliases)) {
    return null;
  }

  const rel = tryLowerStructuredSelect(subquery, schema, cteNames);
  if (!rel || rel.output.length !== 1) {
    return null;
  }

  const outputColumn = rel.output[0]?.name;
  if (!outputColumn) {
    return null;
  }

  return {
    kind: "subquery",
    id: nextRelId("subquery_expr"),
    mode: "scalar",
    rel,
    outputColumn,
  };
}

function mapBinaryOperatorToRelFunction(operator: string): string | null {
  switch (operator) {
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
    case "AND":
      return "and";
    case "OR":
      return "or";
    case "+":
      return "add";
    case "-":
      return "subtract";
    case "*":
      return "multiply";
    case "/":
      return "divide";
    case "%":
      return "mod";
    case "||":
      return "concat";
    case "LIKE":
      return "like";
    case "NOT LIKE":
      return "not_like";
    case "IS DISTINCT FROM":
      return "is_distinct_from";
    case "IS NOT DISTINCT FROM":
      return "is_not_distinct_from";
    default:
      return null;
  }
}

function readWindowFunctionName(expr: {
  type?: unknown;
  name?: unknown;
}): RelWindowFunction["fn"] | null {
  if (expr.type === "aggr_func" && typeof expr.name === "string") {
    const lowered = expr.name.toLowerCase();
    return lowered === "count" ||
      lowered === "sum" ||
      lowered === "avg" ||
      lowered === "min" ||
      lowered === "max"
      ? lowered
      : null;
  }
  if (expr.type !== "function") {
    return null;
  }

  const raw = expr.name as { name?: Array<{ value?: unknown }> } | undefined;
  const head = raw?.name?.[0]?.value;
  if (typeof head !== "string") {
    return null;
  }
  const lowered = head.toLowerCase();
  return lowered === "dense_rank" || lowered === "rank" || lowered === "row_number"
    ? lowered
    : null;
}

function supportsRankWindowArgs(args: unknown): boolean {
  if (!args || typeof args !== "object") {
    return true;
  }
  const value = (args as { value?: unknown }).value;
  if (!Array.isArray(value)) {
    return true;
  }
  return value.length === 0;
}

function parseNamedWindowSpecifications(
  entries: WindowClauseEntryAst[] | undefined,
): Map<string, WindowSpecificationAst> {
  const out = new Map<string, WindowSpecificationAst>();
  for (const entry of entries ?? []) {
    const spec = entry.as_window_specification?.window_specification;
    if (!spec) {
      continue;
    }
    out.set(entry.name, spec);
  }
  return out;
}

function parseWindowOver(
  over: unknown,
  windowDefinitions: Map<string, WindowSpecificationAst>,
): WindowSpecificationAst | null {
  if (!over || typeof over !== "object") {
    return null;
  }

  const rawSpec = (over as { as_window_specification?: unknown }).as_window_specification;
  if (!rawSpec) {
    return null;
  }

  if (typeof rawSpec === "string") {
    const resolved = windowDefinitions.get(rawSpec);
    if (!resolved) {
      return null;
    }
    if (resolved.window_frame_clause) {
      return null;
    }
    return resolved;
  }

  if (typeof rawSpec !== "object") {
    return null;
  }

  const spec = (rawSpec as { window_specification?: unknown }).window_specification;
  if (!spec || typeof spec !== "object") {
    return null;
  }
  const typed = spec as WindowSpecificationAst;
  if (typed.window_frame_clause) {
    return null;
  }
  return typed;
}

function hasAggregateProjection(rawColumns: unknown): boolean {
  if (rawColumns === "*") {
    return false;
  }

  const columns = Array.isArray(rawColumns) ? (rawColumns as SelectColumnAst[]) : [];
  return columns.some((entry) => {
    const expr = entry.expr as { type?: unknown; over?: unknown };
    return expr.type === "aggr_func" && !expr.over;
  });
}

function getGroupByColumns(rawGroupBy: unknown): unknown[] {
  if (!rawGroupBy || typeof rawGroupBy !== "object") {
    return [];
  }

  const groupBy = rawGroupBy as { columns?: unknown };
  return Array.isArray(groupBy.columns) ? groupBy.columns : [];
}

function parsePositiveOrdinalLiteral(
  raw: unknown,
  clause: "GROUP BY" | "ORDER BY",
): number | undefined {
  const expr = raw as { type?: unknown; value?: unknown };
  if (expr?.type !== "number") {
    return undefined;
  }

  if (typeof expr.value !== "number" || !Number.isInteger(expr.value) || expr.value <= 0) {
    throw new Error(`${clause} ordinal must be a positive integer.`);
  }

  return expr.value;
}

function parseGroupBy(
  rawGroupBy: unknown,
  bindings: Binding[],
  aliasToBinding: Map<string, Binding>,
): ParsedGroupByTerm[] | null {
  const refs: ParsedGroupByTerm[] = [];

  for (const entry of getGroupByColumns(rawGroupBy)) {
    const ordinal = parsePositiveOrdinalLiteral(entry, "GROUP BY");
    if (ordinal != null) {
      refs.push({
        kind: "ordinal",
        position: ordinal,
      });
      continue;
    }

    const resolved = resolveColumnRef(entry, bindings, aliasToBinding);
    if (!resolved) {
      return null;
    }

    refs.push({
      kind: "ref",
      ref: {
        alias: resolved.alias,
        column: resolved.column,
      },
    });
  }

  return refs;
}

function parseAggregateProjections(
  rawColumns: unknown,
  bindings: Binding[],
  aliasToBinding: Map<string, Binding>,
  schema: SchemaDefinition,
  cteNames: Set<string>,
): ParsedAggregateProjection[] | null {
  if (rawColumns === "*") {
    return null;
  }

  const columns = Array.isArray(rawColumns) ? (rawColumns as SelectColumnAst[]) : [];
  if (columns.length === 0) {
    return null;
  }

  const out: ParsedAggregateProjection[] = [];

  for (const entry of columns) {
    const exprType = (entry.expr as { type?: unknown })?.type;
    if (exprType === "aggr_func") {
      const output =
        typeof entry.as === "string" && entry.as.length > 0
          ? entry.as
          : deriveDefaultAggregateOutputName(entry.expr);
      const metric = parseAggregateMetric(entry.expr, output, bindings, aliasToBinding);
      if (!metric) {
        return null;
      }

      out.push({
        kind: "metric",
        output,
        metric,
      });
      continue;
    }

    const column = resolveColumnRef(entry.expr, bindings, aliasToBinding);
    const output =
      typeof entry.as === "string" && entry.as.length > 0 ? entry.as : (column?.column ?? "expr");
    if (column) {
      out.push({
        kind: "group",
        source: {
          alias: column.alias,
          column: column.column,
        },
        output,
      });
      continue;
    }

    const expr = lowerSqlAstToRelExpr(entry.expr, bindings, aliasToBinding, schema, cteNames);
    if (!expr) {
      return null;
    }

    out.push({
      kind: "group",
      expr,
      output,
    });
  }

  return out;
}

function parseAggregateMetric(
  raw: unknown,
  output: string,
  bindings: Binding[],
  aliasToBinding: Map<string, Binding>,
): ParsedAggregateMetricProjection["metric"] | null {
  const expr = raw as {
    type?: unknown;
    name?: unknown;
    args?: {
      expr?: unknown;
      distinct?: unknown;
    };
  };

  if (expr.type !== "aggr_func" || typeof expr.name !== "string") {
    return null;
  }

  const fn = expr.name.toLowerCase();
  if (fn !== "count" && fn !== "sum" && fn !== "avg" && fn !== "min" && fn !== "max") {
    return null;
  }

  const distinct = expr.args?.distinct === "DISTINCT";
  const arg = expr.args?.expr;
  const column = parseAggregateMetricColumn(arg, bindings, aliasToBinding);

  if (fn === "count") {
    if (column === null) {
      return null;
    }
    if (!column && distinct) {
      return null;
    }

    return {
      fn,
      as: output,
      ...(column ? { column } : {}),
      ...(distinct ? { distinct: true } : {}),
    };
  }

  if (!column) {
    return null;
  }

  return {
    fn,
    as: output,
    column,
    ...(distinct ? { distinct: true } : {}),
  };
}

function parseAggregateMetricColumn(
  raw: unknown,
  bindings: Binding[],
  aliasToBinding: Map<string, Binding>,
): RelColumnRef | null | undefined {
  if (!raw) {
    return undefined;
  }

  const maybeStar = raw as { type?: unknown; value?: unknown };
  if (maybeStar.type === "star" || maybeStar.value === "*") {
    return undefined;
  }

  const resolved = resolveColumnRef(raw, bindings, aliasToBinding);
  if (!resolved) {
    return null;
  }

  return {
    alias: resolved.alias,
    column: resolved.column,
  };
}

function deriveDefaultAggregateOutputName(raw: unknown): string {
  const expr = raw as { name?: unknown };
  const fn = typeof expr.name === "string" ? expr.name.toLowerCase() : "agg";
  return `${fn}_value`;
}

function getAggregateMetricSignature(
  metric: Extract<RelNode, { kind: "aggregate" }>["metrics"][number],
): string {
  const ref = metric.column;
  return `${metric.fn}|${metric.distinct ? "distinct" : "all"}|${ref?.alias ?? ref?.table ?? ""}.${ref?.column ?? "*"}`;
}

function lowerHavingExpr(
  raw: unknown,
  bindings: Binding[],
  aliasToBinding: Map<string, Binding>,
  aggregateMetricAliases: Map<string, string>,
  hiddenMetrics: Extract<RelNode, { kind: "aggregate" }>["metrics"],
): RelExpr | null {
  if (!raw || typeof raw !== "object") {
    return null;
  }

  const expr = raw as {
    type?: unknown;
    value?: unknown;
    table?: unknown;
    column?: unknown;
    operator?: unknown;
    left?: unknown;
    right?: unknown;
    name?: unknown;
    args?: { value?: unknown; expr?: unknown; distinct?: unknown };
    ast?: unknown;
  };

  switch (expr.type) {
    case "string":
      return { kind: "literal", value: typeof expr.value === "string" ? expr.value : "" };
    case "number":
      return typeof expr.value === "number" ? { kind: "literal", value: expr.value } : null;
    case "bool":
      return typeof expr.value === "boolean" ? { kind: "literal", value: expr.value } : null;
    case "null":
      return { kind: "literal", value: null };
    case "column_ref": {
      const resolved = resolveColumnRef(expr, bindings, aliasToBinding);
      if (!resolved) {
        return null;
      }
      return {
        kind: "column",
        ref: {
          column: resolved.column,
        },
      };
    }
    case "aggr_func": {
      const metric = parseAggregateMetric(
        expr,
        deriveDefaultAggregateOutputName(expr),
        bindings,
        aliasToBinding,
      );
      if (!metric) {
        return null;
      }
      const signature = getAggregateMetricSignature(metric);
      let alias = aggregateMetricAliases.get(signature);
      if (!alias) {
        alias = `__having_metric_${aggregateMetricAliases.size + 1}`;
        aggregateMetricAliases.set(signature, alias);
        hiddenMetrics.push({
          ...metric,
          as: alias,
        });
      }
      return {
        kind: "column",
        ref: {
          column: alias,
        },
      };
    }
    case "binary_expr":
      return lowerHavingBinaryExpr(
        expr,
        bindings,
        aliasToBinding,
        aggregateMetricAliases,
        hiddenMetrics,
      );
    case "function":
      return lowerHavingFunctionExpr(
        expr,
        bindings,
        aliasToBinding,
        aggregateMetricAliases,
        hiddenMetrics,
      );
    default:
      if ("ast" in expr) {
        return null;
      }
      return null;
  }
}

function lowerHavingBinaryExpr(
  expr: {
    operator?: unknown;
    left?: unknown;
    right?: unknown;
  },
  bindings: Binding[],
  aliasToBinding: Map<string, Binding>,
  aggregateMetricAliases: Map<string, string>,
  hiddenMetrics: Extract<RelNode, { kind: "aggregate" }>["metrics"],
): RelExpr | null {
  const operator = typeof expr.operator === "string" ? expr.operator.toUpperCase() : null;
  if (!operator) {
    return null;
  }

  if (operator === "BETWEEN") {
    const range = expr.right as { type?: unknown; value?: unknown } | undefined;
    if (range?.type !== "expr_list" || !Array.isArray(range.value) || range.value.length !== 2) {
      return null;
    }
    const left = lowerHavingExpr(
      expr.left,
      bindings,
      aliasToBinding,
      aggregateMetricAliases,
      hiddenMetrics,
    );
    const low = lowerHavingExpr(
      range.value[0],
      bindings,
      aliasToBinding,
      aggregateMetricAliases,
      hiddenMetrics,
    );
    const high = lowerHavingExpr(
      range.value[1],
      bindings,
      aliasToBinding,
      aggregateMetricAliases,
      hiddenMetrics,
    );
    if (!left || !low || !high) {
      return null;
    }
    return {
      kind: "function",
      name: "between",
      args: [left, low, high],
    };
  }

  if (operator === "IN" || operator === "NOT IN") {
    const left = lowerHavingExpr(
      expr.left,
      bindings,
      aliasToBinding,
      aggregateMetricAliases,
      hiddenMetrics,
    );
    const values = parseHavingExprListToRelExprArgs(
      expr.right,
      bindings,
      aliasToBinding,
      aggregateMetricAliases,
      hiddenMetrics,
    );
    if (!left || !values) {
      return null;
    }
    return {
      kind: "function",
      name: operator === "NOT IN" ? "not_in" : "in",
      args: [left, ...values],
    };
  }

  if (operator === "IS" || operator === "IS NOT") {
    const left = lowerHavingExpr(
      expr.left,
      bindings,
      aliasToBinding,
      aggregateMetricAliases,
      hiddenMetrics,
    );
    const rightLiteral = parseLiteral(expr.right);
    if (!left || rightLiteral !== null) {
      return null;
    }
    return {
      kind: "function",
      name: operator === "IS NOT" ? "is_not_null" : "is_null",
      args: [left],
    };
  }

  const left = lowerHavingExpr(
    expr.left,
    bindings,
    aliasToBinding,
    aggregateMetricAliases,
    hiddenMetrics,
  );
  const right = lowerHavingExpr(
    expr.right,
    bindings,
    aliasToBinding,
    aggregateMetricAliases,
    hiddenMetrics,
  );
  if (!left || !right) {
    return null;
  }

  const mapped = mapBinaryOperatorToRelFunction(operator);
  if (!mapped) {
    return null;
  }

  return {
    kind: "function",
    name: mapped,
    args: [left, right],
  };
}

function lowerHavingFunctionExpr(
  expr: {
    name?: unknown;
    args?: { value?: unknown };
    over?: unknown;
  },
  bindings: Binding[],
  aliasToBinding: Map<string, Binding>,
  aggregateMetricAliases: Map<string, string>,
  hiddenMetrics: Extract<RelNode, { kind: "aggregate" }>["metrics"],
): RelExpr | null {
  if (expr.over) {
    return null;
  }

  const rawName = (expr.name as { name?: Array<{ value?: unknown }> } | undefined)?.name?.[0]
    ?.value;
  if (typeof rawName !== "string") {
    return null;
  }

  const normalized = rawName.toLowerCase();
  const args = parseHavingFunctionArgsToRelExpr(
    expr.args?.value,
    bindings,
    aliasToBinding,
    aggregateMetricAliases,
    hiddenMetrics,
  );
  if (!args) {
    return null;
  }

  if (normalized === "not") {
    return args.length === 1
      ? {
          kind: "function",
          name: "not",
          args,
        }
      : null;
  }

  if (
    normalized !== "lower" &&
    normalized !== "upper" &&
    normalized !== "trim" &&
    normalized !== "length" &&
    normalized !== "substr" &&
    normalized !== "substring" &&
    normalized !== "coalesce" &&
    normalized !== "nullif" &&
    normalized !== "abs" &&
    normalized !== "round" &&
    normalized !== "cast" &&
    normalized !== "case"
  ) {
    return null;
  }

  return {
    kind: "function",
    name: normalized === "substring" ? "substr" : normalized,
    args,
  };
}

function parseHavingFunctionArgsToRelExpr(
  raw: unknown,
  bindings: Binding[],
  aliasToBinding: Map<string, Binding>,
  aggregateMetricAliases: Map<string, string>,
  hiddenMetrics: Extract<RelNode, { kind: "aggregate" }>["metrics"],
): RelExpr[] | null {
  if (raw == null) {
    return [];
  }
  const values = Array.isArray(raw) ? raw : [raw];
  const args: RelExpr[] = [];
  for (const value of values) {
    const arg = lowerHavingExpr(
      value,
      bindings,
      aliasToBinding,
      aggregateMetricAliases,
      hiddenMetrics,
    );
    if (!arg) {
      return null;
    }
    args.push(arg);
  }
  return args;
}

function parseHavingExprListToRelExprArgs(
  raw: unknown,
  bindings: Binding[],
  aliasToBinding: Map<string, Binding>,
  aggregateMetricAliases: Map<string, string>,
  hiddenMetrics: Extract<RelNode, { kind: "aggregate" }>["metrics"],
): RelExpr[] | null {
  const expr = raw as { type?: unknown; value?: unknown };
  if (expr?.type !== "expr_list" || !Array.isArray(expr.value)) {
    return null;
  }
  return parseHavingFunctionArgsToRelExpr(
    expr.value,
    bindings,
    aliasToBinding,
    aggregateMetricAliases,
    hiddenMetrics,
  );
}

function validateAggregateProjectionGroupBy(
  projections: ParsedAggregateProjection[],
  groupBy: RelColumnRef[],
): boolean {
  const groupBySet = new Set(groupBy.map((ref) => `${ref.alias ?? ""}.${ref.column}`));

  for (const projection of projections) {
    if (projection.kind !== "group") {
      continue;
    }

    if (!projection.source) {
      return false;
    }

    const key = `${projection.source.alias ?? ""}.${projection.source.column}`;
    if (!groupBySet.has(key)) {
      return false;
    }
  }

  return true;
}

function parseOrderBy(
  rawOrderBy: unknown,
  bindings: Binding[],
  aliasToBinding: Map<string, Binding>,
  outputs: Set<string>,
): ParsedOrderByTerm[] | null {
  const orderBy = Array.isArray(rawOrderBy) ? (rawOrderBy as OrderByTermAst[]) : [];
  const out: ParsedOrderByTerm[] = [];

  for (const term of orderBy) {
    const ordinal = parsePositiveOrdinalLiteral(term.expr, "ORDER BY");
    if (ordinal != null) {
      out.push({
        kind: "ordinal",
        position: ordinal,
        direction: term.type === "DESC" ? "desc" : "asc",
      });
      continue;
    }

    const rawRef = toRawColumnRef(term.expr);
    if (rawRef && !rawRef.table && outputs.has(rawRef.column)) {
      out.push({
        kind: "output",
        output: rawRef.column,
        direction: term.type === "DESC" ? "desc" : "asc",
      });
      continue;
    }

    const resolved = resolveColumnRef(term.expr, bindings, aliasToBinding);
    if (!resolved) {
      return null;
    }

    if (!resolved.alias) {
      return null;
    }

    out.push({
      kind: "ref",
      source: {
        alias: resolved.alias,
        column: resolved.column,
      },
      direction: term.type === "DESC" ? "desc" : "asc",
    });
  }

  return out;
}

function materializeSelectExprProjection(
  projection: SelectExprProjection,
  prefix: string,
): RelProjectExprMapping | null {
  if (projection.source) {
    return null;
  }

  const column = nextSyntheticColumnName(prefix);
  projection.source = { column };
  return {
    kind: "expr",
    expr: projection.expr,
    output: column,
  };
}

function materializeAggregateGroupProjection(
  projection: ParsedAggregateGroupProjection,
  prefix: string,
): RelProjectExprMapping | null {
  if (projection.source || !projection.expr) {
    return null;
  }

  const column = nextSyntheticColumnName(prefix);
  projection.source = { column };
  return {
    kind: "expr",
    expr: projection.expr,
    output: column,
  };
}

function resolveAggregateGroupBy(
  groupByTerms: ParsedGroupByTerm[],
  projections: ParsedAggregateProjection[],
): {
  groupBy: RelColumnRef[];
  materializations: RelProjectExprMapping[];
} {
  const groupBy: RelColumnRef[] = [];
  const materializations: RelProjectExprMapping[] = [];

  for (const term of groupByTerms) {
    if (term.kind === "ref") {
      groupBy.push(term.ref);
      continue;
    }

    const projection = projections[term.position - 1];
    if (!projection) {
      throw new Error(`GROUP BY ordinal ${term.position} is out of range.`);
    }
    if (projection.kind === "metric") {
      throw new Error(`GROUP BY ordinal ${term.position} cannot reference an aggregate output.`);
    }

    const materialization = materializeAggregateGroupProjection(projection, "group_by");
    if (materialization) {
      materializations.push(materialization);
    }
    if (!projection.source) {
      throw new Error(`GROUP BY ordinal ${term.position} could not be resolved.`);
    }

    groupBy.push(projection.source);
  }

  return {
    groupBy,
    materializations,
  };
}

function resolveNonAggregateOrderBy(
  orderByTerms: ParsedOrderByTerm[],
  projections: SelectProjection[],
): {
  orderBy: ResolvedOrderTerm[];
  materializations: RelProjectExprMapping[];
} {
  const projectionsByOutput = new Map(
    projections.map((projection) => [projection.output, projection] as const),
  );
  const materializations: RelProjectExprMapping[] = [];
  const orderBy: ResolvedOrderTerm[] = [];

  const resolveProjectionSource = (
    projection: SelectProjection,
    ordinal?: number,
  ): ResolvedOrderTerm["source"] => {
    if (projection.kind === "column") {
      return toParsedOrderSource(projection.source, projection.output);
    }
    if (projection.kind === "window") {
      return { column: projection.function.as };
    }

    const materialization = materializeSelectExprProjection(projection, "order_by");
    if (materialization) {
      materializations.push(materialization);
    }
    if (!projection.source) {
      throw new Error(
        ordinal != null
          ? `ORDER BY ordinal ${ordinal} could not be resolved.`
          : `ORDER BY expression "${projection.output}" could not be resolved.`,
      );
    }
    return { column: projection.source.column };
  };

  for (const term of orderByTerms) {
    if (term.kind === "ref") {
      orderBy.push({
        source: term.source,
        direction: term.direction,
      });
      continue;
    }

    const projection =
      term.kind === "ordinal"
        ? projections[term.position - 1]
        : projectionsByOutput.get(term.output);
    if (!projection) {
      if (term.kind === "ordinal") {
        throw new Error(`ORDER BY ordinal ${term.position} is out of range.`);
      }
      throw new Error(`Unknown ORDER BY output "${term.output}".`);
    }

    orderBy.push({
      source: resolveProjectionSource(
        projection,
        term.kind === "ordinal" ? term.position : undefined,
      ),
      direction: term.direction,
    });
  }

  return {
    orderBy,
    materializations,
  };
}

function resolveAggregateOrderBy(
  orderByTerms: ParsedOrderByTerm[],
  projections: ParsedAggregateProjection[],
): ResolvedOrderTerm[] {
  const projectionsByOutput = new Map(
    projections.map((projection) => [projection.output, projection] as const),
  );
  const groupOutputsBySource = new Map<string, string>();

  for (const projection of projections) {
    if (projection.kind !== "group" || !projection.source) {
      continue;
    }
    groupOutputsBySource.set(
      `${projection.source.alias ?? ""}.${projection.source.column}`,
      projection.source.column,
    );
  }

  const resolveProjectionSource = (
    projection: ParsedAggregateProjection,
    ordinal?: number,
  ): ResolvedOrderTerm["source"] => {
    if (projection.kind === "metric") {
      return { column: projection.metric.as };
    }
    if (!projection.source) {
      throw new Error(
        ordinal != null
          ? `ORDER BY ordinal ${ordinal} could not be resolved.`
          : `ORDER BY expression "${projection.output}" could not be resolved.`,
      );
    }
    return { column: projection.source.column };
  };

  return orderByTerms.map((term) => {
    if (term.kind === "ref") {
      const key = `${term.source.alias ?? ""}.${term.source.column}`;
      return {
        source: { column: groupOutputsBySource.get(key) ?? term.source.column },
        direction: term.direction,
      };
    }

    const projection =
      term.kind === "ordinal"
        ? projections[term.position - 1]
        : projectionsByOutput.get(term.output);
    if (!projection) {
      if (term.kind === "ordinal") {
        throw new Error(`ORDER BY ordinal ${term.position} is out of range.`);
      }
      throw new Error(`Unknown ORDER BY output "${term.output}".`);
    }

    return {
      source: resolveProjectionSource(
        projection,
        term.kind === "ordinal" ? term.position : undefined,
      ),
      direction: term.direction,
    };
  });
}

function appendProjectExpressions(
  input: RelNode,
  mappings: RelProjectExprMapping[],
): RelProjectNode {
  return {
    id: nextRelId("project"),
    kind: "project",
    convention: "local",
    input,
    columns: [
      ...input.output.map((column) => ({
        kind: "column" as const,
        source: parseRelColumnRef(column.name),
        output: column.name,
      })),
      ...mappings,
    ],
    output: [...input.output, ...mappings.map((mapping) => ({ name: mapping.output }))],
  };
}

function parseWhereFilters(
  where: unknown,
  bindings: Binding[],
  aliasToBinding: Map<string, Binding>,
  schema: SchemaDefinition,
  cteNames: Set<string>,
): ParsedWhereFilters | null {
  if (!where) {
    return {
      literals: [],
      inSubqueries: [],
    };
  }

  const parts = flattenConjunctiveWhere(where);
  if (parts == null) {
    const residualExpr = lowerSqlAstToRelExpr(where, bindings, aliasToBinding, schema, cteNames);
    if (!residualExpr) {
      return null;
    }
    return {
      literals: [],
      inSubqueries: [],
      residualExpr,
    };
  }

  const literals: LiteralFilter[] = [];
  const inSubqueries: InSubqueryFilter[] = [];
  const residualParts: RelExpr[] = [];
  for (const part of parts) {
    const parsed = parseLiteralFilter(part, bindings, aliasToBinding);
    if (!parsed) {
      const residual = lowerSqlAstToRelExpr(part, bindings, aliasToBinding, schema, cteNames);
      if (!residual) {
        return null;
      }
      residualParts.push(residual);
      continue;
    }
    if ("subquery" in parsed) {
      inSubqueries.push(parsed);
      continue;
    }
    literals.push(parsed);
  }

  const residualExpr = residualParts.reduce<RelExpr | null>(
    (acc, current) =>
      acc
        ? {
            kind: "function",
            name: "and",
            args: [acc, current],
          }
        : current,
    null,
  );

  return residualExpr
    ? {
        literals,
        inSubqueries,
        residualExpr,
      }
    : {
        literals,
        inSubqueries,
      };
}

function parseLiteralFilter(
  raw: unknown,
  bindings: Binding[],
  aliasToBinding: Map<string, Binding>,
): LiteralFilter | InSubqueryFilter | null {
  const expr = raw as {
    type?: unknown;
    operator?: unknown;
    left?: unknown;
    right?: unknown;
  };

  if (expr.type !== "binary_expr") {
    return null;
  }

  const operator = tryNormalizeBinaryOperator(expr.operator);
  if (!operator) {
    return null;
  }

  if (operator === "in") {
    const col = resolveColumnRef(expr.left, bindings, aliasToBinding);
    const subquery = parseSubqueryAst(expr.right);
    if (col && subquery) {
      return {
        alias: col.alias,
        column: col.column,
        subquery,
      };
    }

    const values = tryParseLiteralExpressionList(expr.right);
    if (!col || !values) {
      return null;
    }

    return {
      alias: col.alias,
      clause: {
        op: "in",
        column: col.column,
        values,
      },
    };
  }

  if (operator === "not_in") {
    const col = resolveColumnRef(expr.left, bindings, aliasToBinding);
    const values = tryParseLiteralExpressionList(expr.right);
    if (!col || !values) {
      return null;
    }

    return {
      alias: col.alias,
      clause: {
        op: "not_in",
        column: col.column,
        values,
      },
    };
  }

  if (operator === "is_null" || operator === "is_not_null") {
    const col = resolveColumnRef(expr.left, bindings, aliasToBinding);
    const value = parseLiteral(expr.right);
    if (!col || value !== null) {
      return null;
    }

    return {
      alias: col.alias,
      clause: {
        op: operator,
        column: col.column,
      },
    };
  }

  if (operator === "like" || operator === "not_like") {
    const col = resolveColumnRef(expr.left, bindings, aliasToBinding);
    const value = parseLiteral(expr.right);
    if (!col || typeof value !== "string") {
      return null;
    }

    return {
      alias: col.alias,
      clause: {
        op: operator,
        column: col.column,
        value,
      },
    };
  }

  const leftCol = resolveColumnRef(expr.left, bindings, aliasToBinding);
  const rightCol = resolveColumnRef(expr.right, bindings, aliasToBinding);

  if (leftCol && rightCol) {
    // This becomes join/relational filter territory; this simple lowering handles literal filters only.
    return null;
  }

  if (leftCol) {
    const value = parseLiteral(expr.right);
    if (value === undefined) {
      return null;
    }

    return {
      alias: leftCol.alias,
      clause: {
        op: operator,
        column: leftCol.column,
        value,
      },
    };
  }

  if (rightCol) {
    const value = parseLiteral(expr.left);
    if (value === undefined) {
      return null;
    }

    return {
      alias: rightCol.alias,
      clause: {
        op: invertOperator(operator),
        column: rightCol.column,
        value,
      },
    };
  }

  return null;
}

function parseSubqueryAst(raw: unknown): SelectAst | null {
  if (!raw || typeof raw !== "object") {
    return null;
  }
  const ast = (raw as { ast?: unknown }).ast;
  if (!ast || typeof ast !== "object") {
    return null;
  }
  if ((ast as { type?: unknown }).type !== "select") {
    return null;
  }
  return ast as SelectAst;
}

function isCorrelatedSubquery(ast: SelectAst, outerAliases: Set<string>): boolean {
  let correlated = false;

  const visit = (value: unknown): void => {
    if (correlated || !value || typeof value !== "object") {
      return;
    }
    if (Array.isArray(value)) {
      for (const item of value) {
        visit(item);
        if (correlated) {
          return;
        }
      }
      return;
    }

    const record = value as Record<string, unknown>;
    if (record.type === "column_ref") {
      const table = typeof record.table === "string" ? record.table : null;
      if (table && outerAliases.has(table)) {
        correlated = true;
        return;
      }
    }

    for (const nested of Object.values(record)) {
      visit(nested);
      if (correlated) {
        return;
      }
    }
  };

  visit(ast);
  return correlated;
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

  return [expr];
}

function tryNormalizeBinaryOperator(raw: unknown): Exclude<ScanFilterClause["op"], never> | null {
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
    case "NOT IN":
      return "not_in";
    case "LIKE":
      return "like";
    case "NOT LIKE":
      return "not_like";
    case "IS DISTINCT FROM":
      return "is_distinct_from";
    case "IS NOT DISTINCT FROM":
      return "is_not_distinct_from";
    case "IS":
      return "is_null";
    case "IS NOT":
      return "is_not_null";
    default:
      return null;
  }
}

function invertOperator(
  op: Exclude<
    ScanFilterClause["op"],
    "in" | "not_in" | "like" | "not_like" | "is_null" | "is_not_null"
  >,
): Exclude<
  ScanFilterClause["op"],
  "in" | "not_in" | "like" | "not_like" | "is_null" | "is_not_null"
> {
  switch (op) {
    case "eq":
      return "eq";
    case "neq":
      return "neq";
    case "is_distinct_from":
      return "is_distinct_from";
    case "is_not_distinct_from":
      return "is_not_distinct_from";
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

  if (typeof expr.column !== "string" || expr.column.length === 0 || expr.column === "*") {
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
  bindings: Binding[],
  aliasToBinding: Map<string, Binding>,
): { alias: string; column: string } | undefined {
  const rawRef = toRawColumnRef(raw);
  if (!rawRef) {
    return undefined;
  }

  if (rawRef.table) {
    if (!aliasToBinding.has(rawRef.table)) {
      return undefined;
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

  return undefined;
}

function parseLiteral(raw: unknown): unknown {
  const expr = raw as { type?: unknown; value?: unknown };

  switch (expr?.type) {
    case "single_quote_string":
    case "double_quote_string":
    case "string":
      return typeof expr.value === "string" ? expr.value : "";
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
    return {
      limit: first,
      ...(second != null ? { offset: second } : {}),
    };
  }

  if (separator === ",") {
    return {
      ...(second != null ? { limit: second } : {}),
      offset: first,
    };
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

function appearsInRel(node: RelNode, alias: string): boolean {
  switch (node.kind) {
    case "scan":
      return node.alias === alias;
    case "filter":
    case "project":
    case "aggregate":
    case "window":
    case "sort":
    case "limit_offset":
      return appearsInRel(node.input, alias);
    case "join":
    case "set_op":
      return appearsInRel(node.left, alias) || appearsInRel(node.right, alias);
    case "with":
      return appearsInRel(node.body, alias);
    case "sql":
      return false;
  }
}

function resolveLookupJoinCandidate<TContext>(
  join: RelJoinNode,
  schema: SchemaDefinition,
  providers: ProvidersMap<TContext>,
): {
  leftProvider: string;
  rightProvider: string;
  leftScan: RelScanNode;
  rightScan: RelScanNode;
  leftKey: string;
  rightKey: string;
  joinType: "inner" | "left";
} | null {
  if (join.joinType !== "inner" && join.joinType !== "left") {
    return null;
  }

  const leftScan = findFirstScanNode(join.left);
  const rightScan = findFirstScanNode(join.right);
  if (!leftScan || !rightScan) {
    return null;
  }

  const leftBinding = getNormalizedTableBinding(schema, leftScan.table);
  const rightBinding = getNormalizedTableBinding(schema, rightScan.table);
  if (leftBinding?.kind === "view" || rightBinding?.kind === "view") {
    return null;
  }

  const leftProvider = leftScan.entity?.provider ?? resolveTableProvider(schema, leftScan.table);
  const rightProvider = rightScan.entity?.provider ?? resolveTableProvider(schema, rightScan.table);
  if (leftProvider === rightProvider) {
    return null;
  }

  const rightAdapter = providers[rightProvider];
  if (!rightAdapter || !supportsLookupMany(rightAdapter)) {
    return null;
  }

  return {
    leftProvider,
    rightProvider,
    leftScan,
    rightScan,
    leftKey: join.leftKey.column,
    rightKey: join.rightKey.column,
    joinType: join.joinType,
  };
}

function findFirstScanNode(node: RelNode): RelScanNode | null {
  switch (node.kind) {
    case "scan":
      return node;
    case "filter":
    case "project":
    case "aggregate":
    case "window":
    case "sort":
    case "limit_offset":
      return findFirstScanNode(node.input);
    case "join":
    case "set_op":
      return findFirstScanNode(node.left) ?? findFirstScanNode(node.right);
    case "with":
      return findFirstScanNode(node.body);
    case "sql":
      return null;
  }
}

function collectTablesFromSelectAst(ast: SelectAst): string[] {
  const tables = new Set<string>();
  const cteNames = new Set<string>();

  const visit = (value: unknown): void => {
    if (!value || typeof value !== "object") {
      return;
    }

    if (Array.isArray(value)) {
      for (const item of value) {
        visit(item);
      }
      return;
    }

    const record = value as Record<string, unknown>;
    const rawName = record.name;
    if (typeof rawName === "string") {
      cteNames.add(rawName);
    } else if (
      rawName &&
      typeof rawName === "object" &&
      typeof (rawName as { value?: unknown }).value === "string"
    ) {
      cteNames.add((rawName as { value: string }).value);
    }

    const from = record.from;
    if (Array.isArray(from)) {
      for (const entry of from) {
        if (entry && typeof entry === "object") {
          const table = (entry as { table?: unknown }).table;
          if (typeof table === "string" && !cteNames.has(table)) {
            tables.add(table);
          }
        }
      }
    }

    for (const nested of Object.values(record)) {
      visit(nested);
    }
  };

  visit(ast.with);
  visit(ast);

  return [...tables];
}
