import type {
  RelColumnRef,
  RelNode,
  RelProjectExprMapping,
  RelProjectNode,
} from "@tupl/foundation";
import type { ScanFilterClause } from "@tupl/foundation";
import type { FromEntryAst, SelectAst, SelectColumnAst } from "./sqlite-parser/ast";
import type { SchemaDefinition } from "@tupl/schema-model";
import {
  getGroupByColumns,
  hasAggregateProjection,
  lowerHavingExpr,
  parseAggregateMetric,
  parseAggregateProjections,
  parseGroupBy,
  parseOrderBy,
  resolveAggregateGroupBy,
  resolveAggregateOrderBy,
  resolveNonAggregateOrderBy,
  validateAggregateProjectionGroupBy,
  getAggregateMetricSignature,
} from "./aggregate-lowering";
import { nextRelId } from "./planner-ids";
import type {
  Binding,
  ParsedJoin,
  SelectProjection,
  SelectWindowProjection,
} from "./planner-types";
import {
  collectRelExprRefs,
  isCorrelatedSubquery,
  lowerSqlAstToRelExpr,
  parseLimitAndOffset,
  parseNamedWindowSpecifications,
  parseWindowOver,
  readWindowFunctionName,
  resolveColumnRef,
  supportsRankWindowArgs,
  type SqlExprLoweringContext,
  collectTablesFromSelectAst,
} from "./sql-expr-lowering";
import {
  combineAndExprs,
  getPushableWhereAliases,
  literalFilterToRelExpr,
  parseWhereFilters,
  validateEnumLiteralFilters,
} from "./where-lowering";

/**
 * Structured select lowering owns select/set-op/CTE lowering into relational nodes.
 */
export function tryLowerStructuredSelect(
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
    loweredCtes.push({ name: cteName, query: loweredCte });
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
  if (ast.type !== "select" || ast.with || ast.set_op || ast._next) {
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

    return { table, alias, index };
  });

  const aliasToBinding = new Map(bindings.map((binding) => [binding.alias, binding]));
  const lowerExprContext: SqlExprLoweringContext = {
    schema,
    cteNames,
    tryLowerSelect: (subqueryAst) => tryLowerStructuredSelect(subqueryAst, schema, cteNames),
  };

  const joins = parseJoins(from, bindings, aliasToBinding);
  if (joins == null) {
    return null;
  }

  const whereFilters = parseWhereFilters(ast.where, bindings, aliasToBinding, lowerExprContext);
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
        lowerExprContext,
      );
  if (!aggregateMode && projections == null) {
    return null;
  }

  const aggregateProjections = aggregateMode
    ? parseAggregateProjections(ast.columns, bindings, aliasToBinding, lowerExprContext)
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
      (projection): projection is import("./planner-types").ParsedAggregateMetricProjection =>
        projection.kind === "metric",
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
    : resolveNonAggregateOrderBy(orderByTerms, safeProjections, toParsedOrderSource);

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

  const scansByAlias = new Map<string, Extract<RelNode, { kind: "scan" }>>();
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

  let current: RelNode = scansByAlias.get(rootBinding.alias)!;

  for (const join of joins) {
    const right = scansByAlias.get(join.alias);
    if (!right) {
      return null;
    }

    const joinLeftOnCurrent = appearsInRel(current, join.leftAlias);
    const leftKey: RelColumnRef = joinLeftOnCurrent
      ? { alias: join.leftAlias, column: join.leftColumn }
      : { alias: join.rightAlias, column: join.rightColumn };

    const rightKey: RelColumnRef = joinLeftOnCurrent
      ? { alias: join.rightAlias, column: join.rightColumn }
      : { alias: join.leftAlias, column: join.leftColumn };

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

  return {
    id: nextRelId("project"),
    kind: "project",
    convention: "local",
    input: current,
    columns: aggregateMode
      ? safeAggregateProjections.map((projection) =>
          projection.kind === "group" && projection.source
            ? {
                kind: "column" as const,
                source: { column: projection.source.column },
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
  windowDefinitions: Map<string, import("./sqlite-parser/ast").WindowSpecificationAst>,
  lowerExprContext: SqlExprLoweringContext,
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

    const expr = lowerSqlAstToRelExpr(entry.expr, bindings, aliasToBinding, lowerExprContext);
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
  windowDefinitions: Map<string, import("./sqlite-parser/ast").WindowSpecificationAst>,
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

function toParsedOrderSource(
  ref: RelColumnRef | null | undefined,
  fallbackColumn: string,
): { alias?: string; column: string } {
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

export { collectTablesFromSelectAst };
