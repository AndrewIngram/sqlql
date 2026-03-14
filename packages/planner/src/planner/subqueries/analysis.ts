import type { ExpressionAst, SelectAst, SelectColumnAst } from "../sqlite-parser/ast";

/**
 * Subquery analysis owns correlated-subquery shape detection and supported rewrite extraction.
 */
export function parseSubqueryAst(raw: unknown): SelectAst | null {
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

export function isCorrelatedSubquery(ast: SelectAst, outerAliases: Set<string>): boolean {
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

export interface SupportedCorrelatedExistsRewrite {
  negated: boolean;
  subquery: SelectAst;
  rewrittenSubquery: SelectAst;
  outer: {
    alias: string;
    column: string;
  };
  inner: {
    alias: string;
    column: string;
  };
}

export interface SupportedCorrelatedInRewrite {
  negated: boolean;
  subquery: SelectAst;
  rewrittenSubquery: SelectAst;
  outer: {
    alias: string;
    column: string;
  };
  inner: {
    alias: string;
    column: string;
  };
}

export interface SupportedCorrelatedScalarAggregateRewrite {
  rewrittenSubquery: SelectAst;
  outerCompare: {
    alias: string;
    column: string;
  };
  outerKey: {
    alias: string;
    column: string;
  };
  innerKey: {
    alias: string;
    column: string;
  };
  operator: string;
  correlationOutput: string;
  metricOutput: string;
}

export interface SupportedCorrelatedScalarAggregateProjectionRewrite {
  subquery: SelectAst;
  rewrittenSubquery: SelectAst;
  outerKey: {
    alias: string;
    column: string;
  };
  innerKey: {
    alias: string;
    column: string;
  };
  correlationOutput: string;
  metricOutput: string;
}

function parseExistsSubqueryAst(raw: unknown): { negated: boolean; subquery: SelectAst } | null {
  if (!raw || typeof raw !== "object") {
    return null;
  }

  const expr = raw as {
    type?: unknown;
    name?: { name?: Array<{ value?: unknown }> };
    args?: { value?: unknown };
  };
  if (expr.type !== "function") {
    return null;
  }

  const rawName = expr.name?.name?.[0]?.value;
  if (typeof rawName === "string" && rawName.toLowerCase() === "exists") {
    const values = Array.isArray(expr.args?.value) ? expr.args.value : [expr.args?.value];
    if (values.length !== 1) {
      return null;
    }

    const subquery = parseSubqueryAst(values[0]);
    return subquery ? { negated: false, subquery } : null;
  }

  if (typeof rawName !== "string" || rawName.toLowerCase() !== "not") {
    return null;
  }

  const values = Array.isArray(expr.args?.value) ? expr.args.value : [expr.args?.value];
  if (values.length !== 1) {
    return null;
  }

  const nested = parseExistsSubqueryAst(values[0]);
  if (!nested || nested.negated) {
    return null;
  }

  return {
    negated: true,
    subquery: nested.subquery,
  };
}

function flattenAndParts(where: unknown): ExpressionAst[] | null {
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
    const left = flattenAndParts(expr.left);
    const right = flattenAndParts(expr.right);
    if (!left || !right) {
      return null;
    }
    return [...left, ...right];
  }

  if (expr.type === "binary_expr" && expr.operator === "OR") {
    return null;
  }

  return [expr as ExpressionAst];
}

function rebuildAndParts(parts: ExpressionAst[]): ExpressionAst {
  const [first, ...rest] = parts;
  return rest.reduce(
    (left, right) => ({
      type: "binary_expr",
      operator: "AND",
      left,
      right,
    }),
    first!,
  );
}

function readColumnRef(raw: unknown): { alias?: string; column: string } | null {
  if (!raw || typeof raw !== "object") {
    return null;
  }
  const record = raw as { type?: unknown; table?: unknown; column?: unknown };
  if (record.type !== "column_ref" || typeof record.column !== "string") {
    return null;
  }

  return {
    ...(typeof record.table === "string" ? { alias: record.table } : {}),
    column: record.column,
  };
}

function containsOuterAlias(value: unknown, outerAliases: Set<string>): boolean {
  let found = false;

  const visit = (current: unknown): void => {
    if (found || !current || typeof current !== "object") {
      return;
    }
    if (Array.isArray(current)) {
      for (const item of current) {
        visit(item);
        if (found) {
          return;
        }
      }
      return;
    }

    const record = current as Record<string, unknown>;
    if (record.type === "column_ref" && typeof record.table === "string") {
      if (outerAliases.has(record.table)) {
        found = true;
        return;
      }
    }

    for (const nested of Object.values(record)) {
      visit(nested);
      if (found) {
        return;
      }
    }
  };

  visit(value);
  return found;
}

function parseCorrelationEquality(
  raw: unknown,
  outerAliases: Set<string>,
): {
  outer: { alias: string; column: string };
  inner: { alias: string; column: string };
} | null {
  if (!raw || typeof raw !== "object") {
    return null;
  }

  const expr = raw as {
    type?: unknown;
    operator?: unknown;
    left?: unknown;
    right?: unknown;
  };
  if (expr.type !== "binary_expr" || expr.operator !== "=") {
    return null;
  }

  const left = readColumnRef(expr.left);
  const right = readColumnRef(expr.right);
  if (!left?.alias || !right?.alias) {
    return null;
  }

  const leftOuter = outerAliases.has(left.alias);
  const rightOuter = outerAliases.has(right.alias);
  if (leftOuter === rightOuter) {
    return null;
  }

  return leftOuter
    ? {
        outer: { alias: left.alias, column: left.column },
        inner: { alias: right.alias, column: right.column },
      }
    : {
        outer: { alias: right.alias, column: right.column },
        inner: { alias: left.alias, column: left.column },
      };
}

export function parseSupportedCorrelatedExistsSubquery(
  raw: unknown,
  outerAliases: Set<string>,
): SupportedCorrelatedExistsRewrite | null {
  const parsed = parseExistsSubqueryAst(raw);
  if (!parsed || !isCorrelatedSubquery(parsed.subquery, outerAliases)) {
    return null;
  }

  const subquery = parsed.subquery;

  if (subquery.groupby || subquery.having || subquery.limit || subquery.window) {
    return null;
  }
  if (subquery.set_op || subquery._next || subquery.with) {
    return null;
  }

  const whereParts = flattenAndParts(subquery.where);
  if (whereParts == null) {
    return null;
  }

  let correlation: {
    outer: { alias: string; column: string };
    inner: { alias: string; column: string };
  } | null = null;
  const remainingParts: ExpressionAst[] = [];

  for (const part of whereParts) {
    const maybeCorrelation = parseCorrelationEquality(part, outerAliases);
    if (maybeCorrelation) {
      if (correlation) {
        return null;
      }
      correlation = maybeCorrelation;
      continue;
    }

    if (containsOuterAlias(part, outerAliases)) {
      return null;
    }
    remainingParts.push(part);
  }

  if (!correlation) {
    return null;
  }

  const { where: _ignoredWhere, ...subqueryWithoutWhere } = subquery;

  return {
    negated: parsed.negated,
    subquery,
    rewrittenSubquery:
      remainingParts.length > 0
        ? {
            ...subqueryWithoutWhere,
            where: rebuildAndParts(remainingParts),
          }
        : subqueryWithoutWhere,
    outer: correlation.outer,
    inner: correlation.inner,
  };
}

export function parseSupportedCorrelatedInSubquery(
  raw: unknown,
  outerAliases: Set<string>,
): SupportedCorrelatedInRewrite | null {
  if (!raw || typeof raw !== "object") {
    return null;
  }

  const expr = raw as {
    type?: unknown;
    operator?: unknown;
    left?: unknown;
    right?: unknown;
  };
  if (expr.type !== "binary_expr" || (expr.operator !== "IN" && expr.operator !== "NOT IN")) {
    return null;
  }
  const negated = expr.operator === "NOT IN";

  const outer = readColumnRef(expr.left);
  const subquery = parseSubqueryAst(expr.right);
  if (!outer?.alias || !subquery || !isCorrelatedSubquery(subquery, outerAliases)) {
    return null;
  }

  if (subquery.groupby || subquery.having || subquery.limit || subquery.window) {
    return null;
  }
  if (subquery.set_op || subquery._next || subquery.with) {
    return null;
  }

  const columns = subquery.columns === "*" ? [] : (subquery.columns ?? []);
  if (columns.length !== 1) {
    return null;
  }
  const selectedExpr = (columns[0] as { expr?: unknown }).expr;
  const selected = readColumnRef(selectedExpr);
  if (!selected?.alias) {
    return null;
  }

  const whereParts = flattenAndParts(subquery.where);
  if (whereParts == null) {
    return null;
  }

  let correlation: {
    outer: { alias: string; column: string };
    inner: { alias: string; column: string };
  } | null = null;
  const remainingParts: ExpressionAst[] = [];

  for (const part of whereParts) {
    const maybeCorrelation = parseCorrelationEquality(part, outerAliases);
    if (maybeCorrelation) {
      if (correlation) {
        return null;
      }
      correlation = maybeCorrelation;
      continue;
    }

    if (containsOuterAlias(part, outerAliases)) {
      return null;
    }
    remainingParts.push(part);
  }

  if (!correlation) {
    return null;
  }

  if (
    correlation.outer.alias !== outer.alias ||
    correlation.outer.column !== outer.column ||
    correlation.inner.alias !== selected.alias ||
    correlation.inner.column !== selected.column
  ) {
    return null;
  }

  const { where: _ignoredWhere, ...subqueryWithoutWhere } = subquery;

  return {
    negated,
    subquery,
    rewrittenSubquery:
      remainingParts.length > 0
        ? {
            ...subqueryWithoutWhere,
            where: rebuildAndParts(remainingParts),
          }
        : subqueryWithoutWhere,
    outer: correlation.outer,
    inner: correlation.inner,
  };
}

export function parseSupportedCorrelatedScalarAggregateSubquery(
  raw: unknown,
  outerAliases: Set<string>,
): SupportedCorrelatedScalarAggregateRewrite | null {
  if (!raw || typeof raw !== "object") {
    return null;
  }

  const expr = raw as {
    type?: unknown;
    operator?: unknown;
    left?: unknown;
    right?: unknown;
  };
  if (expr.type !== "binary_expr" || typeof expr.operator !== "string") {
    return null;
  }
  if (
    expr.operator !== "=" &&
    expr.operator !== "!=" &&
    expr.operator !== "<>" &&
    expr.operator !== ">" &&
    expr.operator !== ">=" &&
    expr.operator !== "<" &&
    expr.operator !== "<="
  ) {
    return null;
  }

  const outerCompare = readColumnRef(expr.left);
  const subquery = parseSubqueryAst(expr.right);
  if (!outerCompare?.alias || !subquery || !isCorrelatedSubquery(subquery, outerAliases)) {
    return null;
  }

  if (
    subquery.groupby ||
    subquery.having ||
    subquery.limit ||
    subquery.window ||
    subquery.orderby
  ) {
    return null;
  }
  if (subquery.set_op || subquery._next || subquery.with) {
    return null;
  }

  const columns = subquery.columns === "*" ? [] : (subquery.columns ?? []);
  if (columns.length !== 1) {
    return null;
  }
  const aggregateColumn = columns[0] as SelectColumnAst;
  if ((aggregateColumn.expr as { type?: unknown }).type !== "aggr_func") {
    return null;
  }

  const whereParts = flattenAndParts(subquery.where);
  if (whereParts == null) {
    return null;
  }

  let correlation: {
    outer: { alias: string; column: string };
    inner: { alias: string; column: string };
  } | null = null;
  const remainingParts: ExpressionAst[] = [];

  for (const part of whereParts) {
    const maybeCorrelation = parseCorrelationEquality(part, outerAliases);
    if (maybeCorrelation) {
      if (correlation) {
        return null;
      }
      correlation = maybeCorrelation;
      continue;
    }

    if (containsOuterAlias(part, outerAliases)) {
      return null;
    }
    remainingParts.push(part);
  }

  if (!correlation) {
    return null;
  }

  const rewritten = buildCorrelatedScalarAggregateRewrite(
    subquery,
    aggregateColumn,
    correlation,
    remainingParts,
  );
  if (!rewritten) {
    return null;
  }

  return {
    rewrittenSubquery: rewritten.rewrittenSubquery,
    outerCompare: {
      alias: outerCompare.alias,
      column: outerCompare.column,
    },
    outerKey: rewritten.outerKey,
    innerKey: rewritten.innerKey,
    operator: expr.operator,
    correlationOutput: rewritten.correlationOutput,
    metricOutput: rewritten.metricOutput,
  };
}

export function parseSupportedCorrelatedScalarAggregateProjectionSubquery(
  raw: unknown,
  outerAliases: Set<string>,
): SupportedCorrelatedScalarAggregateProjectionRewrite | null {
  const subquery = parseSubqueryAst(raw);
  if (!subquery || !isCorrelatedSubquery(subquery, outerAliases)) {
    return null;
  }

  if (
    subquery.groupby ||
    subquery.having ||
    subquery.limit ||
    subquery.window ||
    subquery.orderby
  ) {
    return null;
  }
  if (subquery.set_op || subquery._next || subquery.with) {
    return null;
  }

  const columns = subquery.columns === "*" ? [] : (subquery.columns ?? []);
  if (columns.length !== 1) {
    return null;
  }
  const aggregateColumn = columns[0] as SelectColumnAst;
  if ((aggregateColumn.expr as { type?: unknown }).type !== "aggr_func") {
    return null;
  }

  const whereParts = flattenAndParts(subquery.where);
  if (whereParts == null) {
    return null;
  }

  let correlation: {
    outer: { alias: string; column: string };
    inner: { alias: string; column: string };
  } | null = null;
  const remainingParts: ExpressionAst[] = [];

  for (const part of whereParts) {
    const maybeCorrelation = parseCorrelationEquality(part, outerAliases);
    if (maybeCorrelation) {
      if (correlation) {
        return null;
      }
      correlation = maybeCorrelation;
      continue;
    }

    if (containsOuterAlias(part, outerAliases)) {
      return null;
    }
    remainingParts.push(part);
  }

  if (!correlation) {
    return null;
  }

  return buildCorrelatedScalarAggregateRewrite(
    subquery,
    aggregateColumn,
    correlation,
    remainingParts,
  );
}

function buildCorrelatedScalarAggregateRewrite(
  subquery: SelectAst,
  aggregateColumn: SelectColumnAst,
  correlation: {
    outer: { alias: string; column: string };
    inner: { alias: string; column: string };
  },
  remainingParts: ExpressionAst[],
): SupportedCorrelatedScalarAggregateProjectionRewrite {
  const correlationOutput = "__tupl_scalar_corr_key";
  const metricOutput = "__tupl_scalar_metric";
  const innerColumnRef: ExpressionAst = {
    type: "column_ref",
    table: correlation.inner.alias,
    column: correlation.inner.column,
  };
  const { where: _ignoredWhere, ...subqueryWithoutWhere } = subquery;

  return {
    subquery,
    rewrittenSubquery:
      remainingParts.length > 0
        ? {
            ...subqueryWithoutWhere,
            columns: [
              {
                expr: innerColumnRef,
                as: correlationOutput,
              },
              {
                ...aggregateColumn,
                as: metricOutput,
              },
            ],
            groupby: {
              columns: [innerColumnRef],
            },
            where: rebuildAndParts(remainingParts),
          }
        : {
            ...subqueryWithoutWhere,
            columns: [
              {
                expr: innerColumnRef,
                as: correlationOutput,
              },
              {
                ...aggregateColumn,
                as: metricOutput,
              },
            ],
            groupby: {
              columns: [innerColumnRef],
            },
          },
    outerKey: correlation.outer,
    innerKey: correlation.inner,
    correlationOutput,
    metricOutput,
  };
}
