import type { RelColumnRef, RelExpr, RelWindowFunction } from "@tupl/foundation";
import type { SelectAst, WindowClauseEntryAst, WindowSpecificationAst } from "./sqlite-parser/ast";
import type { SchemaDefinition } from "@tupl/schema-model";
import type { Binding } from "./planner-types";
import { nextRelId } from "./planner-ids";

export interface SqlExprLoweringContext {
  schema: SchemaDefinition;
  cteNames: Set<string>;
  tryLowerSelect(ast: SelectAst): import("@tupl/foundation").RelNode | null;
}

/**
 * SQL expression lowering owns translation from parser AST fragments into RelExpr.
 * Callers provide a structured-select callback so subqueries stay planner-owned without
 * coupling this module back to the higher-level select lowering implementation.
 */
export function lowerSqlAstToRelExpr(
  raw: unknown,
  bindings: Binding[],
  aliasToBinding: Map<string, Binding>,
  context: SqlExprLoweringContext,
): RelExpr | null {
  if (!raw || typeof raw !== "object") {
    return null;
  }

  const expr = raw as {
    type?: unknown;
    value?: unknown;
    operator?: unknown;
    left?: unknown;
    right?: unknown;
    name?: unknown;
    args?: { value?: unknown };
    ast?: unknown;
  };

  if ("ast" in expr) {
    return lowerScalarSubqueryExpr(raw, bindings, context);
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
      return lowerBinaryExprToRelExpr(expr, bindings, aliasToBinding, context);
    case "function":
      return lowerFunctionExprToRelExpr(expr, bindings, aliasToBinding, context);
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
  context: SqlExprLoweringContext,
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
    const left = lowerSqlAstToRelExpr(expr.left, bindings, aliasToBinding, context);
    const low = lowerSqlAstToRelExpr(range.value[0], bindings, aliasToBinding, context);
    const high = lowerSqlAstToRelExpr(range.value[1], bindings, aliasToBinding, context);
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
    const left = lowerSqlAstToRelExpr(expr.left, bindings, aliasToBinding, context);
    const values = parseExprListToRelExprArgs(expr.right, bindings, aliasToBinding, context);
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
    const left = lowerSqlAstToRelExpr(expr.left, bindings, aliasToBinding, context);
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

  const left = lowerSqlAstToRelExpr(expr.left, bindings, aliasToBinding, context);
  const right = lowerSqlAstToRelExpr(expr.right, bindings, aliasToBinding, context);
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
  context: SqlExprLoweringContext,
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
    return lowerExistsSubqueryExpr(values[0], bindings, context);
  }

  const args = parseFunctionArgsToRelExpr(expr.args?.value, bindings, aliasToBinding, context);
  if (!args) {
    return null;
  }

  if (normalized === "not") {
    return args.length === 1 ? { kind: "function", name: "not", args } : null;
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
  context: SqlExprLoweringContext,
): RelExpr[] | null {
  if (raw == null) {
    return [];
  }
  const values = Array.isArray(raw) ? raw : [raw];
  const args: RelExpr[] = [];
  for (const value of values) {
    const arg = lowerSqlAstToRelExpr(value, bindings, aliasToBinding, context);
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
  context: SqlExprLoweringContext,
): RelExpr[] | null {
  const expr = raw as { type?: unknown; value?: unknown };
  if (expr?.type !== "expr_list" || !Array.isArray(expr.value)) {
    return null;
  }
  return parseFunctionArgsToRelExpr(expr.value, bindings, aliasToBinding, context);
}

function lowerExistsSubqueryExpr(
  raw: unknown,
  bindings: Binding[],
  context: SqlExprLoweringContext,
): RelExpr | null {
  const subquery = parseSubqueryAst(raw);
  if (!subquery) {
    return null;
  }

  const outerAliases = new Set(bindings.map((binding) => binding.alias));
  if (isCorrelatedSubquery(subquery, outerAliases)) {
    return null;
  }

  const rel = context.tryLowerSelect(subquery);
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
  context: SqlExprLoweringContext,
): RelExpr | null {
  const subquery = parseSubqueryAst(raw);
  if (!subquery) {
    return null;
  }

  const outerAliases = new Set(bindings.map((binding) => binding.alias));
  if (isCorrelatedSubquery(subquery, outerAliases)) {
    return null;
  }

  const rel = context.tryLowerSelect(subquery);
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

export function mapBinaryOperatorToRelFunction(operator: string): string | null {
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

export function readWindowFunctionName(expr: {
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

export function supportsRankWindowArgs(args: unknown): boolean {
  if (!args || typeof args !== "object") {
    return true;
  }
  const value = (args as { value?: unknown }).value;
  if (!Array.isArray(value)) {
    return true;
  }
  return value.length === 0;
}

export function parseNamedWindowSpecifications(
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

export function parseWindowOver(
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
    if (!resolved || resolved.window_frame_clause) {
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

export function parsePositiveOrdinalLiteral(
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

export function toRawColumnRef(raw: unknown): { table: string | null; column: string } | undefined {
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

export function resolveColumnRef(
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

export function parseLiteral(raw: unknown): unknown {
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

export function tryParseLiteralExpressionList(raw: unknown): unknown[] | undefined {
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

export function parseLimitAndOffset(rawLimit: unknown): { limit?: number; offset?: number } {
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

export function collectRelExprRefs(expr: RelExpr): RelColumnRef[] {
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

export function collectTablesFromSelectAst(ast: SelectAst): string[] {
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
