import type { RelColumnRef, RelExpr } from "@tupl/foundation";

import type { SelectAst } from "./sqlite-parser/ast";
import type { Binding } from "./planner-types";

/**
 * Expr column refs own raw column-ref parsing, bound ref resolution, and select-table collection.
 */
export function toRawColumnRef(raw: unknown): { table: string | null; column: string } | undefined {
  const expr = raw as { type?: unknown; table?: unknown; column?: unknown };
  if (expr?.type !== "column_ref") {
    return undefined;
  }

  if (typeof expr.column !== "string" || expr.column.length === 0 || expr.column === "*") {
    return undefined;
  }

  const table = typeof expr.table === "string" && expr.table.length > 0 ? expr.table : null;
  return { table, column: expr.column };
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
