import type { SchemaDefinition } from "@tupl/schema";

interface SqlBindingInfo {
  table: string;
  isCte: boolean;
}

function readCteName(raw: unknown): string | null {
  const name = (raw as { name?: unknown }).name;
  if (typeof name === "string" && name.length > 0) {
    return name;
  }
  const nested = (name as { value?: unknown })?.value;
  if (typeof nested === "string" && nested.length > 0) {
    return nested;
  }
  return null;
}

function validateWindowSpecification(
  rawSpec: unknown,
  bindings: Map<string, SqlBindingInfo>,
  schema: SchemaDefinition,
  availableCtes: Set<string>,
): string | null {
  if (!rawSpec || typeof rawSpec !== "object") {
    return null;
  }

  const partitionBy = (rawSpec as { partitionby?: unknown }).partitionby;
  if (Array.isArray(partitionBy)) {
    for (const part of partitionBy) {
      const issue = validateExpressionReferences(
        (part as { expr?: unknown }).expr,
        bindings,
        schema,
        availableCtes,
        true,
      );
      if (issue) {
        return issue;
      }
    }
  }

  const orderBy = (rawSpec as { orderby?: unknown }).orderby;
  if (Array.isArray(orderBy)) {
    for (const term of orderBy) {
      const issue = validateExpressionReferences(
        (term as { expr?: unknown }).expr,
        bindings,
        schema,
        availableCtes,
        true,
      );
      if (issue) {
        return issue;
      }
    }
  }

  return null;
}

function validateExpressionReferences(
  rawExpr: unknown,
  bindings: Map<string, SqlBindingInfo>,
  schema: SchemaDefinition,
  availableCtes: Set<string>,
  allowUnqualified: boolean,
): string | null {
  if (!rawExpr || typeof rawExpr !== "object") {
    return null;
  }

  const maybeSubquery = (rawExpr as { ast?: unknown }).ast;
  if (maybeSubquery && typeof maybeSubquery === "object") {
    return validateSelectReferences(maybeSubquery, schema, availableCtes);
  }

  const expr = rawExpr as { type?: unknown; table?: unknown; column?: unknown };
  if (expr.type === "column_ref") {
    const table = typeof expr.table === "string" && expr.table.length > 0 ? expr.table : null;
    const column = typeof expr.column === "string" ? expr.column : "";
    if (!column || column === "*") {
      return null;
    }

    if (table) {
      const binding = bindings.get(table);
      if (!binding) {
        return `Unknown table alias: ${table}`;
      }
      if (binding.isCte) {
        return null;
      }
      const tableDef = schema.tables[binding.table];
      if (!tableDef) {
        return `Unknown table: ${binding.table}`;
      }
      if (!(column in tableDef.columns)) {
        return `Unknown column: ${table}.${column}`;
      }
      return null;
    }

    if (!allowUnqualified) {
      return null;
    }

    if (bindings.size === 1) {
      const binding = [...bindings.values()][0];
      if (!binding || binding.isCte) {
        return null;
      }
      const tableDef = schema.tables[binding.table];
      if (!tableDef) {
        return `Unknown table: ${binding.table}`;
      }
      if (!(column in tableDef.columns)) {
        return `Unknown column: ${column}`;
      }
    }
    return null;
  }

  const binary = rawExpr as { type?: unknown; left?: unknown; right?: unknown };
  if (binary.type === "binary_expr") {
    const leftIssue = validateExpressionReferences(
      binary.left,
      bindings,
      schema,
      availableCtes,
      allowUnqualified,
    );
    if (leftIssue) {
      return leftIssue;
    }
    return validateExpressionReferences(
      binary.right,
      bindings,
      schema,
      availableCtes,
      allowUnqualified,
    );
  }

  const exprList =
    (rawExpr as { type?: unknown; value?: unknown }).type === "expr_list"
      ? (rawExpr as { value?: unknown }).value
      : undefined;
  if (Array.isArray(exprList)) {
    for (const item of exprList) {
      const issue = validateExpressionReferences(
        item,
        bindings,
        schema,
        availableCtes,
        allowUnqualified,
      );
      if (issue) {
        return issue;
      }
    }
  }

  const argsRaw = (rawExpr as { args?: { value?: unknown } }).args?.value;
  const args = Array.isArray(argsRaw) ? argsRaw : argsRaw != null ? [argsRaw] : [];
  for (const arg of args) {
    const issue = validateExpressionReferences(
      arg,
      bindings,
      schema,
      availableCtes,
      allowUnqualified,
    );
    if (issue) {
      return issue;
    }
  }

  const overClause = (rawExpr as { over?: unknown }).over;
  if (overClause && typeof overClause === "object") {
    const spec = (overClause as { as_window_specification?: unknown }).as_window_specification;
    if (spec && typeof spec === "object") {
      const issue = validateWindowSpecification(
        (spec as { window_specification?: unknown }).window_specification,
        bindings,
        schema,
        availableCtes,
      );
      if (issue) {
        return issue;
      }
    }
  }

  return null;
}

export function validateSelectReferences(
  rawAst: unknown,
  schema: SchemaDefinition,
  parentCteNames: Set<string>,
): string | null {
  if (!rawAst || typeof rawAst !== "object") {
    return null;
  }

  const ast = rawAst as {
    with?: unknown;
    from?: unknown;
    columns?: unknown;
    where?: unknown;
    groupby?: { columns?: unknown };
    having?: unknown;
    orderby?: unknown;
    window?: unknown;
    _next?: unknown;
  };

  const withClauses = Array.isArray(ast.with) ? ast.with : [];
  const localCteNames = withClauses
    .map((entry) => readCteName(entry))
    .filter((name): name is string => typeof name === "string");
  const availableCtes = new Set<string>([...parentCteNames, ...localCteNames]);

  for (const entry of withClauses) {
    const cteAst = (entry as { stmt?: { ast?: unknown } }).stmt?.ast;
    const issue = validateSelectReferences(cteAst, schema, availableCtes);
    if (issue) {
      return issue;
    }
  }

  const bindings = new Map<string, SqlBindingInfo>();
  const fromEntries = Array.isArray(ast.from) ? ast.from : [];
  for (const rawFrom of fromEntries) {
    const entry = rawFrom as { table?: unknown; as?: unknown; on?: unknown };
    if (typeof entry.table === "string" && entry.table.length > 0) {
      const table = entry.table;
      const isCte = availableCtes.has(table);
      if (!isCte && !schema.tables[table]) {
        return `Unknown table: ${table}`;
      }

      const alias = typeof entry.as === "string" && entry.as.length > 0 ? entry.as : table;
      bindings.set(alias, {
        table,
        isCte,
      });
    }

    const onIssue = validateExpressionReferences(entry.on, bindings, schema, availableCtes, true);
    if (onIssue) {
      return onIssue;
    }
  }

  const columns = ast.columns;
  if (Array.isArray(columns)) {
    for (const column of columns) {
      const issue = validateExpressionReferences(
        (column as { expr?: unknown }).expr,
        bindings,
        schema,
        availableCtes,
        true,
      );
      if (issue) {
        return issue;
      }
    }
  }

  const whereIssue = validateExpressionReferences(ast.where, bindings, schema, availableCtes, true);
  if (whereIssue) {
    return whereIssue;
  }

  const groupByColumns = Array.isArray(ast.groupby?.columns) ? ast.groupby?.columns : [];
  for (const column of groupByColumns) {
    const issue = validateExpressionReferences(column, bindings, schema, availableCtes, true);
    if (issue) {
      return issue;
    }
  }

  const havingIssue = validateExpressionReferences(
    ast.having,
    bindings,
    schema,
    availableCtes,
    false,
  );
  if (havingIssue) {
    return havingIssue;
  }

  const orderByTerms = Array.isArray(ast.orderby) ? ast.orderby : [];
  for (const term of orderByTerms) {
    const issue = validateExpressionReferences(
      (term as { expr?: unknown }).expr,
      bindings,
      schema,
      availableCtes,
      false,
    );
    if (issue) {
      return issue;
    }
  }

  const windowEntries = Array.isArray(ast.window) ? ast.window : [];
  for (const entry of windowEntries) {
    const spec = (entry as { as_window_specification?: { window_specification?: unknown } })
      .as_window_specification?.window_specification;
    const issue = validateWindowSpecification(spec, bindings, schema, availableCtes);
    if (issue) {
      return issue;
    }
  }

  const nextIssue = validateSelectReferences(ast._next, schema, availableCtes);
  if (nextIssue) {
    return nextIssue;
  }

  return null;
}

export { relContainsSqlNode as hasSqlNode } from "@tupl/foundation";
