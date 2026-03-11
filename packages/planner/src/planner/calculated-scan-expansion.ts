import type { RelExpr, RelNode, RelScanNode } from "@tupl/foundation";
import {
  getNormalizedColumnBindings,
  isNormalizedSourceColumnBinding,
  type NormalizedPhysicalTableBinding,
} from "@tupl/schema-model";

import { nextRelId } from "./planner-ids";
import type { ViewAliasColumnMap } from "./planner-types";

/**
 * Calculated scan expansion owns rewriting physical scans with calculated columns into local rels.
 */
export function hasCalculatedColumns(binding: NormalizedPhysicalTableBinding): boolean {
  return Object.values(getNormalizedColumnBindings(binding)).some(
    (columnBinding) => !isNormalizedSourceColumnBinding(columnBinding),
  );
}

export function expandCalculatedScan(
  node: RelScanNode,
  binding: NormalizedPhysicalTableBinding,
): { node: RelNode; aliases: Map<string, ViewAliasColumnMap> } | null {
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

  current = {
    id: nextRelId("project"),
    kind: "project",
    convention: "local",
    input: current,
    columns: [...referencedColumns].map((column) => {
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
    }),
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
