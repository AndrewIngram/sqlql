import { Result } from "better-result";

import {
  isRelProjectColumnMapping,
  type RelColumnRef,
  type RelExpr,
  type RelNode,
  type RelScanNode,
} from "@tupl/foundation";
import {
  getNormalizedColumnBindings,
  getNormalizedTableBinding,
  isNormalizedSourceColumnBinding,
  type SchemaDefinition,
  type NormalizedColumnBinding,
  type NormalizedPhysicalTableBinding,
} from "@tupl/schema-model";
import { compileViewRelForPlanner } from "./view-lowering";
import { nextRelId } from "./planner-ids";
import { toTuplPlanningError } from "./planner-errors";
import type { ViewAliasColumnMap } from "./planner-types";

interface ViewExpansionResult {
  node: RelNode;
  aliases: Map<string, ViewAliasColumnMap>;
}

/**
 * View expansion owns planner-side lowering of normalized view bindings into ordinary RelNode trees.
 */
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
      let current = compileViewRelForPlanner(binding.rel(context as unknown), schema, nextRelId);
      const expandedView = expandRelViewsInternal(current, schema, context);
      current = expandedView.node;

      const columnBindings = getNormalizedColumnBindings(binding);
      let viewAliasMapping: ViewAliasColumnMap;
      if (needsPlannerViewProjection(binding)) {
        current = buildPlannerViewProjection(alias, current, binding, expandedView.aliases);
        viewAliasMapping = Object.fromEntries(
          Object.keys(columnBindings).map((column) => [column, { alias, column }]),
        );
      } else {
        viewAliasMapping = {};
        for (const [logicalColumn, source] of Object.entries(columnBindings)) {
          if (!isNormalizedSourceColumnBinding(source)) {
            throw new Error(
              "Planner view projection was skipped for a calculated view column binding.",
            );
          }
          viewAliasMapping[logicalColumn] = resolveViewSourceRef(
            source.source,
            expandedView.aliases,
          );
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
      return {
        node: {
          ...node,
          input: input.node,
          groupBy: node.groupBy.map((column) => resolveMappedColumnRef(column, input.aliases)),
          metrics: node.metrics.map((metric) => ({
            ...metric,
            ...(metric.column
              ? { column: resolveMappedColumnRef(metric.column, input.aliases) }
              : {}),
          })),
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

function needsPlannerViewProjection(
  binding: Parameters<typeof getNormalizedColumnBindings>[0],
): boolean {
  const columnBindings = getNormalizedColumnBindings(binding);
  return Object.values(columnBindings).some(
    (columnBinding) => !isNormalizedSourceColumnBinding(columnBinding),
  );
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
