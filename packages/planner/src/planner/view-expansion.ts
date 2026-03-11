import { Result } from "better-result";

import { isRelProjectColumnMapping, type RelNode } from "@tupl/foundation";
import {
  getNormalizedColumnBindings,
  getNormalizedTableBinding,
  isNormalizedSourceColumnBinding,
  type SchemaDefinition,
} from "@tupl/schema-model";
import { expandCalculatedScan, hasCalculatedColumns } from "./views/calculated-scan-expansion";
import { compileViewRelForPlanner } from "./view-lowering";
import { nextRelId } from "./planner-ids";
import { toTuplPlanningError } from "./planner-errors";
import { buildPlannerViewProjection, needsPlannerViewProjection } from "./views/view-projection";
import type { ViewAliasColumnMap } from "./planner-types";
import {
  mapRelExprRefs,
  mapViewColumnName,
  mergeAliasMaps,
  parseRelColumnRef,
  resolveMappedColumnRef,
  resolveViewSourceRef,
  rewriteColumnNameWithAliases,
} from "./views/view-aliases";

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
