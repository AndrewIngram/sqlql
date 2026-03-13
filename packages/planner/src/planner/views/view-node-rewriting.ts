import { isRelProjectColumnMapping, type RelNode } from "@tupl/foundation";
import type { SchemaDefinition } from "@tupl/schema-model";

import type { ViewExpansionResult } from "./view-expansion-types";
import {
  mapRelExprRefs,
  mergeAliasMaps,
  resolveMappedColumnRef,
  rewriteColumnNameWithAliases,
} from "./view-aliases";

/**
 * View node rewriting owns alias-aware recursive rewriting of non-scan relational nodes.
 */
export function rewriteExpandedViewNode<TContext>(
  node: Exclude<RelNode, { kind: "scan" | "sql" }>,
  schema: SchemaDefinition,
  context: TContext | undefined,
  expandRelViewsInternal: (
    node: RelNode,
    schema: SchemaDefinition,
    context?: TContext,
  ) => ViewExpansionResult,
): ViewExpansionResult {
  switch (node.kind) {
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
    case "repeat_union": {
      const seed = expandRelViewsInternal(node.seed, schema, context);
      const iterative = expandRelViewsInternal(node.iterative, schema, context);
      return {
        node: {
          ...node,
          seed: seed.node,
          iterative: iterative.node,
        },
        aliases: mergeAliasMaps(seed.aliases, iterative.aliases),
      };
    }
    case "with": {
      const cteAliases: Array<ViewExpansionResult["aliases"]> = [];
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
  }
}
