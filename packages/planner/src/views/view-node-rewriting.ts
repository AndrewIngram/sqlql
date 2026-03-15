import { Result } from "better-result";

import { isRelProjectColumnMapping, type RelNode } from "@tupl/foundation";
import type { SchemaDefinition } from "@tupl/schema-model";

import type { ViewExpansionResult, ViewExpansionResultValue } from "./view-expansion-types";
import {
  mapRelExprRefs,
  mergeAliasMaps,
  resolveMappedColumnRef,
  rewriteColumnNameWithAliases,
} from "./view-aliases";

/**
 * View node rewriting owns alias-aware recursive rewriting of non-scan relational nodes after
 * view scans have been expanded. Its job is to preserve logical column semantics across the
 * lowered subtree; it intentionally does not perform planner validation or physical ownership
 * decisions.
 */
export function rewriteExpandedViewNode<TContext>(
  node: Exclude<RelNode, { kind: "scan" | "values" | "cte_ref" }>,
  schema: SchemaDefinition,
  context: TContext | undefined,
  expandRelViewsInternal: (
    node: RelNode,
    schema: SchemaDefinition,
    context?: TContext,
  ) => ViewExpansionResultValue,
): ViewExpansionResultValue {
  switch (node.kind) {
    case "filter": {
      return Result.gen(function* () {
        const input = yield* expandRelViewsInternal(node.input, schema, context);
        return Result.ok({
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
        });
      });
    }
    case "project": {
      return Result.gen(function* () {
        const input = yield* expandRelViewsInternal(node.input, schema, context);
        return Result.ok({
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
        });
      });
    }
    case "aggregate": {
      return Result.gen(function* () {
        const input = yield* expandRelViewsInternal(node.input, schema, context);
        return Result.ok({
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
        });
      });
    }
    case "window": {
      return Result.gen(function* () {
        const input = yield* expandRelViewsInternal(node.input, schema, context);
        return Result.ok({
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
              ...("column" in fn && fn.column
                ? { column: resolveMappedColumnRef(fn.column, input.aliases) }
                : {}),
              ...("value" in fn ? { value: mapRelExprRefs(fn.value, input.aliases) } : {}),
              ...("defaultExpr" in fn && fn.defaultExpr
                ? { defaultExpr: mapRelExprRefs(fn.defaultExpr, input.aliases) }
                : {}),
            })),
          },
          aliases: input.aliases,
        });
      });
    }
    case "sort": {
      return Result.gen(function* () {
        const input = yield* expandRelViewsInternal(node.input, schema, context);
        return Result.ok({
          node: {
            ...node,
            input: input.node,
            orderBy: node.orderBy.map((term) => ({
              ...term,
              source: resolveMappedColumnRef(term.source, input.aliases),
            })),
          },
          aliases: input.aliases,
        });
      });
    }
    case "limit_offset": {
      return Result.gen(function* () {
        const input = yield* expandRelViewsInternal(node.input, schema, context);
        return Result.ok({
          node: {
            ...node,
            input: input.node,
          },
          aliases: input.aliases,
        });
      });
    }
    case "correlate": {
      return Result.gen(function* () {
        const left = yield* expandRelViewsInternal(node.left, schema, context);
        const right = yield* expandRelViewsInternal(node.right, schema, context);
        const aliases = mergeAliasMaps(left.aliases, right.aliases);
        return Result.ok({
          node: {
            ...node,
            left: left.node,
            right: right.node,
            correlation: {
              outer: resolveMappedColumnRef(node.correlation.outer, aliases),
              inner: resolveMappedColumnRef(node.correlation.inner, aliases),
            },
            apply:
              node.apply.kind === "scalar_filter"
                ? {
                    ...node.apply,
                    outerCompare: resolveMappedColumnRef(node.apply.outerCompare, aliases),
                  }
                : node.apply,
          },
          aliases,
        });
      });
    }
    case "join": {
      return Result.gen(function* () {
        const left = yield* expandRelViewsInternal(node.left, schema, context);
        const right = yield* expandRelViewsInternal(node.right, schema, context);
        const aliases = mergeAliasMaps(left.aliases, right.aliases);
        return Result.ok({
          node: {
            ...node,
            left: left.node,
            right: right.node,
            leftKey: resolveMappedColumnRef(node.leftKey, aliases),
            rightKey: resolveMappedColumnRef(node.rightKey, aliases),
          },
          aliases,
        });
      });
    }
    case "set_op": {
      return Result.gen(function* () {
        const left = yield* expandRelViewsInternal(node.left, schema, context);
        const right = yield* expandRelViewsInternal(node.right, schema, context);
        return Result.ok({
          node: {
            ...node,
            left: left.node,
            right: right.node,
          },
          aliases: mergeAliasMaps(left.aliases, right.aliases),
        });
      });
    }
    case "repeat_union": {
      return Result.gen(function* () {
        const seed = yield* expandRelViewsInternal(node.seed, schema, context);
        const iterative = yield* expandRelViewsInternal(node.iterative, schema, context);
        return Result.ok({
          node: {
            ...node,
            seed: seed.node,
            iterative: iterative.node,
          },
          aliases: mergeAliasMaps(seed.aliases, iterative.aliases),
        });
      });
    }
    case "with": {
      return Result.gen(function* () {
        const cteAliases: Array<ViewExpansionResult["aliases"]> = [];
        const ctes = [];
        for (const cte of node.ctes) {
          const expanded = yield* expandRelViewsInternal(cte.query, schema, context);
          cteAliases.push(expanded.aliases);
          ctes.push({
            ...cte,
            query: expanded.node,
          });
        }
        const body = yield* expandRelViewsInternal(node.body, schema, context);
        return Result.ok({
          node: {
            ...node,
            ctes,
            body: body.node,
          },
          aliases: mergeAliasMaps(...cteAliases, body.aliases),
        });
      });
    }
  }
}
