import type { RelColumnRef, RelNode } from "@tupl/foundation";

import { nextRelId } from "../planner-ids";
import type { PreparedSimpleSelect } from "./select-shape";
import { appendProjectExpressions } from "./select-projections";

/**
 * Select projection lowering owns aggregate/window/sort wrapping and the final SELECT projection.
 */
export function finalizeSimpleSelectRel(current: RelNode, shape: PreparedSimpleSelect): RelNode {
  let next = current;

  if (shape.aggregateMode && shape.aggregateGroupByResolution.materializations.length > 0) {
    next = appendProjectExpressions(next, shape.aggregateGroupByResolution.materializations);
  }

  if (shape.aggregateMode) {
    next = {
      id: nextRelId("aggregate"),
      kind: "aggregate",
      convention: "local",
      input: next,
      groupBy: shape.effectiveGroupBy,
      metrics: shape.allAggregateMetrics,
      output: [
        ...shape.effectiveGroupBy.map((ref: RelColumnRef) => ({ name: ref.column })),
        ...shape.allAggregateMetrics.map((metric) => ({ name: metric.as })),
      ],
    };
  }

  if (shape.havingExpr) {
    next = {
      id: nextRelId("filter"),
      kind: "filter",
      convention: "local",
      input: next,
      expr: shape.havingExpr,
      output: next.output,
    };
  }

  if (!shape.aggregateMode && shape.windowFunctions.length > 0) {
    next = {
      id: nextRelId("window"),
      kind: "window",
      convention: "local",
      input: next,
      functions: shape.windowFunctions,
      output: [...next.output, ...shape.windowFunctions.map((fn) => ({ name: fn.as }))],
    };
  }

  if (!shape.aggregateMode && shape.orderByMaterializations.length > 0) {
    next = appendProjectExpressions(next, shape.orderByMaterializations);
  }

  if (shape.orderBy.length > 0) {
    next = {
      id: nextRelId("sort"),
      kind: "sort",
      convention: "local",
      input: next,
      orderBy: shape.orderBy.map((term) => ({
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
      output: next.output,
    };
  }

  if (shape.limit != null || shape.offset != null) {
    next = {
      id: nextRelId("limit_offset"),
      kind: "limit_offset",
      convention: "local",
      input: next,
      ...(shape.limit != null ? { limit: shape.limit } : {}),
      ...(shape.offset != null ? { offset: shape.offset } : {}),
      output: next.output,
    };
  }

  return buildFinalProject(next, shape);
}

function buildFinalProject(current: RelNode, shape: PreparedSimpleSelect): RelNode {
  return {
    id: nextRelId("project"),
    kind: "project",
    convention: "local",
    input: current,
    columns: shape.aggregateMode
      ? shape.safeAggregateProjections.map((projection) =>
          projection.kind === "group" && projection.source
            ? {
                kind: "column" as const,
                source: { column: projection.source.column },
                output: projection.output,
              }
            : projection.kind === "metric"
              ? {
                  kind: "column" as const,
                  source: { column: projection.metric.as },
                  output: projection.output,
                }
              : {
                  kind: "expr" as const,
                  expr: projection.expr!,
                  output: projection.output,
                },
        )
      : shape.safeProjections.map((projection) => ({
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
                      ? { column: projection.source!.column }
                      : { column: projection.function.as },
              }),
          output: projection.output,
        })),
    output: (shape.aggregateMode ? shape.safeAggregateProjections : shape.safeProjections).map(
      (projection) => ({
        name: projection.output,
      }),
    ),
  };
}
