import type { RelExpr, RelNode } from "@tupl/foundation";
import {
  getNormalizedColumnBindings,
  isNormalizedSourceColumnBinding,
  type NormalizedColumnBinding,
} from "@tupl/schema-model";

import { nextRelId } from "./planner-ids";
import type { ViewAliasColumnMap } from "./planner-types";
import { resolveMappedColumnRef, resolveViewSourceRef } from "./view-aliases";

/**
 * Planner view projection owns explicit projection building for calculated normalized view columns.
 */
export function buildPlannerViewProjection(
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

export function needsPlannerViewProjection(
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
