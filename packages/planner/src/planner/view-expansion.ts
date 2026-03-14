import { Result } from "better-result";

import type { RelNode } from "@tupl/foundation";
import type { SchemaDefinition } from "@tupl/schema-model";

import { toTuplPlanningError } from "./planner-errors";
import type { ViewExpansionResult } from "./views/view-expansion-types";
import { rewriteExpandedViewNode } from "./views/view-node-rewriting";
import { expandViewScanNode } from "./views/view-scan-expansion";

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
    case "scan":
      return expandViewScanNode(node, schema, context, expandRelViewsInternal);
    case "values":
    case "sql":
      return {
        node,
        aliases: new Map(),
      };
    default:
      return rewriteExpandedViewNode(node, schema, context, expandRelViewsInternal);
  }
}
