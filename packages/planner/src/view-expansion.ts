import type { RelNode } from "@tupl/foundation";
import type { SchemaDefinition } from "@tupl/schema-model";

import { Result } from "better-result";

import type { ViewExpansionResultValue } from "./views/view-expansion-types";
import { rewriteExpandedViewNode } from "./views/view-node-rewriting";
import { expandViewScanNode } from "./views/view-scan-expansion";

/**
 * View expansion is the planner rewrite boundary where normalized facade views stop existing as
 * bindings and become ordinary rel subtrees. Execution should never need to understand view
 * bindings directly once this pass has run.
 */
export function expandRelViewsResult<TContext>(
  rel: RelNode,
  schema: SchemaDefinition,
  context?: TContext,
) {
  return Result.gen(function* () {
    const expanded = yield* expandRelViewsInternal(rel, schema, context);
    return Result.ok(expanded.node);
  });
}

function expandRelViewsInternal<TContext>(
  node: RelNode,
  schema: SchemaDefinition,
  context?: TContext,
): ViewExpansionResultValue {
  switch (node.kind) {
    case "scan":
      return expandViewScanNode(node, schema, context, expandRelViewsInternal);
    case "values":
    case "cte_ref":
      return Result.ok({
        node,
        aliases: new Map(),
      });
    default:
      return rewriteExpandedViewNode(node, schema, context, expandRelViewsInternal);
  }
}
