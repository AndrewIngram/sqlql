import { Result } from "better-result";

import { RelRewriteError, type RelNode } from "@tupl/foundation";
import { type SchemaDefinition } from "@tupl/schema-model";
import { isNormalizedSourceColumnBinding } from "@tupl/schema-model/mapping";
import {
  getNormalizedColumnBindings,
  getNormalizedTableBinding,
} from "@tupl/schema-model/normalization";

import { compileViewRelForPlanner } from "./view-lowering";
import { nextRelId } from "../physical/planner-ids";
import { expandCalculatedScan, hasCalculatedColumns } from "./calculated-scan-expansion";
import { buildPlannerViewProjection, needsPlannerViewProjection } from "./view-projection";
import type { ViewExpansionResult, ViewExpansionResultValue } from "./view-expansion-types";
import {
  mapViewColumnName,
  mergeAliasMaps,
  parseRelColumnRef,
  resolveViewSourceRef,
} from "./view-aliases";

/**
 * View scan expansion owns planner-side lowering of scans over normalized schema views.
 */
export function expandViewScanNode<TContext>(
  node: Extract<RelNode, { kind: "scan" }>,
  schema: SchemaDefinition,
  context: TContext | undefined,
  expandRelViewsInternal: (
    node: RelNode,
    schema: SchemaDefinition,
    context?: TContext,
  ) => ViewExpansionResultValue,
): ViewExpansionResultValue {
  const binding = getNormalizedTableBinding(schema, node.table);
  if (binding?.kind === "physical" && hasCalculatedColumns(binding)) {
    const expanded = expandCalculatedScan(node, binding);
    if (expanded) {
      return Result.ok(expanded);
    }
  }

  if (!binding || binding.kind !== "view") {
    return Result.ok({
      node,
      aliases: new Map(),
    });
  }

  const alias = node.alias ?? node.table;
  return Result.gen(function* () {
    let current = yield* compileViewRelForPlanner(
      binding.rel(context as unknown),
      schema,
      nextRelId,
    );
    const expandedView = yield* expandRelViewsInternal(current, schema, context);
    current = expandedView.node;

    const columnBindings = getNormalizedColumnBindings(binding);
    let viewAliasMapping: ViewExpansionResult["aliases"] extends Map<string, infer T> ? T : never;
    if (needsPlannerViewProjection(binding)) {
      current = buildPlannerViewProjection(alias, current, binding, expandedView.aliases);
      viewAliasMapping = Object.fromEntries(
        Object.keys(columnBindings).map((column) => [column, { alias, column }]),
      );
    } else {
      viewAliasMapping = {};
      for (const [logicalColumn, source] of Object.entries(columnBindings)) {
        if (!isNormalizedSourceColumnBinding(source)) {
          return Result.err(
            new RelRewriteError({
              operation: "expand relational views",
              message: "Planner view projection was skipped for a calculated view column binding.",
            }),
          );
        }
        viewAliasMapping[logicalColumn] = resolveViewSourceRef(source.source, expandedView.aliases);
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
    return Result.ok({
      node: current,
      aliases,
    });
  });
}
