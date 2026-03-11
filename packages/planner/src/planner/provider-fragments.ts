import { Result } from "better-result";

import { type RelNode } from "@tupl/foundation";
import { type SchemaDefinition } from "@tupl/schema-model";
import type { ProviderFragment } from "@tupl/provider-kit";
import { buildAggregateProviderFragment } from "./aggregate/aggregate-provider-fragment";
import { toTuplPlanningError } from "./planner-errors";
import { resolveSingleProvider } from "./conventions";
import { normalizeRelForProvider } from "./provider/provider-rel-normalization";
import { normalizeScanForProvider } from "./provider/provider-scan-normalization";
import { expandRelViewsResult } from "./view-expansion";

/**
 * Provider fragments own normalization of planner nodes into provider-facing fragment requests.
 */
export function buildProviderFragmentForRel<TContext = unknown>(
  node: RelNode,
  schema: SchemaDefinition,
  context?: TContext,
): ProviderFragment | null {
  const result = buildProviderFragmentForRelResult(node, schema, context);
  if (Result.isError(result)) {
    throw result.error;
  }
  return result.value;
}

export function buildProviderFragmentForRelResult<TContext = unknown>(
  node: RelNode,
  schema: SchemaDefinition,
  context?: TContext,
) {
  return Result.gen(function* () {
    const expanded = yield* expandRelViewsResult(node, schema, context);
    const provider = resolveSingleProvider(expanded, schema);
    if (!provider) {
      return Result.ok(null);
    }

    return buildProviderFragmentForNodeResult(expanded, schema, provider);
  });
}

export function buildProviderFragmentForNodeResult(
  node: RelNode,
  schema: SchemaDefinition,
  provider: string,
) {
  return Result.try({
    try: () => buildProviderFragmentForNode(node, schema, provider),
    catch: (error) => toTuplPlanningError(error, "build provider fragment"),
  });
}

function buildProviderFragmentForNode(
  node: RelNode,
  schema: SchemaDefinition,
  provider: string,
): ProviderFragment {
  if (node.kind === "scan") {
    const normalizedScan = normalizeScanForProvider(node, schema);
    return {
      kind: "scan",
      provider,
      table: normalizedScan.table,
      request: {
        table: normalizedScan.table,
        ...(normalizedScan.alias ? { alias: normalizedScan.alias } : {}),
        select: normalizedScan.select,
        ...(normalizedScan.where ? { where: normalizedScan.where } : {}),
        ...(normalizedScan.orderBy ? { orderBy: normalizedScan.orderBy } : {}),
        ...(normalizedScan.limit != null ? { limit: normalizedScan.limit } : {}),
        ...(normalizedScan.offset != null ? { offset: normalizedScan.offset } : {}),
      },
    };
  }

  if (node.kind === "aggregate") {
    const aggregateFragment = buildAggregateProviderFragment(node, schema, provider);
    if (aggregateFragment) {
      return aggregateFragment;
    }
  }

  return {
    kind: "rel",
    provider,
    rel: normalizeRelForProvider(node, schema),
  };
}
