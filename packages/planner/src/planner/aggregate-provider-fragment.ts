import type { RelNode, RelScanNode, ScanFilterClause } from "@tupl/foundation";
import type { ProviderFragment } from "@tupl/provider-kit";
import type { SchemaDefinition } from "@tupl/schema-model";

import { mapColumnRefForAlias, collectAliasToSourceMappings } from "./provider-alias-mapping";
import { normalizeScanForProvider } from "./provider-scan-normalization";

/**
 * Aggregate provider fragments own extraction of pushable aggregate requests from scan/filter chains.
 */
export function buildAggregateProviderFragment(
  node: Extract<RelNode, { kind: "aggregate" }>,
  schema: SchemaDefinition,
  provider: string,
): ProviderFragment | null {
  const extracted = extractAggregateProviderInput(node.input);
  if (!extracted) {
    return null;
  }

  const mergedScan: RelScanNode = {
    ...extracted.scan,
    ...(extracted.where.length > 0
      ? {
          where: [...(extracted.scan.where ?? []), ...extracted.where],
        }
      : {}),
  };
  const normalizedScan = normalizeScanForProvider(mergedScan, schema);
  const aliasToSource = collectAliasToSourceMappings(mergedScan, schema);

  return {
    kind: "aggregate",
    provider,
    table: normalizedScan.table,
    request: {
      table: normalizedScan.table,
      ...(normalizedScan.alias ? { alias: normalizedScan.alias } : {}),
      ...(normalizedScan.where?.length ? { where: normalizedScan.where } : {}),
      ...(node.groupBy.length
        ? {
            groupBy: node.groupBy.map(
              (column) => mapColumnRefForAlias(column, aliasToSource).column,
            ),
          }
        : {}),
      metrics: node.metrics.map((metric) => ({
        fn: metric.fn,
        as: metric.as,
        ...(metric.distinct ? { distinct: true } : {}),
        ...(metric.column
          ? {
              column: mapColumnRefForAlias(metric.column, aliasToSource).column,
            }
          : {}),
      })),
    },
  };
}

function extractAggregateProviderInput(node: RelNode): {
  scan: RelScanNode;
  where: ScanFilterClause[];
} | null {
  const where: ScanFilterClause[] = [];
  let current = node;

  while (current.kind === "filter") {
    if (current.expr) {
      return null;
    }
    if (current.where) {
      where.push(...current.where);
    }
    current = current.input;
  }

  if (current.kind !== "scan") {
    return null;
  }

  if (current.orderBy?.length || current.limit != null || current.offset != null) {
    return null;
  }

  return {
    scan: current,
    where,
  };
}
