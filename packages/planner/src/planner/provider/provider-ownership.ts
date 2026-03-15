import type { RelNode, RelScanNode } from "@tupl/foundation";
import { Result } from "better-result";
import type { SchemaDefinition } from "@tupl/schema-model";
import { getNormalizedTableBinding, resolveTableProvider } from "@tupl/schema-model/normalization";

/**
 * These nodes are execution-locality barriers, not syntax restrictions. They may appear in valid
 * logical plans, but under the current runtime model they cannot be part of provider-owned
 * fragments because their rows are produced or materialized locally.
 */
export function isLocalOnlyProviderBarrierNode(node: RelNode): boolean {
  switch (node.kind) {
    case "values":
    case "cte_ref":
    case "correlate":
    case "repeat_union":
      return true;
    default:
      return false;
  }
}

export function resolveScanProviderName(
  scan: RelScanNode,
  schema: SchemaDefinition,
): string | null {
  if (!schema.tables[scan.table] && !scan.entity) {
    return null;
  }

  const normalized = getNormalizedTableBinding(schema, scan.table);
  if (normalized?.kind === "view") {
    return null;
  }

  const providerName = scan.entity?.provider ?? resolveTableProvider(schema, scan.table);
  const providerNameResult =
    typeof providerName === "string" ? Result.ok(providerName) : providerName;
  if (Result.isError(providerNameResult)) {
    return null;
  }

  return providerNameResult.value;
}

export function resolveSingleProvider(node: RelNode, schema: SchemaDefinition): string | null {
  const providers = new Set<string>();

  const visit = (current: RelNode): boolean => {
    if (isLocalOnlyProviderBarrierNode(current)) {
      return false;
    }

    switch (current.kind) {
      case "scan": {
        const provider = resolveScanProviderName(current, schema);
        if (provider) {
          providers.add(provider);
          return true;
        }

        return !schema.tables[current.table] && !current.entity;
      }
      case "filter":
      case "project":
      case "aggregate":
      case "window":
      case "sort":
      case "limit_offset":
        return visit(current.input);
      case "join":
      case "set_op":
        return visit(current.left) && visit(current.right);
      case "with":
        for (const cte of current.ctes) {
          if (!visit(cte.query)) {
            return false;
          }
        }
        return visit(current.body);
      default:
        return false;
    }
  };

  if (!visit(node) || providers.size !== 1) {
    return null;
  }

  return [...providers][0] ?? null;
}
