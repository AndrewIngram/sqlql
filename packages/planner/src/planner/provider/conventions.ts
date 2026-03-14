import type { RelJoinNode, RelNode, RelScanNode } from "@tupl/foundation";
import type { ProvidersMap } from "@tupl/provider-kit";
import { Result } from "better-result";
import {
  getNormalizedTableBinding,
  resolveTableProvider,
  type SchemaDefinition,
} from "@tupl/schema-model";

/**
 * Conventions own provider assignment and lookup-join viability analysis for planner nodes.
 */
export function resolveSingleProvider(node: RelNode, schema: SchemaDefinition): string | null {
  const providers = new Set<string>();

  const visit = (current: RelNode): boolean => {
    switch (current.kind) {
      case "values":
        return false;
      case "cte_ref":
        return true;
      case "scan": {
        if (!schema.tables[current.table] && !current.entity) {
          return true;
        }
        const normalized = getNormalizedTableBinding(schema, current.table);
        if (normalized?.kind === "view") {
          return false;
        }
        const providerName =
          current.entity?.provider ?? resolveTableProvider(schema, current.table);
        const providerNameResult =
          typeof providerName === "string" ? Result.ok(providerName) : providerName;
        if (Result.isError(providerNameResult)) {
          return false;
        }
        providers.add(providerNameResult.value);
        return true;
      }
      case "filter":
      case "project":
      case "aggregate":
      case "window":
      case "sort":
      case "limit_offset":
        return visit(current.input);
      case "correlate":
        return false;
      case "join":
      case "set_op":
        return visit(current.left) && visit(current.right);
      case "repeat_union":
        return false;
      case "with":
        for (const cte of current.ctes) {
          if (!visit(cte.query)) {
            return false;
          }
        }
        return visit(current.body);
    }
  };

  if (!visit(node) || providers.size !== 1) {
    return null;
  }
  return [...providers][0] ?? null;
}

export function assignConventions(node: RelNode, schema: SchemaDefinition): RelNode {
  switch (node.kind) {
    case "values":
    case "cte_ref":
      return { ...node, convention: "local" };
    case "scan": {
      if (!schema.tables[node.table] && !node.entity) {
        return { ...node, convention: "local" };
      }
      const normalized = getNormalizedTableBinding(schema, node.table);
      if (normalized?.kind === "view") {
        return { ...node, convention: "local" };
      }
      const providerName = node.entity?.provider ?? resolveTableProvider(schema, node.table);
      const providerNameResult =
        typeof providerName === "string" ? Result.ok(providerName) : providerName;
      if (Result.isError(providerNameResult)) {
        return { ...node, convention: "local" };
      }
      return {
        ...node,
        convention: `provider:${providerNameResult.value}`,
      };
    }
    case "filter":
    case "project":
    case "aggregate":
    case "window":
    case "sort":
    case "limit_offset": {
      const input = assignConventions(node.input, schema);
      const provider = resolveSingleProvider(input, schema);
      return {
        ...node,
        input,
        convention: provider ? (`provider:${provider}` as const) : "local",
      };
    }
    case "correlate": {
      const left = assignConventions(node.left, schema);
      const right = assignConventions(node.right, schema);
      return {
        ...node,
        left,
        right,
        convention: "local",
      };
    }
    case "join":
    case "set_op": {
      const left = assignConventions(node.left, schema);
      const right = assignConventions(node.right, schema);
      const provider = resolveSingleProvider({ ...node, left, right } as RelNode, schema);
      return {
        ...node,
        left,
        right,
        convention: provider ? (`provider:${provider}` as const) : "local",
      };
    }
    case "repeat_union": {
      const seed = assignConventions(node.seed, schema);
      const iterative = assignConventions(node.iterative, schema);
      return {
        ...node,
        seed,
        iterative,
        convention: "local",
      };
    }
    case "with": {
      const ctes = node.ctes.map((cte) => ({
        ...cte,
        query: assignConventions(cte.query, schema),
      }));
      const body = assignConventions(node.body, schema);
      const provider = resolveSingleProvider(body, schema);
      return {
        ...node,
        ctes,
        body,
        convention: provider ? (`provider:${provider}` as const) : "local",
      };
    }
  }
}

export function resolveLookupJoinCandidate<TContext>(
  join: RelJoinNode,
  schema: SchemaDefinition,
  _providers: ProvidersMap<TContext>,
): {
  leftProvider: string;
  rightProvider: string;
  leftScan: RelScanNode;
  rightScan: RelScanNode;
  leftKey: string;
  rightKey: string;
  joinType: "inner" | "left";
} | null {
  if (join.joinType !== "inner" && join.joinType !== "left") {
    return null;
  }

  const leftScan = findFirstScanNode(join.left);
  const rightScan = findFirstScanNode(join.right);
  if (!leftScan || !rightScan) {
    return null;
  }

  const leftBinding = getNormalizedTableBinding(schema, leftScan.table);
  const rightBinding = getNormalizedTableBinding(schema, rightScan.table);
  if (leftBinding?.kind === "view" || rightBinding?.kind === "view") {
    return null;
  }

  const leftProviderName =
    leftScan.entity?.provider ?? resolveTableProvider(schema, leftScan.table);
  const rightProviderName =
    rightScan.entity?.provider ?? resolveTableProvider(schema, rightScan.table);
  const leftProviderResult =
    typeof leftProviderName === "string" ? Result.ok(leftProviderName) : leftProviderName;
  const rightProviderResult =
    typeof rightProviderName === "string" ? Result.ok(rightProviderName) : rightProviderName;
  if (Result.isError(leftProviderResult) || Result.isError(rightProviderResult)) {
    return null;
  }
  const leftProvider = leftProviderResult.value;
  const rightProvider = rightProviderResult.value;
  if (leftProvider === rightProvider) {
    return null;
  }

  return {
    leftProvider,
    rightProvider,
    leftScan,
    rightScan,
    leftKey: join.leftKey.column,
    rightKey: join.rightKey.column,
    joinType: join.joinType,
  };
}

function findFirstScanNode(node: RelNode): RelScanNode | null {
  switch (node.kind) {
    case "scan":
      return node;
    case "values":
    case "cte_ref":
      return null;
    case "filter":
    case "project":
    case "aggregate":
    case "window":
    case "sort":
    case "limit_offset":
      return findFirstScanNode(node.input);
    case "correlate":
      return findFirstScanNode(node.left) ?? findFirstScanNode(node.right);
    case "join":
    case "set_op":
      return findFirstScanNode(node.left) ?? findFirstScanNode(node.right);
    case "repeat_union":
      return findFirstScanNode(node.seed) ?? findFirstScanNode(node.iterative);
    case "with":
      return findFirstScanNode(node.body);
  }
}
