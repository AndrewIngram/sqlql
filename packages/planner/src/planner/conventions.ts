import type { RelJoinNode, RelNode, RelScanNode } from "@tupl/foundation";
import { supportsLookupMany, type ProvidersMap } from "@tupl/provider-kit";
import {
  getNormalizedTableBinding,
  resolveTableProvider,
  type SchemaDefinition,
} from "@tupl/schema-model";

/**
 * Conventions own provider assignment and lookup-join viability analysis for planner nodes.
 */
export function resolveSingleProvider(
  node: RelNode,
  schema: SchemaDefinition,
  cteNames: Set<string> = new Set<string>(),
): string | null {
  const providers = new Set<string>();

  const visit = (current: RelNode, scopedCteNames: Set<string>): boolean => {
    switch (current.kind) {
      case "scan": {
        if (scopedCteNames.has(current.table)) {
          return true;
        }
        if (!schema.tables[current.table] && !current.entity) {
          return true;
        }
        const normalized = getNormalizedTableBinding(schema, current.table);
        if (normalized?.kind === "view") {
          return false;
        }
        providers.add(current.entity?.provider ?? resolveTableProvider(schema, current.table));
        return true;
      }
      case "sql": {
        for (const table of current.tables) {
          if (scopedCteNames.has(table) || !schema.tables[table]) {
            continue;
          }
          const normalized = getNormalizedTableBinding(schema, table);
          if (normalized?.kind === "view") {
            return false;
          }
          providers.add(resolveTableProvider(schema, table));
        }
        return true;
      }
      case "filter":
      case "project":
      case "aggregate":
      case "window":
      case "sort":
      case "limit_offset":
        return visit(current.input, scopedCteNames);
      case "join":
      case "set_op":
        return visit(current.left, scopedCteNames) && visit(current.right, scopedCteNames);
      case "with": {
        const nextScopedCteNames = new Set(scopedCteNames);
        for (const cte of current.ctes) {
          nextScopedCteNames.add(cte.name);
        }
        for (const cte of current.ctes) {
          if (!visit(cte.query, nextScopedCteNames)) {
            return false;
          }
        }
        return visit(current.body, nextScopedCteNames);
      }
    }
  };

  if (!visit(node, cteNames) || providers.size !== 1) {
    return null;
  }
  return [...providers][0] ?? null;
}

export function assignConventions(
  node: RelNode,
  schema: SchemaDefinition,
  cteNames: Set<string> = new Set<string>(),
): RelNode {
  switch (node.kind) {
    case "scan": {
      if (cteNames.has(node.table) || (!schema.tables[node.table] && !node.entity)) {
        return { ...node, convention: "local" };
      }
      const normalized = getNormalizedTableBinding(schema, node.table);
      if (normalized?.kind === "view") {
        return { ...node, convention: "local" };
      }
      const provider = node.entity?.provider ?? resolveTableProvider(schema, node.table);
      return {
        ...node,
        convention: `provider:${provider}`,
      };
    }
    case "filter":
    case "project":
    case "aggregate":
    case "window":
    case "sort":
    case "limit_offset": {
      const input = assignConventions(node.input, schema, cteNames);
      const provider = resolveSingleProvider(input, schema, cteNames);
      return {
        ...node,
        input,
        convention: provider ? (`provider:${provider}` as const) : "local",
      };
    }
    case "join":
    case "set_op": {
      const left = assignConventions(node.left, schema, cteNames);
      const right = assignConventions(node.right, schema, cteNames);
      const provider = resolveSingleProvider({ ...node, left, right } as RelNode, schema, cteNames);
      return {
        ...node,
        left,
        right,
        convention: provider ? (`provider:${provider}` as const) : "local",
      };
    }
    case "with": {
      const nextCteNames = new Set(cteNames);
      for (const cte of node.ctes) {
        nextCteNames.add(cte.name);
      }
      const ctes = node.ctes.map((cte) => ({
        ...cte,
        query: assignConventions(cte.query, schema, nextCteNames),
      }));
      const body = assignConventions(node.body, schema, nextCteNames);
      const provider = resolveSingleProvider(body, schema, nextCteNames);
      return {
        ...node,
        ctes,
        body,
        convention: provider ? (`provider:${provider}` as const) : "local",
      };
    }
    case "sql": {
      const provider = resolveSingleProvider(node, schema, cteNames);
      return {
        ...node,
        convention: provider ? (`provider:${provider}` as const) : "local",
      };
    }
  }
}

export function resolveLookupJoinCandidate<TContext>(
  join: RelJoinNode,
  schema: SchemaDefinition,
  providers: ProvidersMap<TContext>,
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

  const leftProvider = leftScan.entity?.provider ?? resolveTableProvider(schema, leftScan.table);
  const rightProvider = rightScan.entity?.provider ?? resolveTableProvider(schema, rightScan.table);
  if (leftProvider === rightProvider) {
    return null;
  }

  const rightAdapter = providers[rightProvider];
  if (!rightAdapter || !supportsLookupMany(rightAdapter)) {
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
    case "filter":
    case "project":
    case "aggregate":
    case "window":
    case "sort":
    case "limit_offset":
      return findFirstScanNode(node.input);
    case "join":
    case "set_op":
      return findFirstScanNode(node.left) ?? findFirstScanNode(node.right);
    case "with":
      return findFirstScanNode(node.body);
    case "sql":
      return null;
  }
}
