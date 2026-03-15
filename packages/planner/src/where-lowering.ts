import type { RelExpr } from "@tupl/foundation";

import type { ParsedJoin } from "./planner-types";
import { validateEnumLiteralFilters } from "./enum-filter-validation";
import { literalFilterToRelExpr, parseWhereFilters } from "./literal-filter-lowering";

/**
 * Where lowering is the curated internal export surface for pushdown-friendly predicate handling.
 */
export { literalFilterToRelExpr, parseWhereFilters, validateEnumLiteralFilters };

export function combineAndExprs(exprs: RelExpr[]): RelExpr | undefined {
  return exprs.reduce<RelExpr | undefined>(
    (acc, current) =>
      acc
        ? {
            kind: "function",
            name: "and",
            args: [acc, current],
          }
        : current,
    undefined,
  );
}

export function getPushableWhereAliases(rootAlias: string, joins: ParsedJoin[]): Set<string> {
  const reachable = new Set<string>([rootAlias]);

  for (const join of joins) {
    if (join.joinType !== "inner") {
      continue;
    }
    if (reachable.has(join.leftAlias)) {
      reachable.add(join.rightAlias);
      continue;
    }
    if (reachable.has(join.rightAlias)) {
      reachable.add(join.leftAlias);
    }
  }

  return reachable;
}
