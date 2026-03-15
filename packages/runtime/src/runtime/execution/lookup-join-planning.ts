import type { RelNode } from "@tupl/foundation";
import { Result } from "better-result";
import { supportsLookupMany } from "@tupl/provider-kit/shapes";
import { getNormalizedTableBinding, resolveTableProvider } from "@tupl/schema-model/normalization";

import type { QuerySessionInput } from "../session/contracts";
import { isPromiseLike } from "../policy";

/**
 * Lookup-join planning owns sync lookup-join candidacy checks for session plan graphs.
 */
export function resolveSyncLookupJoinCandidate<TContext>(
  join: Extract<RelNode, { kind: "join" }>,
  input: QuerySessionInput<TContext>,
): {
  leftProvider: string;
  rightProvider: string;
  leftTable: string;
  rightTable: string;
  leftKey: string;
  rightKey: string;
  joinType: "inner" | "left";
} | null {
  if (join.joinType !== "inner" && join.joinType !== "left") {
    return null;
  }

  const leftScan = findFirstScanForPlan(join.left);
  const rightScan = findFirstScanForPlan(join.right);
  if (!leftScan || !rightScan) {
    return null;
  }
  const leftScanAlias = leftScan.alias ?? leftScan.table;
  const rightScanAlias = rightScan.alias ?? rightScan.table;
  if ((join.leftKey.alias ?? join.leftKey.table ?? leftScanAlias) !== leftScanAlias) {
    return null;
  }
  if ((join.rightKey.alias ?? join.rightKey.table ?? rightScanAlias) !== rightScanAlias) {
    return null;
  }
  if (
    (!input.preparedSchema.schema.tables[leftScan.table] && !leftScan.entity) ||
    (!input.preparedSchema.schema.tables[rightScan.table] && !rightScan.entity)
  ) {
    return null;
  }

  const leftBinding = getNormalizedTableBinding(input.preparedSchema.schema, leftScan.table);
  const rightBinding = getNormalizedTableBinding(input.preparedSchema.schema, rightScan.table);
  if (leftBinding?.kind === "view" || rightBinding?.kind === "view") {
    return null;
  }

  const leftProviderName =
    leftScan.entity?.provider ?? resolveTableProvider(input.preparedSchema.schema, leftScan.table);
  const rightProviderName =
    rightScan.entity?.provider ??
    resolveTableProvider(input.preparedSchema.schema, rightScan.table);
  const leftProviderResult =
    typeof leftProviderName === "string" ? Result.ok(leftProviderName) : leftProviderName;
  const rightProviderResult =
    typeof rightProviderName === "string" ? Result.ok(rightProviderName) : rightProviderName;
  if (Result.isError(leftProviderResult) || Result.isError(rightProviderResult)) {
    return null;
  }
  const leftProvider = leftProviderResult.value;
  const rightProvider = rightProviderResult.value;

  const rightAdapter = input.preparedSchema.providers[rightProvider];
  if (!rightAdapter) {
    return null;
  }

  const rightLookupAdapter = rightAdapter;
  if (!supportsLookupMany(rightLookupAdapter)) {
    return null;
  }

  const capability = rightAdapter.canExecute(rightScan, input.context);
  if (isPromiseLike(capability)) {
    return null;
  }

  return {
    leftProvider,
    rightProvider,
    leftTable: leftScan.table,
    rightTable: rightScan.table,
    leftKey: join.leftKey.column,
    rightKey: join.rightKey.column,
    joinType: join.joinType,
  };
}

function findFirstScanForPlan(node: RelNode): Extract<RelNode, { kind: "scan" }> | null {
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
      return findFirstScanForPlan(node.input);
    case "correlate":
      return null;
    case "join":
    case "set_op":
      return findFirstScanForPlan(node.left) ?? findFirstScanForPlan(node.right);
    case "repeat_union":
      return findFirstScanForPlan(node.seed) ?? findFirstScanForPlan(node.iterative);
    case "with":
      return findFirstScanForPlan(node.body);
  }
}
