import { Result } from "better-result";
import { supportsLookupMany } from "@tupl/provider-kit";
import type { RelNode } from "@tupl/foundation";
import { getNormalizedTableBinding, resolveTableProvider } from "@tupl/schema-model";

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
  if (
    (!input.schema.tables[leftScan.table] && !leftScan.entity) ||
    (!input.schema.tables[rightScan.table] && !rightScan.entity)
  ) {
    return null;
  }

  const leftBinding = getNormalizedTableBinding(input.schema, leftScan.table);
  const rightBinding = getNormalizedTableBinding(input.schema, rightScan.table);
  if (leftBinding?.kind === "view" || rightBinding?.kind === "view") {
    return null;
  }

  const leftProvider =
    leftScan.entity?.provider ?? readResolvedTableProvider(input.schema, leftScan.table);
  const rightProvider =
    rightScan.entity?.provider ?? readResolvedTableProvider(input.schema, rightScan.table);

  const rightAdapter = input.providers[rightProvider];
  if (!rightAdapter || !supportsLookupMany(rightAdapter)) {
    return null;
  }

  const capability = rightAdapter.canExecute(
    {
      kind: "scan",
      provider: rightProvider,
      table: rightScan.entity?.entity ?? rightScan.table,
      request: {
        table: rightScan.entity?.entity ?? rightScan.table,
        select: rightScan.select,
      },
    },
    input.context,
  );
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
      return null;
    case "filter":
    case "project":
    case "aggregate":
    case "window":
    case "sort":
    case "limit_offset":
      return findFirstScanForPlan(node.input);
    case "join":
    case "set_op":
      return findFirstScanForPlan(node.left) ?? findFirstScanForPlan(node.right);
    case "repeat_union":
      return findFirstScanForPlan(node.seed) ?? findFirstScanForPlan(node.iterative);
    case "with":
      return findFirstScanForPlan(node.body);
    case "sql":
      return null;
  }
}

function readResolvedTableProvider(
  schema: QuerySessionInput<unknown>["schema"],
  table: string,
): string {
  const result = resolveTableProvider(schema, table);
  if (Result.isError(result)) {
    throw result.error;
  }

  return result.value;
}
