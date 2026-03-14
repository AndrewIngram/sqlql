import { Result } from "better-result";

import {
  TuplGuardrailError,
  type RelJoinNode,
  type RelNode,
  type RelScanNode,
} from "@tupl/foundation";
import {
  getDataEntityProvider,
  unwrapProviderOperationResult,
  type ProviderAdapter,
} from "@tupl/provider-kit";
import { supportsLookupMany } from "@tupl/provider-kit/shapes";
import {
  createPhysicalBindingFromEntity,
  createTableDefinitionFromEntity,
  getNormalizedTableBinding,
  mapProviderRowsToLogical,
  resolveNormalizedColumnSource,
  resolveTableProvider,
} from "@tupl/schema-model";

import {
  tryExecutionStep,
  tryExecutionStepAsync,
  type RelExecutionContext,
} from "./local-execution";
import { prefixRow, toColumnKey, type InternalRow } from "./row-ops";

/**
 * Lookup joins own batched provider-assisted join execution and local hash join fallback.
 */
export async function maybeExecuteLookupJoinResult<TContext>(
  join: RelJoinNode,
  leftRows: InternalRow[],
  context: RelExecutionContext<TContext>,
) {
  if (join.joinType !== "inner" && join.joinType !== "left") {
    return Result.ok(null);
  }

  const leftScan = findFirstScan(join.left);
  const rightScan = findLookupEligibleScan(join.right);
  if (!leftScan || !rightScan) {
    return Result.ok(null);
  }

  const rightBinding = getNormalizedTableBinding(context.schema, rightScan.table);
  const rightProviderName =
    rightScan.entity?.provider ?? resolveTableProvider(context.schema, rightScan.table);
  const rightProviderResult =
    typeof rightProviderName === "string" ? Result.ok(rightProviderName) : rightProviderName;
  if (Result.isError(rightProviderResult)) {
    return Result.ok(null);
  }
  const rightProvider =
    context.providers[rightProviderResult.value] ??
    (rightScan.entity
      ? (getDataEntityProvider(rightScan.entity) as ProviderAdapter<TContext> | undefined)
      : undefined);
  if (!rightProvider || !supportsLookupMany(rightProvider)) {
    return Result.ok(null);
  }

  const leftKey = `${join.leftKey.alias}.${join.leftKey.column}`;
  const rightPhysicalBinding =
    rightBinding?.kind === "physical"
      ? rightBinding
      : rightScan.entity
        ? createPhysicalBindingFromEntity(rightScan.entity)
        : null;
  const rightKey = rightPhysicalBinding
    ? resolveNormalizedColumnSource(rightPhysicalBinding, join.rightKey.column)
    : join.rightKey.column;
  const dedupedKeys = [
    ...new Set(leftRows.map((row) => row[leftKey]).filter((value) => value != null)),
  ];

  const rightRows: InternalRow[] = [];
  for (
    let startIndex = 0;
    startIndex < dedupedKeys.length;
    startIndex += context.guardrails.maxLookupKeysPerBatch
  ) {
    context.lookupBatches += 1;
    if (context.lookupBatches > context.guardrails.maxLookupBatches) {
      return Result.err(
        new TuplGuardrailError({
          guardrail: "maxLookupBatches",
          limit: context.guardrails.maxLookupBatches,
          actual: context.lookupBatches,
          message: `Query exceeded maxLookupBatches guardrail (${context.guardrails.maxLookupBatches}).`,
        }),
      );
    }

    const batch = dedupedKeys.slice(
      startIndex,
      startIndex + context.guardrails.maxLookupKeysPerBatch,
    );
    const lookedUpResult = await tryExecutionStepAsync("execute lookup join batch", async () =>
      unwrapProviderOperationResult(
        await rightProvider.lookupMany(
          {
            table: rightPhysicalBinding?.entity ?? rightScan.table,
            ...(rightScan.alias ? { alias: rightScan.alias } : {}),
            key: rightKey,
            keys: batch,
            select: rightScan.select.map((column) =>
              rightPhysicalBinding
                ? resolveNormalizedColumnSource(rightPhysicalBinding, column)
                : column,
            ),
            ...(rightScan.where
              ? {
                  where: rightScan.where.map((clause) => ({
                    ...clause,
                    column: rightPhysicalBinding
                      ? resolveNormalizedColumnSource(rightPhysicalBinding, clause.column)
                      : clause.column,
                  })),
                }
              : {}),
          },
          context.context,
        ),
      ),
    );
    if (Result.isError(lookedUpResult)) {
      return lookedUpResult;
    }

    const mappedRowsResult = tryExecutionStep("map lookup join rows to logical rows", () =>
      mapProviderRowsToLogical(
        lookedUpResult.value,
        rightScan.select,
        rightPhysicalBinding,
        context.schema.tables[rightScan.table] ??
          (rightScan.entity ? createTableDefinitionFromEntity(rightScan.entity) : undefined),
        {
          enforceNotNull:
            !context.constraintValidation || context.constraintValidation.mode === "off",
          enforceEnum: !context.constraintValidation || context.constraintValidation.mode === "off",
        },
      ),
    );
    if (Result.isError(mappedRowsResult)) {
      return mappedRowsResult;
    }

    const rightAlias = rightScan.alias ?? rightScan.table;
    for (const row of mappedRowsResult.value) {
      rightRows.push(prefixRow(row, rightAlias));
    }
  }

  return Result.ok(applyLocalHashJoin(join, leftRows, rightRows));
}

export function applyLocalHashJoin(
  join: RelJoinNode,
  leftRows: InternalRow[],
  rightRows: InternalRow[],
): InternalRow[] {
  const leftKey = toColumnKey(join.leftKey);
  const rightKey = toColumnKey(join.rightKey);

  const rightIndex = new Map<unknown, InternalRow[]>();
  for (const row of rightRows) {
    const key = row[rightKey];
    if (key == null) {
      continue;
    }

    const bucket = rightIndex.get(key) ?? [];
    bucket.push(row);
    rightIndex.set(key, bucket);
  }

  const joined: InternalRow[] = [];
  const matchedRightRows = new Set<InternalRow>();

  for (const leftRow of leftRows) {
    const key = leftRow[leftKey];
    const matches = key == null ? [] : (rightIndex.get(key) ?? []);

    if (join.joinType === "semi") {
      if (matches.length > 0) {
        joined.push({ ...leftRow });
      }
      continue;
    }

    if (matches.length === 0) {
      if (join.joinType === "left" || join.joinType === "full") {
        joined.push({ ...leftRow });
      }
      continue;
    }

    for (const match of matches) {
      matchedRightRows.add(match);
      joined.push({
        ...leftRow,
        ...match,
      });
    }
  }

  if (join.joinType === "right" || join.joinType === "full") {
    for (const rightRow of rightRows) {
      if (!matchedRightRows.has(rightRow)) {
        joined.push({ ...rightRow });
      }
    }
  }

  return joined;
}

function findLookupEligibleScan(node: RelNode): RelScanNode | null {
  switch (node.kind) {
    case "scan":
      return node;
    case "values":
    case "cte_ref":
      return null;
    case "filter":
    case "project":
    case "sort":
    case "limit_offset":
      return findLookupEligibleScan(node.input);
    case "aggregate":
    case "window":
    case "correlate":
    case "join":
    case "set_op":
    case "repeat_union":
    case "with":
      return null;
  }
}

function findFirstScan(node: RelNode): RelScanNode | null {
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
      return findFirstScan(node.input);
    case "correlate":
      return findFirstScan(node.left) ?? findFirstScan(node.right);
    case "join":
    case "set_op":
      return findFirstScan(node.left) ?? findFirstScan(node.right);
    case "repeat_union":
      return findFirstScan(node.seed) ?? findFirstScan(node.iterative);
    case "with":
      return findFirstScan(node.body);
  }
}
