import { Result, type Result as BetterResult } from "better-result";

import {
  ProviderFragmentBuildError,
  type RelScanNode,
  type ScanFilterClause,
} from "@tupl/foundation";
import { type ColumnDefinition, type SchemaDefinition } from "@tupl/schema-model";
import {
  createPhysicalBindingFromEntity,
  createTableDefinitionFromEntity,
  getNormalizedTableBinding,
  resolveNormalizedColumnSource,
} from "@tupl/schema-model/normalization";

/**
 * Provider scan normalization owns physical-entity scan rewriting and enum filter translation.
 */
export function normalizeScanForProvider(
  node: RelScanNode,
  schema: SchemaDefinition,
): BetterResult<RelScanNode, ProviderFragmentBuildError> {
  const binding =
    getNormalizedTableBinding(schema, node.table) ??
    (node.entity ? createPhysicalBindingFromEntity(node.entity) : undefined);
  if (!binding || binding.kind !== "physical") {
    return Result.ok(node);
  }
  const table =
    schema.tables[node.table] ??
    (node.entity ? createTableDefinitionFromEntity(node.entity) : undefined);

  const mapColumn = (column: string): string => resolveNormalizedColumnSource(binding, column);
  const mappedWhere: ScanFilterClause[] = [];
  for (const clause of node.where ?? []) {
    const mapped = mapEnumFilterForProvider(table?.columns[clause.column], clause);
    if (Result.isError(mapped)) {
      return mapped;
    }
    mappedWhere.push({
      ...mapped.value,
      column: mapColumn(mapped.value.column),
    });
  }

  return Result.ok({
    ...node,
    table: binding.entity,
    ...(node.entity ? { entity: node.entity } : {}),
    select: node.select.map(mapColumn),
    ...(node.where ? { where: mappedWhere } : {}),
    ...(node.orderBy
      ? {
          orderBy: node.orderBy.map((term) => ({
            ...term,
            column: mapColumn(term.column),
          })),
        }
      : {}),
  });
}

function mapEnumFilterForProvider(
  definition: unknown,
  clause: ScanFilterClause,
): BetterResult<ScanFilterClause, ProviderFragmentBuildError> {
  if (!definition || typeof definition === "string") {
    return Result.ok(clause);
  }

  const column = definition as ColumnDefinition;
  if (!column.enumMap || Object.keys(column.enumMap).length === 0) {
    return Result.ok(clause);
  }

  const mapFacadeValueToSource = (value: unknown): string[] => {
    if (typeof value !== "string") {
      return [];
    }
    const out: string[] = [];
    for (const [sourceValue, facadeValue] of Object.entries(column.enumMap ?? {})) {
      if (facadeValue === value) {
        out.push(sourceValue);
      }
    }
    return out;
  };

  if (clause.op === "eq") {
    const mappedValues = mapFacadeValueToSource(clause.value);
    if (mappedValues.length === 0) {
      return Result.err(
        new ProviderFragmentBuildError({
          operation: "normalize provider scan",
          message: `No upstream enum mapping for value ${JSON.stringify(clause.value)} on ${clause.column}.`,
        }),
      );
    }
    if (mappedValues.length === 1) {
      return Result.ok({
        ...clause,
        value: mappedValues[0],
      });
    }
    return Result.ok({
      op: "in",
      column: clause.column,
      values: mappedValues,
    });
  }

  if (clause.op === "in") {
    const mapped = [...new Set(clause.values.flatMap((value) => mapFacadeValueToSource(value)))];
    if (mapped.length === 0) {
      return Result.err(
        new ProviderFragmentBuildError({
          operation: "normalize provider scan",
          message: `No upstream enum mappings for IN predicate on ${clause.column}.`,
        }),
      );
    }
    return Result.ok({
      ...clause,
      values: mapped,
    });
  }

  return Result.ok(clause);
}
