import { Result } from "better-result";

import {
  buildCapabilityReport,
  type ProviderCapabilityAtom,
  type ProviderOperationResult,
  type ProviderCapabilityReport,
} from "..";
import type { QueryRow, RelNode, ScanFilterClause, TableScanRequest } from "@tupl/foundation";
import type { ProviderLookupManyRequest } from "./lookup-optimization";
import {
  checkSimpleRelScanCapability,
  collectSimpleRelScanReferencedColumns,
  type SimpleRelScanSupportPolicy,
} from "./scan-request";

export interface LookupEntityBinding<TColumns extends string = string> {
  lookupKey: TColumns;
  columns: readonly TColumns[];
}

export interface KeyedSimpleRelScan<TColumn extends string = string> {
  request: TableScanRequest;
  key: TColumn;
  keys: unknown[];
  fetchColumns: string[];
}

export interface KeyedSimpleRelScanOptions<TColumn extends string = string> {
  supportedAtoms: readonly ProviderCapabilityAtom[];
  entity: LookupEntityBinding<TColumn>;
  policy?: SimpleRelScanSupportPolicy<TColumn>;
  unsupportedShapeReason?: string;
}

export function buildLookupOnlyUnsupportedReport(
  rel: RelNode,
  reason: string,
  supportedAtoms: readonly ProviderCapabilityAtom[] = ["lookup.bulk"],
): ProviderCapabilityReport {
  return buildCapabilityReport(rel, supportedAtoms, reason);
}

export function validateLookupRequest<TColumns extends string>(
  request: ProviderLookupManyRequest,
  entity: LookupEntityBinding<TColumns>,
): ProviderOperationResult<void> {
  if (request.key !== entity.lookupKey) {
    return Result.err(
      new Error(
        `Unsupported lookup key column for ${request.table}: ${request.key}. Expected ${entity.lookupKey}.`,
      ),
    );
  }

  const allowedColumns = new Set(entity.columns);
  for (const column of request.select) {
    if (!allowedColumns.has(column as TColumns)) {
      return Result.err(
        new Error(`Unsupported lookup select column for ${request.table}: ${column}`),
      );
    }
  }

  return Result.ok(undefined);
}

export function prepareKeyedSimpleRelScan<TColumn extends string = string>(
  rel: RelNode,
  options: KeyedSimpleRelScanOptions<TColumn>,
): Result<KeyedSimpleRelScan<TColumn>, ProviderCapabilityReport> {
  const capability = checkSimpleRelScanCapability(rel, {
    supportedAtoms: options.supportedAtoms,
    unsupportedShapeReason:
      options.unsupportedShapeReason ?? "Provider only supports keyed simple scan pipelines.",
    ...(options.policy ? { policy: options.policy } : {}),
  });
  if (Result.isError(capability)) {
    return capability;
  }

  const request = capability.value;
  const keys = inferExactLookupKeys(request.where, options.entity.lookupKey);
  if (keys === null) {
    return Result.err(
      buildCapabilityReport(
        rel,
        options.supportedAtoms,
        `Provider requires an equality or IN predicate on ${request.table}.${options.entity.lookupKey}.`,
      ),
    );
  }

  const fetchColumns = collectSimpleRelScanReferencedColumns(request, [options.entity.lookupKey]);
  const lookupRequest = {
    table: request.table,
    key: options.entity.lookupKey,
    keys,
    select: fetchColumns,
  } satisfies ProviderLookupManyRequest;
  const validation = validateLookupRequest(lookupRequest, options.entity);
  if (Result.isError(validation)) {
    return Result.err(buildCapabilityReport(rel, options.supportedAtoms, validation.error.message));
  }

  return Result.ok({
    request,
    key: options.entity.lookupKey,
    keys,
    fetchColumns,
  });
}

export function filterLookupRows(rows: QueryRow[], clauses?: ScanFilterClause[]): QueryRow[] {
  return (clauses ?? []).reduce(
    (current, clause) => current.filter((row) => matchesLookupClause(row, clause)),
    rows,
  );
}

export function projectLookupRow(row: QueryRow, select: string[]): QueryRow {
  const out: QueryRow = {};
  for (const column of select) {
    out[column] = row[column] ?? null;
  }
  return out;
}

export function matchesLookupClause(row: QueryRow, clause: ScanFilterClause): boolean {
  const value = row[clause.column];

  switch (clause.op) {
    case "eq":
      return value != null && clause.value != null && value === clause.value;
    case "neq":
      return value != null && clause.value != null && value !== clause.value;
    case "gt":
      return typeof value === "number" && value > Number(clause.value);
    case "gte":
      return typeof value === "number" && value >= Number(clause.value);
    case "lt":
      return typeof value === "number" && value < Number(clause.value);
    case "lte":
      return typeof value === "number" && value <= Number(clause.value);
    case "in":
      return value != null && clause.values.filter((entry) => entry != null).includes(value);
    case "not_in":
      return value != null && !clause.values.filter((entry) => entry != null).includes(value);
    case "like":
      return (
        typeof value === "string" &&
        typeof clause.value === "string" &&
        matchesLikePattern(value, clause.value)
      );
    case "not_like":
      return (
        typeof value === "string" &&
        typeof clause.value === "string" &&
        !matchesLikePattern(value, clause.value)
      );
    case "is_distinct_from":
      return value !== clause.value;
    case "is_not_distinct_from":
      return value === clause.value;
    case "is_null":
      return value == null;
    case "is_not_null":
      return value != null;
  }
}

export function matchesLikePattern(value: string, pattern: string): boolean {
  const escaped = pattern
    .replace(/[.*+?^${}()|[\]\\]/g, "\\$&")
    .replace(/%/g, ".*")
    .replace(/_/g, ".");
  return new RegExp(`^${escaped}$`, "su").test(value);
}

function inferExactLookupKeys(
  where: readonly ScanFilterClause[] | undefined,
  lookupKey: string,
): unknown[] | null {
  let candidateKeys: Set<unknown> | null = null;

  for (const clause of where ?? []) {
    if (clause.column !== lookupKey) {
      continue;
    }

    let clauseKeys: Set<unknown> | null = null;
    switch (clause.op) {
      case "eq":
        clauseKeys = clause.value == null ? new Set() : new Set([clause.value]);
        break;
      case "in":
        clauseKeys = new Set(clause.values.filter((value) => value != null));
        break;
      default:
        return null;
    }

    if (candidateKeys === null) {
      candidateKeys = clauseKeys;
      continue;
    }

    const intersected = new Set<unknown>();
    for (const value of candidateKeys) {
      if (clauseKeys.has(value)) {
        intersected.add(value);
      }
    }
    candidateKeys = intersected;
  }

  return candidateKeys ? [...candidateKeys] : null;
}
