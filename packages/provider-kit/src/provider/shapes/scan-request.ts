import { Result } from "better-result";
import {
  isRelProjectColumnMapping,
  type RelNode,
  type ScanFilterClause,
  type ScanOrderBy,
  type TableScanRequest,
} from "@tupl/foundation";
import { buildCapabilityReport } from "../capabilities";

/**
 * Simple scan extraction owns the narrow "single-source scan pipeline" shape used by providers
 * that can only execute one table with optional filter/sort/limit/project pushdown.
 */
export function extractSimpleRelScanRequest(node: RelNode): TableScanRequest | null {
  switch (node.kind) {
    case "scan":
      return {
        table: node.table,
        ...(node.alias ? { alias: node.alias } : {}),
        select: node.select,
        ...(node.where ? { where: node.where } : {}),
        ...(node.orderBy ? { orderBy: node.orderBy } : {}),
        ...(node.limit != null ? { limit: node.limit } : {}),
        ...(node.offset != null ? { offset: node.offset } : {}),
      };
    case "filter": {
      if (node.expr) {
        return null;
      }
      const request = extractSimpleRelScanRequest(node.input);
      if (!request) {
        return null;
      }
      return {
        ...request,
        ...(node.where?.length
          ? {
              where: [...(request.where ?? []), ...node.where],
            }
          : {}),
      };
    }
    case "project": {
      const request = extractSimpleRelScanRequest(node.input);
      if (!request) {
        return null;
      }

      const select: string[] = [];
      for (const column of node.columns) {
        if (!isRelProjectColumnMapping(column)) {
          return null;
        }
        if (column.source.alias || column.source.table) {
          return null;
        }
        select.push(column.source.column);
      }

      return {
        ...request,
        select,
      };
    }
    case "sort": {
      const request = extractSimpleRelScanRequest(node.input);
      if (!request) {
        return null;
      }
      if (node.orderBy.some((term) => term.source.alias || term.source.table)) {
        return null;
      }
      return {
        ...request,
        orderBy: node.orderBy.map((term) => ({
          column: term.source.column,
          direction: term.direction,
        })),
      };
    }
    case "limit_offset": {
      const request = extractSimpleRelScanRequest(node.input);
      if (!request) {
        return null;
      }
      return {
        ...request,
        ...(node.limit != null ? { limit: node.limit } : {}),
        ...(node.offset != null ? { offset: node.offset } : {}),
      };
    }
    default:
      return null;
  }
}

export interface SimpleRelScanSupportPolicy<TColumn extends string = string> {
  supportsSelectColumn?(column: TColumn): boolean;
  supportsFilterClause?(clause: ScanFilterClause & { column: TColumn }): boolean;
  supportsSortTerm?(term: ScanOrderBy & { column: TColumn }): boolean;
}

export interface SimpleRelScanCapabilityOptions<TColumn extends string = string> {
  policy?: SimpleRelScanSupportPolicy<TColumn>;
  unsupportedShapeReason?: string;
  mapValidationError?(error: Error, request: TableScanRequest): string;
}

/**
 * Simple scan validation lets providers keep `canExecute` field-sensitive without hand-walking
 * the rel tree. Providers decide which projected, filtered, and sorted columns are legal.
 */
export function validateSimpleRelScanRequest<TColumn extends string = string>(
  request: TableScanRequest,
  policy: SimpleRelScanSupportPolicy<TColumn>,
) {
  for (const column of request.select) {
    if (policy.supportsSelectColumn?.(column as TColumn) === false) {
      return Result.err(new Error(`Unsupported projected column for ${request.table}: ${column}`));
    }
  }

  for (const clause of request.where ?? []) {
    if (policy.supportsFilterClause?.(clause as ScanFilterClause & { column: TColumn }) === false) {
      return Result.err(
        new Error(`Unsupported filter clause for ${request.table}: ${clause.column} ${clause.op}`),
      );
    }
  }

  for (const term of request.orderBy ?? []) {
    if (policy.supportsSortTerm?.(term as ScanOrderBy & { column: TColumn }) === false) {
      return Result.err(new Error(`Unsupported sort column for ${request.table}: ${term.column}`));
    }
  }

  return Result.ok(undefined);
}

export function checkSimpleRelScanCapability<TColumn extends string = string>(
  rel: RelNode,
  options: SimpleRelScanCapabilityOptions<TColumn>,
) {
  const request = extractSimpleRelScanRequest(rel);
  if (!request) {
    return Result.err(
      buildCapabilityReport(
        rel,
        options.unsupportedShapeReason ??
          "Provider only supports simple single-source scan pipelines.",
      ),
    );
  }

  if (!options.policy) {
    return Result.ok(request);
  }

  const validation = validateSimpleRelScanRequest(request, options.policy);
  if (Result.isError(validation)) {
    return Result.err(
      buildCapabilityReport(
        rel,
        options.mapValidationError?.(validation.error, request) ?? validation.error.message,
      ),
    );
  }

  return Result.ok(request);
}

export function collectSimpleRelScanReferencedColumns(
  request: TableScanRequest,
  extras: readonly string[] = [],
): string[] {
  const columns = new Set<string>(request.select);

  for (const clause of request.where ?? []) {
    columns.add(clause.column);
  }

  for (const term of request.orderBy ?? []) {
    columns.add(term.column);
  }

  for (const extra of extras) {
    columns.add(extra);
  }

  return [...columns];
}
