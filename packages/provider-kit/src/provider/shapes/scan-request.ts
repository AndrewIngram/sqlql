import { Result } from "better-result";
import {
  isRelProjectColumnMapping,
  type RelNode,
  type ScanFilterClause,
  type ScanOrderBy,
  type TableScanRequest,
} from "@tupl/foundation";

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
