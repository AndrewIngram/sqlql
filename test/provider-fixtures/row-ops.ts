import type { QueryRow, ScanFilterClause, TableScanRequest } from "@tupl/provider-kit";

export function applyScanRequest(rows: QueryRow[], request: TableScanRequest): QueryRow[] {
  let out = rows.filter((row) => matchesFilters(row, request.where ?? []));

  if (request.orderBy && request.orderBy.length > 0) {
    out = [...out].sort((left, right) => compareRows(left, right, request.orderBy ?? []));
  }

  if (request.offset != null) {
    out = out.slice(request.offset);
  }

  if (request.limit != null) {
    out = out.slice(0, request.limit);
  }

  return projectRows(out, request.select);
}

export function projectRows(rows: QueryRow[], columns: readonly string[]): QueryRow[] {
  return rows.map((row) => {
    const projected: QueryRow = {};
    for (const column of columns) {
      projected[column] = row[column] ?? null;
    }
    return projected;
  });
}

export function compareRows(
  left: QueryRow,
  right: QueryRow,
  orderBy: NonNullable<TableScanRequest["orderBy"]>,
): number {
  for (const term of orderBy) {
    const leftValue = left[term.column] ?? null;
    const rightValue = right[term.column] ?? null;
    if (leftValue === rightValue) {
      continue;
    }

    const direction = term.direction === "asc" ? 1 : -1;
    if (leftValue == null) {
      return -1 * direction;
    }
    if (rightValue == null) {
      return 1 * direction;
    }

    if (leftValue < rightValue) {
      return -1 * direction;
    }
    if (leftValue > rightValue) {
      return 1 * direction;
    }
  }

  return 0;
}

export function matchesFilters(row: QueryRow, filters: readonly ScanFilterClause[]): boolean {
  for (const clause of filters) {
    const value = row[clause.column];
    switch (clause.op) {
      case "eq":
        if (value !== clause.value) {
          return false;
        }
        break;
      case "neq":
        if (value === clause.value) {
          return false;
        }
        break;
      case "gt":
        if (value == null || clause.value == null || Number(value) <= Number(clause.value)) {
          return false;
        }
        break;
      case "gte":
        if (value == null || clause.value == null || Number(value) < Number(clause.value)) {
          return false;
        }
        break;
      case "lt":
        if (value == null || clause.value == null || Number(value) >= Number(clause.value)) {
          return false;
        }
        break;
      case "lte":
        if (value == null || clause.value == null || Number(value) > Number(clause.value)) {
          return false;
        }
        break;
      case "in":
        if (!clause.values.includes(value)) {
          return false;
        }
        break;
      case "not_in":
        if (clause.values.includes(value)) {
          return false;
        }
        break;
      case "like":
        if (
          typeof value !== "string" ||
          typeof clause.value !== "string" ||
          !matchesLike(value, clause.value)
        ) {
          return false;
        }
        break;
      case "not_like":
        if (
          typeof value !== "string" ||
          typeof clause.value !== "string" ||
          matchesLike(value, clause.value)
        ) {
          return false;
        }
        break;
      case "is_distinct_from":
        if (value === clause.value) {
          return false;
        }
        break;
      case "is_not_distinct_from":
        if (value !== clause.value) {
          return false;
        }
        break;
      case "is_null":
        if (value != null) {
          return false;
        }
        break;
      case "is_not_null":
        if (value == null) {
          return false;
        }
        break;
    }
  }

  return true;
}

function matchesLike(value: string, pattern: string): boolean {
  const escaped = pattern
    .replace(/[.*+?^${}()|[\]\\]/g, "\\$&")
    .replace(/%/g, ".*")
    .replace(/_/g, ".");
  return new RegExp(`^${escaped}$`, "su").test(value);
}
