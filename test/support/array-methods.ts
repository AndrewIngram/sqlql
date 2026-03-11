import { stringifyUnknownValue } from "@tupl/foundation";

import type {
  AggregateFunction,
  QueryRow,
  TableAggregateMetric,
  TableAggregateRequest,
  TableLookupRequest,
  TableMethods,
  TableScanRequest,
} from "@tupl/schema";

export type ArrayRowSource<TRow extends QueryRow = QueryRow> = TRow[] | (() => TRow[]);

export interface ArrayTableMethodsOptions {
  includeLookup?: boolean;
  includeAggregate?: boolean;
}

export function createArrayTableMethods<
  TContext = unknown,
  TRow extends QueryRow = QueryRow,
  TTable extends string = string,
  TColumn extends Extract<keyof TRow, string> = Extract<keyof TRow, string>,
>(
  rows: ArrayRowSource<TRow>,
  options: ArrayTableMethodsOptions = {},
): TableMethods<TContext, TTable, TColumn, any> {
  const includeLookup = options.includeLookup ?? true;
  const includeAggregate = options.includeAggregate ?? true;

  const methods: TableMethods<TContext, TTable, TColumn, any> = {
    async scan(request) {
      return scanArrayRows<TRow, TTable, TColumn>(readRows(rows), request);
    },
  };

  if (includeLookup) {
    methods.lookup = async (request) =>
      lookupArrayRows<TRow, TTable, TColumn>(readRows(rows), request);
  }

  if (includeAggregate) {
    methods.aggregate = async (request) =>
      aggregateArrayRows<TRow, TTable, TColumn>(readRows(rows), request);
  }

  return methods;
}

export function scanArrayRows<
  TRow extends QueryRow,
  TTable extends string = string,
  TColumn extends Extract<keyof TRow, string> = Extract<keyof TRow, string>,
>(rows: TRow[], request: TableScanRequest<TTable, TColumn>): QueryRow[] {
  const normalized = normalizeDateRows(rows);
  let out = filterRows<TRow, TColumn>(normalized, request.where ?? []);

  if (request.orderBy && request.orderBy.length > 0) {
    out = [...out].sort((left, right) => {
      for (const term of request.orderBy ?? []) {
        const comparison = compareNullableValues(
          left[term.column] ?? null,
          right[term.column] ?? null,
        );
        if (comparison !== 0) {
          return term.direction === "asc" ? comparison : -comparison;
        }
      }
      return 0;
    });
  }

  if (request.offset != null) {
    out = out.slice(request.offset);
  }

  if (request.limit != null) {
    out = out.slice(0, request.limit);
  }

  return projectRows(out, request.select);
}

export function lookupArrayRows<
  TRow extends QueryRow,
  TTable extends string = string,
  TColumn extends Extract<keyof TRow, string> = Extract<keyof TRow, string>,
>(rows: TRow[], request: TableLookupRequest<TTable, TColumn>): QueryRow[] {
  const normalized = normalizeDateRows(rows);
  const keys = new Set(request.values);
  let out = normalized.filter((row) => keys.has(row[request.key]));
  out = filterRows<TRow, TColumn>(out, request.where ?? []);
  return projectRows(out, request.select);
}

export function aggregateArrayRows<
  TRow extends QueryRow,
  TTable extends string = string,
  TColumn extends Extract<keyof TRow, string> = Extract<keyof TRow, string>,
>(rows: TRow[], request: TableAggregateRequest<TTable, TColumn>): QueryRow[] {
  const normalizedRows = normalizeDateRows(rows);
  const scanRequest: TableScanRequest<TTable, TColumn> = {
    table: request.table,
    select: Object.keys(normalizedRows[0] ?? {}) as TColumn[],
  };

  if (request.alias) {
    scanRequest.alias = request.alias;
  }

  if (request.where) {
    scanRequest.where = request.where;
  }

  const scanned = scanArrayRows<TRow, TTable, TColumn>(normalizedRows, scanRequest);
  const groupBy = request.groupBy ?? [];
  const groups = new Map<string, QueryRow[]>();

  for (const row of scanned) {
    const key = JSON.stringify(groupBy.map((column) => row[column] ?? null));
    const bucket = groups.get(key) ?? [];
    bucket.push(row);
    groups.set(key, bucket);
  }

  if (groups.size === 0 && groupBy.length === 0) {
    groups.set("__all__", []);
  }

  const out: QueryRow[] = [];

  for (const [key, bucket] of groups.entries()) {
    const row: QueryRow = {};

    if (groupBy.length > 0) {
      const groupValues = JSON.parse(key) as unknown[];
      groupBy.forEach((column, index) => {
        row[column] = groupValues[index] ?? null;
      });
    }

    for (const metric of request.metrics) {
      row[metric.as] = evaluateMetric<TColumn>(metric, bucket);
    }

    out.push(row);
  }

  if (request.limit != null) {
    return out.slice(0, request.limit);
  }

  return out;
}

function normalizeDateRows<TRow extends QueryRow>(rows: TRow[]): TRow[] {
  return rows.map((row) => {
    let changed = false;
    const next: QueryRow = { ...row };

    for (const [key, value] of Object.entries(next)) {
      if (value instanceof Date) {
        next[key] = value.toISOString();
        changed = true;
      }
    }

    return (changed ? next : row) as TRow;
  });
}

function evaluateMetric<TColumn extends string>(
  metric: TableAggregateMetric<TColumn>,
  bucket: QueryRow[],
): unknown {
  const values = readMetricValues(metric, bucket);

  switch (metric.fn) {
    case "count":
      return metric.column ? values.filter((value) => value != null).length : bucket.length;
    case "sum": {
      const numeric = values
        .filter((value) => value != null)
        .map((value) => toFiniteNumber(value, "SUM"));
      return numeric.length > 0 ? numeric.reduce((sum, value) => sum + value, 0) : null;
    }
    case "avg": {
      const numeric = values
        .filter((value) => value != null)
        .map((value) => toFiniteNumber(value, "AVG"));
      return numeric.length > 0
        ? numeric.reduce((sum, value) => sum + value, 0) / numeric.length
        : null;
    }
    case "min": {
      const candidates = values.filter((value) => value != null);
      return candidates.length > 0
        ? candidates.reduce((left, right) =>
            compareNullableValues(left, right) <= 0 ? left : right,
          )
        : null;
    }
    case "max": {
      const candidates = values.filter((value) => value != null);
      return candidates.length > 0
        ? candidates.reduce((left, right) =>
            compareNullableValues(left, right) >= 0 ? left : right,
          )
        : null;
    }
  }
}

function readMetricValues<TColumn extends string>(
  metric: TableAggregateMetric<TColumn>,
  bucket: QueryRow[],
): unknown[] {
  const metricColumn = metric.column;
  const values =
    metricColumn != null ? bucket.map((row) => row[metricColumn] ?? null) : bucket.map(() => 1);

  if (!metric.distinct) {
    return values;
  }

  return [...new Map(values.map((value) => [JSON.stringify(value), value])).values()];
}

function toFiniteNumber(
  value: unknown,
  functionName: AggregateFunction | Uppercase<AggregateFunction>,
): number {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    throw new Error(`${functionName} expects numeric values.`);
  }

  return parsed;
}

function filterRows<TRow extends QueryRow, TColumn extends string>(
  rows: TRow[],
  clauses: NonNullable<TableScanRequest<string, TColumn>["where"]>,
): TRow[] {
  let out = [...rows];

  for (const clause of clauses) {
    switch (clause.op) {
      case "eq":
        out = out.filter((row) => {
          if (clause.value == null) {
            return false;
          }

          const value = row[clause.column];
          return value != null && value === clause.value;
        });
        break;
      case "neq":
        out = out.filter((row) => {
          if (clause.value == null) {
            return false;
          }

          const value = row[clause.column];
          return value != null && value !== clause.value;
        });
        break;
      case "gt":
        out = out.filter((row) => {
          if (clause.value == null) {
            return false;
          }

          const value = row[clause.column];
          return value != null && compareNonNull(value, clause.value) > 0;
        });
        break;
      case "gte":
        out = out.filter((row) => {
          if (clause.value == null) {
            return false;
          }

          const value = row[clause.column];
          return value != null && compareNonNull(value, clause.value) >= 0;
        });
        break;
      case "lt":
        out = out.filter((row) => {
          if (clause.value == null) {
            return false;
          }

          const value = row[clause.column];
          return value != null && compareNonNull(value, clause.value) < 0;
        });
        break;
      case "lte":
        out = out.filter((row) => {
          if (clause.value == null) {
            return false;
          }

          const value = row[clause.column];
          return value != null && compareNonNull(value, clause.value) <= 0;
        });
        break;
      case "in": {
        const set = new Set<unknown>(clause.values.filter((value) => value != null));
        out = out.filter((row) => {
          const value = row[clause.column];
          return value != null && set.has(value as unknown);
        });
        break;
      }
      case "not_in": {
        const set = new Set<unknown>(clause.values.filter((value) => value != null));
        out = out.filter((row) => {
          const value = row[clause.column];
          return value != null && !set.has(value as unknown);
        });
        break;
      }
      case "like":
        out = out.filter(
          (row) =>
            typeof row[clause.column] === "string" &&
            typeof clause.value === "string" &&
            matchesLikePattern(row[clause.column] as string, clause.value),
        );
        break;
      case "not_like":
        out = out.filter(
          (row) =>
            typeof row[clause.column] === "string" &&
            typeof clause.value === "string" &&
            !matchesLikePattern(row[clause.column] as string, clause.value),
        );
        break;
      case "is_distinct_from":
        out = out.filter((row) => row[clause.column] !== clause.value);
        break;
      case "is_not_distinct_from":
        out = out.filter((row) => row[clause.column] === clause.value);
        break;
      case "is_null":
        out = out.filter((row) => row[clause.column] == null);
        break;
      case "is_not_null":
        out = out.filter((row) => row[clause.column] != null);
        break;
    }
  }

  return out;
}

function matchesLikePattern(value: string, pattern: string): boolean {
  const escaped = pattern
    .replace(/[.*+?^${}()|[\]\\]/g, "\\$&")
    .replace(/%/g, ".*")
    .replace(/_/g, ".");
  return new RegExp(`^${escaped}$`, "su").test(value);
}

function projectRows(rows: QueryRow[], select: string[]): QueryRow[] {
  return rows.map((row) => {
    const out: QueryRow = {};
    for (const column of select) {
      out[column] = row[column] ?? null;
    }
    return out;
  });
}

function compareNullableValues(left: unknown, right: unknown): number {
  if (left === right) {
    return 0;
  }
  if (left == null) {
    return -1;
  }
  if (right == null) {
    return 1;
  }

  if (typeof left === "number" && typeof right === "number") {
    return left < right ? -1 : 1;
  }

  if (typeof left === "boolean" && typeof right === "boolean") {
    return Number(left) < Number(right) ? -1 : 1;
  }

  const leftString = stringifyUnknownValue(left);
  const rightString = stringifyUnknownValue(right);
  return leftString < rightString ? -1 : 1;
}

function compareNonNull(left: unknown, right: unknown): number {
  if (typeof left === "number" && typeof right === "number") {
    return left === right ? 0 : left < right ? -1 : 1;
  }

  if (typeof left === "boolean" && typeof right === "boolean") {
    const leftNum = Number(left);
    const rightNum = Number(right);
    return leftNum === rightNum ? 0 : leftNum < rightNum ? -1 : 1;
  }

  const leftString = stringifyUnknownValue(left);
  const rightString = stringifyUnknownValue(right);
  if (leftString === rightString) {
    return 0;
  }
  return leftString < rightString ? -1 : 1;
}

function readRows<TRow extends QueryRow>(rows: ArrayRowSource<TRow>): TRow[] {
  return typeof rows === "function" ? rows() : rows;
}
