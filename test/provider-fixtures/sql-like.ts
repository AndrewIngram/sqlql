import {
  createSqlRelationalProviderAdapter,
  type QueryRow,
  type RelationalProviderEntityConfig,
  type ScanFilterClause,
  type TableScanRequest,
} from "@tupl/provider-kit";
import type {
  SqlRelationalOrderTerm,
  SqlRelationalQueryTranslationBackend,
  SqlRelationalSelection,
} from "@tupl/provider-kit/relational-sql";

import { applyScanRequest, compareRows, matchesFilters } from "./row-ops";

type FixtureEntityConfig = RelationalProviderEntityConfig & {
  rows: QueryRow[];
};

type FixtureQuery = {
  rows: QueryRow[];
};

const entities = {
  orders: {
    shape: {
      id: "text",
      customer_id: "text",
      total_cents: "integer",
    },
    rows: [
      { id: "o1", customer_id: "c1", total_cents: 900 },
      { id: "o2", customer_id: "c1", total_cents: 1800 },
      { id: "o3", customer_id: "c2", total_cents: 2600 },
    ],
  },
} satisfies Record<string, FixtureEntityConfig>;

export function createSqlLikeFixtureProvider() {
  const planning = {
    createScanBinding(scan: Extract<any, { kind: "scan" }>, resolvedEntities: Record<string, any>) {
      const resolved = resolvedEntities[scan.table];
      if (!resolved) {
        throw new Error(`Unknown fixture entity: ${scan.table}`);
      }

      return {
        alias: scan.alias ?? resolved.table,
        entity: resolved.entity,
        table: resolved.table,
        scan,
        resolved,
      };
    },
  };

  const queryTranslationBackend: SqlRelationalQueryTranslationBackend<
    unknown,
    any,
    any,
    unknown,
    FixtureQuery
  > = {
    createRootQuery({ root }) {
      return {
        rows: [...root.resolved.config.rows],
      };
    },
    applyRegularJoin() {
      throw new Error("Fixture SQL-like provider does not model joins.");
    },
    applySemiJoin() {
      throw new Error("Fixture SQL-like provider does not model semi-joins.");
    },
    applyWhereClause({ query, clause }) {
      return applyFilterClause(query, clause);
    },
    applySelection({ query, selection }) {
      return {
        rows: applySelection(query.rows, selection),
      };
    },
    applyGroupBy({ query }) {
      return query;
    },
    applyOrderBy({ query, orderBy }) {
      const rows = [...query.rows].sort((left, right) =>
        compareRows(left, right, orderTermsToScanOrderBy(orderBy)),
      );
      return { rows };
    },
    applyLimit({ query, limit }) {
      return { rows: query.rows.slice(0, limit) };
    },
    applyOffset({ query, offset }) {
      return { rows: query.rows.slice(offset) };
    },
    applySetOp() {
      throw new Error("Fixture SQL-like provider does not model set operations.");
    },
    buildWithQuery() {
      throw new Error("Fixture SQL-like provider does not model CTEs.");
    },
    async executeQuery({ query }) {
      return query.rows;
    },
  };

  return createSqlRelationalProviderAdapter({
    name: "fixture_sql_like",
    entities,
    queryBackend: queryTranslationBackend,
    advanced: {
      createScanBinding: (scan, resolvedEntities) =>
        planning.createScanBinding(scan, resolvedEntities),
    },
    resolveRuntime() {
      return {};
    },
    async executeScan({ request, resolvedEntities }) {
      const resolved = resolvedEntities[request.table];
      if (!resolved) {
        throw new Error(`Unknown fixture entity: ${request.table}`);
      }

      return applyScanRequest(resolved.config.rows, request);
    },
  });
}

export function createSqlLikeConformanceOptions() {
  return {
    provider: createSqlLikeFixtureProvider(),
    context: {},
    rel: {
      node: {
        id: "limit_orders",
        kind: "limit_offset",
        convention: "local",
        limit: 1,
        input: {
          id: "sort_orders",
          kind: "sort",
          convention: "local",
          orderBy: [{ source: { alias: "orders", column: "total_cents" }, direction: "desc" }],
          input: {
            id: "project_orders",
            kind: "project",
            convention: "local",
            columns: [
              { source: { alias: "orders", column: "id" }, output: "id" },
              { source: { alias: "orders", column: "total_cents" }, output: "total_cents" },
            ],
            input: {
              id: "scan_orders",
              kind: "scan",
              convention: "local",
              table: "orders",
              alias: "orders",
              select: ["id", "total_cents", "customer_id"],
              where: [{ column: "customer_id", op: "eq", value: "c1" }],
              output: [
                { name: "orders.id" },
                { name: "orders.total_cents" },
                { name: "orders.customer_id" },
              ],
            },
            output: [{ name: "id" }, { name: "total_cents" }],
          },
          output: [{ name: "id" }, { name: "total_cents" }],
        },
        output: [{ name: "id" }, { name: "total_cents" }],
      },
      expectedRows: [{ id: "o2", total_cents: 1800 }],
    },
  };
}

function applyFilterClause(query: FixtureQuery, clause: ScanFilterClause): FixtureQuery {
  return {
    rows: query.rows.filter((row) => matchesFilters(row, [clause])),
  };
}

function applySelection(rows: QueryRow[], selection: SqlRelationalSelection[]): QueryRow[] {
  return rows.map((row) => {
    const projected: QueryRow = {};
    for (const entry of selection) {
      if (entry.kind !== "column") {
        throw new Error(
          `Fixture SQL-like provider only supports column selection, got ${entry.kind}.`,
        );
      }
      projected[entry.output] = row[entry.source.column] ?? null;
    }
    return projected;
  });
}

function orderTermsToScanOrderBy(
  orderBy: SqlRelationalOrderTerm[],
): NonNullable<TableScanRequest["orderBy"]> {
  return orderBy.map((term) => {
    if ("kind" in term && term.kind === "window") {
      throw new Error("Fixture SQL-like provider does not model window ordering.");
    }

    if ("kind" in term && term.kind === "output") {
      return {
        column: term.column,
        direction: term.direction,
      };
    }

    if ("kind" in term && term.kind === "qualified") {
      return {
        column: term.source.column,
        direction: term.direction,
      };
    }

    if ("kind" in term && term.kind === "column") {
      return {
        column: term.source.column,
        direction: "asc" as const,
      };
    }

    throw new Error("Unsupported order term in fixture SQL-like provider.");
  });
}
