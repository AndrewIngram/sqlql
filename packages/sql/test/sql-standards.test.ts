import { describe, expect, it } from "vitest";

import {
  defineSchema,
  defineTableMethods,
  type NullFilterClause,
  type QueryRow,
  type ScalarFilterClause,
  type SetFilterClause,
  type TableAggregateRequest,
  type TableScanRequest,
} from "@sqlql/core";
import { query } from "../src";

// Full applyScan helper that handles all filter operators including new ones.
function applyScan(rows: QueryRow[], request: TableScanRequest): QueryRow[] {
  let out = [...rows];

  for (const clause of request.where ?? []) {
    switch (clause.op) {
      case "eq":
        out = out.filter((row) => row[clause.column] === (clause as ScalarFilterClause).value);
        break;
      case "neq":
        out = out.filter((row) => row[clause.column] !== (clause as ScalarFilterClause).value);
        break;
      case "gt":
        out = out.filter(
          (row) => Number(row[clause.column]) > Number((clause as ScalarFilterClause).value),
        );
        break;
      case "gte":
        out = out.filter(
          (row) => Number(row[clause.column]) >= Number((clause as ScalarFilterClause).value),
        );
        break;
      case "lt":
        out = out.filter(
          (row) => Number(row[clause.column]) < Number((clause as ScalarFilterClause).value),
        );
        break;
      case "lte":
        out = out.filter(
          (row) => Number(row[clause.column]) <= Number((clause as ScalarFilterClause).value),
        );
        break;
      case "in": {
        const set = new Set((clause as SetFilterClause).values);
        out = out.filter((row) => set.has(row[clause.column]));
        break;
      }
      case "not_in": {
        const set = new Set((clause as SetFilterClause).values);
        out = out.filter((row) => !set.has(row[clause.column]));
        break;
      }
      case "is_null":
        out = out.filter((row) => row[clause.column] == null);
        break;
      case "is_not_null":
        out = out.filter((row) => row[clause.column] != null);
        break;
      case "like": {
        const pattern = String((clause as ScalarFilterClause).value ?? "");
        const escaped = pattern.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
        const re = new RegExp(`^${escaped.replace(/%/g, ".*").replace(/_/g, ".")}$`);
        out = out.filter((row) => re.test(String(row[clause.column] ?? "")));
        break;
      }
      case "not_like": {
        const pattern = String((clause as ScalarFilterClause).value ?? "");
        const escaped = pattern.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
        const re = new RegExp(`^${escaped.replace(/%/g, ".*").replace(/_/g, ".")}$`);
        out = out.filter((row) => !re.test(String(row[clause.column] ?? "")));
        break;
      }
    }
  }

  if (request.orderBy) {
    out.sort((left, right) => {
      for (const term of request.orderBy ?? []) {
        const leftValue = left[term.column] as string | number;
        const rightValue = right[term.column] as string | number;
        if (leftValue === rightValue) continue;
        const cmp = leftValue < rightValue ? -1 : 1;
        return term.direction === "asc" ? cmp : -cmp;
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

  return out.map((row) => {
    const projected: QueryRow = {};
    for (const column of request.select) {
      projected[column] = row[column] ?? null;
    }
    return projected;
  });
}

const schema = defineSchema({
  tables: {
    orders: {
      columns: {
        id: "text",
        org_id: "text",
        user_id: "text",
        status: "text",
        total_cents: "integer",
        note: "text",
      },
    },
    users: {
      columns: {
        id: "text",
        email: "text",
        manager_id: "text",
      },
    },
  },
});

const data = {
  orders: [
    { id: "o1", org_id: "org1", user_id: "u1", status: "paid", total_cents: 1000, note: null },
    { id: "o2", org_id: "org1", user_id: "u2", status: "pending", total_cents: 2000, note: "rush" },
    { id: "o3", org_id: "org2", user_id: "u1", status: "paid", total_cents: 500, note: "gift" },
    { id: "o4", org_id: "org1", user_id: "u3", status: "paid", total_cents: 3000, note: null },
    { id: "o5", org_id: "org1", user_id: "u1", status: "paid", total_cents: 750, note: "rush" },
  ] as QueryRow[],
  users: [
    { id: "u1", email: "alice@example.com", manager_id: null },
    { id: "u2", email: "bob@example.com", manager_id: "u1" },
    { id: "u3", email: "charlie@test.org", manager_id: "u1" },
  ] as QueryRow[],
};

function makeOrderMethods() {
  return defineTableMethods({
    orders: {
      async scan(request) {
        return applyScan(data.orders, request);
      },
    },
    users: {
      async scan(request) {
        return applyScan(data.users, request);
      },
    },
  });
}

describe("SQL standards extensions", () => {
  // -------------------------------------------------------------------------
  // OFFSET
  // -------------------------------------------------------------------------
  describe("OFFSET", () => {
    it("skips rows using LIMIT ... OFFSET", async () => {
      const methods = makeOrderMethods();
      const rows = await query({
        schema,
        methods,
        context: {},
        sql: `
          SELECT o.id FROM orders o
          WHERE o.org_id = 'org1'
          ORDER BY o.total_cents ASC
          LIMIT 2 OFFSET 1
        `,
      });

      // org1 orders sorted by total_cents: o5(750), o1(1000), o2(2000), o4(3000)
      // skip 1 → start at o1; take 2 → o1, o2
      expect(rows).toEqual([{ id: "o1" }, { id: "o2" }]);
    });
  });

  // -------------------------------------------------------------------------
  // DISTINCT
  // -------------------------------------------------------------------------
  describe("DISTINCT", () => {
    it("removes duplicate rows", async () => {
      const methods = makeOrderMethods();
      const rows = await query({
        schema,
        methods,
        context: {},
        sql: `SELECT DISTINCT o.status FROM orders o ORDER BY o.status ASC`,
      });

      expect(rows).toEqual([{ status: "paid" }, { status: "pending" }]);
    });
  });

  // -------------------------------------------------------------------------
  // LEFT JOIN
  // -------------------------------------------------------------------------
  describe("LEFT JOIN", () => {
    it("preserves left-side rows that have no matching right-side row", async () => {
      const methods = makeOrderMethods();
      const rows = await query({
        schema,
        methods,
        context: {},
        sql: `
          SELECT o.id, u.email
          FROM orders o
          LEFT JOIN users u ON o.user_id = u.id
          WHERE o.org_id = 'org1'
          ORDER BY o.id ASC
        `,
      });

      // All 4 org1 orders; u1→alice, u2→bob, u3→charlie
      expect(rows).toEqual([
        { id: "o1", email: "alice@example.com" },
        { id: "o2", email: "bob@example.com" },
        { id: "o4", email: "charlie@test.org" },
        { id: "o5", email: "alice@example.com" },
      ]);
    });

    it("returns null for unmatched right-side columns", async () => {
      // Add a row referencing a non-existent user
      const ordersWithMissing: QueryRow[] = [
        ...data.orders,
        { id: "o6", org_id: "org1", user_id: "u_missing", status: "paid", total_cents: 100, note: null },
      ];

      const methods = defineTableMethods({
        orders: {
          async scan(request) {
            return applyScan(ordersWithMissing, request);
          },
        },
        users: {
          async scan(request) {
            return applyScan(data.users, request);
          },
        },
      });

      const rows = await query({
        schema,
        methods,
        context: {},
        sql: `
          SELECT o.id, u.email
          FROM orders o
          LEFT JOIN users u ON o.user_id = u.id
          WHERE o.id = 'o6'
        `,
      });

      expect(rows).toEqual([{ id: "o6", email: null }]);
    });
  });

  // -------------------------------------------------------------------------
  // IS NULL / IS NOT NULL
  // -------------------------------------------------------------------------
  describe("IS NULL / IS NOT NULL", () => {
    it("filters rows where column IS NULL", async () => {
      const methods = makeOrderMethods();
      const rows = await query({
        schema,
        methods,
        context: {},
        sql: `SELECT o.id FROM orders o WHERE o.note IS NULL ORDER BY o.id ASC`,
      });

      expect(rows).toEqual([{ id: "o1" }, { id: "o4" }]);
    });

    it("filters rows where column IS NOT NULL", async () => {
      const methods = makeOrderMethods();
      const rows = await query({
        schema,
        methods,
        context: {},
        sql: `SELECT o.id FROM orders o WHERE o.note IS NOT NULL ORDER BY o.id ASC`,
      });

      expect(rows).toEqual([{ id: "o2" }, { id: "o3" }, { id: "o5" }]);
    });

    it("pushes IS NULL down to the scan request", async () => {
      const scanRequests: TableScanRequest[] = [];
      const methods = defineTableMethods({
        orders: {
          async scan(request) {
            scanRequests.push(request);
            return applyScan(data.orders, request);
          },
        },
        users: {
          async scan(request) {
            return applyScan(data.users, request);
          },
        },
      });

      await query({
        schema,
        methods,
        context: {},
        sql: `SELECT o.id FROM orders o WHERE o.note IS NULL`,
      });

      const whereClause = scanRequests[0]?.where;
      expect(whereClause).toContainEqual({ op: "is_null", column: "note" } satisfies NullFilterClause);
    });
  });

  // -------------------------------------------------------------------------
  // NOT IN
  // -------------------------------------------------------------------------
  describe("NOT IN", () => {
    it("excludes rows matching a list", async () => {
      const methods = makeOrderMethods();
      const rows = await query({
        schema,
        methods,
        context: {},
        sql: `SELECT o.id FROM orders o WHERE o.status NOT IN ('pending') ORDER BY o.id ASC`,
      });

      expect(rows).toEqual([{ id: "o1" }, { id: "o3" }, { id: "o4" }, { id: "o5" }]);
    });

    it("pushes NOT IN down to the scan request", async () => {
      const scanRequests: TableScanRequest[] = [];
      const methods = defineTableMethods({
        orders: {
          async scan(request) {
            scanRequests.push(request);
            return applyScan(data.orders, request);
          },
        },
        users: {
          async scan(request) {
            return applyScan(data.users, request);
          },
        },
      });

      await query({
        schema,
        methods,
        context: {},
        sql: `SELECT o.id FROM orders o WHERE o.status NOT IN ('pending')`,
      });

      const whereClause = scanRequests[0]?.where;
      expect(whereClause).toContainEqual({
        op: "not_in",
        column: "status",
        values: ["pending"],
      } satisfies SetFilterClause);
    });
  });

  // -------------------------------------------------------------------------
  // LIKE / NOT LIKE
  // -------------------------------------------------------------------------
  describe("LIKE / NOT LIKE", () => {
    it("filters with LIKE prefix pattern", async () => {
      const methods = makeOrderMethods();
      const rows = await query({
        schema,
        methods,
        context: {},
        sql: `SELECT u.email FROM users u WHERE u.email LIKE 'alice%' ORDER BY u.email ASC`,
      });

      expect(rows).toEqual([{ email: "alice@example.com" }]);
    });

    it("filters with LIKE suffix pattern", async () => {
      const methods = makeOrderMethods();
      const rows = await query({
        schema,
        methods,
        context: {},
        sql: `SELECT u.id FROM users u WHERE u.email LIKE '%@test.org' ORDER BY u.id ASC`,
      });

      expect(rows).toEqual([{ id: "u3" }]);
    });

    it("filters with NOT LIKE", async () => {
      const methods = makeOrderMethods();
      const rows = await query({
        schema,
        methods,
        context: {},
        sql: `SELECT u.id FROM users u WHERE u.email NOT LIKE '%@example.com' ORDER BY u.id ASC`,
      });

      expect(rows).toEqual([{ id: "u3" }]);
    });

    it("pushes LIKE down to the scan request", async () => {
      const scanRequests: TableScanRequest[] = [];
      const methods = defineTableMethods({
        orders: {
          async scan(request) {
            return applyScan(data.orders, request);
          },
        },
        users: {
          async scan(request) {
            scanRequests.push(request);
            return applyScan(data.users, request);
          },
        },
      });

      await query({
        schema,
        methods,
        context: {},
        sql: `SELECT u.id FROM users u WHERE u.email LIKE '%@example.com'`,
      });

      const whereClause = scanRequests[0]?.where;
      expect(whereClause).toContainEqual({
        op: "like",
        column: "email",
        value: "%@example.com",
      } satisfies ScalarFilterClause);
    });
  });

  // -------------------------------------------------------------------------
  // BETWEEN / NOT BETWEEN
  // -------------------------------------------------------------------------
  describe("BETWEEN / NOT BETWEEN", () => {
    it("filters with BETWEEN (desugars to gte + lte)", async () => {
      const methods = makeOrderMethods();
      const rows = await query({
        schema,
        methods,
        context: {},
        sql: `
          SELECT o.id FROM orders o
          WHERE o.total_cents BETWEEN 750 AND 2000
          ORDER BY o.total_cents ASC
        `,
      });

      // Orders with total_cents 750 ≤ x ≤ 2000: o5(750), o1(1000), o2(2000)
      expect(rows).toEqual([{ id: "o5" }, { id: "o1" }, { id: "o2" }]);
    });

    it("pushes BETWEEN bounds down to the scan as gte+lte", async () => {
      const scanRequests: TableScanRequest[] = [];
      const methods = defineTableMethods({
        orders: {
          async scan(request) {
            scanRequests.push(request);
            return applyScan(data.orders, request);
          },
        },
        users: {
          async scan(request) {
            return applyScan(data.users, request);
          },
        },
      });

      await query({
        schema,
        methods,
        context: {},
        sql: `SELECT o.id FROM orders o WHERE o.total_cents BETWEEN 750 AND 2000`,
      });

      const where = scanRequests[0]?.where ?? [];
      expect(where).toContainEqual({ op: "gte", column: "total_cents", value: 750 });
      expect(where).toContainEqual({ op: "lte", column: "total_cents", value: 2000 });
    });

    it("filters with NOT BETWEEN", async () => {
      const methods = makeOrderMethods();
      const rows = await query({
        schema,
        methods,
        context: {},
        sql: `
          SELECT o.id FROM orders o
          WHERE o.total_cents NOT BETWEEN 750 AND 2000
          ORDER BY o.total_cents ASC
        `,
      });

      // Orders with total_cents < 750 or > 2000: o3(500), o4(3000)
      expect(rows).toEqual([{ id: "o3" }, { id: "o4" }]);
    });
  });

  // -------------------------------------------------------------------------
  // OR predicates
  // -------------------------------------------------------------------------
  describe("OR predicates", () => {
    it("applies OR conditions post-scan", async () => {
      const methods = makeOrderMethods();
      const rows = await query({
        schema,
        methods,
        context: {},
        sql: `
          SELECT o.id FROM orders o
          WHERE o.status = 'pending' OR o.total_cents > 2500
          ORDER BY o.id ASC
        `,
      });

      // pending: o2; total_cents > 2500: o4(3000)
      expect(rows).toEqual([{ id: "o2" }, { id: "o4" }]);
    });

    it("handles AND combined with OR", async () => {
      const methods = makeOrderMethods();
      const rows = await query({
        schema,
        methods,
        context: {},
        sql: `
          SELECT o.id FROM orders o
          WHERE o.org_id = 'org1' AND (o.status = 'pending' OR o.total_cents > 2500)
          ORDER BY o.id ASC
        `,
      });

      // org1 orders where pending OR total > 2500: o2, o4
      expect(rows).toEqual([{ id: "o2" }, { id: "o4" }]);
    });
  });

  // -------------------------------------------------------------------------
  // GROUP BY + aggregates (in-memory fallback)
  // -------------------------------------------------------------------------
  describe("GROUP BY + aggregates (in-memory)", () => {
    it("groups by a single column and counts rows", async () => {
      const methods = makeOrderMethods();
      const rows = await query({
        schema,
        methods,
        context: {},
        sql: `
          SELECT o.org_id, COUNT(*) as cnt
          FROM orders o
          GROUP BY o.org_id
          ORDER BY o.org_id ASC
        `,
      });

      // org1 has 4 orders, org2 has 1
      expect(rows).toEqual([
        { org_id: "org1", cnt: 4 },
        { org_id: "org2", cnt: 1 },
      ]);
    });

    it("computes SUM and AVG aggregates", async () => {
      const methods = makeOrderMethods();
      const rows = await query({
        schema,
        methods,
        context: {},
        sql: `
          SELECT o.org_id, SUM(o.total_cents) as total, AVG(o.total_cents) as avg_cents
          FROM orders o
          GROUP BY o.org_id
          ORDER BY o.org_id ASC
        `,
      });

      expect(rows).toEqual([
        { org_id: "org1", total: 1000 + 2000 + 3000 + 750, avg_cents: (1000 + 2000 + 3000 + 750) / 4 },
        { org_id: "org2", total: 500, avg_cents: 500 },
      ]);
    });

    it("applies WHERE before aggregation", async () => {
      const methods = makeOrderMethods();
      const rows = await query({
        schema,
        methods,
        context: {},
        sql: `
          SELECT o.status, COUNT(*) as cnt
          FROM orders o
          WHERE o.org_id = 'org1'
          GROUP BY o.status
          ORDER BY o.status ASC
        `,
      });

      // org1: 3 paid (o1, o4, o5), 1 pending (o2)
      expect(rows).toEqual([
        { status: "paid", cnt: 3 },
        { status: "pending", cnt: 1 },
      ]);
    });

    it("returns COUNT=0 for empty result with no GROUP BY", async () => {
      const methods = makeOrderMethods();
      const rows = await query({
        schema,
        methods,
        context: {},
        sql: `SELECT COUNT(*) as cnt FROM orders o WHERE o.org_id = 'nonexistent'`,
      });

      expect(rows).toEqual([{ cnt: 0 }]);
    });
  });

  // -------------------------------------------------------------------------
  // GROUP BY + aggregates (delegated to aggregate method)
  // -------------------------------------------------------------------------
  describe("GROUP BY + aggregates (delegated)", () => {
    it("delegates to the aggregate method when available", async () => {
      const aggregateCalls: TableAggregateRequest[] = [];

      const methods = defineTableMethods({
        orders: {
          async scan(request) {
            return applyScan(data.orders, request);
          },
          async aggregate(request) {
            aggregateCalls.push(request);
            // Simulate a group-by result
            return [
              { org_id: "org1", cnt: 4 },
              { org_id: "org2", cnt: 1 },
            ];
          },
        },
        users: {
          async scan(request) {
            return applyScan(data.users, request);
          },
        },
      });

      const rows = await query({
        schema,
        methods,
        context: {},
        sql: `
          SELECT o.org_id, COUNT(*) as cnt
          FROM orders o
          GROUP BY o.org_id
        `,
      });

      // Should have called aggregate, not scan
      expect(aggregateCalls).toHaveLength(1);
      expect(aggregateCalls[0]).toMatchObject({
        table: "orders",
        metrics: [{ fn: "count", as: "cnt" }],
        groupBy: ["org_id"],
      });
      expect(rows).toEqual([
        { org_id: "org1", cnt: 4 },
        { org_id: "org2", cnt: 1 },
      ]);
    });

    it("passes WHERE filters to the aggregate method", async () => {
      const aggregateCalls: TableAggregateRequest[] = [];

      const methods = defineTableMethods({
        orders: {
          async scan(request) {
            return applyScan(data.orders, request);
          },
          async aggregate(request) {
            aggregateCalls.push(request);
            return [{ status: "paid", cnt: 3 }];
          },
        },
        users: {
          async scan(request) {
            return applyScan(data.users, request);
          },
        },
      });

      await query({
        schema,
        methods,
        context: {},
        sql: `
          SELECT o.status, COUNT(*) as cnt
          FROM orders o
          WHERE o.org_id = 'org1'
          GROUP BY o.status
        `,
      });

      expect(aggregateCalls[0]?.where).toContainEqual({
        op: "eq",
        column: "org_id",
        value: "org1",
      });
    });
  });
});
