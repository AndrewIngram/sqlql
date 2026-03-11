import { describe, expect, it } from "vitest";
import { queryWithMethods } from "../../../../test/support/methods-provider";
import {
  aggregateArrayRows,
  createArrayTableMethods,
  lookupArrayRows,
  scanArrayRows,
} from "../../../../test/support/array-methods";

import { defineTableMethods, type QueryRow } from "@tupl/schema";
import { buildEntitySchema } from "../../../../test/support/schema-builder";

describe("array methods", () => {
  const orders: QueryRow[] = [
    { id: "ord_1", org_id: "org_1", user_id: "usr_1", total_cents: 1200, created_at: "2026-02-01" },
    { id: "ord_2", org_id: "org_1", user_id: "usr_1", total_cents: 1800, created_at: "2026-02-03" },
    { id: "ord_3", org_id: "org_1", user_id: "usr_2", total_cents: 2400, created_at: "2026-02-04" },
    { id: "ord_4", org_id: "org_2", user_id: "usr_3", total_cents: 9900, created_at: "2026-02-05" },
  ];

  it("scans array rows with filters, sort, offset, and limit", () => {
    const rows = scanArrayRows(orders, {
      table: "orders",
      select: ["id", "total_cents"],
      where: [{ op: "eq", column: "org_id", value: "org_1" }],
      orderBy: [{ column: "created_at", direction: "desc" }],
      offset: 1,
      limit: 1,
    });

    expect(rows).toEqual([{ id: "ord_2", total_cents: 1800 }]);
  });

  it("looks up rows by key set and projects requested columns", () => {
    const rows = lookupArrayRows(orders, {
      table: "orders",
      key: "id",
      values: ["ord_1", "ord_3"],
      select: ["id", "user_id"],
    });

    expect(rows).toEqual([
      { id: "ord_1", user_id: "usr_1" },
      { id: "ord_3", user_id: "usr_2" },
    ]);
  });

  it("aggregates array rows", () => {
    const rows = aggregateArrayRows(orders, {
      table: "orders",
      where: [{ op: "eq", column: "org_id", value: "org_1" }],
      groupBy: ["user_id"],
      metrics: [
        { fn: "count", as: "count" },
        { fn: "sum", column: "total_cents", as: "sum" },
        { fn: "avg", column: "total_cents", as: "avg" },
        { fn: "min", column: "total_cents", as: "min" },
        { fn: "max", column: "total_cents", as: "max" },
      ],
    });

    expect(rows).toEqual([
      { user_id: "usr_1", count: 2, sum: 3000, avg: 1500, min: 1200, max: 1800 },
      { user_id: "usr_2", count: 1, sum: 2400, avg: 2400, min: 2400, max: 2400 },
    ]);
  });

  it("can be used directly as table methods in query execution", async () => {
    const users: QueryRow[] = [
      { id: "usr_1", email: "a@example.com" },
      { id: "usr_2", email: "b@example.com" },
      { id: "usr_3", email: "c@example.com" },
    ];

    const schema = buildEntitySchema({
      orders: {
        columns: {
          id: "text",
          org_id: "text",
          user_id: "text",
          total_cents: "integer",
          created_at: "timestamp",
        },
      },
      users: {
        columns: {
          id: "text",
          email: "text",
        },
      },
    });

    const methods = defineTableMethods(schema, {
      orders: createArrayTableMethods(orders),
      users: createArrayTableMethods(users),
    });

    const joinRows = await queryWithMethods({
      schema,
      methods,
      context: {},
      sql: `
        SELECT o.id, u.email
        FROM orders o
        JOIN users u ON o.user_id = u.id
        WHERE o.org_id = 'org_1'
        ORDER BY o.created_at DESC
        LIMIT 2
      `,
    });

    expect(joinRows).toEqual([
      { id: "ord_3", email: "b@example.com" },
      { id: "ord_2", email: "a@example.com" },
    ]);

    const aggregateRows = await queryWithMethods({
      schema,
      methods,
      context: {},
      sql: `
        SELECT o.user_id, COUNT(*) AS order_count, SUM(o.total_cents) AS total_cents
        FROM orders o
        WHERE o.org_id = 'org_1'
        GROUP BY o.user_id
        ORDER BY total_cents DESC
      `,
    });

    expect(aggregateRows).toEqual([
      { user_id: "usr_1", order_count: 2, total_cents: 3000 },
      { user_id: "usr_2", order_count: 1, total_cents: 2400 },
    ]);
  });
});
