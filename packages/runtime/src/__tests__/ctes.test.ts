import { describe, expect, it } from "vitest";

import { commerceRows, commerceSchema } from "@tupl/test-support/fixtures";
import { withQueryHarness } from "@tupl/test-support/runtime";
import { buildEntitySchema } from "@tupl/test-support/schema";

const EMPTY_CONTEXT = {} as const;

describe("query/ctes", () => {
  it("supports non-recursive CTE queries with sqlite parity", async () => {
    await withQueryHarness(
      {
        schema: commerceSchema,
        rowsByTable: commerceRows,
      },
      async (harness) => {
        const sql = `
          WITH recent_orders AS (
            SELECT id, user_id, total_cents
            FROM orders
            WHERE org_id = 'org_1' AND total_cents >= 1800
          )
          SELECT r.user_id, COUNT(*) AS recent_order_count, SUM(r.total_cents) AS total_cents
          FROM recent_orders r
          GROUP BY r.user_id
          ORDER BY total_cents DESC
        `;

        const { actual, expected } = await harness.runAgainstBoth(sql, EMPTY_CONTEXT);
        expect(actual).toEqual(expected);
      },
    );
  });

  it("supports multi-CTE plans with joins and aggregates", async () => {
    await withQueryHarness(
      {
        schema: commerceSchema,
        rowsByTable: commerceRows,
      },
      async (harness) => {
        const sql = `
          WITH scoped_orders AS (
            SELECT user_id, total_cents
            FROM orders
            WHERE org_id = 'org_1'
          ),
          order_totals AS (
            SELECT user_id, COUNT(*) AS order_count, SUM(total_cents) AS total_cents
            FROM scoped_orders
            GROUP BY user_id
          )
          SELECT u.id, u.email, ot.order_count, ot.total_cents
          FROM order_totals ot
          JOIN users u ON ot.user_id = u.id
          ORDER BY ot.total_cents DESC, u.id ASC
        `;

        const { actual, expected } = await harness.runAgainstBoth(sql, EMPTY_CONTEXT);
        expect(actual).toEqual(expected);
      },
    );
  });

  it("returns correct aggregate results when CTE source is empty", async () => {
    await withQueryHarness(
      {
        schema: commerceSchema,
        rowsByTable: commerceRows,
      },
      async (harness) => {
        const sql = `
          WITH missing_orders AS (
            SELECT id
            FROM orders
            WHERE org_id = 'org_missing'
          )
          SELECT COUNT(*) AS order_count
          FROM missing_orders
        `;

        const { actual, expected } = await harness.runAgainstBoth(sql, EMPTY_CONTEXT);
        expect(actual).toEqual(expected);
        expect(actual).toEqual([{ order_count: 0 }]);
      },
    );
  });

  it("supports recursive CTE queries with sqlite parity", async () => {
    const sql = `
      WITH RECURSIVE reachable AS (
        SELECT source_id AS node_id
        FROM edges
        WHERE source_id = 1
        UNION ALL
        SELECT e.target_id AS node_id
        FROM reachable r
        JOIN edges e ON e.source_id = r.node_id
      )
      SELECT node_id
      FROM reachable
      ORDER BY node_id ASC
    `;

    await withQueryHarness(
      {
        schema: buildEntitySchema({
          edges: {
            columns: {
              source_id: { type: "integer", nullable: false },
              target_id: { type: "integer", nullable: false },
            },
          },
        }),
        rowsByTable: {
          edges: [
            { source_id: 1, target_id: 2 },
            { source_id: 2, target_id: 3 },
            { source_id: 3, target_id: 4 },
          ],
        },
      },
      async (harness) => {
        const { actual, expected } = await harness.runAgainstBoth(sql, EMPTY_CONTEXT);
        expect(actual).toEqual(expected);
        expect(actual).toEqual([{ node_id: 1 }, { node_id: 2 }, { node_id: 3 }, { node_id: 4 }]);
      },
    );
  });

  it("supports SELECT without FROM with sqlite parity", async () => {
    const sql = `
      SELECT 1 AS answer, 2 + 3 AS sum_value
    `;

    await withQueryHarness(
      {
        schema: buildEntitySchema({}),
        rowsByTable: {},
      },
      async (harness) => {
        const { actual, expected } = await harness.runAgainstBoth(sql, EMPTY_CONTEXT);
        expect(actual).toEqual(expected);
        expect(actual).toEqual([{ answer: 1, sum_value: 5 }]);
      },
    );
  });

  it("supports Fibonacci recursive CTE queries with sqlite parity", async () => {
    const sql = `
      WITH RECURSIVE fib AS (
        SELECT 0 AS n, 1 AS next_n, 1 AS depth
        UNION ALL
        SELECT next_n, n + next_n, depth + 1
        FROM fib
        WHERE depth < 10
      )
      SELECT n
      FROM fib
      ORDER BY depth ASC
    `;

    await withQueryHarness(
      {
        schema: buildEntitySchema({}),
        rowsByTable: {},
      },
      async (harness) => {
        const { actual, expected } = await harness.runAgainstBoth(sql, EMPTY_CONTEXT);
        expect(actual).toEqual(expected);
        expect(actual).toEqual([
          { n: 0 },
          { n: 1 },
          { n: 1 },
          { n: 2 },
          { n: 3 },
          { n: 5 },
          { n: 8 },
          { n: 13 },
          { n: 21 },
          { n: 34 },
        ]);
      },
    );
  });

  it("supports FROM subqueries with sqlite parity", async () => {
    const sql = `
      SELECT scoped.id
      FROM (
        SELECT id, total_cents
        FROM orders
        WHERE org_id = 'org_1'
      ) scoped
      WHERE scoped.total_cents >= 1800
      ORDER BY scoped.id ASC
    `;

    await withQueryHarness(
      {
        schema: commerceSchema,
        rowsByTable: commerceRows,
      },
      async (harness) => {
        const { actual, expected } = await harness.runAgainstBoth(sql, EMPTY_CONTEXT);
        expect(actual).toEqual(expected);
        expect(actual).toEqual([{ id: "ord_2" }, { id: "ord_3" }]);
      },
    );
  });
});
