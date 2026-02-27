import { describe, expect, it } from "vitest";

import { commerceRows, commerceSchema } from "../support/commerce-fixture";
import { withQueryHarness } from "../support/query-harness";

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
});
