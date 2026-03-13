import { describe, expect, it } from "vitest";

import { commerceRows, commerceSchema } from "@tupl/test-support/fixtures";
import { withQueryHarness } from "@tupl/test-support/runtime";
import { buildEntitySchema } from "@tupl/test-support/schema";

const EMPTY_CONTEXT = {} as const;

describe("query/window", () => {
  it("supports ROW_NUMBER partitioned ordering with sqlite parity", async () => {
    await withQueryHarness(
      {
        schema: commerceSchema,
        rowsByTable: commerceRows,
      },
      async (harness) => {
        const { actual, expected } = await harness.runAgainstBoth(
          `
            SELECT
              o.id,
              o.org_id,
              ROW_NUMBER() OVER (PARTITION BY o.org_id ORDER BY o.created_at ASC) AS rn
            FROM orders o
            ORDER BY o.id ASC
          `,
          EMPTY_CONTEXT,
        );

        expect(actual).toEqual(expected);
        expect(actual).toEqual([
          { id: "ord_1", org_id: "org_1", rn: 1 },
          { id: "ord_2", org_id: "org_1", rn: 2 },
          { id: "ord_3", org_id: "org_1", rn: 3 },
          { id: "ord_4", org_id: "org_2", rn: 1 },
        ]);
      },
    );
  });

  it("supports RANK and DENSE_RANK with ties", async () => {
    const schema = buildEntitySchema({
      scores: {
        columns: {
          id: { type: "text", nullable: false },
          team: { type: "text", nullable: false },
          score: { type: "integer", nullable: false },
        },
      },
    });

    await withQueryHarness(
      {
        schema,
        rowsByTable: {
          scores: [
            { id: "s1", team: "a", score: 10 },
            { id: "s2", team: "a", score: 8 },
            { id: "s3", team: "a", score: 8 },
            { id: "s4", team: "a", score: 5 },
          ],
        },
      },
      async (harness) => {
        const { actual, expected } = await harness.runAgainstBoth(
          `
            SELECT
              s.id,
              s.score,
              RANK() OVER (PARTITION BY s.team ORDER BY s.score DESC) AS rnk,
              DENSE_RANK() OVER (PARTITION BY s.team ORDER BY s.score DESC) AS dense_rnk
            FROM scores s
            ORDER BY s.id ASC
          `,
          EMPTY_CONTEXT,
        );

        expect(actual).toEqual(expected);
        expect(actual).toEqual([
          { id: "s1", score: 10, rnk: 1, dense_rnk: 1 },
          { id: "s2", score: 8, rnk: 2, dense_rnk: 2 },
          { id: "s3", score: 8, rnk: 2, dense_rnk: 2 },
          { id: "s4", score: 5, rnk: 4, dense_rnk: 3 },
        ]);
      },
    );
  });

  it("supports COUNT and running SUM window aggregates", async () => {
    await withQueryHarness(
      {
        schema: commerceSchema,
        rowsByTable: commerceRows,
      },
      async (harness) => {
        const { actual, expected } = await harness.runAgainstBoth(
          `
            SELECT
              o.id,
              COUNT(*) OVER (PARTITION BY o.org_id) AS org_count,
              SUM(o.total_cents) OVER (PARTITION BY o.org_id ORDER BY o.created_at ASC) AS running_total
            FROM orders o
            WHERE o.org_id = 'org_1'
            ORDER BY o.id ASC
          `,
          EMPTY_CONTEXT,
        );

        expect(actual).toEqual(expected);
        expect(actual).toEqual([
          { id: "ord_1", org_count: 3, running_total: 1200 },
          { id: "ord_2", org_count: 3, running_total: 3000 },
          { id: "ord_3", org_count: 3, running_total: 5400 },
        ]);
      },
    );
  });

  it("supports ORDER BY window output aliases with sqlite parity", async () => {
    await withQueryHarness(
      {
        schema: commerceSchema,
        rowsByTable: commerceRows,
      },
      async (harness) => {
        const { actual, expected } = await harness.runAgainstBoth(
          `
            SELECT
              o.id,
              o.user_id,
              o.total_cents,
              RANK() OVER (PARTITION BY o.user_id ORDER BY o.total_cents DESC) AS spend_rank
            FROM orders o
            ORDER BY o.user_id ASC, spend_rank ASC, o.id ASC
          `,
          EMPTY_CONTEXT,
        );

        expect(actual).toEqual(expected);
        expect(actual).toEqual([
          { id: "ord_2", user_id: "usr_1", total_cents: 1800, spend_rank: 1 },
          { id: "ord_1", user_id: "usr_1", total_cents: 1200, spend_rank: 2 },
          { id: "ord_3", user_id: "usr_2", total_cents: 2400, spend_rank: 1 },
          { id: "ord_4", user_id: "usr_3", total_cents: 9900, spend_rank: 1 },
        ]);
      },
    );
  });

  it("supports explicit ROWS frame clauses", async () => {
    await withQueryHarness(
      {
        schema: commerceSchema,
        rowsByTable: commerceRows,
      },
      async (harness) => {
        const { actual, expected } = await harness.runAgainstBoth(
          `
            SELECT
              id,
              SUM(total_cents) OVER (
                PARTITION BY org_id
                ORDER BY created_at
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
              ) AS running_total
            FROM orders
            WHERE org_id = 'org_1'
            ORDER BY id ASC
          `,
          EMPTY_CONTEXT,
        );

        expect(actual).toEqual(expected);
        expect(actual).toEqual([
          { id: "ord_1", running_total: 1200 },
          { id: "ord_2", running_total: 3000 },
          { id: "ord_3", running_total: 5400 },
        ]);
      },
    );
  });

  it("supports named WINDOW clauses and references", async () => {
    await withQueryHarness(
      {
        schema: commerceSchema,
        rowsByTable: commerceRows,
      },
      async (harness) => {
        const { actual, expected } = await harness.runAgainstBoth(
          `
            SELECT
              id,
              SUM(total_cents) OVER w AS running_total
            FROM orders
            WINDOW w AS (
              PARTITION BY org_id
              ORDER BY created_at
              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
            ORDER BY id ASC
          `,
          EMPTY_CONTEXT,
        );

        expect(actual).toEqual(expected);
        expect(actual).toEqual([
          { id: "ord_1", running_total: 1200 },
          { id: "ord_2", running_total: 3000 },
          { id: "ord_3", running_total: 5400 },
          { id: "ord_4", running_total: 9900 },
        ]);
      },
    );
  });

  it("rejects unsupported navigation window functions", async () => {
    await withQueryHarness(
      {
        schema: commerceSchema,
        rowsByTable: commerceRows,
      },
      async (harness) => {
        await expect(
          harness.runTupl(
            `
              SELECT
                o.id,
                LEAD(o.total_cents) OVER (PARTITION BY o.org_id ORDER BY o.created_at) AS next_total,
                LAG(o.total_cents, 1, 0) OVER (PARTITION BY o.org_id ORDER BY o.created_at) AS prev_total
              FROM orders o
              WHERE o.org_id = 'org_1'
              ORDER BY o.id ASC
            `,
            EMPTY_CONTEXT,
          ),
        ).rejects.toThrow("Unsupported window function: LEAD");
      },
    );
  });

  it("rejects unsupported window functions", async () => {
    await withQueryHarness(
      {
        schema: commerceSchema,
        rowsByTable: commerceRows,
      },
      async (harness) => {
        await expect(
          harness.runTupl(
            `
              SELECT FIRST_VALUE(total_cents) OVER (PARTITION BY org_id ORDER BY created_at) AS first_total
              FROM orders
            `,
            EMPTY_CONTEXT,
          ),
        ).rejects.toThrow("Unsupported window function: FIRST_VALUE");
      },
    );
  });

  it("rejects mixing GROUP BY/HAVING with window functions", async () => {
    await withQueryHarness(
      {
        schema: commerceSchema,
        rowsByTable: commerceRows,
      },
      async (harness) => {
        await expect(
          harness.runTupl(
            `
              SELECT
                org_id,
                COUNT(*) AS order_count,
                ROW_NUMBER() OVER (PARTITION BY org_id ORDER BY org_id) AS rn
              FROM orders
              GROUP BY org_id
            `,
            EMPTY_CONTEXT,
          ),
        ).rejects.toThrow("Window functions cannot be mixed with GROUP BY/HAVING.");
      },
    );
  });
});
