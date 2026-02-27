import { describe, expect, it } from "vitest";

import { commerceRows, commerceSchema } from "../support/commerce-fixture";
import { withQueryHarness } from "../support/query-harness";

const EMPTY_CONTEXT = {} as const;

describe("query/subqueries", () => {
  it("supports IN (SELECT ...)", async () => {
    await withQueryHarness(
      {
        schema: commerceSchema,
        rowsByTable: commerceRows,
      },
      async (harness) => {
        const { actual, expected } = await harness.runAgainstBoth(
          `
            SELECT id
            FROM orders
            WHERE user_id IN (
              SELECT id
              FROM users
              WHERE team_id = 'team_enterprise'
            )
            ORDER BY id ASC
          `,
          EMPTY_CONTEXT,
        );

        expect(actual).toEqual(expected);
        expect(actual).toEqual([{ id: "ord_1" }, { id: "ord_2" }, { id: "ord_4" }]);
      },
    );
  });

  it("supports EXISTS (SELECT ...)", async () => {
    await withQueryHarness(
      {
        schema: commerceSchema,
        rowsByTable: commerceRows,
      },
      async (harness) => {
        const { actual, expected } = await harness.runAgainstBoth(
          `
            SELECT id
            FROM orders
            WHERE EXISTS (
              SELECT id
              FROM users
              WHERE team_id = 'team_smb'
            )
            ORDER BY id ASC
          `,
          EMPTY_CONTEXT,
        );

        expect(actual).toEqual(expected);
        expect(actual).toEqual([
          { id: "ord_1" },
          { id: "ord_2" },
          { id: "ord_3" },
          { id: "ord_4" },
        ]);
      },
    );
  });

  it("supports scalar subqueries in WHERE and SELECT", async () => {
    await withQueryHarness(
      {
        schema: commerceSchema,
        rowsByTable: commerceRows,
      },
      async (harness) => {
        const whereScalar = await harness.runAgainstBoth(
          `
            SELECT id
            FROM orders
            WHERE total_cents = (SELECT MAX(total_cents) FROM orders)
          `,
          EMPTY_CONTEXT,
        );

        expect(whereScalar.actual).toEqual(whereScalar.expected);
        expect(whereScalar.actual).toEqual([{ id: "ord_4" }]);

        const selectScalar = await harness.runAgainstBoth(
          `
            SELECT id, (SELECT MAX(total_cents) FROM orders) AS max_total
            FROM orders
            ORDER BY id ASC
            LIMIT 2
          `,
          EMPTY_CONTEXT,
        );

        expect(selectScalar.actual).toEqual(selectScalar.expected);
        expect(selectScalar.actual).toEqual([
          { id: "ord_1", max_total: 9900 },
          { id: "ord_2", max_total: 9900 },
        ]);
      },
    );
  });
});
