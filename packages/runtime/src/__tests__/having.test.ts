import { describe, expect, it } from "vite-plus/test";

import { commerceRows, commerceSchema } from "@tupl/test-support/fixtures";
import { withQueryHarness } from "@tupl/test-support/runtime";

const EMPTY_CONTEXT = {} as const;

describe("query/having", () => {
  it("supports HAVING with aggregate comparisons", async () => {
    await withQueryHarness(
      {
        schema: commerceSchema,
        rowsByTable: commerceRows,
      },
      async (harness) => {
        const { actual, expected } = await harness.runAgainstBoth(
          `
            SELECT user_id, COUNT(*) AS order_count
            FROM orders
            WHERE org_id = 'org_1'
            GROUP BY user_id
            HAVING COUNT(*) > 1
            ORDER BY user_id ASC
          `,
          EMPTY_CONTEXT,
        );

        expect(actual).toEqual(expected);
        expect(actual).toEqual([{ user_id: "usr_1", order_count: 2 }]);
      },
    );
  });

  it("supports HAVING on aggregates not present in SELECT output", async () => {
    await withQueryHarness(
      {
        schema: commerceSchema,
        rowsByTable: commerceRows,
      },
      async (harness) => {
        const { actual, expected } = await harness.runAgainstBoth(
          `
            SELECT user_id
            FROM orders
            WHERE org_id = 'org_1'
            GROUP BY user_id
            HAVING SUM(total_cents) >= 2500
            ORDER BY user_id ASC
          `,
          EMPTY_CONTEXT,
        );

        expect(actual).toEqual(expected);
        expect(actual).toEqual([{ user_id: "usr_1" }]);
      },
    );
  });
});
