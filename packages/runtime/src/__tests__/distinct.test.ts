import { describe, expect, it } from "vitest";

import { commerceRows, commerceSchema } from "../../../../test/support/commerce-fixture";
import { withQueryHarness } from "../../../../test/support/query-harness";

const EMPTY_CONTEXT = {} as const;

describe("query/distinct", () => {
  it("supports SELECT DISTINCT", async () => {
    await withQueryHarness(
      {
        schema: commerceSchema,
        rowsByTable: commerceRows,
      },
      async (harness) => {
        const { actual, expected } = await harness.runAgainstBoth(
          `
            SELECT DISTINCT user_id
            FROM orders
            ORDER BY user_id ASC
          `,
          EMPTY_CONTEXT,
        );

        expect(actual).toEqual(expected);
        expect(actual).toEqual([{ user_id: "usr_1" }, { user_id: "usr_2" }, { user_id: "usr_3" }]);
      },
    );
  });

  it("supports DISTINCT with LIMIT/OFFSET", async () => {
    await withQueryHarness(
      {
        schema: commerceSchema,
        rowsByTable: commerceRows,
      },
      async (harness) => {
        const { actual, expected } = await harness.runAgainstBoth(
          `
            SELECT DISTINCT org_id
            FROM orders
            ORDER BY org_id ASC
            LIMIT 1 OFFSET 1
          `,
          EMPTY_CONTEXT,
        );

        expect(actual).toEqual(expected);
        expect(actual).toEqual([{ org_id: "org_2" }]);
      },
    );
  });
});
