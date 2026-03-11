import { describe, expect, it } from "vitest";

import { commerceRows, commerceSchema } from "@tupl/test-support/fixtures";
import { withQueryHarness } from "@tupl/test-support/runtime";

const EMPTY_CONTEXT = {} as const;

describe("query/predicates", () => {
  it("applies SQL boolean precedence (AND before OR) without explicit parentheses", async () => {
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
            WHERE org_id = 'org_1' OR status = 'paid' AND total_cents > 5000
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

  it("supports OR predicates", async () => {
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
            WHERE org_id = 'org_2' OR total_cents >= 2400
            ORDER BY id ASC
          `,
          EMPTY_CONTEXT,
        );

        expect(actual).toEqual(expected);
        expect(actual).toEqual([{ id: "ord_3" }, { id: "ord_4" }]);
      },
    );
  });

  it("supports NOT predicates", async () => {
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
            WHERE NOT (org_id = 'org_1')
            ORDER BY id ASC
          `,
          EMPTY_CONTEXT,
        );

        expect(actual).toEqual(expected);
        expect(actual).toEqual([{ id: "ord_4" }]);
      },
    );
  });

  it("supports mixed boolean predicate trees", async () => {
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
            WHERE org_id = 'org_1' AND (status = 'paid' OR total_cents > 2000)
            ORDER BY id ASC
          `,
          EMPTY_CONTEXT,
        );

        expect(actual).toEqual(expected);
        expect(actual).toEqual([{ id: "ord_1" }, { id: "ord_2" }, { id: "ord_3" }]);
      },
    );
  });

  it("supports BETWEEN predicates", async () => {
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
            WHERE total_cents BETWEEN 1500 AND 3000
            ORDER BY id ASC
          `,
          EMPTY_CONTEXT,
        );

        expect(actual).toEqual(expected);
        expect(actual).toEqual([{ id: "ord_2" }, { id: "ord_3" }]);
      },
    );
  });
});
