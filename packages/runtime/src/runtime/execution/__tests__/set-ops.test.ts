import { describe, expect, it } from "vite-plus/test";
import { stringifyUnknownValue } from "@tupl/foundation";

import { commerceRows, commerceSchema } from "@tupl/test-support/fixtures";
import { withQueryHarness } from "@tupl/test-support/runtime";

const EMPTY_CONTEXT = {} as const;

function sortRowsByKey(
  rows: Array<Record<string, unknown>>,
  key: string,
): Array<Record<string, unknown>> {
  return [...rows].sort((left, right) => {
    const leftValue = stringifyUnknownValue(left[key]);
    const rightValue = stringifyUnknownValue(right[key]);
    return leftValue < rightValue ? -1 : leftValue > rightValue ? 1 : 0;
  });
}

describe("query/set-ops", () => {
  it("supports UNION ALL", async () => {
    await withQueryHarness(
      {
        schema: commerceSchema,
        rowsByTable: commerceRows,
      },
      async (harness) => {
        const { actual, expected } = await harness.runAgainstBoth(
          `
            SELECT user_id AS id FROM orders WHERE org_id = 'org_1'
            UNION ALL
            SELECT id FROM users
          `,
          EMPTY_CONTEXT,
        );

        expect(sortRowsByKey(actual, "id")).toEqual(sortRowsByKey(expected, "id"));
      },
    );
  });

  it("supports UNION", async () => {
    await withQueryHarness(
      {
        schema: commerceSchema,
        rowsByTable: commerceRows,
      },
      async (harness) => {
        const { actual, expected } = await harness.runAgainstBoth(
          `
            SELECT user_id AS id FROM orders
            UNION
            SELECT id FROM users
          `,
          EMPTY_CONTEXT,
        );

        expect(sortRowsByKey(actual, "id")).toEqual(sortRowsByKey(expected, "id"));
        expect(sortRowsByKey(actual, "id")).toEqual([
          { id: "usr_1" },
          { id: "usr_2" },
          { id: "usr_3" },
        ]);
      },
    );
  });

  it("supports UNION with duplicates removed", async () => {
    await withQueryHarness(
      {
        schema: commerceSchema,
        rowsByTable: commerceRows,
      },
      async (harness) => {
        const result = await harness.runAgainstBoth(
          `
            SELECT user_id AS id FROM orders WHERE org_id = 'org_1'
            UNION
            SELECT user_id AS id FROM orders WHERE org_id = 'org_1'
          `,
          EMPTY_CONTEXT,
        );

        expect(sortRowsByKey(result.actual, "id")).toEqual(sortRowsByKey(result.expected, "id"));
        expect(sortRowsByKey(result.actual, "id")).toEqual([{ id: "usr_1" }, { id: "usr_2" }]);
      },
    );
  });

  it("supports INTERSECT", async () => {
    await withQueryHarness(
      {
        schema: commerceSchema,
        rowsByTable: commerceRows,
      },
      async (harness) => {
        const { actual, expected } = await harness.runAgainstBoth(
          `
            SELECT user_id AS id FROM orders
            INTERSECT
            SELECT id FROM users
          `,
          EMPTY_CONTEXT,
        );

        expect(sortRowsByKey(actual, "id")).toEqual(sortRowsByKey(expected, "id"));
        expect(sortRowsByKey(actual, "id")).toEqual([
          { id: "usr_1" },
          { id: "usr_2" },
          { id: "usr_3" },
        ]);
      },
    );
  });

  it("supports EXCEPT", async () => {
    await withQueryHarness(
      {
        schema: commerceSchema,
        rowsByTable: commerceRows,
      },
      async (harness) => {
        const { actual, expected } = await harness.runAgainstBoth(
          `
            SELECT id FROM users
            EXCEPT
            SELECT user_id AS id FROM orders WHERE org_id = 'org_1'
          `,
          EMPTY_CONTEXT,
        );

        expect(sortRowsByKey(actual, "id")).toEqual(sortRowsByKey(expected, "id"));
        expect(sortRowsByKey(actual, "id")).toEqual([{ id: "usr_3" }]);
      },
    );
  });
});
