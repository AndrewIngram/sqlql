import { describe, expect, it } from "vitest";

import { commerceRows, commerceSchema } from "../../testing/commerce-fixture";
import { withQueryHarness } from "../../testing/query-harness";

const EMPTY_CONTEXT = {} as const;

function sortRowsByKey(
  rows: Array<Record<string, unknown>>,
  key: string,
): Array<Record<string, unknown>> {
  return [...rows].sort((left, right) => {
    const leftValue = toComparableString(left[key]);
    const rightValue = toComparableString(right[key]);
    return leftValue < rightValue ? -1 : leftValue > rightValue ? 1 : 0;
  });
}

function toComparableString(value: unknown): string {
  if (value == null) {
    return "";
  }
  if (typeof value === "string") {
    return value;
  }
  if (typeof value === "number" || typeof value === "boolean" || typeof value === "bigint") {
    return value.toString();
  }

  try {
    return JSON.stringify(value) ?? Object.prototype.toString.call(value);
  } catch {
    return Object.prototype.toString.call(value);
  }
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
