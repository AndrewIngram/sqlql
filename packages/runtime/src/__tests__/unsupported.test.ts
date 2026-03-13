import { describe, expect, it } from "vitest";

import { commerceRows, commerceSchema } from "@tupl/test-support/fixtures";
import { withQueryHarness } from "@tupl/test-support/runtime";

const EMPTY_CONTEXT = {} as const;

describe("query/unsupported", () => {
  it("rejects write statements", async () => {
    await withQueryHarness(
      {
        schema: commerceSchema,
        rowsByTable: commerceRows,
      },
      async (harness) => {
        await expect(
          harness.runTupl(
            `
              UPDATE orders
              SET status = 'refunded'
              WHERE id = 'ord_1'
            `,
            EMPTY_CONTEXT,
          ),
        ).rejects.toThrow("Only SELECT statements are currently supported.");
      },
    );
  });

  it("rejects correlated subqueries", async () => {
    await withQueryHarness(
      {
        schema: commerceSchema,
        rowsByTable: commerceRows,
      },
      async (harness) => {
        await expect(
          harness.runTupl(
            `
              SELECT o.id
              FROM orders o
              WHERE EXISTS (
                SELECT u.id
                FROM users u
                WHERE u.id = o.user_id
              )
            `,
            EMPTY_CONTEXT,
          ),
        ).rejects.toThrow("Correlated subqueries are not yet supported.");
      },
    );
  });
});
