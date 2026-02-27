import { describe, expect, it } from "vitest";

import { commerceRows, commerceSchema } from "../support/commerce-fixture";
import { withQueryHarness } from "../support/query-harness";

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
          harness.runSqlql(
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
          harness.runSqlql(
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

  it("rejects recursive CTEs", async () => {
    await withQueryHarness(
      {
        schema: commerceSchema,
        rowsByTable: commerceRows,
      },
      async (harness) => {
        await expect(
          harness.runSqlql(
            `
              WITH RECURSIVE n AS (
                SELECT id FROM orders
                UNION ALL
                SELECT id FROM n
              )
              SELECT id FROM n
            `,
            EMPTY_CONTEXT,
          ),
        ).rejects.toThrow("Recursive CTEs are not yet supported.");
      },
    );
  });

  it("rejects FROM subqueries", async () => {
    await withQueryHarness(
      {
        schema: commerceSchema,
        rowsByTable: commerceRows,
      },
      async (harness) => {
        await expect(
          harness.runSqlql(
            `
              SELECT s.id
              FROM (SELECT id FROM orders) s
            `,
            EMPTY_CONTEXT,
          ),
        ).rejects.toThrow("Unsupported FROM clause entry.");
      },
    );
  });
});
