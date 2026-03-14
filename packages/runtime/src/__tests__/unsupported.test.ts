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

  it("supports decorrelatable correlated EXISTS subqueries", async () => {
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
                  AND u.team_id = 'team_smb'
              )
              ORDER BY o.id ASC
            `,
            EMPTY_CONTEXT,
          ),
        ).resolves.toEqual([{ id: "ord_3" }]);
      },
    );
  });

  it("supports decorrelatable correlated NOT EXISTS subqueries", async () => {
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
              WHERE NOT EXISTS (
                SELECT u.id
                FROM users u
                WHERE u.id = o.user_id
                  AND u.team_id = 'team_smb'
              )
              ORDER BY o.id ASC
            `,
            EMPTY_CONTEXT,
          ),
        ).resolves.toEqual([{ id: "ord_1" }, { id: "ord_2" }, { id: "ord_4" }]);
      },
    );
  });

  it("supports decorrelatable correlated IN subqueries", async () => {
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
              WHERE o.user_id IN (
                SELECT u.id
                FROM users u
                WHERE u.team_id = 'team_smb'
                  AND u.id = o.user_id
              )
              ORDER BY o.id ASC
            `,
            EMPTY_CONTEXT,
          ),
        ).resolves.toEqual([{ id: "ord_3" }]);
      },
    );
  });

  it("supports decorrelatable correlated NOT IN subqueries", async () => {
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
              WHERE o.user_id NOT IN (
                SELECT u.id
                FROM users u
                WHERE u.team_id = 'team_smb'
                  AND u.id = o.user_id
              )
              ORDER BY o.id ASC
            `,
            EMPTY_CONTEXT,
          ),
        ).resolves.toEqual([{ id: "ord_1" }, { id: "ord_2" }, { id: "ord_4" }]);
      },
    );
  });

  it("supports decorrelatable correlated scalar aggregate predicates", async () => {
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
              WHERE o.total_cents = (
                SELECT MAX(i.total_cents)
                FROM orders i
                WHERE i.user_id = o.user_id
              )
              ORDER BY o.id ASC
            `,
            EMPTY_CONTEXT,
          ),
        ).resolves.toEqual([{ id: "ord_2" }, { id: "ord_3" }, { id: "ord_4" }]);
      },
    );
  });

  it("supports decorrelatable correlated scalar aggregate projections", async () => {
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
                (
                  SELECT MAX(i.total_cents)
                  FROM orders i
                  WHERE i.user_id = o.user_id
                ) AS user_max_total
              FROM orders o
              ORDER BY o.id ASC
            `,
            EMPTY_CONTEXT,
          ),
        ).resolves.toEqual([
          { id: "ord_1", user_max_total: 1800 },
          { id: "ord_2", user_max_total: 1800 },
          { id: "ord_3", user_max_total: 2400 },
          { id: "ord_4", user_max_total: 9900 },
        ]);
      },
    );
  });
});
