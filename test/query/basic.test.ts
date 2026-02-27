import { describe, expect, it } from "vitest";

import { defineSchema } from "../../src";
import { commerceRows, commerceSchema } from "../support/commerce-fixture";
import { withQueryHarness } from "../support/query-harness";

const EMPTY_CONTEXT = {} as const;

describe("query/basic", () => {
  it("executes basic single-table queries with sqlite parity", async () => {
    await withQueryHarness(
      {
        schema: commerceSchema,
        rowsByTable: commerceRows,
      },
      async (harness) => {
        const sql = `
          SELECT o.id, o.total_cents
          FROM orders o
          WHERE o.org_id = 'org_1' AND o.status = 'paid'
          ORDER BY o.created_at DESC
          LIMIT 2 OFFSET 1
        `;

        const { actual, expected } = await harness.runAgainstBoth(sql, EMPTY_CONTEXT);
        expect(actual).toEqual(expected);
        expect(actual).toEqual([
          { id: "ord_2", total_cents: 1800 },
          { id: "ord_1", total_cents: 1200 },
        ]);
      },
    );
  });

  it("handles null filters and null ordering like sqlite", async () => {
    const schema = defineSchema({
      tables: {
        items: {
          columns: {
            id: { type: "text", nullable: false },
            category: { type: "text", nullable: true },
            score: { type: "integer", nullable: true },
          },
        },
      },
    });

    const rowsByTable = {
      items: [
        { id: "item_1", category: null, score: null },
        { id: "item_2", category: "hardware", score: 10 },
        { id: "item_3", category: "hardware", score: null },
        { id: "item_4", category: "software", score: 5 },
      ],
    };

    await withQueryHarness(
      {
        schema,
        rowsByTable,
      },
      async (harness) => {
        const isNullQuery = await harness.runAgainstBoth(
          `SELECT id FROM items WHERE category IS NULL ORDER BY id ASC`,
          EMPTY_CONTEXT,
        );
        expect(isNullQuery.actual).toEqual(isNullQuery.expected);
        expect(isNullQuery.actual).toEqual([{ id: "item_1" }]);

        const isNotNullQuery = await harness.runAgainstBoth(
          `SELECT id FROM items WHERE category IS NOT NULL ORDER BY id ASC`,
          EMPTY_CONTEXT,
        );
        expect(isNotNullQuery.actual).toEqual(isNotNullQuery.expected);
        expect(isNotNullQuery.actual).toEqual([
          { id: "item_2" },
          { id: "item_3" },
          { id: "item_4" },
        ]);

        const eqNullQuery = await harness.runAgainstBoth(
          `SELECT id FROM items WHERE category = NULL ORDER BY id ASC`,
          EMPTY_CONTEXT,
        );
        expect(eqNullQuery.actual).toEqual(eqNullQuery.expected);
        expect(eqNullQuery.actual).toEqual([]);

        const inWithNullQuery = await harness.runAgainstBoth(
          `SELECT id FROM items WHERE id IN ('item_1', NULL, 'item_4') ORDER BY id ASC`,
          EMPTY_CONTEXT,
        );
        expect(inWithNullQuery.actual).toEqual(inWithNullQuery.expected);
        expect(inWithNullQuery.actual).toEqual([{ id: "item_1" }, { id: "item_4" }]);

        const orderByNullQuery = await harness.runAgainstBoth(
          `SELECT id, category FROM items ORDER BY category ASC, id ASC`,
          EMPTY_CONTEXT,
        );
        expect(orderByNullQuery.actual).toEqual(orderByNullQuery.expected);
      },
    );
  });

  it("returns empty result sets for empty tables", async () => {
    const schema = defineSchema({
      tables: {
        events: {
          columns: {
            id: { type: "text", nullable: false },
            kind: { type: "text", nullable: true },
          },
        },
      },
    });

    await withQueryHarness(
      {
        schema,
        rowsByTable: {
          events: [],
        },
      },
      async (harness) => {
        const { actual, expected } = await harness.runAgainstBoth(
          `
            SELECT e.id
            FROM events e
            WHERE e.kind IS NOT NULL
            ORDER BY e.id DESC
            LIMIT 10
          `,
          EMPTY_CONTEXT,
        );

        expect(actual).toEqual(expected);
        expect(actual).toEqual([]);
      },
    );
  });
});
