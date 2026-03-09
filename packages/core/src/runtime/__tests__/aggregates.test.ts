import { describe, expect, it } from "vitest";
import { createMethodsProvider } from "../../testing/methods-provider";
import { aggregateArrayRows, createArrayTableMethods } from "../../schema/array-methods";

import { defineTableMethods, type TableAggregateRequest } from "@tupl/core/schema";
import { commerceRows, commerceSchema } from "../../testing/commerce-fixture";
import { withQueryHarness } from "../../testing/query-harness";
import { buildEntitySchema } from "../../testing/schema-builder";

const EMPTY_CONTEXT = {} as const;

describe("query/aggregates", () => {
  it("supports aggregate route and local fallback with sqlite parity", async () => {
    const aggregateCalls: TableAggregateRequest[] = [];

    const methods = defineTableMethods(commerceSchema, {
      orders: {
        ...createArrayTableMethods(commerceRows.orders),
        async aggregate(request) {
          aggregateCalls.push(request);
          return aggregateArrayRows(commerceRows.orders, request);
        },
      },
      users: createArrayTableMethods(commerceRows.users),
      teams: createArrayTableMethods(commerceRows.teams),
    });

    await withQueryHarness(
      {
        schema: commerceSchema,
        rowsByTable: commerceRows,
        providers: {
          memory: createMethodsProvider(commerceSchema, methods),
        },
      },
      async (harness) => {
        const routeSql = `
          SELECT o.user_id, COUNT(*) AS order_count
          FROM orders o
          WHERE o.org_id = 'org_1'
          GROUP BY 1
          ORDER BY 2 DESC
        `;

        const route = await harness.runAgainstBoth(routeSql, EMPTY_CONTEXT);
        expect(route.actual).toEqual(route.expected);
        expect(aggregateCalls).toHaveLength(1);
        expect(aggregateCalls[0]?.groupBy).toEqual(["user_id"]);

        const fallbackSql = `
          SELECT u.team_id, COUNT(*) AS order_count
          FROM orders o
          JOIN users u ON o.user_id = u.id
          WHERE o.org_id = 'org_1'
          GROUP BY u.team_id
          ORDER BY order_count DESC
        `;

        const fallback = await harness.runAgainstBoth(fallbackSql, EMPTY_CONTEXT);
        expect(fallback.actual).toEqual(fallback.expected);
      },
    );
  });

  it("falls back locally for GROUP BY ordinals on computed expressions", async () => {
    const schema = buildEntitySchema({
      users: {
        columns: {
          id: { type: "text", nullable: false },
        },
      },
    });

    const rowsByTable = {
      users: [{ id: "alice" }, { id: "adam" }, { id: "beth" }],
    };

    const aggregateCalls: TableAggregateRequest[] = [];
    const methods = defineTableMethods(schema, {
      users: {
        ...createArrayTableMethods(rowsByTable.users),
        async aggregate(request) {
          aggregateCalls.push(request);
          return aggregateArrayRows(rowsByTable.users, request);
        },
      },
    });

    await withQueryHarness(
      {
        schema,
        rowsByTable,
        providers: {
          memory: createMethodsProvider(schema, methods),
        },
      },
      async (harness) => {
        const { actual, expected } = await harness.runAgainstBoth(
          `
            SELECT SUBSTR(id, 1, 1) AS initial, COUNT(*) AS user_count
            FROM users
            GROUP BY 1
            ORDER BY 2 DESC, 1 ASC
          `,
          EMPTY_CONTEXT,
        );

        expect(actual).toEqual(expected);
        expect(actual).toEqual([
          { initial: "a", user_count: 2 },
          { initial: "b", user_count: 1 },
        ]);
        expect(aggregateCalls).toHaveLength(0);
      },
    );
  });

  it("matches sqlite aggregate behavior on empty tables", async () => {
    const schema = buildEntitySchema({
      entries: {
        columns: {
          id: { type: "text", nullable: false },
          amount: { type: "integer", nullable: true },
        },
      },
    });

    await withQueryHarness(
      {
        schema,
        rowsByTable: {
          entries: [],
        },
      },
      async (harness) => {
        const { actual, expected } = await harness.runAgainstBoth(
          `
            SELECT
              COUNT(*) AS count_all,
              SUM(e.amount) AS sum_amount,
              AVG(e.amount) AS avg_amount,
              MIN(e.amount) AS min_amount,
              MAX(e.amount) AS max_amount
            FROM entries e
          `,
          EMPTY_CONTEXT,
        );

        expect(actual).toEqual(expected);
        expect(actual).toEqual([
          {
            count_all: 0,
            sum_amount: null,
            avg_amount: null,
            min_amount: null,
            max_amount: null,
          },
        ]);
      },
    );
  });

  it("handles null grouping keys and null aggregate inputs", async () => {
    const schema = buildEntitySchema({
      events: {
        columns: {
          id: { type: "text", nullable: false },
          group_key: { type: "text", nullable: true },
          value: { type: "integer", nullable: true },
        },
      },
    });

    const rowsByTable = {
      events: [
        { id: "evt_1", group_key: null, value: 5 },
        { id: "evt_2", group_key: null, value: null },
        { id: "evt_3", group_key: "alpha", value: 10 },
        { id: "evt_4", group_key: "alpha", value: null },
        { id: "evt_5", group_key: "beta", value: 3 },
      ],
    };

    await withQueryHarness(
      {
        schema,
        rowsByTable,
      },
      async (harness) => {
        const { actual, expected } = await harness.runAgainstBoth(
          `
            SELECT
              e.group_key,
              COUNT(*) AS row_count,
              COUNT(e.value) AS value_count,
              SUM(e.value) AS value_sum
            FROM events e
            GROUP BY e.group_key
            ORDER BY e.group_key ASC
          `,
          EMPTY_CONTEXT,
        );

        expect(actual).toEqual(expected);
        expect(actual).toEqual([
          { group_key: null, row_count: 2, value_count: 1, value_sum: 5 },
          { group_key: "alpha", row_count: 2, value_count: 1, value_sum: 10 },
          { group_key: "beta", row_count: 1, value_count: 1, value_sum: 3 },
        ]);
      },
    );
  });
});
