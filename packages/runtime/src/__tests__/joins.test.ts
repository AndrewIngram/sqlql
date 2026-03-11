import { describe, expect, it } from "vitest";
import { stringifyUnknownValue } from "@tupl/foundation";
import { createMethodsProvider, withQueryHarness } from "@tupl/test-support/runtime";
import { createArrayTableMethods, scanArrayRows } from "@tupl/test-support/methods";

import {
  defineTableMethods,
  type TableLookupRequest,
  type TableScanRequest,
} from "@tupl/schema-model";
import { commerceRows, commerceSchema } from "@tupl/test-support/fixtures";
import { buildEntitySchema } from "@tupl/test-support/schema";

const EMPTY_CONTEXT = {} as const;

describe("query/joins", () => {
  it("executes joins with sqlite parity across provider/local join strategies", async () => {
    const calls: Array<{ table: string; request: TableScanRequest }> = [];
    const lookupCalls: Array<{ table: string; request: TableLookupRequest }> = [];

    const methods = defineTableMethods(commerceSchema, {
      orders: {
        ...createArrayTableMethods(commerceRows.orders),
        async scan(request) {
          calls.push({ table: "orders", request });
          return scanArrayRows(commerceRows.orders, request);
        },
      },
      users: {
        ...createArrayTableMethods(commerceRows.users),
        async scan(request) {
          calls.push({ table: "users", request });
          return scanArrayRows(commerceRows.users, request);
        },
        async lookup(request) {
          lookupCalls.push({ table: "users", request });
          return createArrayTableMethods(commerceRows.users).lookup!(request, EMPTY_CONTEXT);
        },
      },
      teams: {
        ...createArrayTableMethods(commerceRows.teams),
        async scan(request) {
          calls.push({ table: "teams", request });
          return scanArrayRows(commerceRows.teams, request);
        },
        async lookup(request) {
          lookupCalls.push({ table: "teams", request });
          return createArrayTableMethods(commerceRows.teams).lookup!(request, EMPTY_CONTEXT);
        },
      },
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
        const sql = `
          SELECT o.id, o.total_cents, u.email, t.name
          FROM orders o
          JOIN users u ON o.user_id = u.id
          JOIN teams t ON u.team_id = t.id
          WHERE o.org_id = 'org_1' AND o.status = 'paid' AND t.tier = 'enterprise'
          ORDER BY o.created_at DESC
          LIMIT 2
        `;

        const { actual, expected } = await harness.runAgainstBoth(sql, EMPTY_CONTEXT);
        expect(actual).toEqual(expected);

        const usersLookup = lookupCalls.find((call) => call.table === "users");
        if (usersLookup) {
          expect(usersLookup.request.table).toBe("users");
          expect(usersLookup.request.alias).toBe("u");
          expect(usersLookup.request.key).toBe("id");
          expect(usersLookup.request.values).toEqual(["usr_1", "usr_2"]);
          expect(usersLookup.request.select).toEqual(
            expect.arrayContaining(["id", "team_id", "email"]),
          );
        } else {
          const usersCall = calls.find((call) => call.table === "users");
          expect(usersCall?.request.select).toEqual(
            expect.arrayContaining(["id", "team_id", "email"]),
          );
        }

        const teamsLookup = lookupCalls.find((call) => call.table === "teams");
        if (teamsLookup) {
          expect(teamsLookup.request.table).toBe("teams");
          expect(teamsLookup.request.alias).toBe("t");
          expect(teamsLookup.request.key).toBe("id");
          expect(teamsLookup.request.values).toEqual(["team_enterprise", "team_smb"]);
          expect(teamsLookup.request.select).toEqual(
            expect.arrayContaining(["id", "name", "tier"]),
          );
          expect(teamsLookup.request.where).toEqual([
            { op: "eq", column: "tier", value: "enterprise" },
          ]);
        } else {
          const teamsCall = calls.find((call) => call.table === "teams");
          expect(teamsCall?.request.select).toEqual(expect.arrayContaining(["id", "name", "tier"]));
          expect(teamsCall?.request.where).toEqual(
            expect.arrayContaining([{ op: "eq", column: "tier", value: "enterprise" }]),
          );
        }
      },
    );
  });

  it("does not match null join keys on inner joins", async () => {
    const schema = buildEntitySchema({
      orders: {
        columns: {
          id: { type: "text", nullable: false },
          org_id: { type: "text", nullable: false },
          user_id: { type: "text", nullable: true },
        },
      },
      users: {
        columns: {
          id: { type: "text", nullable: false },
          email: { type: "text", nullable: true },
        },
      },
    });

    const rowsByTable = {
      orders: [
        { id: "ord_1", org_id: "org_1", user_id: "usr_1" },
        { id: "ord_2", org_id: "org_1", user_id: null },
      ],
      users: [{ id: "usr_1", email: "alice@example.com" }],
    };

    await withQueryHarness(
      {
        schema,
        rowsByTable,
      },
      async (harness) => {
        const { actual, expected } = await harness.runAgainstBoth(
          `
            SELECT o.id, u.email
            FROM orders o
            JOIN users u ON o.user_id = u.id
            WHERE o.org_id = 'org_1'
            ORDER BY o.id ASC
          `,
          EMPTY_CONTEXT,
        );

        expect(actual).toEqual(expected);
        expect(actual).toEqual([{ id: "ord_1", email: "alice@example.com" }]);
      },
    );
  });

  it("returns no rows when a joined table is empty", async () => {
    const schema = buildEntitySchema({
      orders: {
        columns: {
          id: { type: "text", nullable: false },
          user_id: { type: "text", nullable: false },
        },
      },
      users: {
        columns: {
          id: { type: "text", nullable: false },
          email: { type: "text", nullable: true },
        },
      },
    });

    await withQueryHarness(
      {
        schema,
        rowsByTable: {
          orders: [{ id: "ord_1", user_id: "usr_1" }],
          users: [],
        },
      },
      async (harness) => {
        const { actual, expected } = await harness.runAgainstBoth(
          `
            SELECT o.id, u.email
            FROM orders o
            JOIN users u ON o.user_id = u.id
          `,
          EMPTY_CONTEXT,
        );

        expect(actual).toEqual(expected);
        expect(actual).toEqual([]);
      },
    );
  });

  it("supports LEFT JOIN with null-extended right rows", async () => {
    const schema = buildEntitySchema({
      orders: {
        columns: {
          id: { type: "text", nullable: false },
          user_id: { type: "text", nullable: true },
        },
      },
      users: {
        columns: {
          id: { type: "text", nullable: false },
          email: { type: "text", nullable: true },
        },
      },
    });

    await withQueryHarness(
      {
        schema,
        rowsByTable: {
          orders: [
            { id: "ord_1", user_id: "usr_1" },
            { id: "ord_2", user_id: "usr_missing" },
            { id: "ord_3", user_id: null },
          ],
          users: [{ id: "usr_1", email: "alice@example.com" }],
        },
      },
      async (harness) => {
        const { actual, expected } = await harness.runAgainstBoth(
          `
            SELECT o.id, u.email
            FROM orders o
            LEFT JOIN users u ON o.user_id = u.id
            ORDER BY o.id ASC
          `,
          EMPTY_CONTEXT,
        );

        expect(actual).toEqual(expected);
        expect(actual).toEqual([
          { id: "ord_1", email: "alice@example.com" },
          { id: "ord_2", email: null },
          { id: "ord_3", email: null },
        ]);
      },
    );
  });

  it("supports RIGHT JOIN with null-extended left rows", async () => {
    const schema = buildEntitySchema({
      orders: {
        columns: {
          id: { type: "text", nullable: false },
          user_id: { type: "text", nullable: true },
        },
      },
      users: {
        columns: {
          id: { type: "text", nullable: false },
          email: { type: "text", nullable: true },
        },
      },
    });

    await withQueryHarness(
      {
        schema,
        rowsByTable: {
          orders: [
            { id: "ord_1", user_id: "usr_1" },
            { id: "ord_2", user_id: "usr_missing" },
          ],
          users: [
            { id: "usr_1", email: "alice@example.com" },
            { id: "usr_2", email: "bob@example.com" },
          ],
        },
      },
      async (harness) => {
        const { actual, expected } = await harness.runAgainstBoth(
          `
            SELECT o.id, u.id AS user_id, u.email
            FROM orders o
            RIGHT JOIN users u ON o.user_id = u.id
            ORDER BY user_id ASC
          `,
          EMPTY_CONTEXT,
        );

        expect(actual).toEqual(expected);
        expect(actual).toEqual([
          { id: "ord_1", user_id: "usr_1", email: "alice@example.com" },
          { id: null, user_id: "usr_2", email: "bob@example.com" },
        ]);
      },
    );
  });

  it("supports FULL JOIN with null-extension on both sides", async () => {
    const schema = buildEntitySchema({
      orders: {
        columns: {
          id: { type: "text", nullable: false },
          user_id: { type: "text", nullable: true },
        },
      },
      users: {
        columns: {
          id: { type: "text", nullable: false },
          email: { type: "text", nullable: true },
        },
      },
    });

    await withQueryHarness(
      {
        schema,
        rowsByTable: {
          orders: [
            { id: "ord_1", user_id: "usr_1" },
            { id: "ord_2", user_id: "usr_missing" },
          ],
          users: [
            { id: "usr_1", email: "alice@example.com" },
            { id: "usr_2", email: "bob@example.com" },
          ],
        },
      },
      async (harness) => {
        const { actual, expected } = await harness.runAgainstBoth(
          `
            SELECT o.id, u.id AS user_id, u.email
            FROM orders o
            FULL JOIN users u ON o.user_id = u.id
          `,
          EMPTY_CONTEXT,
        );

        const sortRows = (rows: Array<Record<string, unknown>>) =>
          [...rows].sort((left, right) => {
            const leftKey = `${stringifyUnknownValue(left.id)}:${stringifyUnknownValue(left.user_id)}`;
            const rightKey = `${stringifyUnknownValue(right.id)}:${stringifyUnknownValue(right.user_id)}`;
            return leftKey < rightKey ? -1 : leftKey > rightKey ? 1 : 0;
          });

        expect(sortRows(actual)).toEqual(sortRows(expected));
        expect(sortRows(actual)).toEqual(
          sortRows([
            { id: "ord_1", user_id: "usr_1", email: "alice@example.com" },
            { id: "ord_2", user_id: null, email: null },
            { id: null, user_id: "usr_2", email: "bob@example.com" },
          ]),
        );
      },
    );
  });
});
