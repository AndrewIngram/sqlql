import { describe, expect, it } from "vitest";
import { createMethodsSession, withQueryHarness } from "@tupl/test-support/runtime";
import { createArrayTableMethods } from "@tupl/test-support/methods";

import { defineTableMethods } from "@tupl/schema-model";
import { commerceRows, commerceSchema } from "@tupl/test-support/fixtures";

const EMPTY_CONTEXT = {} as const;

describe("query/subqueries", () => {
  it("supports IN (SELECT ...)", async () => {
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
            WHERE user_id IN (
              SELECT id
              FROM users
              WHERE team_id = 'team_enterprise'
            )
            ORDER BY id ASC
          `,
          EMPTY_CONTEXT,
        );

        expect(actual).toEqual(expected);
        expect(actual).toEqual([{ id: "ord_1" }, { id: "ord_2" }, { id: "ord_4" }]);
      },
    );
  });

  it("supports EXISTS (SELECT ...)", async () => {
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
            WHERE EXISTS (
              SELECT id
              FROM users
              WHERE team_id = 'team_smb'
            )
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

  it("supports scalar subqueries in WHERE and SELECT", async () => {
    await withQueryHarness(
      {
        schema: commerceSchema,
        rowsByTable: commerceRows,
      },
      async (harness) => {
        const whereScalar = await harness.runAgainstBoth(
          `
            SELECT id
            FROM orders
            WHERE total_cents = (SELECT MAX(total_cents) FROM orders)
          `,
          EMPTY_CONTEXT,
        );

        expect(whereScalar.actual).toEqual(whereScalar.expected);
        expect(whereScalar.actual).toEqual([{ id: "ord_4" }]);

        const selectScalar = await harness.runAgainstBoth(
          `
            SELECT id, (SELECT MAX(total_cents) FROM orders) AS max_total
            FROM orders
            ORDER BY id ASC
            LIMIT 2
          `,
          EMPTY_CONTEXT,
        );

        expect(selectScalar.actual).toEqual(selectScalar.expected);
        expect(selectScalar.actual).toEqual([
          { id: "ord_1", max_total: 9900 },
          { id: "ord_2", max_total: 9900 },
        ]);
      },
    );
  });

  it("builds static subquery scopes and wires consumer dependencies pre-run", () => {
    const methods = defineTableMethods(commerceSchema, {
      orders: createArrayTableMethods(commerceRows.orders),
      users: createArrayTableMethods(commerceRows.users),
      teams: createArrayTableMethods(commerceRows.teams),
    });

    const sql = `
      SELECT
        o.id,
        (SELECT MAX(total_cents) FROM orders) AS max_total
      FROM orders o
      WHERE o.user_id IN (
        SELECT id
        FROM users
        WHERE team_id = 'team_enterprise'
      )
      AND EXISTS (
        SELECT id
        FROM users
        WHERE team_id = 'team_enterprise'
      )
      ORDER BY o.id ASC
    `;

    const session = createMethodsSession({
      schema: commerceSchema,
      methods,
      context: EMPTY_CONTEXT,
      sql,
    });

    const plan = session.getPlan();
    const scopes = plan.scopes ?? [];
    const rootScope = scopes.find((scope) => scope.kind === "root");
    expect(rootScope).toBeDefined();

    const whereScopes = scopes.filter(
      (scope) => scope.kind === "subquery" && scope.label.startsWith("Subquery WHERE #"),
    );
    expect(whereScopes.length).toBeGreaterThan(0);

    const selectScopes = scopes.filter(
      (scope) => scope.kind === "subquery" && scope.label.startsWith("Subquery SELECT #"),
    );
    expect(selectScopes.length).toBeGreaterThan(0);

    const rootWhereStep = plan.steps.find(
      (step) => step.summary === "Apply WHERE filter" && step.scopeId === rootScope?.id,
    );
    expect(rootWhereStep).toBeDefined();
    const whereProducerStepIds = new Set(
      plan.steps
        .filter((step) => whereScopes.some((scope) => scope.id === step.scopeId))
        .map((step) => step.id),
    );
    expect(
      rootWhereStep?.dependsOn.some((dependencyId) => whereProducerStepIds.has(dependencyId)),
    ).toBe(true);

    const rootProjectionStep = plan.steps.find(
      (step) => step.summary === "Project result rows" && step.scopeId === rootScope?.id,
    );
    expect(rootProjectionStep).toBeDefined();
    const selectProducerStepIds = new Set(
      plan.steps
        .filter((step) => selectScopes.some((scope) => scope.id === step.scopeId))
        .map((step) => step.id),
    );
    expect(
      rootProjectionStep?.dependsOn.some((dependencyId) => selectProducerStepIds.has(dependencyId)),
    ).toBe(true);
  });

  it("does not grow plan steps during repeated scalar subquery evaluation", async () => {
    let ordersScanCalls = 0;

    const baseOrdersMethods = createArrayTableMethods(commerceRows.orders, {
      includeAggregate: false,
    });
    const ordersMethods = {
      ...baseOrdersMethods,
      scan: async (...args: Parameters<typeof baseOrdersMethods.scan>) => {
        ordersScanCalls += 1;
        return baseOrdersMethods.scan(...args);
      },
    };

    const methods = defineTableMethods(commerceSchema, {
      orders: ordersMethods,
      users: createArrayTableMethods(commerceRows.users),
      teams: createArrayTableMethods(commerceRows.teams),
    });

    const session = createMethodsSession({
      schema: commerceSchema,
      methods,
      context: EMPTY_CONTEXT,
      sql: `
        SELECT id, (SELECT MAX(total_cents) FROM orders) AS max_total
        FROM orders
        ORDER BY id ASC
      `,
    });

    const preRunStepCount = session.getPlan().steps.length;
    await session.runToCompletion();
    const postRunStepCount = session.getPlan().steps.length;

    expect(postRunStepCount).toBe(preRunStepCount);
    expect(ordersScanCalls).toBe(2);
  });
});
