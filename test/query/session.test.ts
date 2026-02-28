import { describe, expect, it } from "vitest";

import {
  createArrayTableMethods,
  createQuerySession,
  defineSchema,
  defineTableMethods,
  query,
} from "../../src";
import { commerceRows, commerceSchema } from "../support/commerce-fixture";

const EMPTY_CONTEXT = {} as const;

describe("query/session", () => {
  it("steps through execution using next() and returns final result", async () => {
    const methods = defineTableMethods(commerceSchema, {
      orders: createArrayTableMethods(commerceRows.orders),
      users: createArrayTableMethods(commerceRows.users),
      teams: createArrayTableMethods(commerceRows.teams),
    });

    const sql = `
      SELECT o.id, u.email
      FROM orders o
      JOIN users u ON o.user_id = u.id
      WHERE o.org_id = 'org_1'
      ORDER BY o.id ASC
    `;

    const session = createQuerySession({
      schema: commerceSchema,
      methods,
      context: EMPTY_CONTEXT,
      sql,
      options: {
        captureRows: "full",
      },
    });

    const first = await session.next();
    expect("done" in first).toBe(false);
    if ("done" in first) {
      throw new Error("Expected a step event.");
    }

    const firstState = session.getStepState(first.id);
    expect(firstState?.status).toBe("done");
    expect(session.getPlan().steps.length).toBeGreaterThan(0);

    let finalRows: Array<Record<string, unknown>> | null = null;
    while (finalRows == null) {
      const next = await session.next();
      if ("done" in next) {
        finalRows = next.result;
      }
    }

    const directRows = await query({
      schema: commerceSchema,
      methods,
      context: EMPTY_CONTEXT,
      sql,
    });
    expect(finalRows).toEqual(directRows);
    expect(session.getResult()).toEqual(directRows);
  });

  it("runToCompletion returns the same output as query()", async () => {
    const methods = defineTableMethods(commerceSchema, {
      orders: createArrayTableMethods(commerceRows.orders),
      users: createArrayTableMethods(commerceRows.users),
      teams: createArrayTableMethods(commerceRows.teams),
    });

    const sql = `
      SELECT user_id, COUNT(*) AS order_count
      FROM orders
      GROUP BY user_id
      ORDER BY user_id ASC
    `;

    const session = createQuerySession({
      schema: commerceSchema,
      methods,
      context: EMPTY_CONTEXT,
      sql,
    });

    const sessionRows = await session.runToCompletion();
    const directRows = await query({
      schema: commerceSchema,
      methods,
      context: EMPTY_CONTEXT,
      sql,
    });

    expect(sessionRows).toEqual(directRows);
  });

  it("propagates execution failures via next()", async () => {
    const schema = defineSchema({
      tables: {
        users: {
          columns: {
            id: { type: "text", nullable: false },
          },
        },
      },
    });

    const session = createQuerySession({
      schema,
      methods: defineTableMethods(schema, {
        users: createArrayTableMethods([{ id: "u1" }]),
      }),
      context: EMPTY_CONTEXT,
      sql: "SELECT * FROM missing_table",
    });

    await expect(session.next()).rejects.toThrow("Unknown table: missing_table");
  });
});
