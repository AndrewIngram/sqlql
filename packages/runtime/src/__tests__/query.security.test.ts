import { describe, expect, it } from "vitest";
import { queryWithMethods } from "../../../../test/support/methods-provider";
import { createArrayTableMethods } from "../../../../test/support/array-methods";
import { defineTableMethods } from "@tupl/schema";
import { buildEntitySchema } from "../../../../test/support/schema-builder";

const EMPTY_CONTEXT = {} as const;

describe("query/security", () => {
  it("rejects selecting columns that are not declared in the schema facade", async () => {
    const schema = buildEntitySchema({
      users: {
        columns: {
          id: "text",
        },
      },
    });

    const methods = defineTableMethods(schema, {
      users: createArrayTableMethods([{ id: "u1", secret: "hidden" }]),
    });

    await expect(
      queryWithMethods({
        schema,
        methods,
        context: EMPTY_CONTEXT,
        sql: "SELECT secret FROM users",
      }),
    ).rejects.toThrow("Unknown column in relational plan: users.secret");
  });

  it("rejects filtering on columns that are not declared in the schema facade", async () => {
    const schema = buildEntitySchema({
      users: {
        columns: {
          id: "text",
          email: "text",
        },
      },
    });

    const methods = defineTableMethods(schema, {
      users: createArrayTableMethods([{ id: "u1", email: "a@example.com", role: "admin" }]),
    });

    await expect(
      queryWithMethods({
        schema,
        methods,
        context: EMPTY_CONTEXT,
        sql: "SELECT id FROM users WHERE role = 'admin'",
      }),
    ).rejects.toThrow("Unknown column in relational plan: users.role");
  });

  it("rejects joining on undeclared columns", async () => {
    const schema = buildEntitySchema({
      orders: {
        columns: {
          id: "text",
          user_id: "text",
        },
      },
      users: {
        columns: {
          id: "text",
          email: "text",
        },
      },
    });

    const methods = defineTableMethods(schema, {
      orders: createArrayTableMethods([{ id: "o1", user_id: "u1", org_id: "org_1" }]),
      users: createArrayTableMethods([{ id: "u1", email: "a@example.com" }]),
    });

    await expect(
      queryWithMethods({
        schema,
        methods,
        context: EMPTY_CONTEXT,
        sql: "SELECT o.id FROM orders o JOIN users u ON o.org_id = u.id",
      }),
    ).rejects.toThrow("Unknown column in relational plan: orders.org_id");
  });

  it("rejects sorting on columns that are not declared in the schema facade", async () => {
    const schema = buildEntitySchema({
      users: {
        columns: {
          id: "text",
          email: "text",
        },
      },
    });

    const methods = defineTableMethods(schema, {
      users: createArrayTableMethods([
        { id: "u1", email: "a@example.com", created_at: "2025-01-01" },
      ]),
    });

    await expect(
      queryWithMethods({
        schema,
        methods,
        context: EMPTY_CONTEXT,
        sql: "SELECT id FROM users ORDER BY created_at DESC",
      }),
    ).rejects.toThrow("Unknown column in relational plan: users.created_at");
  });

  it("allows querying declared facade columns", async () => {
    const schema = buildEntitySchema({
      users: {
        columns: {
          id: "text",
          email: "text",
        },
      },
    });

    const methods = defineTableMethods(schema, {
      users: createArrayTableMethods([{ id: "u1", email: "a@example.com", secret: "hidden" }]),
    });

    const rows = await queryWithMethods({
      schema,
      methods,
      context: EMPTY_CONTEXT,
      sql: "SELECT id, email FROM users WHERE id = 'u1'",
    });

    expect(rows).toEqual([{ id: "u1", email: "a@example.com" }]);
  });
});
