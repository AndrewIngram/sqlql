import { describe, expect, it } from "vitest";

import { createArrayTableMethods, defineSchema, defineTableMethods, query } from "../../src";

const EMPTY_CONTEXT = {} as const;

describe("query/enums", () => {
  it("rejects invalid enum literal for = predicates", async () => {
    const schema = defineSchema({
      tables: {
        orders: {
          columns: {
            id: "text",
            status: { type: "text", enum: ["draft", "paid", "void"] as const },
          },
        },
      },
    });

    const methods = defineTableMethods(schema, {
      orders: createArrayTableMethods([{ id: "o1", status: "paid" }]),
    });

    await expect(
      query({
        schema,
        methods,
        context: EMPTY_CONTEXT,
        sql: "SELECT id FROM orders WHERE status = 'unknown'",
      }),
    ).rejects.toThrow("Invalid enum value for orders.status");
  });

  it("rejects invalid enum literal for IN predicates", async () => {
    const schema = defineSchema({
      tables: {
        orders: {
          columns: {
            id: "text",
            status: { type: "text", enum: ["draft", "paid", "void"] as const },
          },
        },
      },
    });

    const methods = defineTableMethods(schema, {
      orders: createArrayTableMethods([{ id: "o1", status: "paid" }]),
    });

    await expect(
      query({
        schema,
        methods,
        context: EMPTY_CONTEXT,
        sql: "SELECT id FROM orders WHERE status IN ('paid', 'unknown')",
      }),
    ).rejects.toThrow("Invalid enum value for orders.status");
  });

  it("surfaces runtime enum/check violations in constraint validation mode", async () => {
    const schema = defineSchema({
      tables: {
        orders: {
          columns: {
            id: { type: "text", nullable: false },
            status: { type: "text", nullable: false, enum: ["draft", "paid"] as const },
          },
        },
      },
    });

    const methods = defineTableMethods(schema, {
      orders: {
        async scan() {
          return [
            { id: "o1", status: "paid" },
            { id: "o2", status: "refunded" },
          ];
        },
      },
    });

    await expect(
      query({
        schema,
        methods,
        context: EMPTY_CONTEXT,
        sql: "SELECT id, status FROM orders",
        constraintValidation: {
          mode: "error",
        },
      }),
    ).rejects.toThrow("enum check failed");
  });

  it("supports explicit CHECK IN constraints", async () => {
    const schema = defineSchema({
      tables: {
        invoices: {
          columns: {
            id: { type: "text", nullable: false },
            amount_due: { type: "integer", nullable: false },
          },
          constraints: {
            checks: [
              {
                kind: "in",
                column: "amount_due",
                values: [100, 200, 300],
                name: "amount_due_allowed",
              },
            ],
          },
        },
      },
    });

    const methods = defineTableMethods(schema, {
      invoices: createArrayTableMethods([
        { id: "i1", amount_due: 100 },
        { id: "i2", amount_due: 999 },
      ]),
    });

    await expect(
      query({
        schema,
        methods,
        context: EMPTY_CONTEXT,
        sql: "SELECT id, amount_due FROM invoices",
        constraintValidation: {
          mode: "error",
        },
      }),
    ).rejects.toThrow("outside CHECK IN set");
  });
});
