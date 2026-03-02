import { describe, expect, it } from "vitest";

import { defineSchema, defineTableMethods, query, type ConstraintViolation } from "../../src";

const EMPTY_CONTEXT = {} as const;

describe("query/constraints", () => {
  it("does not validate constraints when mode=off", async () => {
    const schema = defineSchema({
      tables: {
        users: {
          columns: {
            id: { type: "text", nullable: false },
            email: { type: "text", nullable: false },
          },
          constraints: {
            primaryKey: {
              columns: ["id"],
            },
            unique: [
              {
                columns: ["email"],
              },
            ],
          },
        },
      },
    });

    const methods = defineTableMethods(schema, {
      users: {
        async scan() {
          return [
            { id: "usr_1", email: "alice@example.com" },
            { id: "usr_1", email: "alice@example.com" },
          ];
        },
      },
    });

    await expect(
      query({
        schema,
        methods,
        context: EMPTY_CONTEXT,
        sql: "SELECT id, email FROM users",
        constraintValidation: {
          mode: "off",
        },
      }),
    ).resolves.toEqual([
      { id: "usr_1", email: "alice@example.com" },
      { id: "usr_1", email: "alice@example.com" },
    ]);
  });

  it("reports violations in warn mode", async () => {
    const schema = defineSchema({
      tables: {
        users: {
          columns: {
            id: { type: "text", nullable: false },
            email: { type: "text", nullable: false },
          },
          constraints: {
            primaryKey: {
              columns: ["id"],
            },
          },
        },
      },
    });

    const methods = defineTableMethods(schema, {
      users: {
        async scan() {
          return [
            { id: "usr_1", email: null },
            { id: "usr_1", email: "alice@example.com" },
          ];
        },
      },
    });

    const violations: ConstraintViolation[] = [];
    const rows = await query({
      schema,
      methods,
      context: EMPTY_CONTEXT,
      sql: "SELECT id, email FROM users",
      constraintValidation: {
        mode: "warn",
        onViolation: (violation) => {
          violations.push(violation);
        },
      },
    });

    expect(rows).toHaveLength(2);
    expect(violations.length).toBeGreaterThan(0);
    expect(violations.some((violation) => violation.type === "not_null")).toBe(true);
    expect(violations.some((violation) => violation.type === "primary_key")).toBe(true);
  });

  it("throws in error mode for not-null violations", async () => {
    const schema = defineSchema({
      tables: {
        users: {
          columns: {
            id: { type: "text", nullable: false },
            email: { type: "text", nullable: false },
          },
        },
      },
    });

    const methods = defineTableMethods(schema, {
      users: {
        async scan() {
          return [{ id: "usr_1", email: null }];
        },
      },
    });

    await expect(
      query({
        schema,
        methods,
        context: EMPTY_CONTEXT,
        sql: "SELECT id, email FROM users",
        constraintValidation: {
          mode: "error",
        },
      }),
    ).rejects.toThrow('column "email" is NOT NULL');
  });

  it("throws in error mode for primary key duplicates", async () => {
    const schema = defineSchema({
      tables: {
        users: {
          columns: {
            id: { type: "text", nullable: false },
            email: { type: "text", nullable: false },
          },
          constraints: {
            primaryKey: {
              columns: ["id"],
            },
          },
        },
      },
    });

    const methods = defineTableMethods(schema, {
      users: {
        async scan() {
          return [
            { id: "usr_1", email: "alice@example.com" },
            { id: "usr_1", email: "alice-2@example.com" },
          ];
        },
      },
    });

    await expect(
      query({
        schema,
        methods,
        context: EMPTY_CONTEXT,
        sql: "SELECT id, email FROM users",
        constraintValidation: {
          mode: "error",
        },
      }),
    ).rejects.toThrow("duplicate primary key");
  });

  it("throws in error mode for unique duplicates", async () => {
    const schema = defineSchema({
      tables: {
        users: {
          columns: {
            id: { type: "text", nullable: false },
            email: { type: "text", nullable: false },
          },
          constraints: {
            unique: [
              {
                columns: ["email"],
              },
            ],
          },
        },
      },
    });

    const methods = defineTableMethods(schema, {
      users: {
        async scan() {
          return [
            { id: "usr_1", email: "alice@example.com" },
            { id: "usr_2", email: "alice@example.com" },
          ];
        },
      },
    });

    await expect(
      query({
        schema,
        methods,
        context: EMPTY_CONTEXT,
        sql: "SELECT id, email FROM users",
        constraintValidation: {
          mode: "error",
        },
      }),
    ).rejects.toThrow("duplicate unique key");
  });

  it("validates field-level primaryKey/unique constraints at runtime", async () => {
    const schema = defineSchema({
      tables: {
        users: {
          columns: {
            id: { type: "text", nullable: false, primaryKey: true },
            email: { type: "text", nullable: false, unique: true },
          },
        },
      },
    });

    const methods = defineTableMethods(schema, {
      users: {
        async scan() {
          return [
            { id: "usr_1", email: "alice@example.com" },
            { id: "usr_1", email: "alice@example.com" },
          ];
        },
      },
    });

    await expect(
      query({
        schema,
        methods,
        context: EMPTY_CONTEXT,
        sql: "SELECT id, email FROM users",
        constraintValidation: {
          mode: "error",
        },
      }),
    ).rejects.toThrow("duplicate primary key");
  });

  it("does not perform foreign-key runtime validation", async () => {
    const schema = defineSchema({
      tables: {
        users: {
          columns: {
            id: { type: "text", nullable: false },
          },
          constraints: {
            primaryKey: {
              columns: ["id"],
            },
          },
        },
        orders: {
          columns: {
            id: { type: "text", nullable: false },
            user_id: { type: "text", nullable: false },
          },
          constraints: {
            primaryKey: {
              columns: ["id"],
            },
            foreignKeys: [
              {
                columns: ["user_id"],
                references: {
                  table: "users",
                  columns: ["id"],
                },
              },
            ],
          },
        },
      },
    });

    const methods = defineTableMethods(schema, {
      users: {
        async scan() {
          return [];
        },
      },
      orders: {
        async scan() {
          return [{ id: "ord_1", user_id: "missing_user" }];
        },
      },
    });

    await expect(
      query({
        schema,
        methods,
        context: EMPTY_CONTEXT,
        sql: "SELECT id, user_id FROM orders",
        constraintValidation: {
          mode: "error",
        },
      }),
    ).resolves.toEqual([{ id: "ord_1", user_id: "missing_user" }]);
  });
});
