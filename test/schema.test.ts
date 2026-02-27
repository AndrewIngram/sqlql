import { describe, expect, it } from "vitest";

import { defineSchema, defineTableMethods, resolveTableQueryBehavior, toSqlDDL } from "../src";

describe("defineSchema", () => {
  it("keeps tables concise and applies permissive query defaults", () => {
    const schema = defineSchema({
      tables: {
        agent_events: {
          columns: {
            event_id: "text",
            org_id: "text",
            created_at: "timestamp",
          },
        },
      },
    });

    expect(resolveTableQueryBehavior(schema, "agent_events")).toEqual({
      filterable: "all",
      sortable: "all",
      maxRows: null,
    });
  });

  it("allows table-level overrides", () => {
    const schema = defineSchema({
      defaults: {
        query: {
          maxRows: 5_000,
        },
      },
      tables: {
        agent_events: {
          columns: {
            event_id: "text",
            org_id: "text",
          },
          query: {
            maxRows: 100,
            sortable: ["event_id"],
          },
        },
      },
    });

    expect(resolveTableQueryBehavior(schema, "agent_events")).toEqual({
      filterable: "all",
      sortable: ["event_id"],
      maxRows: 100,
    });
  });

  it("generates SQL DDL from schema tables", () => {
    const schema = defineSchema({
      tables: {
        orders: {
          columns: {
            id: "text",
            total_cents: "integer",
            created_at: "timestamp",
          },
        },
        users: {
          columns: {
            id: "text",
            active: "boolean",
          },
        },
      },
    });

    expect(toSqlDDL(schema, { ifNotExists: true })).toBe(
      [
        'CREATE TABLE IF NOT EXISTS "orders" (',
        '  "id" TEXT,',
        '  "total_cents" INTEGER,',
        '  "created_at" TIMESTAMP',
        ");",
        "",
        'CREATE TABLE IF NOT EXISTS "users" (',
        '  "id" TEXT,',
        '  "active" BOOLEAN',
        ");",
      ].join("\n"),
    );
  });

  it("generates NOT NULL for non-nullable column definitions", () => {
    const schema = defineSchema({
      tables: {
        users: {
          columns: {
            id: { type: "text", nullable: false },
            email: { type: "text", nullable: true },
            active: "boolean",
          },
        },
      },
    });

    expect(toSqlDDL(schema)).toBe(
      [
        'CREATE TABLE "users" (',
        '  "id" TEXT NOT NULL,',
        '  "email" TEXT,',
        '  "active" BOOLEAN',
        ");",
      ].join("\n"),
    );
  });

  it("infers scan/aggregate request columns from schema", () => {
    const schema = defineSchema({
      tables: {
        orders: {
          columns: {
            id: "text",
            org_id: "text",
            total_cents: "integer",
          },
        },
      },
    });

    const methods = defineTableMethods(schema, {
      orders: {
        async scan(request) {
          request.select.push("id");
          request.where?.push({ op: "eq", column: "org_id", value: "org_1" });
          // @ts-expect-error not a valid orders column
          request.select.push("email");
          return [];
        },
        async aggregate(request) {
          request.groupBy?.push("org_id");
          request.metrics.push({ fn: "sum", column: "total_cents", as: "total" });
          // @ts-expect-error not a valid orders column
          request.groupBy?.push("email");
          // @ts-expect-error not a valid orders column
          request.metrics.push({ fn: "sum", column: "email", as: "sum_email" });
          return [];
        },
      },
    });

    expect(methods.orders).toBeDefined();
  });
});
