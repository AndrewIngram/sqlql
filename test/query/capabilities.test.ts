import { describe, expect, it } from "vitest";

import { createArrayTableMethods, defineSchema, defineTableMethods, query } from "../../src";

const EMPTY_CONTEXT = {} as const;

describe("query/capabilities", () => {
  it("rejects WHERE predicates on non-filterable columns", async () => {
    const schema = defineSchema({
      tables: {
        users: {
          columns: {
            id: { type: "text", nullable: false },
            email: { type: "text", nullable: false, filterable: false },
          },
        },
      },
    });

    const methods = defineTableMethods(schema, {
      users: createArrayTableMethods([{ id: "u1", email: "a@example.com" }]),
    });

    await expect(
      query({
        schema,
        methods,
        context: EMPTY_CONTEXT,
        sql: "SELECT id FROM users WHERE email = 'a@example.com'",
      }),
    ).rejects.toThrow("Filtering on users.email is not supported");
  });

  it("rejects ORDER BY on non-sortable columns", async () => {
    const schema = defineSchema({
      tables: {
        users: {
          columns: {
            id: { type: "text", nullable: false },
            email: { type: "text", nullable: false, sortable: false },
          },
        },
      },
    });

    const methods = defineTableMethods(schema, {
      users: createArrayTableMethods([{ id: "u1", email: "a@example.com" }]),
    });

    await expect(
      query({
        schema,
        methods,
        context: EMPTY_CONTEXT,
        sql: "SELECT id FROM users ORDER BY email ASC",
      }),
    ).rejects.toThrow("Sorting by users.email is not supported");
  });

  it("enforces requiresLimit reject policy", async () => {
    const schema = defineSchema({
      tables: {
        events: {
          columns: {
            id: "text",
            org_id: "text",
          },
          query: {
            reject: {
              requiresLimit: true,
            },
          },
        },
      },
    });

    const methods = defineTableMethods(schema, {
      events: createArrayTableMethods([{ id: "e1", org_id: "o1" }]),
    });

    await expect(
      query({
        schema,
        methods,
        context: EMPTY_CONTEXT,
        sql: "SELECT id FROM events WHERE org_id = 'o1'",
      }),
    ).rejects.toThrow("LIMIT is required");
  });

  it("enforces forbidFullScan reject policy", async () => {
    const schema = defineSchema({
      tables: {
        events: {
          columns: {
            id: "text",
            org_id: "text",
          },
          query: {
            reject: {
              forbidFullScan: true,
            },
          },
        },
      },
    });

    const methods = defineTableMethods(schema, {
      events: createArrayTableMethods([{ id: "e1", org_id: "o1" }]),
    });

    await expect(
      query({
        schema,
        methods,
        context: EMPTY_CONTEXT,
        sql: "SELECT id FROM events LIMIT 10",
      }),
    ).rejects.toThrow("full scans are forbidden");
  });

  it("enforces requireAnyFilterOn reject policy", async () => {
    const schema = defineSchema({
      tables: {
        events: {
          columns: {
            id: "text",
            org_id: "text",
            actor_id: "text",
          },
          query: {
            reject: {
              requireAnyFilterOn: ["org_id", "actor_id"],
            },
          },
        },
      },
    });

    const methods = defineTableMethods(schema, {
      events: createArrayTableMethods([{ id: "e1", org_id: "o1", actor_id: "u1" }]),
    });

    await expect(
      query({
        schema,
        methods,
        context: EMPTY_CONTEXT,
        sql: "SELECT id FROM events WHERE id = 'e1'",
      }),
    ).rejects.toThrow("expected a WHERE filter on one of [org_id, actor_id]");
  });

  it("rejects non-pushdown filters when fallback.filters=require_pushdown", async () => {
    const schema = defineSchema({
      tables: {
        users: {
          columns: {
            id: "text",
            email: "text",
          },
          query: {
            fallback: {
              filters: "require_pushdown",
            },
          },
        },
      },
    });

    const methods = defineTableMethods(schema, {
      users: createArrayTableMethods([
        { id: "u1", email: "a@example.com" },
        { id: "u2", email: "b@example.com" },
      ]),
    });

    await expect(
      query({
        schema,
        methods,
        context: EMPTY_CONTEXT,
        sql: "SELECT id FROM users WHERE id = 'u1' OR email = 'b@example.com'",
      }),
    ).rejects.toThrow("non-pushdown WHERE predicates are not allowed");
  });
});
