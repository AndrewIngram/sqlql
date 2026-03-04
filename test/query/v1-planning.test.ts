import { describe, expect, it } from "vitest";

import { defineProviders, defineSchema } from "../../src";
import { lowerSqlToRel, planPhysicalQuery } from "../../src/planning";

describe("query/v1 planning", () => {
  it("lowers simple select/join into relational operators", () => {
    const schema = defineSchema({
      tables: {
        orders: {
          provider: "orders",
          columns: {
            id: "text",
            user_id: "text",
            total_cents: "integer",
          },
        },
        users: {
          provider: "users",
          columns: {
            id: "text",
            email: "text",
          },
        },
      },
    });

    const lowered = lowerSqlToRel(
      `
        SELECT o.id, u.email
        FROM orders o
        JOIN users u ON o.user_id = u.id
        WHERE o.total_cents > 1000
        ORDER BY o.id ASC
        LIMIT 5
      `,
      schema,
    );

    expect(lowered.rel.kind).toBe("project");

    const project = lowered.rel;
    if (project.kind !== "project") {
      throw new Error("Expected project root.");
    }

    expect(project.columns).toEqual([
      {
        source: { alias: "o", column: "id" },
        output: "id",
      },
      {
        source: { alias: "u", column: "email" },
        output: "email",
      },
    ]);

    const limitNode = project.input;
    expect(limitNode.kind).toBe("limit_offset");
    if (limitNode.kind !== "limit_offset") {
      throw new Error("Expected limit_offset node.");
    }

    expect(limitNode.limit).toBe(5);
    expect(project.convention).toBe("local");
  });

  it("plans lookup_join for cross-provider joins with lookupMany", async () => {
    const schema = defineSchema({
      tables: {
        orders: {
          provider: "orders",
          columns: {
            id: "text",
            user_id: "text",
          },
        },
        users: {
          provider: "users",
          columns: {
            id: "text",
            email: "text",
          },
        },
      },
    });

    const providers = defineProviders({
      orders: {
        canExecute() {
          return true;
        },
        async compile(fragment) {
          return {
            provider: "orders",
            kind: fragment.kind,
            payload: fragment,
          };
        },
        async execute() {
          return [];
        },
      },
      users: {
        canExecute() {
          return true;
        },
        async compile(fragment) {
          return {
            provider: "users",
            kind: fragment.kind,
            payload: fragment,
          };
        },
        async execute() {
          return [];
        },
        async lookupMany() {
          return [];
        },
      },
    });

    const lowered = lowerSqlToRel(
      `
        SELECT o.id, u.email
        FROM orders o
        JOIN users u ON o.user_id = u.id
      `,
      schema,
    );

    const physical = await planPhysicalQuery(
      lowered.rel,
      schema,
      providers,
      {},
      `
        SELECT o.id, u.email
        FROM orders o
        JOIN users u ON o.user_id = u.id
      `,
    );

    expect(physical.steps.some((step) => step.kind === "lookup_join")).toBe(true);
  });

  it("plans same-provider subtree as a remote rel fragment when supported", async () => {
    const schema = defineSchema({
      tables: {
        orders: {
          provider: "warehouse",
          columns: {
            id: "text",
            user_id: "text",
            total_cents: "integer",
          },
        },
        users: {
          provider: "warehouse",
          columns: {
            id: "text",
            email: "text",
          },
        },
      },
    });

    const providers = defineProviders({
      warehouse: {
        canExecute(fragment) {
          return fragment.kind === "rel";
        },
        async compile(fragment) {
          return {
            provider: "warehouse",
            kind: fragment.kind,
            payload: fragment,
          };
        },
        async execute() {
          return [];
        },
      },
    });

    const lowered = lowerSqlToRel(
      `
        SELECT o.id, u.email
        FROM orders o
        JOIN users u ON o.user_id = u.id
      `,
      schema,
    );

    const physical = await planPhysicalQuery(
      lowered.rel,
      schema,
      providers,
      {},
      `
        SELECT o.id, u.email
        FROM orders o
        JOIN users u ON o.user_id = u.id
      `,
    );

    expect(physical.steps).toHaveLength(1);
    expect(physical.steps[0]?.kind).toBe("remote_fragment");
    if (physical.steps[0]?.kind !== "remote_fragment") {
      throw new Error("Expected remote fragment step.");
    }
    expect(physical.steps[0].fragment.kind).toBe("rel");
  });

  it("splits deterministically to remote scans + local join when rel pushdown is rejected", async () => {
    const schema = defineSchema({
      tables: {
        orders: {
          provider: "warehouse",
          columns: {
            id: "text",
            user_id: "text",
          },
        },
        users: {
          provider: "warehouse",
          columns: {
            id: "text",
            email: "text",
          },
        },
      },
    });

    const providers = defineProviders({
      warehouse: {
        canExecute(fragment) {
          return fragment.kind === "scan";
        },
        async compile(fragment) {
          return {
            provider: "warehouse",
            kind: fragment.kind,
            payload: fragment,
          };
        },
        async execute() {
          return [];
        },
      },
    });

    const lowered = lowerSqlToRel(
      `
        SELECT o.id, u.email
        FROM orders o
        JOIN users u ON o.user_id = u.id
      `,
      schema,
    );

    const physical = await planPhysicalQuery(
      lowered.rel,
      schema,
      providers,
      {},
      `
        SELECT o.id, u.email
        FROM orders o
        JOIN users u ON o.user_id = u.id
      `,
    );

    expect(physical.steps.some((step) => step.kind === "remote_fragment")).toBe(true);
    expect(physical.steps.some((step) => step.kind === "local_hash_join")).toBe(true);
  });

  it("does not plan lookup_join for RIGHT joins", async () => {
    const schema = defineSchema({
      tables: {
        orders: {
          provider: "orders",
          columns: {
            id: "text",
            user_id: "text",
          },
        },
        users: {
          provider: "users",
          columns: {
            id: "text",
            email: "text",
          },
        },
      },
    });

    const providers = defineProviders({
      orders: {
        canExecute() {
          return true;
        },
        async compile(fragment) {
          return {
            provider: "orders",
            kind: fragment.kind,
            payload: fragment,
          };
        },
        async execute() {
          return [];
        },
      },
      users: {
        canExecute() {
          return true;
        },
        async compile(fragment) {
          return {
            provider: "users",
            kind: fragment.kind,
            payload: fragment,
          };
        },
        async execute() {
          return [];
        },
        async lookupMany() {
          return [];
        },
      },
    });

    const lowered = lowerSqlToRel(
      `
        SELECT o.id, u.email
        FROM orders o
        RIGHT JOIN users u ON o.user_id = u.id
      `,
      schema,
    );

    const physical = await planPhysicalQuery(
      lowered.rel,
      schema,
      providers,
      {},
      `
        SELECT o.id, u.email
        FROM orders o
        RIGHT JOIN users u ON o.user_id = u.id
      `,
    );

    expect(physical.steps.some((step) => step.kind === "lookup_join")).toBe(false);
  });

  it("lowers non-correlated IN (SELECT ...) to a semi join (not sql rel fallback)", () => {
    const schema = defineSchema({
      tables: {
        my_orders: {
          provider: "warehouse",
          columns: {
            id: "text",
            vendor_id: "text",
          },
        },
        preferred_vendors: {
          provider: "warehouse",
          columns: {
            vendor_id: "text",
          },
        },
      },
    });

    const lowered = lowerSqlToRel(
      `
        SELECT o.id
        FROM my_orders o
        WHERE o.vendor_id IN (
          SELECT vendor_id
          FROM preferred_vendors
        )
      `,
      schema,
    );

    expect(lowered.rel.kind).toBe("project");
    if (lowered.rel.kind !== "project") {
      throw new Error("Expected project root.");
    }
    expect(lowered.rel.input.kind).toBe("join");
    if (lowered.rel.input.kind !== "join") {
      throw new Error("Expected semi join input.");
    }
    expect(lowered.rel.input.joinType).toBe("semi");
  });

  it("lowers UNION ALL to structured set_op rel", () => {
    const schema = defineSchema({
      tables: {
        a: {
          provider: "warehouse",
          columns: { id: "text" },
        },
        b: {
          provider: "warehouse",
          columns: { id: "text" },
        },
      },
    });

    const lowered = lowerSqlToRel(
      `
        SELECT id FROM a
        UNION ALL
        SELECT id FROM b
      `,
      schema,
    );

    expect(lowered.rel.kind).toBe("set_op");
    if (lowered.rel.kind !== "set_op") {
      throw new Error("Expected set_op root.");
    }
    expect(lowered.rel.op).toBe("union_all");
  });

  it("lowers WITH queries to structured with rel", () => {
    const schema = defineSchema({
      tables: {
        users: {
          provider: "warehouse",
          columns: {
            id: "text",
            org_id: "text",
          },
        },
      },
    });

    const lowered = lowerSqlToRel(
      `
        WITH scoped AS (
          SELECT id
          FROM users
          WHERE org_id = 'org_1'
        )
        SELECT id
        FROM scoped
      `,
      schema,
    );

    expect(lowered.rel.kind).toBe("with");
    if (lowered.rel.kind !== "with") {
      throw new Error("Expected with root.");
    }
    expect(lowered.rel.ctes).toHaveLength(1);
    expect(lowered.rel.ctes[0]?.name).toBe("scoped");
    expect(lowered.rel.body.kind).toBe("project");
  });

  it("lowers DENSE_RANK window projections to a window rel node", () => {
    const schema = defineSchema({
      tables: {
        orders: {
          provider: "warehouse",
          columns: {
            id: "text",
            total_cents: "integer",
          },
        },
      },
    });

    const lowered = lowerSqlToRel(
      `
        SELECT
          id,
          DENSE_RANK() OVER (ORDER BY total_cents DESC) AS spend_rank
        FROM orders
        ORDER BY spend_rank ASC
      `,
      schema,
    );

    expect(lowered.rel.kind).toBe("project");
    if (lowered.rel.kind !== "project") {
      throw new Error("Expected project root.");
    }
    expect(lowered.rel.input.kind).toBe("sort");
    if (lowered.rel.input.kind !== "sort") {
      throw new Error("Expected sort node.");
    }
    expect(lowered.rel.input.input.kind).toBe("window");
    if (lowered.rel.input.input.kind !== "window") {
      throw new Error("Expected window node.");
    }
    expect(lowered.rel.input.input.functions[0]?.fn).toBe("dense_rank");
    expect(lowered.rel.input.input.functions[0]?.as).toBe("spend_rank");
  });
});
