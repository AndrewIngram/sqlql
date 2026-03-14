import { Result } from "better-result";
import { describe, expect, it } from "vitest";

import {
  buildLogicalQueryPlanResult,
  buildProviderFragmentForRelResult,
  expandRelViewsResult,
  lowerSqlToRelResult,
  planPhysicalQueryResult,
} from "@tupl/planner";
import { buildSchema, buildEntitySchema } from "@tupl/test-support/schema";
import { finalizeProviders } from "@tupl/test-support/runtime";

function lowerSqlToRel(sql: string, schema: Parameters<typeof lowerSqlToRelResult>[1]) {
  return lowerSqlToRelResult(sql, schema).unwrap();
}

function buildLogicalQueryPlan(
  sql: string,
  schema: Parameters<typeof buildLogicalQueryPlanResult>[1],
) {
  return buildLogicalQueryPlanResult(sql, schema, {}).unwrap();
}

async function planPhysicalQuery<TContext>(
  rel: Parameters<typeof planPhysicalQueryResult<TContext>>[0],
  schema: Parameters<typeof planPhysicalQueryResult<TContext>>[1],
  providers: Parameters<typeof planPhysicalQueryResult<TContext>>[2],
  context: Parameters<typeof planPhysicalQueryResult<TContext>>[3],
  _sql?: string,
) {
  return (await planPhysicalQueryResult(rel, schema, providers, context)).unwrap();
}

describe("query/planning", () => {
  it("lowers simple select/join into relational operators", () => {
    const schema = buildEntitySchema({
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
        kind: "column",
        source: { alias: "o", column: "id" },
        output: "id",
      },
      {
        kind: "column",
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

  it("returns tagged planning errors from the result API", () => {
    const schema = buildEntitySchema({
      users: {
        provider: "warehouse",
        columns: {
          id: "text",
        },
      },
    });

    const result = lowerSqlToRelResult("SELECT id FROM missing_table", schema);
    expect(Result.isError(result)).toBe(true);
    if (Result.isOk(result)) {
      throw new Error("Expected lowering result to fail.");
    }

    expect(result.error).toMatchObject({
      _tag: "RelLoweringError",
      name: "RelLoweringError",
      message: "Unknown table: missing_table",
    });
  });

  it("returns tagged planning errors when expanding invalid view rels", () => {
    const schema = buildSchema((builder) => {
      builder.view("broken_view", ({ scan }) => scan("missing_table"), {
        columns: {
          id: { source: "missing_table.id" },
        },
      });
    });

    const lowered = lowerSqlToRel("SELECT id FROM broken_view", schema);
    const result = expandRelViewsResult(lowered.rel, schema, {});
    expect(Result.isError(result)).toBe(true);
    if (Result.isOk(result)) {
      throw new Error("Expected view expansion to fail.");
    }

    expect(result.error).toMatchObject({
      _tag: "RelRewriteError",
      name: "RelRewriteError",
      message: "Unknown table in view rel scan: missing_table",
    });
  });

  it("returns tagged planning errors when building provider fragments for invalid expanded rels", () => {
    const schema = buildSchema((builder) => {
      builder.view("broken_view", ({ scan }) => scan("missing_table"), {
        columns: {
          id: { source: "missing_table.id" },
        },
      });
    });

    const lowered = lowerSqlToRel("SELECT id FROM broken_view", schema);
    const result = buildProviderFragmentForRelResult(lowered.rel, schema, {});
    expect(Result.isError(result)).toBe(true);
    if (Result.isOk(result)) {
      throw new Error("Expected provider fragment build to fail.");
    }

    expect(result.error).toMatchObject({
      _tag: "RelRewriteError",
      name: "RelRewriteError",
      message: "Unknown table in view rel scan: missing_table",
    });
  });

  it("returns tagged planning errors from physical planning when a provider adapter is missing", async () => {
    const schema = buildEntitySchema({
      users: {
        provider: "warehouse",
        columns: {
          id: "text",
        },
      },
    });

    const lowered = lowerSqlToRel("SELECT id FROM users", schema);
    const result = await planPhysicalQueryResult(lowered.rel, schema, {}, {});

    expect(Result.isError(result)).toBe(true);
    if (Result.isOk(result)) {
      throw new Error("Expected physical planning to fail.");
    }

    expect(result.error).toMatchObject({
      _tag: "PhysicalPlanningError",
      name: "PhysicalPlanningError",
      message: "Missing provider adapter: warehouse",
    });
  });

  it("plans lookup_join for cross-provider joins with lookupMany", async () => {
    const schema = buildEntitySchema({
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
    });

    const providers = finalizeProviders({
      orders: {
        canExecute() {
          return true;
        },
        async compile(fragment) {
          return Result.ok({
            provider: "orders",
            kind: fragment.kind,
            payload: fragment,
          });
        },
        async execute() {
          return Result.ok([]);
        },
      },
      users: {
        canExecute() {
          return true;
        },
        async compile(fragment) {
          return Result.ok({
            provider: "users",
            kind: fragment.kind,
            payload: fragment,
          });
        },
        async execute() {
          return Result.ok([]);
        },
        async lookupMany() {
          return Result.ok([]);
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

    expect(physical.steps.some((step) => step.kind === "lookup_join")).toBe(false);
    expect(physical.steps.some((step) => step.kind === "remote_fragment")).toBe(true);
    expect(physical.steps.some((step) => step.kind === "local_hash_join")).toBe(true);
  });

  it("lowers FROM subqueries into local relational plans instead of rejecting them", () => {
    const schema = buildEntitySchema({
      orders: {
        provider: "warehouse",
        columns: {
          id: "text",
          org_id: "text",
        },
      },
    });

    const lowered = lowerSqlToRel(
      `
        SELECT scoped.id
        FROM (
          SELECT id
          FROM orders
          WHERE org_id = 'org_1'
        ) scoped
        ORDER BY scoped.id ASC
      `,
      schema,
    );

    expect(lowered.rel.kind).toBe("with");
  });

  it("lowers recursive CTEs to repeat_union rel nodes", () => {
    const schema = buildEntitySchema({
      edges: {
        provider: "warehouse",
        columns: {
          source_id: "integer",
          target_id: "integer",
        },
      },
    });

    const lowered = lowerSqlToRel(
      `
        WITH RECURSIVE reachable AS (
          SELECT source_id AS node_id
          FROM edges
          WHERE source_id = 1
          UNION ALL
          SELECT e.target_id AS node_id
          FROM reachable r
          JOIN edges e ON e.source_id = r.node_id
        )
        SELECT node_id
        FROM reachable
      `,
      schema,
    );

    expect(lowered.rel.kind).toBe("with");
    if (lowered.rel.kind !== "with") {
      throw new Error("Expected with root.");
    }

    expect(lowered.rel.ctes[0]?.query.kind).toBe("repeat_union");
    expect(lowered.rel.body.kind).toBe("project");
    if (lowered.rel.body.kind !== "project") {
      throw new Error("Expected project body.");
    }
    expect(lowered.rel.body.input.kind).toBe("cte_ref");

    const iterative = lowered.rel.ctes[0]?.query;
    if (!iterative || iterative.kind !== "repeat_union") {
      throw new Error("Expected repeat_union CTE.");
    }
    expect(iterative.iterative.kind).toBe("project");
    if (iterative.iterative.kind !== "project") {
      throw new Error("Expected recursive project.");
    }
    expect(iterative.iterative.input.kind).toBe("join");
    if (iterative.iterative.input.kind !== "join") {
      throw new Error("Expected recursive join.");
    }
    expect(iterative.iterative.input.left.kind).toBe("cte_ref");
  });

  it("lowers SELECT without FROM through a singleton values rel", () => {
    const schema = buildEntitySchema({});

    const lowered = lowerSqlToRel(
      `
        SELECT 1 AS answer, 2 + 3 AS sum_value
      `,
      schema,
    );

    expect(lowered.rel.kind).toBe("project");
    if (lowered.rel.kind !== "project") {
      throw new Error("Expected project root.");
    }

    expect(lowered.rel.input.kind).toBe("values");
  });

  it("plans same-provider subtree as a remote rel fragment when supported", async () => {
    const schema = buildEntitySchema({
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
    });

    const providers = finalizeProviders({
      warehouse: {
        canExecute() {
          return true;
        },
        async compile(rel) {
          return Result.ok({
            provider: "warehouse",
            kind: "rel",
            payload: rel,
          });
        },
        async execute() {
          return Result.ok([]);
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
    expect(physical.steps[0].fragment.rel.kind).toBe("project");
  });

  it("splits deterministically to remote scans + local join when rel pushdown is rejected", async () => {
    const schema = buildEntitySchema({
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
    });

    const providers = finalizeProviders({
      warehouse: {
        canExecute(rel) {
          return rel.kind === "scan";
        },
        async compile(rel) {
          return Result.ok({
            provider: "warehouse",
            kind: "rel",
            payload: rel,
          });
        },
        async execute() {
          return Result.ok([]);
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
    const schema = buildEntitySchema({
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
    });

    const providers = finalizeProviders({
      orders: {
        canExecute() {
          return true;
        },
        async compile(fragment) {
          return Result.ok({
            provider: "orders",
            kind: fragment.kind,
            payload: fragment,
          });
        },
        async execute() {
          return Result.ok([]);
        },
      },
      users: {
        canExecute() {
          return true;
        },
        async compile(fragment) {
          return Result.ok({
            provider: "users",
            kind: fragment.kind,
            payload: fragment,
          });
        },
        async execute() {
          return Result.ok([]);
        },
        async lookupMany() {
          return Result.ok([]);
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
    const schema = buildEntitySchema({
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

  it("lowers supported correlated EXISTS to an explicit correlate node", () => {
    const schema = buildEntitySchema({
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
          team_id: "text",
        },
      },
    });

    const lowered = lowerSqlToRel(
      `
        SELECT o.id
        FROM orders o
        WHERE EXISTS (
          SELECT 1
          FROM users u
          WHERE u.id = o.user_id
            AND u.team_id = 'team_smb'
        )
      `,
      schema,
    );

    expect(lowered.rel.kind).toBe("project");
    if (lowered.rel.kind !== "project") {
      throw new Error("Expected project root.");
    }
    expect(lowered.rel.input.kind).toBe("correlate");
    if (lowered.rel.input.kind !== "correlate") {
      throw new Error("Expected correlate input.");
    }
    expect(lowered.rel.input.apply).toEqual({ kind: "semi" });
    expect(lowered.rel.input.correlation).toEqual({
      outer: { alias: "o", column: "user_id" },
      inner: { alias: "u", column: "id" },
    });
  });

  it("lowers supported correlated EXISTS to a semi join after rewrite", () => {
    const schema = buildEntitySchema({
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
          team_id: "text",
        },
      },
    });

    const planned = buildLogicalQueryPlan(
      `
        SELECT o.id
        FROM orders o
        WHERE EXISTS (
          SELECT 1
          FROM users u
          WHERE u.id = o.user_id
            AND u.team_id = 'team_smb'
        )
      `,
      schema,
    );

    expect(planned.rewrittenRel.kind).toBe("project");
    if (planned.rewrittenRel.kind !== "project") {
      throw new Error("Expected project root.");
    }
    expect(planned.rewrittenRel.input.kind).toBe("join");
    if (planned.rewrittenRel.input.kind !== "join") {
      throw new Error("Expected semi join input.");
    }
    expect(planned.rewrittenRel.input.joinType).toBe("semi");
  });

  it("lowers supported correlated NOT EXISTS to an explicit correlate node", () => {
    const schema = buildEntitySchema({
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
          team_id: "text",
        },
      },
    });

    const lowered = lowerSqlToRel(
      `
        SELECT o.id
        FROM orders o
        WHERE NOT EXISTS (
          SELECT 1
          FROM users u
          WHERE u.id = o.user_id
            AND u.team_id = 'team_smb'
        )
      `,
      schema,
    );

    expect(lowered.rel.kind).toBe("project");
    if (lowered.rel.kind !== "project") {
      throw new Error("Expected project root.");
    }
    expect(lowered.rel.input.kind).toBe("correlate");
    if (lowered.rel.input.kind !== "correlate") {
      throw new Error("Expected correlate input.");
    }
    expect(lowered.rel.input.apply).toEqual({ kind: "anti" });
  });

  it("lowers supported correlated NOT EXISTS to anti-join rewrite shape", () => {
    const schema = buildEntitySchema({
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
          team_id: "text",
        },
      },
    });

    const planned = buildLogicalQueryPlan(
      `
        SELECT o.id
        FROM orders o
        WHERE NOT EXISTS (
          SELECT 1
          FROM users u
          WHERE u.id = o.user_id
            AND u.team_id = 'team_smb'
        )
      `,
      schema,
    );

    expect(planned.rewrittenRel.kind).toBe("project");
    if (planned.rewrittenRel.kind !== "project") {
      throw new Error("Expected project root.");
    }
    expect(planned.rewrittenRel.input.kind).toBe("project");
    if (planned.rewrittenRel.input.kind !== "project") {
      throw new Error("Expected cleanup project after anti-join emulation.");
    }
    expect(planned.rewrittenRel.input.input.kind).toBe("filter");
    if (planned.rewrittenRel.input.input.kind !== "filter") {
      throw new Error("Expected is-null filter above left join.");
    }
    expect(planned.rewrittenRel.input.input.input.kind).toBe("join");
    if (planned.rewrittenRel.input.input.input.kind !== "join") {
      throw new Error("Expected left join input.");
    }
    expect(planned.rewrittenRel.input.input.input.joinType).toBe("left");
  });

  it("lowers supported correlated IN to an explicit correlate node", () => {
    const schema = buildEntitySchema({
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
          team_id: "text",
        },
      },
    });

    const lowered = lowerSqlToRel(
      `
        SELECT o.id
        FROM orders o
        WHERE o.user_id IN (
          SELECT u.id
          FROM users u
          WHERE u.team_id = 'team_smb'
            AND u.id = o.user_id
        )
      `,
      schema,
    );

    expect(lowered.rel.kind).toBe("project");
    if (lowered.rel.kind !== "project") {
      throw new Error("Expected project root.");
    }
    expect(lowered.rel.input.kind).toBe("correlate");
    if (lowered.rel.input.kind !== "correlate") {
      throw new Error("Expected correlate input.");
    }
    expect(lowered.rel.input.apply).toEqual({ kind: "semi" });
    expect(lowered.rel.input.correlation).toEqual({
      outer: { alias: "o", column: "user_id" },
      inner: { alias: "u", column: "id" },
    });
  });

  it("lowers supported correlated NOT IN to an explicit correlate node", () => {
    const schema = buildEntitySchema({
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
          team_id: "text",
        },
      },
    });

    const lowered = lowerSqlToRel(
      `
        SELECT o.id
        FROM orders o
        WHERE o.user_id NOT IN (
          SELECT u.id
          FROM users u
          WHERE u.team_id = 'team_smb'
            AND u.id = o.user_id
        )
      `,
      schema,
    );

    expect(lowered.rel.kind).toBe("project");
    if (lowered.rel.kind !== "project") {
      throw new Error("Expected project root.");
    }
    expect(lowered.rel.input.kind).toBe("correlate");
    if (lowered.rel.input.kind !== "correlate") {
      throw new Error("Expected correlate input.");
    }
    expect(lowered.rel.input.apply).toEqual({ kind: "anti" });
    expect(lowered.rel.input.correlation).toEqual({
      outer: { alias: "o", column: "user_id" },
      inner: { alias: "u", column: "id" },
    });
  });

  it("lowers supported correlated NOT IN to anti-join rewrite shape", () => {
    const schema = buildEntitySchema({
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
          team_id: "text",
        },
      },
    });

    const planned = buildLogicalQueryPlan(
      `
        SELECT o.id
        FROM orders o
        WHERE o.user_id NOT IN (
          SELECT u.id
          FROM users u
          WHERE u.team_id = 'team_smb'
            AND u.id = o.user_id
        )
      `,
      schema,
    );

    expect(planned.rewrittenRel.kind).toBe("project");
    if (planned.rewrittenRel.kind !== "project") {
      throw new Error("Expected project root.");
    }
    expect(planned.rewrittenRel.input.kind).toBe("project");
    if (planned.rewrittenRel.input.kind !== "project") {
      throw new Error("Expected cleanup project after anti-join emulation.");
    }
    expect(planned.rewrittenRel.input.input.kind).toBe("filter");
    if (planned.rewrittenRel.input.input.kind !== "filter") {
      throw new Error("Expected is-null filter above left join.");
    }
    expect(planned.rewrittenRel.input.input.input.kind).toBe("join");
    if (planned.rewrittenRel.input.input.input.kind !== "join") {
      throw new Error("Expected left join input.");
    }
    expect(planned.rewrittenRel.input.input.input.joinType).toBe("left");
  });

  it("lowers supported correlated scalar aggregate predicates to an explicit correlate node", () => {
    const schema = buildEntitySchema({
      orders: {
        provider: "warehouse",
        columns: {
          id: "text",
          user_id: "text",
          total_cents: "integer",
        },
      },
    });

    const lowered = lowerSqlToRel(
      `
        SELECT o.id
        FROM orders o
        WHERE o.total_cents = (
          SELECT MAX(i.total_cents)
          FROM orders i
          WHERE i.user_id = o.user_id
        )
      `,
      schema,
    );

    expect(lowered.rel.kind).toBe("project");
    if (lowered.rel.kind !== "project") {
      throw new Error("Expected project root.");
    }
    expect(lowered.rel.input.kind).toBe("correlate");
    if (lowered.rel.input.kind !== "correlate") {
      throw new Error("Expected correlate input.");
    }
    expect(lowered.rel.input.apply).toEqual({
      kind: "scalar_filter",
      comparison: "=",
      outerCompare: { alias: "o", column: "total_cents" },
      correlationColumn: "__tupl_scalar_corr_key",
      metricColumn: "__tupl_scalar_metric",
    });
  });

  it("lowers supported correlated scalar aggregate predicates to join plus filter after rewrite", () => {
    const schema = buildEntitySchema({
      orders: {
        provider: "warehouse",
        columns: {
          id: "text",
          user_id: "text",
          total_cents: "integer",
        },
      },
    });

    const planned = buildLogicalQueryPlan(
      `
        SELECT o.id
        FROM orders o
        WHERE o.total_cents = (
          SELECT MAX(i.total_cents)
          FROM orders i
          WHERE i.user_id = o.user_id
        )
      `,
      schema,
    );

    expect(planned.rewrittenRel.kind).toBe("project");
    if (planned.rewrittenRel.kind !== "project") {
      throw new Error("Expected project root.");
    }
    expect(planned.rewrittenRel.input.kind).toBe("project");
    if (planned.rewrittenRel.input.kind !== "project") {
      throw new Error("Expected cleanup project after scalar correlate rewrite.");
    }
    expect(planned.rewrittenRel.input.input.kind).toBe("filter");
    if (planned.rewrittenRel.input.input.kind !== "filter") {
      throw new Error("Expected comparison filter above join.");
    }
    expect(planned.rewrittenRel.input.input.input.kind).toBe("join");
    if (planned.rewrittenRel.input.input.input.kind !== "join") {
      throw new Error("Expected join input.");
    }
    expect(planned.rewrittenRel.input.input.input.joinType).toBe("inner");
  });

  it("lowers UNION ALL to structured set_op rel", () => {
    const schema = buildEntitySchema({
      a: {
        provider: "warehouse",
        columns: { id: "text" },
      },
      b: {
        provider: "warehouse",
        columns: { id: "text" },
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
    const schema = buildEntitySchema({
      users: {
        provider: "warehouse",
        columns: {
          id: "text",
          org_id: "text",
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
    if (lowered.rel.body.kind !== "project") {
      throw new Error("Expected project body.");
    }
    expect(lowered.rel.body.input.kind).toBe("cte_ref");
  });

  it("lowers DENSE_RANK window projections to a window rel node", () => {
    const schema = buildEntitySchema({
      orders: {
        provider: "warehouse",
        columns: {
          id: "text",
          total_cents: "integer",
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

  it("materializes computed select expressions for ORDER BY ordinals", () => {
    const schema = buildEntitySchema({
      users: {
        provider: "warehouse",
        columns: {
          id: "text",
        },
      },
    });

    const lowered = lowerSqlToRel(
      `
        SELECT id, SUBSTR(id, 1, 1) AS initial
        FROM users
        ORDER BY 2 ASC, 1 DESC
      `,
      schema,
    );

    expect(lowered.rel.kind).toBe("project");
    if (lowered.rel.kind !== "project") {
      throw new Error("Expected project root.");
    }
    expect(lowered.rel.columns[1]).toMatchObject({
      kind: "column",
      output: "initial",
      source: {
        column: expect.stringMatching(/^__order_by_/),
      },
    });

    const sort = lowered.rel.input;
    expect(sort.kind).toBe("sort");
    if (sort.kind !== "sort") {
      throw new Error("Expected sort node.");
    }
    expect(sort.orderBy[0]?.source).toMatchObject({
      column: expect.stringMatching(/^__order_by_/),
    });
    expect(sort.orderBy[1]?.source).toEqual({ alias: "users", column: "id" });
    expect(sort.input.kind).toBe("project");
  });

  it("materializes computed select expressions for GROUP BY ordinals", () => {
    const schema = buildEntitySchema({
      users: {
        provider: "warehouse",
        columns: {
          id: "text",
        },
      },
    });

    const lowered = lowerSqlToRel(
      `
        SELECT SUBSTR(id, 1, 1) AS initial, COUNT(*) AS user_count
        FROM users
        GROUP BY 1
        ORDER BY 2 DESC
      `,
      schema,
    );

    expect(lowered.rel.kind).toBe("project");
    if (lowered.rel.kind !== "project") {
      throw new Error("Expected project root.");
    }
    expect(lowered.rel.columns[0]).toMatchObject({
      kind: "column",
      output: "initial",
      source: {
        column: expect.stringMatching(/^__group_by_/),
      },
    });

    const sort = lowered.rel.input;
    expect(sort.kind).toBe("sort");
    if (sort.kind !== "sort") {
      throw new Error("Expected sort node.");
    }
    expect(sort.orderBy[0]?.source).toEqual({ column: "user_count" });

    const aggregate = sort.input;
    expect(aggregate.kind).toBe("aggregate");
    if (aggregate.kind !== "aggregate") {
      throw new Error("Expected aggregate node.");
    }
    expect(aggregate.groupBy[0]?.column).toMatch(/^__group_by_/);
    expect(aggregate.input.kind).toBe("project");
  });

  it("rejects invalid ordinal references in ORDER BY and GROUP BY", () => {
    const schema = buildEntitySchema({
      users: {
        provider: "warehouse",
        columns: {
          id: "text",
        },
      },
    });

    expect(() =>
      lowerSqlToRel(
        `
          SELECT id, COUNT(*) AS user_count
          FROM users
          GROUP BY 2
        `,
        schema,
      ),
    ).toThrow("GROUP BY ordinal 2 cannot reference an aggregate output.");

    expect(() =>
      lowerSqlToRel(
        `
          SELECT id
          FROM users
          ORDER BY 0
        `,
        schema,
      ),
    ).toThrow("ORDER BY ordinal must be a positive integer.");
  });
});
