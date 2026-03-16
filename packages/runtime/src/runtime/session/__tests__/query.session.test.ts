import { describe, expect, it } from "vite-plus/test";
import { createMethodsSession, queryWithMethods } from "@tupl/test-support/runtime";
import { createArrayTableMethods } from "@tupl/test-support/methods";

import { defineTableMethods } from "@tupl/schema-model";
import { commerceRows, commerceSchema } from "@tupl/test-support/fixtures";
import { buildEntitySchema } from "@tupl/test-support/schema";

const EMPTY_CONTEXT = {} as const;

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

describe("query/session", () => {
  it("links CTE-backed scans to their producer step in the static plan", () => {
    const schema = buildEntitySchema({
      athletes: {
        columns: {
          id: { type: "text", nullable: false },
          display_name: { type: "text", nullable: false },
        },
      },
      workouts: {
        columns: {
          id: { type: "text", nullable: false },
          athlete_id: { type: "text", nullable: false },
          duration_min: { type: "integer", nullable: false },
          completed_at: { type: "timestamp", nullable: false },
        },
      },
    });

    const methods = defineTableMethods(schema, {
      athletes: createArrayTableMethods([
        { id: "ath_1", display_name: "Ada" },
        { id: "ath_2", display_name: "Ben" },
      ]),
      workouts: createArrayTableMethods([
        {
          id: "wo_1",
          athlete_id: "ath_1",
          duration_min: 30,
          completed_at: "2026-02-01T07:00:00Z",
        },
        {
          id: "wo_2",
          athlete_id: "ath_2",
          duration_min: 45,
          completed_at: "2026-02-02T07:00:00Z",
        },
      ]),
    });

    const sql = `
      WITH recent_workouts AS (
        SELECT athlete_id, duration_min
        FROM workouts
        WHERE completed_at >= '2026-02-01'
      )
      SELECT a.display_name, SUM(r.duration_min) AS total_minutes
      FROM recent_workouts r
      JOIN athletes a ON r.athlete_id = a.id
      GROUP BY a.display_name
      ORDER BY total_minutes DESC
    `;

    const session = createMethodsSession({
      schema,
      methods,
      context: EMPTY_CONTEXT,
      sql,
    });

    const plan = session.getPlan();
    const cteStep = plan.steps.find(
      (step) => step.kind === "cte" && step.summary === "CTE recent_workouts",
    );
    expect(cteStep).toBeDefined();
    expect(cteStep?.scopeId).toBeDefined();

    const finalizeStep = plan.steps.find(
      (step) => step.kind === "projection" && step.summary === "Finalize WITH query",
    );
    expect(finalizeStep).toBeDefined();
    expect(finalizeStep?.dependsOn).toContain(cteStep?.id);

    const planScopes = plan.scopes ?? [];
    const rootScope = planScopes.find((scope) => scope.kind === "root");
    expect(rootScope).toBeDefined();
    const cteScope = planScopes.find((scope) => scope.id === cteStep?.scopeId);
    expect(cteScope?.kind).toBe("cte");
    expect(cteScope?.label).toBe("CTE recent_workouts");
    expect(cteScope?.parentId).toBe(rootScope?.id);
  });

  it("builds a full execution plan before stepping", async () => {
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

    const session = createMethodsSession({
      schema: commerceSchema,
      methods,
      context: EMPTY_CONTEXT,
      sql,
      options: {
        captureRows: "full",
      },
    });

    const preRunPlan = session.getPlan();
    expect(preRunPlan.steps.length).toBeGreaterThan(0);
    const scanStep = preRunPlan.steps.find((step) => step.kind === "scan");
    expect(scanStep).toBeDefined();
    expect(scanStep?.phase).toBe("fetch");
    expect(scanStep?.operation.name).toBe("scan");
    expect(scanStep?.sqlOrigin).toBe("FROM");
    expect(scanStep?.request).toBeDefined();

    const first = await session.next();
    expect("done" in first).toBe(false);
    if ("done" in first) {
      throw new Error("Expected a step event.");
    }
    expect(first.executionIndex).toBe(1);
    expect(session.getPlan().steps.length).toBe(preRunPlan.steps.length);
  });

  it("creates scoped set-op branches in the static plan", () => {
    const methods = defineTableMethods(commerceSchema, {
      orders: createArrayTableMethods(commerceRows.orders),
      users: createArrayTableMethods(commerceRows.users),
      teams: createArrayTableMethods(commerceRows.teams),
    });

    const sql = `
      SELECT user_id FROM orders
      UNION ALL
      SELECT id AS user_id FROM users
    `;

    const session = createMethodsSession({
      schema: commerceSchema,
      methods,
      context: EMPTY_CONTEXT,
      sql,
    });

    const plan = session.getPlan();
    const scopes = plan.scopes ?? [];
    const branchScopes = scopes.filter((scope) => scope.kind === "set_op_branch");
    expect(branchScopes).toHaveLength(2);

    const leftBranch = branchScopes.find((scope) => scope.label === "Set operation left branch");
    const rightBranch = branchScopes.find((scope) => scope.label === "Set operation right branch");
    expect(leftBranch).toBeDefined();
    expect(rightBranch).toBeDefined();

    const leftStep = plan.steps.find(
      (step) => step.kind === "set_op_branch" && step.summary === "Set operation left branch",
    );
    const rightStep = plan.steps.find(
      (step) => step.kind === "set_op_branch" && step.summary === "Set operation right branch",
    );
    expect(leftStep?.scopeId).toBe(leftBranch?.id);
    expect(rightStep?.scopeId).toBe(rightBranch?.id);
  });

  it("adds a projected ORDER step when ordering by window output alias", () => {
    const methods = defineTableMethods(commerceSchema, {
      orders: createArrayTableMethods(commerceRows.orders),
      users: createArrayTableMethods(commerceRows.users),
      teams: createArrayTableMethods(commerceRows.teams),
    });

    const session = createMethodsSession({
      schema: commerceSchema,
      methods,
      context: EMPTY_CONTEXT,
      sql: `
        SELECT
          o.id,
          o.user_id,
          RANK() OVER (PARTITION BY o.user_id ORDER BY o.total_cents DESC) AS spend_rank
        FROM orders o
        ORDER BY o.user_id ASC, spend_rank ASC, o.id ASC
      `,
    });

    const plan = session.getPlan();
    const windowStep = plan.steps.find((step) => step.summary === "Compute window functions");
    const orderStep = plan.steps.find((step) => step.summary === "Order result rows");

    expect(windowStep).toBeDefined();
    expect(orderStep).toBeDefined();
    expect(orderStep?.dependsOn).toContain(windowStep?.id);
  });

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

    const session = createMethodsSession({
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
    expect(first.executionIndex).toBe(1);

    const firstState = session.getStepState(first.id);
    expect(firstState?.status).toBe("done");
    expect(firstState?.executionIndex).toBe(1);
    expect(session.getPlan().steps.length).toBeGreaterThan(0);

    let finalRows: Array<Record<string, unknown>> | null = null;
    while (finalRows == null) {
      const next = await session.next();
      if ("done" in next) {
        finalRows = next.result;
      }
    }

    const directRows = await queryWithMethods({
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

    const session = createMethodsSession({
      schema: commerceSchema,
      methods,
      context: EMPTY_CONTEXT,
      sql,
    });

    const sessionRows = await session.runToCompletion();
    const directRows = await queryWithMethods({
      schema: commerceSchema,
      methods,
      context: EMPTY_CONTEXT,
      sql,
    });

    expect(sessionRows).toEqual(directRows);
  });

  it("propagates execution failures via next()", async () => {
    const schema = buildEntitySchema({
      users: {
        columns: {
          id: { type: "text", nullable: false },
        },
      },
    });

    const methods = defineTableMethods(schema, {
      users: createArrayTableMethods([{ id: "u1" }]),
    });

    expect(() =>
      createMethodsSession({
        schema,
        methods,
        context: EMPTY_CONTEXT,
        sql: "SELECT * FROM missing_table",
      }),
    ).toThrow("Unknown table: missing_table");
  });

  it("marks the root step failed when execution times out", async () => {
    const schema = buildEntitySchema({
      users: {
        columns: {
          id: { type: "text", nullable: false },
        },
      },
    });

    const methods = defineTableMethods(schema, {
      users: {
        ...createArrayTableMethods([{ id: "u1" }]),
        async scan(request, context) {
          await sleep(25);
          return createArrayTableMethods([{ id: "u1" }]).scan(request, context);
        },
      },
    });

    const session = createMethodsSession({
      schema,
      methods,
      context: EMPTY_CONTEXT,
      sql: "SELECT id FROM users",
      queryGuardrails: {
        timeoutMs: 5,
      },
    });

    await expect(session.next()).rejects.toMatchObject({
      _tag: "TuplTimeoutError",
      name: "TuplTimeoutError",
      message: "Query timed out after 5ms.",
    });

    const rootStepId = session.getPlan().steps[session.getPlan().steps.length - 1]?.id;
    expect(rootStepId).toBeDefined();
    expect(session.getStepState(rootStepId ?? "")).toMatchObject({
      status: "failed",
      error: "Query timed out after 5ms.",
    });
  });
});
