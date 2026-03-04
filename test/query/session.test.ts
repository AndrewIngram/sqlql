import { describe, expect, it } from "vitest";
import { providersFromMethods } from "../support/methods-provider";
import { createArrayTableMethods } from "../../src/array-methods";

import {
  createQuerySession,
  defineSchema,
  defineTableMethods,
  query,
} from "../../src";
import { commerceRows, commerceSchema } from "../support/commerce-fixture";

const EMPTY_CONTEXT = {} as const;

describe("query/session", () => {
  it("links CTE-backed scans to their producer step in the static plan", () => {
    const schema = defineSchema({
      tables: {
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

    const session = createQuerySession({
      schema,
      providers: providersFromMethods(methods),
      context: EMPTY_CONTEXT,
      sql,
    });

    const plan = session.getPlan();
    const cteStep = plan.steps.find(
      (step) => step.kind === "cte" && step.summary === "CTE recent_workouts",
    );
    expect(cteStep).toBeDefined();
    expect(cteStep?.scopeId).toBeDefined();

    const cteScanStep = plan.steps.find(
      (step) => step.kind === "scan" && step.summary === "Scan r (recent_workouts)",
    );
    expect(cteScanStep).toBeDefined();
    expect(cteScanStep?.dependsOn).toContain(cteStep?.id);

    const planScopes = plan.scopes ?? [];
    const rootScope = planScopes.find((scope) => scope.kind === "root");
    expect(rootScope).toBeDefined();
    expect(cteScanStep?.scopeId).toBe(rootScope?.id);
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

    const session = createQuerySession({
      schema: commerceSchema,
      providers: providersFromMethods(methods),
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
    expect(scanStep?.pushdown).toBeDefined();

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

    const session = createQuerySession({
      schema: commerceSchema,
      providers: providersFromMethods(methods),
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

    const session = createQuerySession({
      schema: commerceSchema,
      providers: providersFromMethods(methods),
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
    const projectionStep = plan.steps.find((step) => step.summary === "Project result rows");
    const projectedOrderStep = plan.steps.find(
      (step) => step.summary === "Apply ORDER/LIMIT/OFFSET on projected rows",
    );

    expect(projectionStep).toBeDefined();
    expect(projectedOrderStep).toBeDefined();
    expect(projectedOrderStep?.dependsOn).toContain(projectionStep?.id);
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

    const session = createQuerySession({
      schema: commerceSchema,
      providers: providersFromMethods(methods),
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

    const directRows = await query({
      schema: commerceSchema,
      providers: providersFromMethods(methods),
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
      providers: providersFromMethods(methods),
      context: EMPTY_CONTEXT,
      sql,
    });

    const sessionRows = await session.runToCompletion();
    const directRows = await query({
      schema: commerceSchema,
      providers: providersFromMethods(methods),
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
      providers: providersFromMethods(defineTableMethods(schema, {
        users: createArrayTableMethods([{ id: "u1" }]),
      })),
      context: EMPTY_CONTEXT,
      sql: "SELECT * FROM missing_table",
    });

    await expect(session.next()).rejects.toThrow("Unknown table: missing_table");
  });
});
