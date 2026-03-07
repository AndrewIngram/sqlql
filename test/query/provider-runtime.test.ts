import { Result } from "better-result";
import { describe, expect, it } from "vitest";

import {
  createDataEntityHandle,
  defineSchema,
  getNormalizedTableBinding,
  type ProviderAdapter,
  type ProviderFragment,
  type QueryRow,
  type ScanFilterClause,
  type TableScanRequest,
} from "../../src";
import { validateProviderBindingsResult } from "../../src/provider";
import { createExecutableSchemaFromProviders } from "../support/executable-schema";

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

function scanRows(rows: QueryRow[], request: TableScanRequest): QueryRow[] {
  let out = rows.filter((row) => applyFilters(row, request.where ?? []));

  if (request.orderBy && request.orderBy.length > 0) {
    out = [...out].sort((left, right) => {
      for (const term of request.orderBy ?? []) {
        const leftValue = left[term.column] ?? null;
        const rightValue = right[term.column] ?? null;
        if (leftValue === rightValue) {
          continue;
        }

        const direction = term.direction === "asc" ? 1 : -1;
        if (leftValue == null) {
          return -1 * direction;
        }
        if (rightValue == null) {
          return 1 * direction;
        }

        if (leftValue < rightValue) {
          return -1 * direction;
        }
        if (leftValue > rightValue) {
          return 1 * direction;
        }
      }
      return 0;
    });
  }

  if (request.offset != null) {
    out = out.slice(request.offset);
  }

  if (request.limit != null) {
    out = out.slice(0, request.limit);
  }

  return out.map((row) => {
    const projected: QueryRow = {};
    for (const column of request.select) {
      projected[column] = row[column] ?? null;
    }
    return projected;
  });
}

function applyFilters(row: QueryRow, filters: ScanFilterClause[]): boolean {
  for (const clause of filters) {
    const value = row[clause.column];
    switch (clause.op) {
      case "eq":
        if (value !== clause.value) {
          return false;
        }
        break;
      case "neq":
        if (value === clause.value) {
          return false;
        }
        break;
      case "gt":
        if (value == null || clause.value == null || Number(value) <= Number(clause.value)) {
          return false;
        }
        break;
      case "gte":
        if (value == null || clause.value == null || Number(value) < Number(clause.value)) {
          return false;
        }
        break;
      case "lt":
        if (value == null || clause.value == null || Number(value) >= Number(clause.value)) {
          return false;
        }
        break;
      case "lte":
        if (value == null || clause.value == null || Number(value) > Number(clause.value)) {
          return false;
        }
        break;
      case "in":
        if (!clause.values.includes(value)) {
          return false;
        }
        break;
      case "not_in":
        if (clause.values.includes(value)) {
          return false;
        }
        break;
      case "like":
        if (typeof value !== "string" || typeof clause.value !== "string" || !matchesLike(value, clause.value)) {
          return false;
        }
        break;
      case "not_like":
        if (typeof value !== "string" || typeof clause.value !== "string" || matchesLike(value, clause.value)) {
          return false;
        }
        break;
      case "is_distinct_from":
        if (value === clause.value) {
          return false;
        }
        break;
      case "is_not_distinct_from":
        if (value !== clause.value) {
          return false;
        }
        break;
      case "is_null":
        if (value != null) {
          return false;
        }
        break;
      case "is_not_null":
        if (value == null) {
          return false;
        }
        break;
    }
  }

  return true;
}

function matchesLike(value: string, pattern: string): boolean {
  const escaped = pattern
    .replace(/[.*+?^${}()|[\]\\]/g, "\\$&")
    .replace(/%/g, ".*")
    .replace(/_/g, ".");
  return new RegExp(`^${escaped}$`, "su").test(value);
}

describe("query/provider runtime", () => {
  it("routes same-provider queries through scan fragments", async () => {
    const schema = defineSchema({
      tables: {
        orders: {
          provider: "warehouse",
          columns: {
            id: "text",
            org_id: "text",
            total_cents: "integer",
          },
        },
      },
    });

    let canExecuteCalls = 0;
    let executeCalls = 0;

    const executableSchema = createExecutableSchemaFromProviders(schema, {
      warehouse: {
        canExecute(fragment: ProviderFragment) {
          canExecuteCalls += 1;
          return fragment.kind === "scan";
        },
        async compile(fragment: ProviderFragment) {
          return Result.ok({
            provider: "warehouse",
            kind: fragment.kind,
            payload: fragment,
          });
        },
        async execute() {
          executeCalls += 1;
          return Result.ok([{ id: "o2" }]);
        },
      } satisfies Omit<ProviderAdapter, "name">,
    });

    const rows = await executableSchema.query({
      context: {},
      sql: `
        SELECT id
        FROM orders
        WHERE org_id = 'org_1'
        ORDER BY total_cents DESC
        LIMIT 1
      `,
    });

    expect(rows).toEqual([{ id: "o2" }]);
    expect(canExecuteCalls).toBeGreaterThan(0);
    expect(executeCalls).toBeGreaterThan(0);
  });

  it("executes same-provider rel fragment when provider supports rel pushdown", async () => {
    const schema = defineSchema({
      tables: {
        orders: {
          provider: "warehouse",
          columns: {
            id: "text",
            org_id: "text",
          },
        },
      },
    });

    let sawRelCompile = false;

    const executableSchema = createExecutableSchemaFromProviders(schema, {
      warehouse: {
        canExecute(fragment: ProviderFragment) {
          return fragment.kind === "rel" || fragment.kind === "scan";
        },
        async compile(fragment: ProviderFragment) {
          if (fragment.kind === "rel") {
            sawRelCompile = true;
          }
          return Result.ok({
            provider: "warehouse",
            kind: fragment.kind,
            payload: fragment,
          });
        },
        async execute() {
          return Result.ok([{ id: "o1" }]);
        },
      } satisfies Omit<ProviderAdapter, "name">,
    });

    const rows = await executableSchema.query({
      context: {},
      sql: `
        SELECT id
        FROM orders
        WHERE org_id = 'org_1'
      `,
    });

    expect(rows).toEqual([{ id: "o1" }]);
    expect(sawRelCompile).toBe(true);
  });

  it("normalizes provider rel fragments to physical entities and source columns", async () => {
    const ordersEntity = createDataEntityHandle({
      entity: "orders_raw",
      provider: "warehouse",
    });

    const schema = defineSchema(({ table, col }) => ({
      tables: {
        my_orders: table({
          from: ordersEntity,
          columns: {
            id: col("id"),
            totalCents: col("total_cents"),
            status: {
              source: col("status"),
              type: "text",
              enumFrom: "orders.status",
              enumMap: {
                pending: "open",
                paid: "settled",
                shipped: "settled",
              },
              enum: ["open", "settled"] as const,
            },
          },
        }),
        orders: table({
          from: ordersEntity,
          columns: {
            status: { source: "status", type: "text", enum: ["pending", "paid", "shipped"] as const },
          },
        }),
      },
    }));

    let capturedRel: unknown = null;

    const executableSchema = createExecutableSchemaFromProviders(schema, {
      warehouse: {
        canExecute(fragment: ProviderFragment) {
          return fragment.kind === "rel";
        },
        async compile(fragment: ProviderFragment) {
          if (fragment.kind === "rel") {
            capturedRel = fragment.rel;
          }

          return Result.ok({
            provider: "warehouse",
            kind: fragment.kind,
            payload: fragment,
          });
        },
        async execute() {
          return Result.ok([{ id: "o1" }]);
        },
      } satisfies Omit<ProviderAdapter, "name">,
    });

    const rows = await executableSchema.query({
      context: {},
      sql: `
        SELECT id
        FROM my_orders
        WHERE totalCents >= 1000 AND status = 'settled'
      `,
    });

    expect(rows).toEqual([{ id: "o1" }]);
    expect(capturedRel).not.toBeNull();

    const projectNode = capturedRel as {
      kind?: string;
      input?: {
        kind?: string;
        table?: string;
        select?: string[];
        where?: Array<{ column?: string; op?: string }>;
      };
    } | null;
    const scanNode = projectNode?.kind === "project" && projectNode.input?.kind === "scan"
      ? projectNode.input
      : null;
    expect(scanNode?.table).toBe("orders_raw");
    expect(scanNode?.select).toContain("total_cents");
    expect(scanNode?.where?.[0]?.column).toBe("total_cents");
    expect(scanNode?.where?.some((clause) => clause.column === "status" && clause.op === "in")).toBe(true);
  });

  it("maps typed table columns back to logical names and applies built-in coercions on provider scans", async () => {
    const ordersEntity = createDataEntityHandle({
      entity: "orders_raw",
      provider: "warehouse",
      columns: {
        id: { source: "id", type: "text", nullable: false, primaryKey: true },
        totalCents: { source: "total_cents", type: "integer", nullable: false },
        createdAt: { source: "created_at", type: "timestamp", nullable: false },
      },
    });

    const schema = defineSchema(({ table, col }) => ({
      tables: {
        my_orders: table({
          from: ordersEntity,
          columns: {
            id: col.id("id"),
            totalCents: col.integer("totalCents"),
            createdAt: col.string("createdAt", { coerce: "isoTimestamp" }),
          },
        }),
      },
    }));

    let capturedScan: { request: TableScanRequest } | null = null;
    const executableSchema = createExecutableSchemaFromProviders(schema, {
      warehouse: {
        canExecute(fragment: ProviderFragment) {
          return fragment.kind === "scan";
        },
        async compile(fragment: ProviderFragment) {
          capturedScan = fragment.kind === "scan" ? fragment : null;
          return Result.ok({
            provider: "warehouse",
            kind: fragment.kind,
            payload: fragment,
          });
        },
        async execute() {
          return Result.ok([
            {
              id: "o1",
              total_cents: 1500,
              created_at: new Date("2026-02-03T10:00:00.000Z"),
            },
          ]);
        },
      } satisfies Omit<ProviderAdapter, "name">,
    });

    const rows = await executableSchema.query({
      context: {},
      sql: "SELECT id, totalCents, createdAt FROM my_orders",
    });

    expect(capturedScan).not.toBeNull();
    const capturedRequest = (capturedScan as unknown as { request: TableScanRequest }).request;
    expect(capturedRequest.select).toEqual([
      "id",
      "total_cents",
      "created_at",
    ]);
    expect(rows).toEqual([
      {
        id: "o1",
        totalCents: 1500,
        createdAt: "2026-02-03T10:00:00.000Z",
      },
    ]);
  });

  it("uses lookupMany for cross-provider lookup join paths", async () => {
    const schema = defineSchema({
      tables: {
        orders: {
          provider: "orders_provider",
          columns: {
            id: "text",
            user_id: "text",
          },
        },
        users: {
          provider: "users_provider",
          columns: {
            id: "text",
            email: "text",
          },
        },
      },
    });

    const ordersRows: QueryRow[] = [
      { id: "o1", user_id: "u1" },
      { id: "o2", user_id: "u2" },
    ];
    const usersRows: QueryRow[] = [
      { id: "u1", email: "ada@example.com" },
      { id: "u2", email: "ben@example.com" },
    ];

    let lookupCalls = 0;

    const executableSchema = createExecutableSchemaFromProviders(schema, {
      orders_provider: {
        canExecute(fragment: ProviderFragment) {
          return fragment.kind === "scan";
        },
        async compile(fragment: ProviderFragment) {
          return Result.ok({
            provider: "orders_provider",
            kind: fragment.kind,
            payload: fragment,
          });
        },
        async execute(plan) {
          const fragment = plan.payload as ProviderFragment;
          if (fragment.kind !== "scan") {
            return Result.ok([]);
          }
          return Result.ok(scanRows(ordersRows, fragment.request));
        },
      } satisfies Omit<ProviderAdapter, "name">,
      users_provider: {
        canExecute(fragment: ProviderFragment) {
          return fragment.kind === "scan";
        },
        async compile(fragment: ProviderFragment) {
          return Result.ok({
            provider: "users_provider",
            kind: fragment.kind,
            payload: fragment,
          });
        },
        async execute(plan) {
          const fragment = plan.payload as ProviderFragment;
          if (fragment.kind !== "scan") {
            return Result.ok([]);
          }
          return Result.ok(scanRows(usersRows, fragment.request));
        },
        async lookupMany(request) {
          lookupCalls += 1;
          const keys = new Set(request.keys);
          return Result.ok(
            usersRows.filter((row) => keys.has(row[request.key])).map((row) => {
              const out: QueryRow = {};
              for (const column of request.select) {
                out[column] = row[column] ?? null;
              }
              return out;
            }),
          );
        },
      } satisfies Omit<ProviderAdapter, "name">,
    });

    const rows = await executableSchema.query({
      context: {},
      sql: `
        SELECT o.id, u.email
        FROM orders o
        JOIN users u ON o.user_id = u.id
        ORDER BY o.id ASC
      `,
    });

    expect(rows).toEqual([
      { id: "o1", email: "ada@example.com" },
      { id: "o2", email: "ben@example.com" },
    ]);
    expect(lookupCalls).toBeGreaterThan(0);
  });

  it("enforces lookup batching guardrails", async () => {
    const schema = defineSchema({
      tables: {
        orders: {
          provider: "orders_provider",
          columns: {
            id: "text",
            user_id: "text",
          },
        },
        users: {
          provider: "users_provider",
          columns: {
            id: "text",
            email: "text",
          },
        },
      },
    });

    const ordersRows: QueryRow[] = [
      { id: "o1", user_id: "u1" },
      { id: "o2", user_id: "u2" },
      { id: "o3", user_id: "u3" },
    ];

    const usersRows: QueryRow[] = [
      { id: "u1", email: "a@example.com" },
      { id: "u2", email: "b@example.com" },
      { id: "u3", email: "c@example.com" },
    ];

    const executableSchema = createExecutableSchemaFromProviders(schema, {
      orders_provider: {
        canExecute() {
          return true;
        },
        async compile(fragment: ProviderFragment) {
          return Result.ok({
            provider: "orders_provider",
            kind: fragment.kind,
            payload: fragment,
          });
        },
        async execute(plan) {
          const fragment = plan.payload as ProviderFragment;
          if (fragment.kind !== "scan") {
            return Result.ok([]);
          }
          return Result.ok(scanRows(ordersRows, fragment.request));
        },
      } satisfies Omit<ProviderAdapter, "name">,
      users_provider: {
        canExecute() {
          return true;
        },
        async compile(fragment: ProviderFragment) {
          return Result.ok({
            provider: "users_provider",
            kind: fragment.kind,
            payload: fragment,
          });
        },
        async execute(plan) {
          const fragment = plan.payload as ProviderFragment;
          if (fragment.kind !== "scan") {
            return Result.ok([]);
          }
          return Result.ok(scanRows(usersRows, fragment.request));
        },
        async lookupMany(request) {
          const keys = new Set(request.keys);
          return Result.ok(usersRows.filter((row) => keys.has(row.id)));
        },
      } satisfies Omit<ProviderAdapter, "name">,
    });

    await expect(
      executableSchema.query({
        context: {},
        sql: `
          SELECT o.id, u.email
          FROM orders o
          JOIN users u ON o.user_id = u.id
          ORDER BY o.id ASC
        `,
        queryGuardrails: {
          maxLookupKeysPerBatch: 1,
          maxLookupBatches: 1,
        },
      }),
    ).rejects.toThrow("maxLookupBatches guardrail");
  });

  it("preserves LEFT JOIN null semantics on lookup joins", async () => {
    const schema = defineSchema({
      tables: {
        orders: {
          provider: "orders_provider",
          columns: {
            id: "text",
            user_id: "text",
          },
        },
        users: {
          provider: "users_provider",
          columns: {
            id: "text",
            email: "text",
          },
        },
      },
    });

    const ordersRows: QueryRow[] = [
      { id: "o1", user_id: "u1" },
      { id: "o2", user_id: "u_missing" },
    ];
    const usersRows: QueryRow[] = [{ id: "u1", email: "ada@example.com" }];

    const executableSchema = createExecutableSchemaFromProviders(schema, {
      orders_provider: {
        canExecute(fragment: ProviderFragment) {
          return fragment.kind === "scan";
        },
        async compile(fragment: ProviderFragment) {
          return Result.ok({
            provider: "orders_provider",
            kind: fragment.kind,
            payload: fragment,
          });
        },
        async execute(plan) {
          const fragment = plan.payload as ProviderFragment;
          if (fragment.kind !== "scan") {
            return Result.ok([]);
          }
          return Result.ok(scanRows(ordersRows, fragment.request));
        },
      } satisfies Omit<ProviderAdapter, "name">,
      users_provider: {
        canExecute(fragment: ProviderFragment) {
          return fragment.kind === "scan";
        },
        async compile(fragment: ProviderFragment) {
          return Result.ok({
            provider: "users_provider",
            kind: fragment.kind,
            payload: fragment,
          });
        },
        async execute(plan) {
          const fragment = plan.payload as ProviderFragment;
          if (fragment.kind !== "scan") {
            return Result.ok([]);
          }
          return Result.ok(scanRows(usersRows, fragment.request));
        },
        async lookupMany(request) {
          const keys = new Set(request.keys);
          return Result.ok(
            usersRows.filter((row) => keys.has(row[request.key])).map((row) => {
              const out: QueryRow = {};
              for (const column of request.select) {
                out[column] = row[column] ?? null;
              }
              return out;
            }),
          );
        },
      } satisfies Omit<ProviderAdapter, "name">,
    });

    const rows = await executableSchema.query({
      context: {},
      sql: `
        SELECT o.id, u.email
        FROM orders o
        LEFT JOIN users u ON o.user_id = u.id
        ORDER BY o.id ASC
      `,
    });

    expect(rows).toEqual([
      { id: "o1", email: "ada@example.com" },
      { id: "o2", email: null },
    ]);
  });

  it("executes synthetic view tables defined with rel DSL", async () => {
    const ordersEntity = createDataEntityHandle({
      entity: "orders_raw",
      provider: "warehouse",
    });

    const schema = defineSchema(({ table, view, rel, col, agg }) => ({
      tables: {
        my_orders: table({
          from: ordersEntity,
          columns: {
            id: { source: "id", type: "text", nullable: false },
            total_cents: { source: "total_cents", type: "integer", nullable: false },
          },
        }),
        order_spend: view(
          () =>
            rel.aggregate({
              from: rel.scan("my_orders"),
              groupBy: {
                order_id: col("my_orders.id"),
              },
              measures: {
                spend: agg.sum(col("my_orders.total_cents")),
              },
            }),
          {
            columns: ({ col }) => ({
              order_id: col.string("order_id", { nullable: false }),
              spend: col.integer("spend"),
            }),
          },
        ),
      },
    }));

    const executableSchema = createExecutableSchemaFromProviders(schema, {
      warehouse: {
        canExecute(fragment: ProviderFragment) {
          return fragment.kind === "scan";
        },
        async compile(fragment: ProviderFragment) {
          return Result.ok({
            provider: "warehouse",
            kind: fragment.kind,
            payload: fragment,
          });
        },
        async execute(plan) {
          const fragment = plan.payload as ProviderFragment;
          if (fragment.kind !== "scan") {
            return Result.ok([]);
          }

          return Result.ok(
            scanRows(
              [
                { id: "o1", total_cents: 1500 },
                { id: "o1", total_cents: 500 },
                { id: "o2", total_cents: 700 },
              ],
              fragment.request,
            ),
          );
        },
      } satisfies Omit<ProviderAdapter, "name">,
    });

    const rows = await executableSchema.query({
      context: {},
      sql: `
        SELECT order_id, spend
        FROM order_spend
        ORDER BY order_id ASC
      `,
    });

    expect(rows).toEqual([
      { order_id: "o1", spend: 2000 },
      { order_id: "o2", spend: 700 },
    ]);
  });

  it("executes calculated columns on physical tables with select, filter, and order by", async () => {
    const ordersEntity = createDataEntityHandle({
      entity: "orders_raw",
      provider: "warehouse",
      columns: {
        id: { source: "id", type: "text", nullable: false, primaryKey: true },
        totalCents: { source: "total_cents", type: "integer", nullable: false },
      },
    });

    const schema = defineSchema(({ table }) => ({
      tables: {
        myOrders: table(ordersEntity, {
          columns: ({ col, expr }) => ({
            id: col.id("id"),
            totalCents: col.integer("totalCents"),
            totalDollars: col.real(
              expr.divide(col("totalCents"), expr.literal(100)),
              { nullable: false },
            ),
            isLargeOrder: col.boolean(
              expr.gte(col("totalCents"), expr.literal(2000)),
              { nullable: false },
            ),
          }),
        }),
      },
    }));

    expect(getNormalizedTableBinding(schema, "myOrders")).toMatchObject({
      kind: "physical",
      columnBindings: {
        totalDollars: { kind: "expr" },
      },
    });

    const executableSchema = createExecutableSchemaFromProviders(schema, {
      warehouse: {
        canExecute(fragment: ProviderFragment) {
          return fragment.kind === "scan";
        },
        async compile(fragment: ProviderFragment) {
          return Result.ok({
            provider: "warehouse",
            kind: fragment.kind,
            payload: fragment,
          });
        },
        async execute(plan) {
          const fragment = plan.payload as ProviderFragment;
          if (fragment.kind !== "scan") {
            return Result.ok([]);
          }

          return Result.ok(scanRows([
            { id: "o1", total_cents: 1200 },
            { id: "o2", total_cents: 3200 },
            { id: "o3", total_cents: 2100 },
          ], fragment.request));
        },
      } satisfies Omit<ProviderAdapter, "name">,
    });

    const rows = await executableSchema.query({
      context: {},
      sql: `
        SELECT id, totalDollars, isLargeOrder
        FROM myOrders
        WHERE totalDollars >= 12
        ORDER BY totalDollars DESC
      `,
    });

    expect(rows).toEqual([
      { id: "o2", totalDollars: 32, isLargeOrder: true },
      { id: "o3", totalDollars: 21, isLargeOrder: true },
      { id: "o1", totalDollars: 12, isLargeOrder: false },
    ]);
  });

  it("coerces pushed-down calculated table outputs back to declared logical types", async () => {
    const ordersEntity = createDataEntityHandle({
      entity: "orders_raw",
      provider: "warehouse",
      columns: {
        id: { source: "id", type: "text", nullable: false, primaryKey: true },
        totalCents: { source: "total_cents", type: "integer", nullable: false },
      },
    });

    const schema = defineSchema(({ table }) => ({
      tables: {
        myOrders: table(ordersEntity, {
          columns: ({ col, expr }) => ({
            id: col.id("id"),
            totalCents: col.integer("totalCents"),
            totalDollars: col.real(
              expr.divide(col("totalCents"), expr.literal(100)),
              { nullable: false },
            ),
            isLargeOrder: col.boolean(
              expr.gte(col("totalCents"), expr.literal(20000)),
              { nullable: false },
            ),
          }),
        }),
      },
    }));

    const executableSchema = createExecutableSchemaFromProviders(schema, {
      warehouse: {
        canExecute(fragment: ProviderFragment) {
          return fragment.kind === "rel";
        },
        async compile(fragment: ProviderFragment) {
          return Result.ok({
            provider: "warehouse",
            kind: fragment.kind,
            payload: fragment,
          });
        },
        async execute() {
          return Result.ok([
            {
              id: "o1",
              totalDollars: "250",
              isLargeOrder: false,
            },
          ]);
        },
      } satisfies Omit<ProviderAdapter, "name">,
    });

    const rows = await executableSchema.query({
      context: {},
      sql: `
        SELECT id, totalDollars, isLargeOrder
        FROM myOrders
        WHERE totalDollars >= 200
        ORDER BY totalDollars DESC, id
      `,
    });

    expect(rows).toEqual([
      {
        id: "o1",
        totalDollars: 250,
        isLargeOrder: false,
      },
    ]);
    expect(typeof rows[0]?.totalDollars).toBe("number");
  });

  it("coerces pushed-down aggregate and window outputs back to numeric query types", async () => {
    const schema = defineSchema({
      tables: {
        my_orders: {
          provider: "warehouse",
          columns: {
            id: "text",
            vendor_id: "text",
            total_cents: { type: "integer", nullable: false },
          },
        },
        vendors_for_org: {
          provider: "warehouse",
          columns: {
            id: "text",
            name: "text",
          },
        },
      },
    });

    const executableSchema = createExecutableSchemaFromProviders(schema, {
      warehouse: {
        canExecute(fragment: ProviderFragment) {
          return fragment.kind === "rel";
        },
        async compile(fragment: ProviderFragment) {
          return Result.ok({
            provider: "warehouse",
            kind: fragment.kind,
            payload: fragment,
          });
        },
        async execute() {
          return Result.ok([
            {
              vendor_name: "Acme",
              spend_cents: "25000",
              spend_rank: "1",
            },
          ]);
        },
      } satisfies Omit<ProviderAdapter, "name">,
    });

    const rows = await executableSchema.query({
      context: {},
      sql: `
        WITH vendor_totals AS (
          SELECT
            v.name AS vendor_name,
            SUM(o.total_cents) AS spend_cents
          FROM my_orders o
          JOIN vendors_for_org v ON o.vendor_id = v.id
          GROUP BY v.name
        )
        SELECT
          vendor_name,
          spend_cents,
          DENSE_RANK() OVER (ORDER BY spend_cents DESC) AS spend_rank
        FROM vendor_totals
        ORDER BY spend_rank, vendor_name
      `,
    });

    expect(rows).toEqual([
      {
        vendor_name: "Acme",
        spend_cents: 25000,
        spend_rank: 1,
      },
    ]);
    expect(typeof rows[0]?.spend_cents).toBe("number");
    expect(typeof rows[0]?.spend_rank).toBe("number");
  });

  it("executes views that scan data entities directly without intermediate facade tables", async () => {
    const warehouseProvider = {
      canExecute(fragment: ProviderFragment) {
        return fragment.kind === "scan";
      },
      async compile(fragment: ProviderFragment) {
        return Result.ok({
          provider: "warehouse",
          kind: fragment.kind,
          payload: fragment,
        });
      },
      async execute(plan) {
        const fragment = plan.payload as ProviderFragment;
        if (fragment.kind !== "scan") {
          return Result.ok([]);
        }

        if (fragment.request.table === "orders_raw") {
          return Result.ok(scanRows([
            { id: "o1", vendor_id: "v1", total_cents: 1200 },
            { id: "o2", vendor_id: "v1", total_cents: 800 },
            { id: "o3", vendor_id: "v2", total_cents: 500 },
          ], fragment.request));
        }

        if (fragment.request.table === "vendors_raw") {
          return Result.ok(scanRows([
            { id: "v1", name: "Acme" },
            { id: "v2", name: "Bolt" },
          ], fragment.request));
        }

        return Result.ok([]);
      },
    } satisfies Omit<ProviderAdapter, "name">;

    const ordersEntity = createDataEntityHandle({
      entity: "orders_raw",
      provider: "warehouse",
      adapter: warehouseProvider as unknown as ProviderAdapter,
      columns: {
        id: { source: "id", type: "text", nullable: false },
        vendorId: { source: "vendor_id", type: "text", nullable: false },
        totalCents: { source: "total_cents", type: "integer", nullable: false },
      },
    });
    const vendorsEntity = createDataEntityHandle({
      entity: "vendors_raw",
      provider: "warehouse",
      adapter: warehouseProvider as unknown as ProviderAdapter,
      columns: {
        id: { source: "id", type: "text", nullable: false },
        name: { source: "name", type: "text", nullable: false },
      },
    });

    const schema = defineSchema(({ view }) => ({
      tables: {
        spendByVendor: view(
          ({ scan, join, aggregate, col, expr, agg }) =>
            aggregate({
              from: join({
                left: scan(ordersEntity),
                right: scan(vendorsEntity),
                on: expr.eq(col(ordersEntity, "vendorId"), col(vendorsEntity, "id")),
                type: "inner",
              }),
              groupBy: {
                vendorName: col(vendorsEntity, "name"),
              },
              measures: {
                spendCents: agg.sum(col(ordersEntity, "totalCents")),
              },
            }),
          {
            columns: ({ col }) => ({
              vendorName: col.string("vendorName", { nullable: false }),
              spendCents: col.integer("spendCents"),
            }),
          },
        ),
      },
    }));

    const executableSchema = createExecutableSchemaFromProviders(schema, {
      warehouse: warehouseProvider,
    });

    const rows = await executableSchema.query({
      context: {},
      sql: `
        SELECT vendorName, spendCents
        FROM spendByVendor
        ORDER BY spendCents DESC, vendorName
      `,
    });

    expect(rows).toEqual([
      { vendorName: "Acme", spendCents: 2000 },
      { vendorName: "Bolt", spendCents: 500 },
    ]);
  });

  it("executes composed views that scan other views", async () => {
    const orderItemsEntity = createDataEntityHandle({
      entity: "order_items_raw",
      provider: "warehouse",
      columns: {
        orderId: { source: "order_id", type: "text", nullable: false },
        productId: { source: "product_id", type: "text", nullable: false },
        quantity: { source: "quantity", type: "integer", nullable: false },
        lineTotalCents: { source: "line_total_cents", type: "integer", nullable: false },
      },
    });
    const productsEntity = createDataEntityHandle({
      entity: "products_raw",
      provider: "warehouse",
      columns: {
        id: { source: "id", type: "text", nullable: false, primaryKey: true },
        name: { source: "name", type: "text", nullable: false },
      },
    });
    const accessEntity = createDataEntityHandle({
      entity: "product_access_raw",
      provider: "warehouse",
      columns: {
        productId: { source: "product_id", type: "text", nullable: false },
      },
    });

    const schema = defineSchema(({ table, view }) => {
      const myOrderItems = table(orderItemsEntity, {
        columns: ({ col, expr }) => ({
          orderId: col.string("orderId"),
          productId: col.string("productId"),
          quantity: col.integer("quantity"),
          lineTotalCents: col.integer("lineTotalCents"),
          unitPriceCents: col.real(
            expr.divide(col("lineTotalCents"), col("quantity")),
            { nullable: false },
          ),
        }),
      });

      const productsForOrg = table(productsEntity, {
        columns: ({ col }) => ({
          id: col.id("id"),
          name: col.string("name"),
        }),
      });

      const productAccess = table(accessEntity, {
        columns: ({ col }) => ({
          productId: col.string("productId"),
        }),
      });

      const activeProducts = view(
        ({ scan, join, col, expr }) =>
          join({
            left: scan(productsForOrg),
            right: scan(productAccess),
            on: expr.eq(col(productsForOrg, "id"), col(productAccess, "productId")),
            type: "inner",
          }),
        {
          columns: ({ col }) => ({
            id: col.id(productsForOrg, "id"),
            name: col.string(productsForOrg, "name", { nullable: false }),
          }),
        },
      );

      const myOrderLines = view(
        ({ scan, join, col, expr }) =>
          join({
            left: scan(myOrderItems),
            right: scan(activeProducts),
            on: expr.eq(col(myOrderItems, "productId"), col(activeProducts, "id")),
            type: "inner",
          }),
        {
          columns: ({ col }) => ({
            orderId: col.string(myOrderItems, "orderId", { nullable: false }),
            productId: col.string(activeProducts, "id", { nullable: false }),
            productName: col.string(activeProducts, "name", { nullable: false }),
            unitPriceCents: col.real(myOrderItems, "unitPriceCents", { nullable: false }),
          }),
        },
      );

      return {
        tables: {
          myOrderItems,
          productsForOrg,
          productAccess,
          activeProducts,
          myOrderLines,
        },
      };
    });

    const executableSchema = createExecutableSchemaFromProviders(schema, {
      warehouse: {
        canExecute(fragment: ProviderFragment) {
          return fragment.kind === "scan";
        },
        async compile(fragment: ProviderFragment) {
          return Result.ok({
            provider: "warehouse",
            kind: fragment.kind,
            payload: fragment,
          });
        },
        async execute(plan) {
          const fragment = plan.payload as ProviderFragment;
          if (fragment.kind !== "scan") {
            return Result.ok([]);
          }

          const rowsByTable: Record<string, QueryRow[]> = {
            order_items_raw: [
              { order_id: "o1", product_id: "p1", quantity: 2, line_total_cents: 3600 },
              { order_id: "o1", product_id: "p2", quantity: 1, line_total_cents: 1200 },
            ],
            products_raw: [
              { id: "p1", name: "Edge Router" },
              { id: "p2", name: "Backup Service" },
            ],
            product_access_raw: [
              { product_id: "p1" },
              { product_id: "p2" },
            ],
          };

          return Result.ok(scanRows(rowsByTable[fragment.table] ?? [], fragment.request));
        },
      } satisfies Omit<ProviderAdapter, "name">,
    });

    const rows = await executableSchema.query({
      context: {},
      sql: `
        SELECT orderId, productName, unitPriceCents
        FROM myOrderLines
        ORDER BY orderId ASC, productName ASC
      `,
    });

    expect(rows).toEqual([
      { orderId: "o1", productName: "Backup Service", unitPriceCents: 1200 },
      { orderId: "o1", productName: "Edge Router", unitPriceCents: 1800 },
    ]);
  });

  it("executes aggregate views built from derived views", async () => {
    const orderItemsEntity = createDataEntityHandle({
      entity: "order_items_raw",
      provider: "warehouse",
      columns: {
        orderId: { source: "order_id", type: "text", nullable: false },
        productId: { source: "product_id", type: "text", nullable: false },
        quantity: { source: "quantity", type: "integer", nullable: false },
        lineTotalCents: { source: "line_total_cents", type: "integer", nullable: false },
      },
    });
    const productsEntity = createDataEntityHandle({
      entity: "products_raw",
      provider: "warehouse",
      columns: {
        id: { source: "id", type: "text", nullable: false, primaryKey: true },
        name: { source: "name", type: "text", nullable: false },
      },
    });
    const accessEntity = createDataEntityHandle({
      entity: "product_access_raw",
      provider: "warehouse",
      columns: {
        productId: { source: "product_id", type: "text", nullable: false },
      },
    });

    const schema = defineSchema(({ table, view }) => {
      const myOrderItems = table(orderItemsEntity, {
        columns: ({ col }) => ({
          orderId: col.string("orderId"),
          productId: col.string("productId"),
          quantity: col.integer("quantity"),
          lineTotalCents: col.integer("lineTotalCents"),
        }),
      });

      const productsForOrg = table(productsEntity, {
        columns: ({ col }) => ({
          id: col.id("id"),
          name: col.string("name"),
        }),
      });

      const productAccess = table(accessEntity, {
        columns: ({ col }) => ({
          productId: col.string("productId"),
        }),
      });

      const activeProducts = view(
        ({ scan, join, col, expr }) =>
          join({
            left: scan(productsForOrg),
            right: scan(productAccess),
            on: expr.eq(col(productsForOrg, "id"), col(productAccess, "productId")),
            type: "inner",
          }),
        {
          columns: ({ col }) => ({
            id: col.id(productsForOrg, "id"),
            name: col.string(productsForOrg, "name", { nullable: false }),
          }),
        },
      );

      const myOrderLines = view(
        ({ scan, join, col, expr }) =>
          join({
            left: scan(myOrderItems),
            right: scan(activeProducts),
            on: expr.eq(col(myOrderItems, "productId"), col(activeProducts, "id")),
            type: "inner",
          }),
        {
          columns: ({ col }) => ({
            productId: col.string(activeProducts, "id", { nullable: false }),
            productName: col.string(activeProducts, "name", { nullable: false }),
            quantity: col.integer(myOrderItems, "quantity", { nullable: false }),
            lineTotalCents: col.integer(myOrderItems, "lineTotalCents", { nullable: false }),
          }),
        },
      );

      const productPerformance = view({
        rel: ({ scan, aggregate, col, agg }) =>
          aggregate({
            from: scan(myOrderLines),
            groupBy: {
              productId: col(myOrderLines, "productId"),
              productName: col(myOrderLines, "productName"),
            },
            measures: {
              unitsSold: agg.sum(col(myOrderLines, "quantity")),
              revenueCents: agg.sum(col(myOrderLines, "lineTotalCents")),
            },
          }),
        columns: ({ col }) => ({
          productId: col.id("productId"),
          productName: col.string("productName"),
          unitsSold: col.integer("unitsSold"),
          revenueCents: col.integer("revenueCents"),
        }),
      });

      return {
        tables: {
          myOrderItems,
          productsForOrg,
          productAccess,
          activeProducts,
          myOrderLines,
          productPerformance,
        },
      };
    });

    const executableSchema = createExecutableSchemaFromProviders(schema, {
      warehouse: {
        canExecute(fragment: ProviderFragment) {
          return fragment.kind === "scan";
        },
        async compile(fragment: ProviderFragment) {
          return Result.ok({
            provider: "warehouse",
            kind: fragment.kind,
            payload: fragment,
          });
        },
        async execute(plan) {
          const fragment = plan.payload as ProviderFragment;
          if (fragment.kind !== "scan") {
            return Result.ok([]);
          }

          const rowsByTable: Record<string, QueryRow[]> = {
            order_items_raw: [
              { order_id: "o1", product_id: "p1", quantity: 2, line_total_cents: 3600 },
              { order_id: "o2", product_id: "p1", quantity: 1, line_total_cents: 1800 },
              { order_id: "o3", product_id: "p2", quantity: 1, line_total_cents: 1200 },
            ],
            products_raw: [
              { id: "p1", name: "Edge Router" },
              { id: "p2", name: "Backup Service" },
            ],
            product_access_raw: [
              { product_id: "p1" },
              { product_id: "p2" },
            ],
          };

          return Result.ok(scanRows(rowsByTable[fragment.table] ?? [], fragment.request));
        },
      } satisfies Omit<ProviderAdapter, "name">,
    });

    const rows = await executableSchema.query({
      context: {},
      sql: `
        SELECT productName, unitsSold, revenueCents
        FROM productPerformance
        ORDER BY revenueCents DESC
      `,
    });

    expect(rows).toEqual([
      { productName: "Edge Router", unitsSold: 3, revenueCents: 5400 },
      { productName: "Backup Service", unitsSold: 1, revenueCents: 1200 },
    ]);
  });

  it("executes local cross-provider views", async () => {
    const productsEntity = createDataEntityHandle({
      entity: "products_raw",
      provider: "warehouse",
      columns: {
        id: { source: "id", type: "text", nullable: false, primaryKey: true },
        name: { source: "name", type: "text", nullable: false },
      },
    });
    const viewCountsEntity = createDataEntityHandle({
      entity: "product_view_counts",
      provider: "kv",
      columns: {
        productId: { source: "product_id", type: "text", nullable: false },
        viewCount: { source: "view_count", type: "integer", nullable: false },
      },
    });

    const schema = defineSchema(({ table, view }) => {
      const products = table(productsEntity, {
        columns: ({ col }) => ({
          id: col.id("id"),
          productName: col.string("name"),
        }),
      });

      const productViewCounts = table(viewCountsEntity, {
        columns: ({ col }) => ({
          productId: col.string("productId"),
          viewCount: col.integer("viewCount"),
        }),
      });

      const productEngagement = view({
        rel: ({ scan, join, col, expr }) =>
          join({
            left: scan(products),
            right: scan(productViewCounts),
            on: expr.eq(col(products, "id"), col(productViewCounts, "productId")),
            type: "left",
          }),
        columns: {
          productName: { source: "products.productName", type: "text", nullable: false },
          viewCount: { source: "productViewCounts.viewCount", type: "integer", nullable: true },
        },
      });

      return {
        tables: {
          products,
          productViewCounts,
          productEngagement,
        },
      };
    });

    const executableSchema = createExecutableSchemaFromProviders(schema, {
      warehouse: {
        canExecute(fragment: ProviderFragment) {
          return fragment.kind === "scan";
        },
        async compile(fragment: ProviderFragment) {
          return Result.ok({
            provider: "warehouse",
            kind: fragment.kind,
            payload: fragment,
          });
        },
        async execute(plan) {
          const fragment = plan.payload as ProviderFragment;
          if (fragment.kind !== "scan") {
            return Result.ok([]);
          }

          return Result.ok(scanRows([
            { id: "p1", name: "Edge Router" },
            { id: "p2", name: "Backup Service" },
          ], fragment.request));
        },
      } satisfies Omit<ProviderAdapter, "name">,
      kv: {
        canExecute(fragment: ProviderFragment) {
          return fragment.kind === "scan";
        },
        async compile(fragment: ProviderFragment) {
          return Result.ok({
            provider: "kv",
            kind: fragment.kind,
            payload: fragment,
          });
        },
        async execute(plan) {
          const fragment = plan.payload as ProviderFragment;
          if (fragment.kind !== "scan") {
            return Result.ok([]);
          }

          return Result.ok(scanRows([
            { product_id: "p1", view_count: 12 },
          ], fragment.request));
        },
      } satisfies Omit<ProviderAdapter, "name">,
    });

    const rows = await executableSchema.query({
      context: {},
      sql: `
        SELECT productName, viewCount
        FROM productEngagement
        ORDER BY productName ASC
      `,
    });

    expect(rows).toEqual([
      { productName: "Backup Service", viewCount: null },
      { productName: "Edge Router", viewCount: 12 },
    ]);
  });

  it("exposes scan stages in session plans", () => {
    const schema = defineSchema({
      tables: {
        orders: {
          provider: "warehouse",
          columns: {
            id: "text",
          },
        },
      },
    });

    const executableSchema = createExecutableSchemaFromProviders(schema, {
      warehouse: {
        canExecute(fragment: ProviderFragment) {
          return fragment.kind === "scan";
        },
        async compile(fragment: ProviderFragment) {
          return Result.ok({
            provider: "warehouse",
            kind: fragment.kind,
            payload: fragment,
          });
        },
        async execute(plan) {
          const fragment = plan.payload as ProviderFragment;
          if (fragment.kind !== "scan") {
            return Result.ok([]);
          }
          return Result.ok(scanRows([{ id: "o1" }], fragment.request));
        },
      } satisfies Omit<ProviderAdapter, "name">,
    });

    const session = executableSchema.createSession({
      context: {},
      sql: "SELECT id FROM orders",
    });

    const plan = session.getPlan();
    expect(plan.steps.length).toBeGreaterThan(0);
    expect(plan.steps.some((step) => step.kind === "scan")).toBe(true);
  });

  it("includes fallback diagnostics in explain output for unsupported rel pushdown", () => {
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

    const executableSchema = createExecutableSchemaFromProviders(schema, {
      warehouse: {
        canExecute(fragment: ProviderFragment) {
          return fragment.kind === "scan";
        },
        async compile(fragment: ProviderFragment) {
          return Result.ok({
            provider: "warehouse",
            kind: fragment.kind,
            payload: fragment,
          });
        },
        async execute(plan) {
          const fragment = plan.payload as ProviderFragment;
          if (fragment.kind !== "scan") {
            return Result.ok([]);
          }
          return Result.ok(
            scanRows(
              fragment.table === "orders"
                ? [{ id: "o1", user_id: "u1" }]
                : [{ id: "u1", email: "a@example.com" }],
              fragment.request,
            ),
          );
        },
      } satisfies Omit<ProviderAdapter, "name">,
    });

    const explained = executableSchema.explain({
      context: {},
      sql: `
        SELECT o.id, u.email
        FROM orders o
        JOIN users u ON o.user_id = u.id
      `,
    });

    expect(explained.diagnostics).toEqual(expect.arrayContaining([
      expect.objectContaining({
        code: "SQLQL_WARN_FALLBACK",
        severity: "warning",
      }),
    ]));
  });

  it("rejects fallback when query policy forbids missing capability atoms", async () => {
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

    const executableSchema = createExecutableSchemaFromProviders(schema, {
      warehouse: {
        canExecute(fragment: ProviderFragment) {
          return fragment.kind === "scan";
        },
        async compile(fragment: ProviderFragment) {
          return Result.ok({
            provider: "warehouse",
            kind: fragment.kind,
            payload: fragment,
          });
        },
        async execute(plan) {
          const fragment = plan.payload as ProviderFragment;
          if (fragment.kind !== "scan") {
            return Result.ok([]);
          }
          return Result.ok(scanRows([], fragment.request));
        },
      } satisfies Omit<ProviderAdapter, "name">,
    });

    await expect(executableSchema.query({
      context: {},
      fallbackPolicy: {
        rejectOnMissingAtom: true,
      },
      sql: `
        SELECT o.id, u.email
        FROM orders o
        JOIN users u ON o.user_id = u.id
      `,
    })).rejects.toMatchObject({
      _tag: "SqlqlDiagnosticError",
      diagnostics: expect.any(Array),
      name: "SqlqlDiagnosticError",
    });
  });

  it("surfaces tagged timeout errors through the Promise query API", async () => {
    const schema = defineSchema({
      tables: {
        users: {
          provider: "warehouse",
          columns: {
            id: "text",
          },
        },
      },
    });

    const executableSchema = createExecutableSchemaFromProviders(schema, {
      warehouse: {
        canExecute(fragment: ProviderFragment) {
          return fragment.kind === "scan";
        },
        async compile(fragment: ProviderFragment) {
          return Result.ok({
            provider: "warehouse",
            kind: fragment.kind,
            payload: fragment,
          });
        },
        async execute(plan) {
          const fragment = plan.payload as ProviderFragment;
          if (fragment.kind !== "scan") {
            return Result.ok([]);
          }
          await sleep(25);
          return Result.ok(scanRows([{ id: "u1" }], fragment.request));
        },
      } satisfies Omit<ProviderAdapter, "name">,
    });

    await expect(executableSchema.query({
      context: {},
      queryGuardrails: {
        timeoutMs: 5,
      },
      sql: "SELECT id FROM users",
    })).rejects.toMatchObject({
      _tag: "SqlqlTimeoutError",
      name: "SqlqlTimeoutError",
      message: "Query timed out after 5ms.",
    });
  });

  it("returns tagged provider binding errors from the result API", () => {
    const schema = defineSchema({
      tables: {
        users: {
          provider: "warehouse",
          columns: {
            id: "text",
          },
        },
      },
    });

    const result = validateProviderBindingsResult(schema, {});
    expect(Result.isError(result)).toBe(true);
    if (Result.isOk(result)) {
      throw new Error("Expected provider binding validation to fail.");
    }

    expect(result.error).toMatchObject({
      _tag: "SqlqlProviderBindingError",
      name: "SqlqlProviderBindingError",
      message: "Table users is bound to provider warehouse, but no such provider is registered.",
      provider: "warehouse",
      table: "users",
    });
  });

  it("executes scalar expressions and missing operators locally when scan pushdown is the only provider capability", async () => {
    const schema = defineSchema({
      tables: {
        users: {
          provider: "warehouse",
          columns: {
            id: "text",
            email: "text",
            score: { type: "integer" },
          },
        },
      },
    });

    const executableSchema = createExecutableSchemaFromProviders(schema, {
      warehouse: {
        canExecute(fragment: ProviderFragment) {
          return fragment.kind === "scan";
        },
        async compile(fragment: ProviderFragment) {
          return Result.ok({
            provider: "warehouse",
            kind: fragment.kind,
            payload: fragment,
          });
        },
        async execute(plan) {
          const fragment = plan.payload as ProviderFragment;
          if (fragment.kind !== "scan") {
            return Result.ok([]);
          }
          return Result.ok(
            scanRows(
              [
                { id: "u1", email: "Alpha@Example.com", score: 10 },
                { id: "u2", email: "beta@sample.com", score: 7 },
                { id: "u3", email: "Gamma@Example.com", score: 4 },
              ],
              fragment.request,
            ),
          );
        },
      } satisfies Omit<ProviderAdapter, "name">,
    });

    const rows = await executableSchema.query({
      context: {},
      sql: `
        SELECT
          id,
          LOWER(email) AS email_lower,
          score + 2 AS bumped_score,
          CASE
            WHEN score >= 8 THEN 'high'
            ELSE 'low'
          END AS bucket
        FROM users
        WHERE email LIKE '%@Example.com' AND id NOT IN ('u3')
        ORDER BY id ASC
      `,
    });

    expect(rows).toEqual([
      {
        id: "u1",
        email_lower: "alpha@example.com",
        bumped_score: 12,
        bucket: "high",
      },
    ]);
  });
});
