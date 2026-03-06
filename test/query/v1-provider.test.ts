import { describe, expect, it } from "vitest";

import {
  createDataEntityHandle,
  defineSchema,
  type ProviderAdapter,
  type ProviderFragment,
  type QueryRow,
  type ScanFilterClause,
  type TableScanRequest,
} from "../../src";
import { createExecutableSchemaFromProviders } from "../support/executable-schema";

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

describe("query/v1 provider runtime", () => {
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
          return {
            provider: "warehouse",
            kind: fragment.kind,
            payload: fragment,
          };
        },
        async execute() {
          executeCalls += 1;
          return [{ id: "o2" }];
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
          return {
            provider: "warehouse",
            kind: fragment.kind,
            payload: fragment,
          };
        },
        async execute() {
          return [{ id: "o1" }];
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

          return {
            provider: "warehouse",
            kind: fragment.kind,
            payload: fragment,
          };
        },
        async execute() {
          return [{ id: "o1" }];
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
          return {
            provider: "warehouse",
            kind: fragment.kind,
            payload: fragment,
          };
        },
        async execute() {
          return [
            {
              id: "o1",
              total_cents: 1500,
              created_at: new Date("2026-02-03T10:00:00.000Z"),
            },
          ];
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
          return {
            provider: "orders_provider",
            kind: fragment.kind,
            payload: fragment,
          };
        },
        async execute(plan) {
          const fragment = plan.payload as ProviderFragment;
          if (fragment.kind !== "scan") {
            return [];
          }
          return scanRows(ordersRows, fragment.request);
        },
      } satisfies Omit<ProviderAdapter, "name">,
      users_provider: {
        canExecute(fragment: ProviderFragment) {
          return fragment.kind === "scan";
        },
        async compile(fragment: ProviderFragment) {
          return {
            provider: "users_provider",
            kind: fragment.kind,
            payload: fragment,
          };
        },
        async execute(plan) {
          const fragment = plan.payload as ProviderFragment;
          if (fragment.kind !== "scan") {
            return [];
          }
          return scanRows(usersRows, fragment.request);
        },
        async lookupMany(request) {
          lookupCalls += 1;
          const keys = new Set(request.keys);
          return usersRows
            .filter((row) => keys.has(row[request.key]))
            .map((row) => {
              const out: QueryRow = {};
              for (const column of request.select) {
                out[column] = row[column] ?? null;
              }
              return out;
            });
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
          return {
            provider: "orders_provider",
            kind: fragment.kind,
            payload: fragment,
          };
        },
        async execute(plan) {
          const fragment = plan.payload as ProviderFragment;
          if (fragment.kind !== "scan") {
            return [];
          }
          return scanRows(ordersRows, fragment.request);
        },
      } satisfies Omit<ProviderAdapter, "name">,
      users_provider: {
        canExecute() {
          return true;
        },
        async compile(fragment: ProviderFragment) {
          return {
            provider: "users_provider",
            kind: fragment.kind,
            payload: fragment,
          };
        },
        async execute(plan) {
          const fragment = plan.payload as ProviderFragment;
          if (fragment.kind !== "scan") {
            return [];
          }
          return scanRows(usersRows, fragment.request);
        },
        async lookupMany(request) {
          const keys = new Set(request.keys);
          return usersRows.filter((row) => keys.has(row.id));
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
          return {
            provider: "orders_provider",
            kind: fragment.kind,
            payload: fragment,
          };
        },
        async execute(plan) {
          const fragment = plan.payload as ProviderFragment;
          if (fragment.kind !== "scan") {
            return [];
          }
          return scanRows(ordersRows, fragment.request);
        },
      } satisfies Omit<ProviderAdapter, "name">,
      users_provider: {
        canExecute(fragment: ProviderFragment) {
          return fragment.kind === "scan";
        },
        async compile(fragment: ProviderFragment) {
          return {
            provider: "users_provider",
            kind: fragment.kind,
            payload: fragment,
          };
        },
        async execute(plan) {
          const fragment = plan.payload as ProviderFragment;
          if (fragment.kind !== "scan") {
            return [];
          }
          return scanRows(usersRows, fragment.request);
        },
        async lookupMany(request) {
          const keys = new Set(request.keys);
          return usersRows
            .filter((row) => keys.has(row[request.key]))
            .map((row) => {
              const out: QueryRow = {};
              for (const column of request.select) {
                out[column] = row[column] ?? null;
              }
              return out;
            });
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
        order_spend: view({
            rel: () =>
              rel.aggregate({
                from: rel.scan("my_orders"),
                groupBy: {
                  order_id: col("my_orders.id"),
                },
                measures: {
                  spend: agg.sum(col("my_orders.total_cents")),
                },
            }),
          columns: {
            order_id: col("id"),
            spend: col("spend"),
          },
        }),
      },
    }));

    const executableSchema = createExecutableSchemaFromProviders(schema, {
      warehouse: {
        canExecute(fragment: ProviderFragment) {
          return fragment.kind === "scan";
        },
        async compile(fragment: ProviderFragment) {
          return {
            provider: "warehouse",
            kind: fragment.kind,
            payload: fragment,
          };
        },
        async execute(plan) {
          const fragment = plan.payload as ProviderFragment;
          if (fragment.kind !== "scan") {
            return [];
          }

          return scanRows(
            [
              { id: "o1", total_cents: 1500 },
              { id: "o1", total_cents: 500 },
              { id: "o2", total_cents: 700 },
            ],
            fragment.request,
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
          return {
            provider: "warehouse",
            kind: fragment.kind,
            payload: fragment,
          };
        },
        async execute(plan) {
          const fragment = plan.payload as ProviderFragment;
          if (fragment.kind !== "scan") {
            return [];
          }
          return scanRows([{ id: "o1" }], fragment.request);
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
});
