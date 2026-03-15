import { Result } from "better-result";
import { describe, expect, it } from "vitest";
import type { RelNode } from "@tupl/foundation";

import {
  buildCapabilityReport,
  createDataEntityHandle,
  createRelationalProviderAdapter,
  type FragmentProviderAdapter,
  getDataEntityProvider,
  type QueryRow,
  type ProviderAdapter,
  type ScanFilterClause,
  type TableScanRequest,
} from "@tupl/provider-kit";
import type {
  LookupManyCapableProviderAdapter,
  ProviderLookupManyRequest,
} from "@tupl/provider-kit/shapes";
import {
  getNormalizedTableBinding,
  validateProviderBindings,
} from "@tupl/schema-model/normalization";
import {
  createExecutableSchemaFromProviders,
  createSessionFromExecutableSchema,
} from "@tupl/test-support/runtime";
import { buildSchema, buildEntitySchema } from "@tupl/test-support/schema";

type TestProvider = Omit<FragmentProviderAdapter, "name"> &
  Partial<LookupManyCapableProviderAdapter>;

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

function createScanFragment(input: {
  provider?: string;
  table: string;
  select: string[];
  where?: TableScanRequest["where"];
  orderBy?: TableScanRequest["orderBy"];
  limit?: number;
  offset?: number;
}): RelNode {
  const provider = input.provider ?? "warehouse";
  return {
    id: `${provider}:${input.table}`,
    kind: "scan",
    convention: `provider:${provider}`,
    table: input.table,
    select: input.select,
    output: input.select.map((name) => ({ name })),
    ...(input.where ? { where: input.where } : {}),
    ...(input.orderBy ? { orderBy: input.orderBy } : {}),
    ...(input.limit != null ? { limit: input.limit } : {}),
    ...(input.offset != null ? { offset: input.offset } : {}),
  };
}

function toScanRequest(rel: RelNode): TableScanRequest | null {
  if (rel.kind !== "scan") {
    return null;
  }

  return {
    table: rel.table,
    ...(rel.kind === "scan" && rel.alias ? { alias: rel.alias } : {}),
    select: rel.select,
    ...("where" in rel && rel.where ? { where: rel.where } : {}),
    ...("orderBy" in rel && rel.orderBy ? { orderBy: rel.orderBy } : {}),
    ...("limit" in rel && rel.limit != null ? { limit: rel.limit } : {}),
    ...("offset" in rel && rel.offset != null ? { offset: rel.offset } : {}),
  };
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
        if (
          typeof value !== "string" ||
          typeof clause.value !== "string" ||
          !matchesLike(value, clause.value)
        ) {
          return false;
        }
        break;
      case "not_like":
        if (
          typeof value !== "string" ||
          typeof clause.value !== "string" ||
          matchesLike(value, clause.value)
        ) {
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
  it("creates and binds relational provider entity handles automatically", () => {
    const adapter = createRelationalProviderAdapter({
      name: "warehouse",
      entities: {
        orders: {
          shape: {
            id: "text",
            total_cents: "integer",
          },
        },
      },
      resolveRelCompileStrategy() {
        return null;
      },
      async compileRelFragment() {
        return Result.ok({
          provider: "warehouse",
          kind: "rel",
          payload: null,
        });
      },
      async executeCompiledPlan() {
        return Result.ok([]);
      },
    });

    expect(adapter.entities.orders.entity).toBe("orders");
    expect(adapter.entities.orders.provider).toBe("warehouse");
    expect(adapter.entities.orders.columns).toMatchObject({
      id: { source: "id", type: "text" },
      total_cents: { source: "total_cents", type: "integer" },
    });
    expect(getDataEntityProvider(adapter.entities.orders)).toBe(adapter);
  });

  it("returns structured capability reports for unsupported relational fragments", async () => {
    const adapter = createRelationalProviderAdapter({
      name: "warehouse",
      entities: {
        orders: {},
      },
      resolveRelCompileStrategy() {
        return null;
      },
      unsupportedRelReason(args) {
        return buildCapabilityReport(args.rel, "Rel fragment is not supported for this provider.");
      },
      async compileRelFragment() {
        return Result.ok({
          provider: "warehouse",
          kind: "rel",
          payload: null,
        });
      },
      async executeCompiledPlan() {
        return Result.ok([]);
      },
    });

    const capability = await adapter.canExecute(
      {
        id: "join",
        kind: "join",
        convention: "local",
        joinType: "inner",
        left: {
          id: "left_scan",
          kind: "scan",
          convention: "local",
          table: "orders",
          select: ["id"],
          output: [],
        },
        right: {
          id: "right_scan",
          kind: "scan",
          convention: "local",
          table: "orders",
          alias: "o2",
          select: ["id"],
          output: [],
        },
        leftKey: { column: "id" },
        rightKey: { alias: "o2", column: "id" },
        output: [],
      },
      {},
    );

    expect(capability).toEqual({
      supported: false,
      routeFamily: "rel-core",
      reason: "Rel fragment is not supported for this provider.",
    });
  });

  it("passes route family into supported relational capability checks", async () => {
    const observed: {
      routeFamily: string;
      strategy: string | null;
    }[] = [];
    const adapter = createRelationalProviderAdapter({
      name: "warehouse",
      entities: {
        orders: {},
      },
      resolveRelCompileStrategy() {
        return "basic";
      },
      isRelStrategySupported(args) {
        observed.push({
          routeFamily: args.routeFamily,
          strategy: args.strategy,
        });
        return true;
      },
      async compileRelFragment() {
        return Result.ok({
          provider: "warehouse",
          kind: "rel",
          payload: null,
        });
      },
      async executeCompiledPlan() {
        return Result.ok([]);
      },
    });

    const capability = await adapter.canExecute(
      {
        id: "join",
        kind: "join",
        convention: "local",
        joinType: "inner",
        left: {
          id: "left_scan",
          kind: "scan",
          convention: "local",
          table: "orders",
          select: ["id"],
          output: [],
        },
        right: {
          id: "right_scan",
          kind: "scan",
          convention: "local",
          table: "orders",
          alias: "o2",
          select: ["id"],
          output: [],
        },
        leftKey: { column: "id" },
        rightKey: { alias: "o2", column: "id" },
        output: [],
      },
      {},
    );

    expect(capability).toBe(true);
    expect(observed).toEqual([
      {
        routeFamily: "rel-core",
        strategy: "basic",
      },
    ]);
  });

  it("bases scan fragment support on registered relational entities", async () => {
    const adapter = createRelationalProviderAdapter({
      name: "warehouse",
      entities: {
        orders: {},
      },
      resolveRelCompileStrategy({ rel }) {
        return rel.kind === "scan" && rel.table === "orders" ? "basic" : null;
      },
      async compileRelFragment() {
        return Result.ok({
          provider: "warehouse",
          kind: "rel",
          payload: null,
        });
      },
      async executeCompiledPlan() {
        return Result.ok([]);
      },
    });

    expect(
      adapter.canExecute(
        createScanFragment({
          provider: "warehouse",
          table: "orders",
          select: ["id"],
        }),
        {},
      ),
    ).toBe(true);
    expect(
      adapter.canExecute(
        createScanFragment({
          provider: "warehouse",
          table: "users",
          select: ["id"],
        }),
        {},
      ),
    ).toEqual(
      expect.objectContaining({
        supported: false,
        routeFamily: "scan",
      }),
    );
  });

  it("wires lookupMany only when relational lookup hooks are provided", async () => {
    const withoutLookup = createRelationalProviderAdapter({
      name: "warehouse",
      entities: {
        orders: {},
      },
      resolveRelCompileStrategy() {
        return null;
      },
      async compileRelFragment() {
        return Result.ok({
          provider: "warehouse",
          kind: "rel",
          payload: null,
        });
      },
      async executeCompiledPlan() {
        return Result.ok([]);
      },
    });
    const withLookup = createRelationalProviderAdapter({
      name: "warehouse",
      entities: {
        orders: {},
      },
      resolveRelCompileStrategy() {
        return null;
      },
      async compileRelFragment() {
        return Result.ok({
          provider: "warehouse",
          kind: "rel",
          payload: null,
        });
      },
      async executeCompiledPlan() {
        return Result.ok([]);
      },
      async lookupMany() {
        return Result.ok([{ id: "o1" }]);
      },
    });

    expect("lookupMany" in withoutLookup).toBe(false);
    expect("lookupMany" in withLookup).toBe(true);
    await expect(
      withLookup.lookupMany?.(
        {
          table: "orders",
          key: "id",
          keys: ["o1"],
          select: ["id"],
        },
        {},
      ),
    ).resolves.toEqual(Result.ok([{ id: "o1" }]));
  });

  it("routes same-provider queries through rel fragments", async () => {
    const schema = buildEntitySchema({
      orders: {
        provider: "warehouse",
        columns: {
          id: "text",
          org_id: "text",
          total_cents: "integer",
        },
      },
    });

    let canExecuteCalls = 0;
    let executeCalls = 0;

    const executableSchema = createExecutableSchemaFromProviders(schema, {
      warehouse: {
        canExecute(rel: RelNode) {
          canExecuteCalls += 1;
          return rel.kind === "scan";
        },
        async compile(rel: RelNode) {
          return Result.ok({
            provider: "warehouse",
            kind: "rel",
            payload: rel,
          });
        },
        async execute() {
          executeCalls += 1;
          return Result.ok([{ id: "o2" }]);
        },
      } satisfies TestProvider,
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
    const schema = buildEntitySchema({
      orders: {
        provider: "warehouse",
        columns: {
          id: "text",
          org_id: "text",
        },
      },
    });

    let sawRelCompile = false;

    const executableSchema = createExecutableSchemaFromProviders(schema, {
      warehouse: {
        canExecute(_rel: RelNode) {
          return true;
        },
        async compile(rel: RelNode) {
          sawRelCompile = true;
          return Result.ok({
            provider: "warehouse",
            kind: "rel",
            payload: rel,
          });
        },
        async execute() {
          return Result.ok([{ id: "o1" }]);
        },
      } satisfies TestProvider,
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

    const schema = buildSchema((builder) => {
      builder.table("my_orders", ordersEntity, {
        columns: {
          id: { source: "id" },
          totalCents: { source: "total_cents" },
          status: {
            source: "status",
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
      });
      builder.table("orders", ordersEntity, {
        columns: {
          status: { source: "status", type: "text", enum: ["pending", "paid", "shipped"] as const },
        },
      });
    });

    let capturedRel: unknown = null;

    const executableSchema = createExecutableSchemaFromProviders(schema, {
      warehouse: {
        canExecute(_rel: RelNode) {
          return true;
        },
        async compile(rel: RelNode) {
          capturedRel = rel;

          return Result.ok({
            provider: "warehouse",
            kind: "rel",
            payload: rel,
          });
        },
        async execute() {
          return Result.ok([{ id: "o1" }]);
        },
      } satisfies TestProvider,
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
    const scanNode =
      projectNode?.kind === "project" && projectNode.input?.kind === "scan"
        ? projectNode.input
        : null;
    expect(scanNode?.table).toBe("orders_raw");
    expect(scanNode?.select).toContain("total_cents");
    expect(scanNode?.where?.[0]?.column).toBe("total_cents");
    expect(
      scanNode?.where?.some((clause) => clause.column === "status" && clause.op === "in"),
    ).toBe(true);
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

    const schema = buildSchema((builder) => {
      builder.table("my_orders", ordersEntity, {
        columns: ({ col }) => ({
          id: col.id("id"),
          totalCents: col.integer("totalCents"),
          createdAt: col.string("createdAt", { coerce: "isoTimestamp" }),
        }),
      });
    });

    let capturedScan: TableScanRequest | null = null;
    const executableSchema = createExecutableSchemaFromProviders(schema, {
      warehouse: {
        canExecute(rel: RelNode) {
          return rel.kind === "scan";
        },
        async compile(rel: RelNode) {
          capturedScan = toScanRequest(rel);
          return Result.ok({
            provider: "warehouse",
            kind: "rel",
            payload: rel,
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
      } satisfies TestProvider,
    });

    const rows = await executableSchema.query({
      context: {},
      sql: "SELECT id, totalCents, createdAt FROM my_orders",
    });

    expect(capturedScan).not.toBeNull();
    if (!capturedScan) {
      throw new Error("Expected provider scan request to be captured.");
    }
    const capturedRequest = capturedScan as TableScanRequest;
    expect(capturedRequest.select).toEqual(["id", "total_cents", "created_at"]);
    expect(rows).toEqual([
      {
        id: "o1",
        totalCents: 1500,
        createdAt: "2026-02-03T10:00:00.000Z",
      },
    ]);
  });

  it("uses lookupMany for cross-provider lookup join paths", async () => {
    const schema = buildEntitySchema({
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
        canExecute(rel: RelNode) {
          return rel.kind === "scan";
        },
        async compile(rel: RelNode) {
          return Result.ok({
            provider: "orders_provider",
            kind: "rel",
            payload: rel,
          });
        },
        async execute(plan) {
          const rel = plan.payload as RelNode;
          const request = toScanRequest(rel);
          if (!request) {
            return Result.ok([]);
          }
          return Result.ok(scanRows(ordersRows, request));
        },
      } satisfies TestProvider,
      users_provider: {
        canExecute(rel: RelNode) {
          return rel.kind === "scan";
        },
        async compile(rel: RelNode) {
          return Result.ok({
            provider: "users_provider",
            kind: "rel",
            payload: rel,
          });
        },
        async execute(plan) {
          const rel = plan.payload as RelNode;
          const request = toScanRequest(rel);
          if (!request) {
            return Result.ok([]);
          }
          return Result.ok(scanRows(usersRows, request));
        },
        async lookupMany(request: ProviderLookupManyRequest) {
          lookupCalls += 1;
          const keys = new Set(request.keys);
          return Result.ok(
            usersRows
              .filter((row) => keys.has(row[request.key]))
              .map((row) => {
                const out: QueryRow = {};
                for (const column of request.select) {
                  out[column] = row[column] ?? null;
                }
                return out;
              }),
          );
        },
      } satisfies TestProvider,
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
    const schema = buildEntitySchema({
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
        async compile(rel: RelNode) {
          return Result.ok({
            provider: "orders_provider",
            kind: "rel",
            payload: rel,
          });
        },
        async execute(plan) {
          const rel = plan.payload as RelNode;
          const request = toScanRequest(rel);
          if (!request) {
            return Result.ok([]);
          }
          return Result.ok(scanRows(ordersRows, request));
        },
      } satisfies TestProvider,
      users_provider: {
        canExecute() {
          return true;
        },
        async compile(rel: RelNode) {
          return Result.ok({
            provider: "users_provider",
            kind: "rel",
            payload: rel,
          });
        },
        async execute(plan) {
          const rel = plan.payload as RelNode;
          const request = toScanRequest(rel);
          if (!request) {
            return Result.ok([]);
          }
          return Result.ok(scanRows(usersRows, request));
        },
        async lookupMany(request: ProviderLookupManyRequest) {
          const keys = new Set(request.keys);
          return Result.ok(usersRows.filter((row) => keys.has(row.id)));
        },
      } satisfies TestProvider,
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
    const schema = buildEntitySchema({
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
    });

    const ordersRows: QueryRow[] = [
      { id: "o1", user_id: "u1" },
      { id: "o2", user_id: "u_missing" },
    ];
    const usersRows: QueryRow[] = [{ id: "u1", email: "ada@example.com" }];

    const executableSchema = createExecutableSchemaFromProviders(schema, {
      orders_provider: {
        canExecute(rel: RelNode) {
          return rel.kind === "scan";
        },
        async compile(rel: RelNode) {
          return Result.ok({
            provider: "orders_provider",
            kind: "rel",
            payload: rel,
          });
        },
        async execute(plan) {
          const rel = plan.payload as RelNode;
          const request = toScanRequest(rel);
          if (!request) {
            return Result.ok([]);
          }
          return Result.ok(scanRows(ordersRows, request));
        },
      } satisfies TestProvider,
      users_provider: {
        canExecute(rel: RelNode) {
          return rel.kind === "scan";
        },
        async compile(rel: RelNode) {
          return Result.ok({
            provider: "users_provider",
            kind: "rel",
            payload: rel,
          });
        },
        async execute(plan) {
          const rel = plan.payload as RelNode;
          const request = toScanRequest(rel);
          if (!request) {
            return Result.ok([]);
          }
          return Result.ok(scanRows(usersRows, request));
        },
        async lookupMany(request: ProviderLookupManyRequest) {
          const keys = new Set(request.keys);
          return Result.ok(
            usersRows
              .filter((row) => keys.has(row[request.key]))
              .map((row) => {
                const out: QueryRow = {};
                for (const column of request.select) {
                  out[column] = row[column] ?? null;
                }
                return out;
              }),
          );
        },
      } satisfies TestProvider,
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

    const schema = buildSchema((builder) => {
      builder.table("my_orders", ordersEntity, {
        columns: {
          id: { source: "id", type: "text", nullable: false },
          total_cents: { source: "total_cents", type: "integer", nullable: false },
        },
      });
      builder.view(
        "order_spend",
        ({ scan, aggregate, col, agg }) =>
          aggregate({
            from: scan("my_orders"),
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
      );
    });

    const executableSchema = createExecutableSchemaFromProviders(schema, {
      warehouse: {
        canExecute(rel: RelNode) {
          return rel.kind === "scan";
        },
        async compile(rel: RelNode) {
          return Result.ok({
            provider: "warehouse",
            kind: "rel",
            payload: rel,
          });
        },
        async execute(plan) {
          const rel = plan.payload as RelNode;
          const request = toScanRequest(rel);
          if (!request) {
            return Result.ok([]);
          }

          return Result.ok(
            scanRows(
              [
                { id: "o1", total_cents: 1500 },
                { id: "o1", total_cents: 500 },
                { id: "o2", total_cents: 700 },
              ],
              request,
            ),
          );
        },
      } satisfies TestProvider,
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

    const schema = buildSchema((builder) => {
      builder.table("myOrders", ordersEntity, {
        columns: ({ col, expr }) => ({
          id: col.id("id"),
          totalCents: col.integer("totalCents"),
          totalDollars: col.real(expr.divide(col("totalCents"), expr.literal(100)), {
            nullable: false,
          }),
          isLargeOrder: col.boolean(expr.gte(col("totalCents"), expr.literal(2000)), {
            nullable: false,
          }),
        }),
      });
    });

    expect(getNormalizedTableBinding(schema, "myOrders")).toMatchObject({
      kind: "physical",
      columnBindings: {
        totalDollars: { kind: "expr" },
      },
    });

    const executableSchema = createExecutableSchemaFromProviders(schema, {
      warehouse: {
        canExecute(rel: RelNode) {
          return rel.kind === "scan";
        },
        async compile(rel: RelNode) {
          return Result.ok({
            provider: "warehouse",
            kind: "rel",
            payload: rel,
          });
        },
        async execute(plan) {
          const rel = plan.payload as RelNode;
          const request = toScanRequest(rel);
          if (!request) {
            return Result.ok([]);
          }

          return Result.ok(
            scanRows(
              [
                { id: "o1", total_cents: 1200 },
                { id: "o2", total_cents: 3200 },
                { id: "o3", total_cents: 2100 },
              ],
              request,
            ),
          );
        },
      } satisfies TestProvider,
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

    const schema = buildSchema((builder) => {
      builder.table("myOrders", ordersEntity, {
        columns: ({ col, expr }) => ({
          id: col.id("id"),
          totalCents: col.integer("totalCents"),
          totalDollars: col.real(expr.divide(col("totalCents"), expr.literal(100)), {
            nullable: false,
          }),
          isLargeOrder: col.boolean(expr.gte(col("totalCents"), expr.literal(20000)), {
            nullable: false,
          }),
        }),
      });
    });

    const executableSchema = createExecutableSchemaFromProviders(schema, {
      warehouse: {
        canExecute(_rel: RelNode) {
          return true;
        },
        async compile(rel: RelNode) {
          return Result.ok({
            provider: "warehouse",
            kind: "rel",
            payload: rel,
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
      } satisfies TestProvider,
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
    const schema = buildEntitySchema({
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
    });

    const executableSchema = createExecutableSchemaFromProviders(schema, {
      warehouse: {
        canExecute(_rel: RelNode) {
          return true;
        },
        async compile(rel: RelNode) {
          return Result.ok({
            provider: "warehouse",
            kind: "rel",
            payload: rel,
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
      } satisfies TestProvider,
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
      canExecute(rel: RelNode) {
        return rel.kind === "scan";
      },
      async compile(rel: RelNode) {
        return Result.ok({
          provider: "warehouse",
          kind: "rel",
          payload: rel,
        });
      },
      async execute(plan) {
        const rel = plan.payload as RelNode;
        const request = toScanRequest(rel);
        if (!request) {
          return Result.ok([]);
        }

        if (request.table === "orders_raw") {
          return Result.ok(
            scanRows(
              [
                { id: "o1", vendor_id: "v1", total_cents: 1200 },
                { id: "o2", vendor_id: "v1", total_cents: 800 },
                { id: "o3", vendor_id: "v2", total_cents: 500 },
              ],
              request,
            ),
          );
        }

        if (request.table === "vendors_raw") {
          return Result.ok(
            scanRows(
              [
                { id: "v1", name: "Acme" },
                { id: "v2", name: "Bolt" },
              ],
              request,
            ),
          );
        }

        return Result.ok([]);
      },
    } satisfies TestProvider;

    const ordersEntity = createDataEntityHandle({
      entity: "orders_raw",
      provider: "warehouse",
      providerInstance: warehouseProvider as unknown as ProviderAdapter,
      columns: {
        id: { source: "id", type: "text", nullable: false },
        vendorId: { source: "vendor_id", type: "text", nullable: false },
        totalCents: { source: "total_cents", type: "integer", nullable: false },
      },
    });
    const vendorsEntity = createDataEntityHandle({
      entity: "vendors_raw",
      provider: "warehouse",
      providerInstance: warehouseProvider as unknown as ProviderAdapter,
      columns: {
        id: { source: "id", type: "text", nullable: false },
        name: { source: "name", type: "text", nullable: false },
      },
    });

    const schema = buildSchema((builder) => {
      builder.view(
        "spendByVendor",
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
      );
    });

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

    const schema = buildSchema((builder) => {
      const myOrderItems = builder.table("myOrderItems", orderItemsEntity, {
        columns: ({ col, expr }) => ({
          orderId: col.string("orderId"),
          productId: col.string("productId"),
          quantity: col.integer("quantity"),
          lineTotalCents: col.integer("lineTotalCents"),
          unitPriceCents: col.real(expr.divide(col("lineTotalCents"), col("quantity")), {
            nullable: false,
          }),
        }),
      });

      const productsForOrg = builder.table("productsForOrg", productsEntity, {
        columns: ({ col }) => ({
          id: col.id("id"),
          name: col.string("name"),
        }),
      });

      const productAccess = builder.table("productAccess", accessEntity, {
        columns: ({ col }) => ({
          productId: col.string("productId"),
        }),
      });

      const activeProducts = builder.view(
        "activeProducts",
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

      builder.view(
        "myOrderLines",
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
    });

    const executableSchema = createExecutableSchemaFromProviders(schema, {
      warehouse: {
        canExecute(rel: RelNode) {
          return rel.kind === "scan";
        },
        async compile(rel: RelNode) {
          return Result.ok({
            provider: "warehouse",
            kind: "rel",
            payload: rel,
          });
        },
        async execute(plan) {
          const rel = plan.payload as RelNode;
          const request = toScanRequest(rel);
          if (!request) {
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
            product_access_raw: [{ product_id: "p1" }, { product_id: "p2" }],
          };

          return Result.ok(scanRows(rowsByTable[request.table] ?? [], request));
        },
      } satisfies TestProvider,
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

    const schema = buildSchema((builder) => {
      const myOrderItems = builder.table("myOrderItems", orderItemsEntity, {
        columns: ({ col }) => ({
          orderId: col.string("orderId"),
          productId: col.string("productId"),
          quantity: col.integer("quantity"),
          lineTotalCents: col.integer("lineTotalCents"),
        }),
      });

      const productsForOrg = builder.table("productsForOrg", productsEntity, {
        columns: ({ col }) => ({
          id: col.id("id"),
          name: col.string("name"),
        }),
      });

      const productAccess = builder.table("productAccess", accessEntity, {
        columns: ({ col }) => ({
          productId: col.string("productId"),
        }),
      });

      const activeProducts = builder.view(
        "activeProducts",
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

      const myOrderLines = builder.view(
        "myOrderLines",
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

      builder.view(
        "productPerformance",
        ({ scan, aggregate, col, agg }) =>
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
        {
          columns: ({ col }) => ({
            productId: col.id("productId"),
            productName: col.string("productName"),
            unitsSold: col.integer("unitsSold"),
            revenueCents: col.integer("revenueCents"),
          }),
        },
      );
    });

    const executableSchema = createExecutableSchemaFromProviders(schema, {
      warehouse: {
        canExecute(rel: RelNode) {
          return rel.kind === "scan";
        },
        async compile(rel: RelNode) {
          return Result.ok({
            provider: "warehouse",
            kind: "rel",
            payload: rel,
          });
        },
        async execute(plan) {
          const rel = plan.payload as RelNode;
          const request = toScanRequest(rel);
          if (!request) {
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
            product_access_raw: [{ product_id: "p1" }, { product_id: "p2" }],
          };

          return Result.ok(scanRows(rowsByTable[request.table] ?? [], request));
        },
      } satisfies TestProvider,
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

    const schema = buildSchema((builder) => {
      const products = builder.table("products", productsEntity, {
        columns: ({ col }) => ({
          id: col.id("id"),
          productName: col.string("name"),
        }),
      });

      const productViewCounts = builder.table("productViewCounts", viewCountsEntity, {
        columns: ({ col }) => ({
          productId: col.string("productId"),
          viewCount: col.integer("viewCount"),
        }),
      });

      builder.view(
        "productEngagement",
        ({ scan, join, col, expr }) =>
          join({
            left: scan(products),
            right: scan(productViewCounts),
            on: expr.eq(col(products, "id"), col(productViewCounts, "productId")),
            type: "left",
          }),
        {
          columns: {
            productName: { source: "products.productName", type: "text", nullable: false },
            viewCount: { source: "productViewCounts.viewCount", type: "integer", nullable: true },
          },
        },
      );
    });

    const executableSchema = createExecutableSchemaFromProviders(schema, {
      warehouse: {
        canExecute(rel: RelNode) {
          return rel.kind === "scan";
        },
        async compile(rel: RelNode) {
          return Result.ok({
            provider: "warehouse",
            kind: "rel",
            payload: rel,
          });
        },
        async execute(plan) {
          const rel = plan.payload as RelNode;
          const request = toScanRequest(rel);
          if (!request) {
            return Result.ok([]);
          }

          return Result.ok(
            scanRows(
              [
                { id: "p1", name: "Edge Router" },
                { id: "p2", name: "Backup Service" },
              ],
              request,
            ),
          );
        },
      } satisfies TestProvider,
      kv: {
        canExecute(rel: RelNode) {
          return rel.kind === "scan";
        },
        async compile(rel: RelNode) {
          return Result.ok({
            provider: "kv",
            kind: "rel",
            payload: rel,
          });
        },
        async execute(plan) {
          const rel = plan.payload as RelNode;
          const request = toScanRequest(rel);
          if (!request) {
            return Result.ok([]);
          }

          return Result.ok(scanRows([{ product_id: "p1", view_count: 12 }], request));
        },
      } satisfies TestProvider,
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

  it("pushes down single-provider view scans before cross-provider lookup joins", async () => {
    const productsEntity = createDataEntityHandle({
      entity: "products_raw",
      provider: "warehouse",
      columns: {
        id: { source: "id", type: "text", nullable: false, primaryKey: true },
        name: { source: "name", type: "text", nullable: false },
      },
    });
    const productAccessEntity = createDataEntityHandle({
      entity: "product_access_raw",
      provider: "warehouse",
      columns: {
        product_id: { source: "product_id", type: "text", nullable: false },
      },
    });
    const viewCountsEntity = createDataEntityHandle({
      entity: "product_view_counts",
      provider: "kv",
      columns: {
        product_id: { source: "product_id", type: "text", nullable: false },
        view_count: { source: "view_count", type: "integer", nullable: false },
      },
    });

    const schema = buildSchema((builder) => {
      const products = builder.table("products", productsEntity, {
        columns: ({ col }) => ({
          id: col.id("id"),
          name: col.string("name", { nullable: false }),
        }),
      });

      const productAccess = builder.table("productAccess", productAccessEntity, {
        columns: ({ col }) => ({
          product_id: col.string("product_id", { nullable: false }),
        }),
      });

      builder.view(
        "active_products",
        ({ scan, join, col, expr }) =>
          join({
            left: scan(products),
            right: scan(productAccess),
            on: expr.eq(col(products, "id"), col(productAccess, "product_id")),
            type: "inner",
          }),
        {
          columns: {
            id: { source: "products.id", type: "text", nullable: false, primaryKey: true },
            name: { source: "products.name", type: "text", nullable: false },
          },
        },
      );

      builder.table("product_view_counts", viewCountsEntity, {
        columns: ({ col }) => ({
          product_id: col.string("product_id", { nullable: false }),
          view_count: col.integer("view_count", { nullable: false }),
        }),
      });
    });

    let warehouseRelExecutions = 0;
    let warehouseScanExecutions = 0;
    let kvLookupCalls = 0;
    let kvScanExecutions = 0;

    const executableSchema = createExecutableSchemaFromProviders(schema, {
      warehouse: {
        canExecute(_rel: RelNode) {
          return true;
        },
        async compile(rel: RelNode) {
          return Result.ok({
            provider: "warehouse",
            kind: "rel",
            payload: rel,
          });
        },
        async execute(plan) {
          const rel = plan.payload as RelNode;
          warehouseRelExecutions += 1;
          const products = [
            { id: "p1", name: "Edge Router" },
            { id: "p2", name: "Backup Service" },
          ];

          return Result.ok(
            products.map((product) =>
              Object.fromEntries(
                rel.output.map((output) => {
                  if (output.name.endsWith(".id") || output.name === "id") {
                    return [output.name, product.id] as const;
                  }
                  if (output.name.endsWith(".name") || output.name === "name") {
                    return [output.name, product.name] as const;
                  }
                  return [output.name, null] as const;
                }),
              ),
            ),
          );
        },
      } satisfies TestProvider,
      kv: {
        canExecute(rel: RelNode) {
          return rel.kind === "scan";
        },
        async compile(rel: RelNode) {
          return Result.ok({
            provider: "kv",
            kind: "rel",
            payload: rel,
          });
        },
        async execute() {
          kvScanExecutions += 1;
          return Result.ok([]);
        },
        async lookupMany(request: ProviderLookupManyRequest) {
          kvLookupCalls += 1;
          return Result.ok(
            request.keys.includes("p1") ? [{ product_id: "p1", view_count: 12 }] : [],
          );
        },
      } satisfies TestProvider,
    });

    const rows = await executableSchema.query({
      context: {},
      sql: `
        SELECT p.name, v.view_count
        FROM active_products p
        LEFT JOIN product_view_counts v ON v.product_id = p.id
        ORDER BY v.view_count DESC, p.name
      `,
    });

    expect(rows).toEqual([
      { name: "Edge Router", view_count: 12 },
      { name: "Backup Service", view_count: null },
    ]);
    expect(warehouseRelExecutions).toBe(1);
    expect(warehouseScanExecutions).toBe(0);
    expect(kvLookupCalls).toBe(1);
    expect(kvScanExecutions).toBe(0);

    const session = createSessionFromExecutableSchema(executableSchema, {
      context: {},
      sql: `
        SELECT p.name, v.view_count
        FROM active_products p
        LEFT JOIN product_view_counts v ON v.product_id = p.id
        ORDER BY v.view_count DESC, p.name
      `,
    });
    const plan = session.getPlan();
    expect(plan.steps.filter((step) => step.kind === "remote_fragment")).toHaveLength(1);
    expect(plan.steps.some((step) => step.kind === "lookup_join")).toBe(true);
    expect(plan.steps.some((step) => step.kind === "scan")).toBe(false);
  });

  it("exposes scan stages in session plans", () => {
    const schema = buildEntitySchema({
      orders: {
        provider: "warehouse",
        columns: {
          id: "text",
        },
      },
    });

    const executableSchema = createExecutableSchemaFromProviders(schema, {
      warehouse: {
        canExecute(rel: RelNode) {
          return rel.kind === "scan";
        },
        async compile(rel: RelNode) {
          return Result.ok({
            provider: "warehouse",
            kind: "rel",
            payload: rel,
          });
        },
        async execute(plan) {
          const rel = plan.payload as RelNode;
          const request = toScanRequest(rel);
          if (!request) {
            return Result.ok([]);
          }
          return Result.ok(scanRows([{ id: "o1" }], request));
        },
      } satisfies TestProvider,
    });

    const session = createSessionFromExecutableSchema(executableSchema, {
      context: {},
      sql: "SELECT id FROM orders",
    });

    const plan = session.getPlan();
    expect(plan.steps.length).toBeGreaterThan(0);
    expect(plan.steps.some((step) => step.kind === "scan")).toBe(true);
  });

  it("includes fallback diagnostics in explain output for unsupported rel pushdown", async () => {
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

    const executableSchema = createExecutableSchemaFromProviders(schema, {
      warehouse: {
        canExecute(rel: RelNode) {
          return rel.kind === "scan";
        },
        async compile(rel: RelNode) {
          return Result.ok({
            provider: "warehouse",
            kind: "rel",
            payload: rel,
          });
        },
        async execute(plan) {
          const rel = plan.payload as RelNode;
          const request = toScanRequest(rel);
          if (!request) {
            return Result.ok([]);
          }
          return Result.ok(
            scanRows(
              request.table === "orders"
                ? [{ id: "o1", user_id: "u1" }]
                : [{ id: "u1", email: "a@example.com" }],
              request,
            ),
          );
        },
      } satisfies TestProvider,
    });

    const explained = await executableSchema.explain({
      context: {},
      sql: `
        SELECT o.id, u.email
        FROM orders o
        JOIN users u ON o.user_id = u.id
      `,
    });

    expect(explained.diagnostics).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          stage: "physical_planning",
          diagnostic: expect.objectContaining({
            code: "TUPL_WARN_FALLBACK",
            severity: "warning",
          }),
        }),
      ]),
    );
  });

  it("keeps capability analysis out of the relational adapter surface", () => {
    const adapter = createRelationalProviderAdapter({
      name: "warehouse",
      entities: {
        orders: {},
      },
      resolveRelCompileStrategy() {
        return null;
      },
      async executeCompiledPlan() {
        return Result.ok([]);
      },
    });
    expect("capabilityAtoms" in adapter).toBe(false);
  });

  it("builds the default relational compiled-plan payload when no custom compiler is provided", async () => {
    const relFragment: RelNode = {
      id: "scan_orders",
      kind: "scan",
      convention: "local",
      table: "orders",
      select: ["id"],
      output: [{ name: "id" }],
    };
    const adapter = createRelationalProviderAdapter({
      name: "warehouse",
      entities: {
        orders: {},
      },
      resolveRelCompileStrategy() {
        return "basic";
      },
      async executeCompiledPlan() {
        return Result.ok([]);
      },
    });

    const compileResult = await adapter.compile(relFragment, {});
    expect(Result.isOk(compileResult)).toBe(true);
    if (Result.isError(compileResult)) {
      throw compileResult.error;
    }

    expect(compileResult.value).toEqual({
      provider: "warehouse",
      kind: "rel",
      payload: {
        strategy: "basic",
        rel: relFragment,
      },
    });
  });

  it("uses the provider-specific unsupported reason for non-sql unsupported shapes", async () => {
    const adapter = createRelationalProviderAdapter({
      name: "warehouse",
      entities: {
        orders: {},
      },
      resolveRelCompileStrategy() {
        return null;
      },
      unsupportedRelReasonMessage: "Rel fragment needs a provider-specific compilation path.",
      async executeCompiledPlan() {
        return Result.ok([]);
      },
    });

    const capability = await adapter.canExecute(
      {
        id: "window_1",
        kind: "window",
        convention: "local",
        input: {
          id: "scan_orders",
          kind: "scan",
          convention: "local",
          table: "orders",
          select: ["id"],
          output: [{ name: "id" }],
        },
        functions: [
          {
            fn: "count",
            as: "row_count",
            partitionBy: [],
            orderBy: [],
          },
        ],
        output: [{ name: "id" }, { name: "row_count" }],
      },
      {},
    );

    expect(capability).toMatchObject({
      supported: false,
      reason: "Rel fragment needs a provider-specific compilation path.",
    });
  });

  it("rejects fallback when query policy forbids provider fallback", async () => {
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

    const executableSchema = createExecutableSchemaFromProviders(schema, {
      warehouse: {
        canExecute(rel: RelNode) {
          return rel.kind === "scan";
        },
        async compile(rel: RelNode) {
          return Result.ok({
            provider: "warehouse",
            kind: "rel",
            payload: rel,
          });
        },
        async execute(plan) {
          const rel = plan.payload as RelNode;
          const request = toScanRequest(rel);
          if (!request) {
            return Result.ok([]);
          }
          return Result.ok(scanRows([], request));
        },
      } satisfies TestProvider,
    });

    await expect(
      executableSchema.query({
        context: {},
        fallbackPolicy: {
          allowFallback: false,
        },
        sql: `
        SELECT o.id, u.email
        FROM orders o
        JOIN users u ON o.user_id = u.id
      `,
      }),
    ).rejects.toMatchObject({
      _tag: "TuplDiagnosticError",
      diagnostics: expect.any(Array),
      name: "TuplDiagnosticError",
    });
  });

  it("surfaces tagged timeout errors through the Promise query API", async () => {
    const schema = buildEntitySchema({
      users: {
        provider: "warehouse",
        columns: {
          id: "text",
        },
      },
    });

    const executableSchema = createExecutableSchemaFromProviders(schema, {
      warehouse: {
        canExecute(rel: RelNode) {
          return rel.kind === "scan";
        },
        async compile(rel: RelNode) {
          return Result.ok({
            provider: "warehouse",
            kind: "rel",
            payload: rel,
          });
        },
        async execute(plan) {
          const rel = plan.payload as RelNode;
          const request = toScanRequest(rel);
          if (!request) {
            return Result.ok([]);
          }
          await sleep(25);
          return Result.ok(scanRows([{ id: "u1" }], request));
        },
      } satisfies TestProvider,
    });

    await expect(
      executableSchema.query({
        context: {},
        queryGuardrails: {
          timeoutMs: 5,
        },
        sql: "SELECT id FROM users",
      }),
    ).rejects.toMatchObject({
      _tag: "TuplTimeoutError",
      name: "TuplTimeoutError",
      message: "Query timed out after 5ms.",
    });
  });

  it("returns tagged provider binding errors from the result API", () => {
    const schema = buildEntitySchema({
      users: {
        provider: "warehouse",
        columns: {
          id: "text",
        },
      },
    });

    const result = validateProviderBindings(schema, {});
    expect(Result.isError(result)).toBe(true);
    if (Result.isOk(result)) {
      throw new Error("Expected provider binding validation to fail.");
    }

    expect(result.error).toMatchObject({
      _tag: "TuplProviderBindingError",
      name: "TuplProviderBindingError",
      message: "Table users is bound to provider warehouse, but no such provider is registered.",
      provider: "warehouse",
      table: "users",
    });
  });

  it("executes scalar expressions and missing operators locally when scan pushdown is the only provider capability", async () => {
    const schema = buildEntitySchema({
      users: {
        provider: "warehouse",
        columns: {
          id: "text",
          email: "text",
          score: { type: "integer" },
        },
      },
    });

    const executableSchema = createExecutableSchemaFromProviders(schema, {
      warehouse: {
        canExecute(rel: RelNode) {
          return rel.kind === "scan";
        },
        async compile(rel: RelNode) {
          return Result.ok({
            provider: "warehouse",
            kind: "rel",
            payload: rel,
          });
        },
        async execute(plan) {
          const rel = plan.payload as RelNode;
          const request = toScanRequest(rel);
          if (!request) {
            return Result.ok([]);
          }
          return Result.ok(
            scanRows(
              [
                { id: "u1", email: "Alpha@Example.com", score: 10 },
                { id: "u2", email: "beta@sample.com", score: 7 },
                { id: "u3", email: "Gamma@Example.com", score: 4 },
              ],
              request,
            ),
          );
        },
      } satisfies TestProvider,
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
