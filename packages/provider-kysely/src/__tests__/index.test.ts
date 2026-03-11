import { describe, expect, it } from "vitest";

import { type ProviderFragment } from "@tupl/provider-kit";
import type { RelNode } from "@tupl/foundation";
import type { QueryRow, TableScanRequest } from "@tupl/schema-model";
import { createKyselyProvider, type KyselyDatabaseLike } from "../index";

interface KyselyCalls {
  from: string[];
  joins: string[];
  where: unknown[][];
  orderBy: unknown[][];
  limits: number[];
  offsets: number[];
  executeCount: number;
}

function createMockKyselyDb(
  rowsByFrom: Map<string, QueryRow[]>,
  rowsByJoin: Map<string, QueryRow[]>,
  options?: {
    omitFnHelpers?: Array<"count" | "sum" | "avg" | "min" | "max">;
    supportDistinct?: boolean;
  },
): {
  db: KyselyDatabaseLike;
  calls: KyselyCalls;
} {
  const calls: KyselyCalls = {
    from: [],
    joins: [],
    where: [],
    orderBy: [],
    limits: [],
    offsets: [],
    executeCount: 0,
  };

  const db: KyselyDatabaseLike = {
    selectFrom(from: unknown) {
      const root = String(from);
      calls.from.push(root);
      let rows = [...(rowsByFrom.get(root) ?? [])];
      let projections: Array<{ source: string; output: string }> = [];

      const builder: any = {
        innerJoin(right: unknown) {
          const rightKey = String(right);
          calls.joins.push(rightKey);
          rows = [...(rowsByJoin.get(`${root}->${rightKey}`) ?? rows)];
          return builder;
        },
        leftJoin(right: unknown) {
          return builder.innerJoin(right);
        },
        rightJoin(right: unknown) {
          return builder.innerJoin(right);
        },
        fullJoin(right: unknown) {
          return builder.innerJoin(right);
        },
        where(...args: unknown[]) {
          calls.where.push(args);
          return builder;
        },
        groupBy() {
          return builder;
        },
        orderBy(...args: unknown[]) {
          calls.orderBy.push(args);
          return builder;
        },
        limit(value: number) {
          calls.limits.push(value);
          rows = rows.slice(0, value);
          return builder;
        },
        offset(value: number) {
          calls.offsets.push(value);
          rows = rows.slice(value);
          return builder;
        },
        select(selectArg: unknown) {
          if (typeof selectArg === "function") {
            const makeAggregateExpression = (kind: "agg", output: string) => ({
              kind,
              output,
              distinct() {
                return {
                  as(nextOutput: string) {
                    return {
                      kind,
                      output: nextOutput,
                    };
                  },
                };
              },
            });
            const omittedHelpers = new Set(options?.omitFnHelpers ?? []);
            const fnHelpers: Record<string, (ref?: string) => { as: (output: string) => unknown }> =
              {};
            if (!omittedHelpers.has("count")) {
              fnHelpers.count = (_ref?: string) => ({
                as(output: string) {
                  return options?.supportDistinct
                    ? makeAggregateExpression("agg", output)
                    : { kind: "agg", output };
                },
              });
            }
            if (!omittedHelpers.has("sum")) {
              fnHelpers.sum = (_ref?: string) => ({
                as(output: string) {
                  return options?.supportDistinct
                    ? makeAggregateExpression("agg", output)
                    : { kind: "agg", output };
                },
              });
            }
            if (!omittedHelpers.has("avg")) {
              fnHelpers.avg = (_ref?: string) => ({
                as(output: string) {
                  return options?.supportDistinct
                    ? makeAggregateExpression("agg", output)
                    : { kind: "agg", output };
                },
              });
            }
            if (!omittedHelpers.has("min")) {
              fnHelpers.min = (_ref?: string) => ({
                as(output: string) {
                  return { kind: "agg", output };
                },
              });
            }
            if (!omittedHelpers.has("max")) {
              fnHelpers.max = (_ref?: string) => ({
                as(output: string) {
                  return { kind: "agg", output };
                },
              });
            }
            const eb = {
              ref(ref: string) {
                return {
                  as(output: string) {
                    return {
                      kind: "ref",
                      source: ref,
                      output,
                    };
                  },
                };
              },
              fn: {
                countAll() {
                  return {
                    as(output: string) {
                      return { kind: "agg", output };
                    },
                  };
                },
                ...fnHelpers,
              },
            };

            const expressions = selectArg(eb) as Array<Record<string, unknown>>;
            projections = expressions.map((entry) => {
              if (entry.kind === "ref") {
                return {
                  source: String(entry.source),
                  output: String(entry.output),
                };
              }

              return {
                source: String(entry.output),
                output: String(entry.output),
              };
            });
          }
          return builder;
        },
        async execute() {
          calls.executeCount += 1;
          return rows.map((row) => {
            if (projections.length === 0) {
              return row;
            }

            const out: QueryRow = {};
            for (const projection of projections) {
              out[projection.output] = row[projection.source] ?? null;
            }
            return out;
          });
        },
      };

      return builder;
    },
  };

  return { db, calls };
}

function buildJoinProjectRel(): RelNode {
  return {
    id: "project_1",
    kind: "project",
    convention: "provider:dbProvider",
    input: {
      id: "sort_1",
      kind: "sort",
      convention: "provider:dbProvider",
      input: {
        id: "join_1",
        kind: "join",
        convention: "provider:dbProvider",
        joinType: "inner",
        left: {
          id: "orders_scan",
          kind: "scan",
          convention: "provider:dbProvider",
          table: "orders",
          alias: "o",
          select: ["id", "user_id", "total_cents"],
          output: [{ name: "o.id" }, { name: "o.user_id" }, { name: "o.total_cents" }],
        },
        right: {
          id: "users_scan",
          kind: "scan",
          convention: "provider:dbProvider",
          table: "users",
          alias: "u",
          select: ["id", "email"],
          output: [{ name: "u.id" }, { name: "u.email" }],
        },
        leftKey: { alias: "o", column: "user_id" },
        rightKey: { alias: "u", column: "id" },
        output: [
          { name: "o.id" },
          { name: "o.user_id" },
          { name: "o.total_cents" },
          { name: "u.id" },
          { name: "u.email" },
        ],
      },
      orderBy: [{ source: { alias: "o", column: "total_cents" }, direction: "desc" }],
      output: [
        { name: "o.id" },
        { name: "o.user_id" },
        { name: "o.total_cents" },
        { name: "u.id" },
        { name: "u.email" },
      ],
    },
    columns: [
      { source: { alias: "o", column: "id" }, output: "id" },
      { source: { alias: "u", column: "email" }, output: "email" },
      { source: { alias: "o", column: "total_cents" }, output: "total_cents" },
    ],
    output: [{ name: "id" }, { name: "email" }, { name: "total_cents" }],
  };
}

function buildWithWindowRel(): RelNode {
  return {
    id: "with_1",
    kind: "with",
    convention: "provider:dbProvider",
    ctes: [
      {
        name: "vendor_totals",
        query: buildJoinProjectRel(),
      },
    ],
    body: {
      id: "project_2",
      kind: "project",
      convention: "provider:dbProvider",
      input: {
        id: "window_1",
        kind: "window",
        convention: "provider:dbProvider",
        input: {
          id: "scan_cte",
          kind: "scan",
          convention: "provider:dbProvider",
          table: "vendor_totals",
          alias: "vt",
          select: ["total_cents", "email"],
          output: [{ name: "total_cents" }, { name: "email" }],
        },
        functions: [
          {
            fn: "dense_rank",
            as: "spend_rank",
            partitionBy: [],
            orderBy: [{ source: { column: "total_cents" }, direction: "desc" }],
          },
        ],
        output: [{ name: "total_cents" }, { name: "email" }, { name: "spend_rank" }],
      },
      columns: [
        { source: { column: "email" }, output: "vendor_email" },
        { source: { column: "spend_rank" }, output: "spend_rank" },
      ],
      output: [{ name: "vendor_email" }, { name: "spend_rank" }],
    },
    output: [{ name: "vendor_email" }, { name: "spend_rank" }],
  };
}

function buildAggregateRel(
  metric:
    | Extract<RelNode, { kind: "aggregate" }>["metrics"][number]
    | Array<Extract<RelNode, { kind: "aggregate" }>["metrics"][number]> = [
    { fn: "count", as: "order_count" },
    {
      fn: "sum",
      column: { alias: "o", column: "total_cents" },
      as: "total_spend",
      distinct: true,
    },
  ],
): RelNode {
  const metrics = Array.isArray(metric) ? metric : [metric];
  return {
    id: "project_aggregate",
    kind: "project",
    convention: "provider:dbProvider",
    input: {
      id: "sort_aggregate",
      kind: "sort",
      convention: "provider:dbProvider",
      orderBy: [
        { source: { column: metrics[metrics.length - 1]?.as ?? "order_count" }, direction: "desc" },
      ],
      input: {
        id: "aggregate_orders",
        kind: "aggregate",
        convention: "provider:dbProvider",
        input: {
          id: "scan_orders",
          kind: "scan",
          convention: "provider:dbProvider",
          table: "orders",
          alias: "o",
          select: ["user_id", "total_cents"],
          output: [{ name: "o.user_id" }, { name: "o.total_cents" }],
        },
        groupBy: [{ alias: "o", column: "user_id" }],
        metrics,
        output: [{ name: "user_id" }, ...metrics.map((entry) => ({ name: entry.as }))],
      },
      output: [{ name: "user_id" }, ...metrics.map((entry) => ({ name: entry.as }))],
    },
    columns: [
      { source: { column: "user_id" }, output: "user_id" },
      ...metrics.map((entry) => ({ source: { column: entry.as }, output: entry.as })),
    ],
    output: [{ name: "user_id" }, ...metrics.map((entry) => ({ name: entry.as }))],
  };
}

describe("kysely adapter", () => {
  it("applies scoped base filters on scan and lookupMany", async () => {
    const { db, calls } = createMockKyselyDb(
      new Map<string, QueryRow[]>([
        [
          "orders_raw as orders_raw",
          [
            {
              "orders_raw.id": "o1",
              "orders_raw.user_id": "u1",
            },
          ],
        ],
      ]),
      new Map<string, QueryRow[]>(),
    );

    const provider = createKyselyProvider<{ orgId: string; db: typeof db }>({
      name: "dbProvider",
      db: (context: { orgId: string; db: typeof db }) => context.db,
      entities: {
        orders: {
          table: "orders_raw",
          base: ({ query, context, alias }) => query.where(`${alias}.org_id`, "=", context.orgId),
        },
      },
    });

    const scanRequest: TableScanRequest = {
      table: "orders",
      select: ["id", "user_id"],
      where: [{ op: "eq", column: "user_id", value: "u1" }],
    };

    const scanFragment: ProviderFragment = {
      kind: "scan",
      provider: "dbProvider",
      table: "orders",
      request: scanRequest,
    };
    const scanContext = { orgId: "org_1", db };
    const scanPlan = (await provider.compile(scanFragment, scanContext)).unwrap();
    const scanRows = (await provider.execute(scanPlan, scanContext)).unwrap();
    expect(scanRows).toEqual([{ id: "o1", user_id: "u1" }]);

    if (!provider.lookupMany) {
      throw new Error("Expected lookupMany to be implemented.");
    }

    await provider.lookupMany(
      {
        table: "orders",
        key: "id",
        keys: ["o1"],
        select: ["id"],
      },
      scanContext,
    );

    const whereColumns = calls.where.map((entry) => String(entry[0]));
    expect(whereColumns).toContain("orders_raw.org_id");
    expect(whereColumns).toContain("orders_raw.user_id");
    expect(whereColumns).toContain("orders_raw.id");
  });

  it("fails clearly when a context-resolved db binding is missing at runtime", async () => {
    const { db } = createMockKyselyDb(new Map(), new Map());
    const provider = createKyselyProvider<{ db?: typeof db }>({
      name: "dbProvider",
      db: (context: { db?: typeof db }) => context.db,
      entities: {
        orders: {
          table: "orders_raw",
        },
      },
    });

    const plan = (
      await provider.compile(
        {
          kind: "scan",
          provider: "dbProvider",
          table: "orders",
          request: {
            table: "orders",
            select: ["id"],
          },
        },
        {},
      )
    ).unwrap();

    await expect(
      Promise.resolve(provider.execute(plan, {})).then((result) => result.unwrap()),
    ).rejects.toThrow(
      "Kysely provider runtime binding did not resolve to a valid database instance.",
    );
  });

  it("executes supported rel fragments as a single query", async () => {
    const { db, calls } = createMockKyselyDb(
      new Map<string, QueryRow[]>([
        [
          "orders as o",
          [
            { "o.id": "o1", "o.user_id": "u1", "o.total_cents": 1000 },
            { "o.id": "o2", "o.user_id": "u1", "o.total_cents": 3000 },
          ],
        ],
      ]),
      new Map<string, QueryRow[]>([
        [
          "orders as o->users as u",
          [
            {
              "o.id": "o2",
              "o.user_id": "u1",
              "o.total_cents": 3000,
              "u.id": "u1",
              "u.email": "ada@example.com",
            },
            {
              "o.id": "o1",
              "o.user_id": "u1",
              "o.total_cents": 1000,
              "u.id": "u1",
              "u.email": "ada@example.com",
            },
          ],
        ],
      ]),
    );

    const provider = createKyselyProvider({
      name: "dbProvider",
      db,
      entities: {
        orders: { table: "orders" },
        users: { table: "users" },
      },
    });

    const relFragment: ProviderFragment = {
      kind: "rel",
      provider: "dbProvider",
      rel: buildJoinProjectRel(),
    };

    expect(provider.canExecute(relFragment, {})).toBe(true);
    const plan = (await provider.compile(relFragment, {})).unwrap();
    const rows = (await provider.execute(plan, {})).unwrap();

    expect(rows).toEqual([
      { id: "o2", email: "ada@example.com", total_cents: 3000 },
      { id: "o1", email: "ada@example.com", total_cents: 1000 },
    ]);
    expect(calls.executeCount).toBe(1);
  });

  it("reports unsupported rel shapes", () => {
    const { db } = createMockKyselyDb(new Map(), new Map());
    const provider = createKyselyProvider({
      db,
      entities: {
        orders: { table: "orders" },
      },
    });

    const withNode: RelNode = {
      id: "with_1",
      kind: "with",
      convention: "provider:kysely",
      ctes: [],
      body: {
        id: "scan_1",
        kind: "scan",
        convention: "provider:kysely",
        table: "orders",
        select: ["id"],
        output: [{ name: "id" }],
      },
      output: [{ name: "id" }],
    };

    const result = provider.canExecute(
      {
        kind: "rel",
        provider: "kysely",
        rel: withNode,
      },
      {},
    );

    expect(result).toEqual(
      expect.objectContaining({
        supported: false,
        routeFamily: "rel-advanced",
        requiredAtoms: expect.arrayContaining(["cte.non_recursive"]),
        reason: "Rel fragment is not supported for single-query Kysely pushdown.",
      }),
    );
  });

  it("accepts with+window rel fragments for single-query pushdown", () => {
    const { db } = createMockKyselyDb(new Map(), new Map());
    const provider = createKyselyProvider({
      db,
      entities: {
        orders: { table: "orders" },
        users: { table: "users" },
      },
    });

    const result = provider.canExecute(
      {
        kind: "rel",
        provider: "kysely",
        rel: buildWithWindowRel(),
      },
      {},
    );

    expect(result).toBe(true);
  });

  it("executes aggregate rel fragments with grouped metrics", async () => {
    const { db, calls } = createMockKyselyDb(
      new Map<string, QueryRow[]>([
        ["orders as o", [{ "o.user_id": "u1", order_count: 2, total_spend: 4500 }]],
      ]),
      new Map<string, QueryRow[]>(),
      { supportDistinct: true },
    );

    const provider = createKyselyProvider({
      name: "dbProvider",
      db,
      entities: {
        orders: { table: "orders" },
      },
    });

    const plan = (
      await provider.compile(
        {
          kind: "rel",
          provider: "dbProvider",
          rel: buildAggregateRel(),
        },
        {},
      )
    ).unwrap();
    const rows = (await provider.execute(plan, {})).unwrap();

    expect(rows).toEqual([{ user_id: "u1", order_count: 2, total_spend: 4500 }]);
    expect(calls.executeCount).toBe(1);
    expect(calls.orderBy).toHaveLength(1);
  });

  it("fails clearly when the expression builder lacks a required aggregate helper", async () => {
    const { db } = createMockKyselyDb(new Map(), new Map(), {
      omitFnHelpers: ["avg"],
    });
    const provider = createKyselyProvider({
      name: "dbProvider",
      db,
      entities: {
        orders: { table: "orders" },
      },
    });

    const plan = (
      await provider.compile(
        {
          kind: "rel",
          provider: "dbProvider",
          rel: buildAggregateRel({
            fn: "avg",
            column: { alias: "o", column: "total_cents" },
            as: "average_spend",
          }),
        },
        {},
      )
    ).unwrap();

    await expect(
      Promise.resolve(provider.execute(plan, {})).then((result) => result.unwrap()),
    ).rejects.toThrow("Unsupported aggregate function: avg.");
  });
});
