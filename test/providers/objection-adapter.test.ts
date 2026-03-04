import { describe, expect, it } from "vitest";

import type { ProviderFragment, QueryRow, RelNode, TableScanRequest } from "../../src";
import { createObjectionProvider, type KnexLike, type KnexLikeQueryBuilder } from "../../packages/objection/src";

interface ObjectionCalls {
  where: unknown[][];
  whereIn: unknown[][];
  executeCount: number;
  baseContexts: string[];
}

function createMockKnex(
  rowsBySource: Map<string, QueryRow[]>,
  rowsByJoin: Map<string, QueryRow[]>,
  calls: ObjectionCalls,
): KnexLike {
  const createBuilder = (sourceKey: string): KnexLikeQueryBuilder => {
    const keyParts = sourceKey.split(" as ");
    const tableName = keyParts[0] ?? sourceKey;
    let currentSourceKey = sourceKey;
    let rows = [...(rowsBySource.get(sourceKey) ?? [])];
    const projections: Array<{ output: string; source: string }> = [];

    const builder: KnexLikeQueryBuilder & {
      __sourceKey?: string;
      execute: () => Promise<QueryRow[]>;
    } = {
      __sourceKey: currentSourceKey,
      clone() {
        return builder;
      },
      as(alias: string) {
        currentSourceKey = `${tableName} as ${alias}`;
        builder.__sourceKey = currentSourceKey;
        rows = [...(rowsBySource.get(currentSourceKey) ?? rows)];
        return builder;
      },
      from(source: unknown) {
        if (source && typeof source === "object" && "__sourceKey" in (source as Record<string, unknown>)) {
          currentSourceKey = String((source as { __sourceKey?: unknown }).__sourceKey ?? currentSourceKey);
        } else if (typeof source === "string") {
          currentSourceKey = source;
        } else if (source && typeof source === "object") {
          const entries = Object.entries(source as Record<string, unknown>);
          const first = entries[0];
          if (first) {
            const alias = first[0];
            const table = String(first[1] ?? "");
            currentSourceKey = `${table} as ${alias}`;
          }
        }
        builder.__sourceKey = currentSourceKey;
        rows = [...(rowsBySource.get(currentSourceKey) ?? rows)];
        return builder;
      },
      innerJoin(table: unknown) {
        const rightKey =
          table && typeof table === "object" && "__sourceKey" in (table as Record<string, unknown>)
            ? String((table as { __sourceKey?: unknown }).__sourceKey ?? "right")
            : String(table ?? "right");
        rows = [...(rowsByJoin.get(`${currentSourceKey}->${rightKey}`) ?? rows)];
        return builder;
      },
      leftJoin(table: unknown) {
        return builder.innerJoin?.(table, "", "") as KnexLikeQueryBuilder;
      },
      rightJoin(table: unknown) {
        return builder.innerJoin?.(table, "", "") as KnexLikeQueryBuilder;
      },
      fullJoin(table: unknown) {
        return builder.innerJoin?.(table, "", "") as KnexLikeQueryBuilder;
      },
      where(...args: unknown[]) {
        calls.where.push(args);
        return builder;
      },
      whereIn(...args: unknown[]) {
        calls.whereIn.push(args);
        return builder;
      },
      whereNull() {
        return builder;
      },
      whereNotNull() {
        return builder;
      },
      clearSelect() {
        projections.length = 0;
        return builder;
      },
      select(columnMap: unknown) {
        if (columnMap && typeof columnMap === "object" && !Array.isArray(columnMap)) {
          for (const [output, source] of Object.entries(columnMap as Record<string, unknown>)) {
            projections.push({
              output,
              source: String(source ?? output),
            });
          }
        }
        return builder;
      },
      groupBy() {
        return builder;
      },
      orderBy() {
        return builder;
      },
      limit(value: number) {
        rows = rows.slice(0, value);
        return builder;
      },
      offset(value: number) {
        rows = rows.slice(value);
        return builder;
      },
      count() {
        return builder;
      },
      countDistinct() {
        return builder;
      },
      sum() {
        return builder;
      },
      avg() {
        return builder;
      },
      min() {
        return builder;
      },
      max() {
        return builder;
      },
      async execute() {
        calls.executeCount += 1;
        if (projections.length === 0) {
          return rows;
        }

        return rows.map((row) => {
          const out: QueryRow = {};
          for (const projection of projections) {
            out[projection.output] = row[projection.source] ?? null;
          }
          return out;
        });
      },
    } as KnexLikeQueryBuilder & {
      __sourceKey?: string;
      execute: () => Promise<QueryRow[]>;
    };
    return builder;
  };

  return {
    table(name: string) {
      return createBuilder(name);
    },
    queryBuilder() {
      return createBuilder("query");
    },
  };
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

describe("objection adapter", () => {
  it("applies scoped base queries on scan and lookupMany", async () => {
    const calls: ObjectionCalls = {
      where: [],
      whereIn: [],
      executeCount: 0,
      baseContexts: [],
    };
    const knex = createMockKnex(
      new Map<string, QueryRow[]>([
        [
          "orders",
          [{ "orders.id": "o1", "orders.user_id": "u1" }],
        ],
      ]),
      new Map<string, QueryRow[]>(),
      calls,
    );

    const provider = createObjectionProvider<{ orgId: string }>({
      name: "dbProvider",
      knex,
      entities: {
        orders: {
          table: "orders",
          base: (context) => {
            calls.baseContexts.push(context.orgId);
            return knex.table("orders").where("orders.org_id", "=", context.orgId);
          },
        },
      },
    });

    const scanFragment: ProviderFragment = {
      kind: "scan",
      provider: "dbProvider",
      table: "orders",
      request: {
        table: "orders",
        select: ["id", "user_id"],
      } satisfies TableScanRequest,
    };
    const scanPlan = await provider.compile(scanFragment, { orgId: "org_1" });
    const rows = await provider.execute(scanPlan, { orgId: "org_1" });
    expect(rows).toEqual([{ id: "o1", user_id: "u1" }]);

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
      { orgId: "org_1" },
    );

    expect(calls.baseContexts).toEqual(["org_1", "org_1"]);
    expect(calls.whereIn.some((entry) => String(entry[0]) === "orders.id")).toBe(true);
  });

  it("preserves scoped roots for both sides of joined rel fragments", async () => {
    const calls: ObjectionCalls = {
      where: [],
      whereIn: [],
      executeCount: 0,
      baseContexts: [],
    };
    const knex = createMockKnex(
      new Map<string, QueryRow[]>(),
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
          ],
        ],
      ]),
      calls,
    );

    const provider = createObjectionProvider<{ orgId: string }>({
      name: "dbProvider",
      knex,
      entities: {
        orders: {
          table: "orders",
          base: (context) => {
            calls.baseContexts.push(`orders:${context.orgId}`);
            return knex.table("orders").where("orders.org_id", "=", context.orgId);
          },
        },
        users: {
          table: "users",
          base: (context) => {
            calls.baseContexts.push(`users:${context.orgId}`);
            return knex.table("users").where("users.org_id", "=", context.orgId);
          },
        },
      },
    });

    const relFragment: ProviderFragment = {
      kind: "rel",
      provider: "dbProvider",
      rel: buildJoinProjectRel(),
    };
    const plan = await provider.compile(relFragment, { orgId: "org_1" });
    const rows = await provider.execute(plan, { orgId: "org_1" });

    expect(rows).toEqual([{ id: "o2", email: "ada@example.com", total_cents: 3000 }]);
    expect(calls.executeCount).toBe(1);
    expect(calls.baseContexts).toEqual(["orders:org_1", "users:org_1"]);
  });

  it("reports unsupported rel shapes", () => {
    const calls: ObjectionCalls = {
      where: [],
      whereIn: [],
      executeCount: 0,
      baseContexts: [],
    };
    const knex = createMockKnex(new Map(), new Map(), calls);
    const provider = createObjectionProvider({
      knex,
      entities: {
        orders: { table: "orders", base: () => knex.table("orders") },
      },
    });

    const withNode: RelNode = {
      id: "with_1",
      kind: "with",
      convention: "provider:objection",
      ctes: [],
      body: {
        id: "scan_1",
        kind: "scan",
        convention: "provider:objection",
        table: "orders",
        select: ["id"],
        output: [{ name: "id" }],
      },
      output: [{ name: "id" }],
    };

    const result = provider.canExecute({
      kind: "rel",
      provider: "objection",
      rel: withNode,
    }, {});

    expect(result).toEqual({
      supported: false,
      reason: "Rel fragment is not supported for single-query Objection pushdown.",
    });
  });

  it("accepts with+window rel fragments for single-query pushdown", () => {
    const calls: ObjectionCalls = {
      where: [],
      whereIn: [],
      executeCount: 0,
      baseContexts: [],
    };
    const knex = createMockKnex(new Map(), new Map(), calls);
    const provider = createObjectionProvider({
      knex,
      entities: {
        orders: { table: "orders", base: () => knex.table("orders") },
        users: { table: "users", base: () => knex.table("users") },
      },
    });

    const result = provider.canExecute({
      kind: "rel",
      provider: "objection",
      rel: buildWithWindowRel(),
    }, {});

    expect(result).toBe(true);
  });
});
