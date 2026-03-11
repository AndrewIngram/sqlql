import { describe, it } from "vitest";

import { stringifyUnknownValue, type RelNode } from "@tupl/foundation";
import { type ProviderFragment } from "@tupl/provider-kit";
import {
  createProviderConformanceCases,
  type ProviderConformanceCase,
} from "@tupl/provider-kit/testing";
import type { QueryRow } from "@tupl/schema-model";
import {
  createDrizzleProvider,
  type DrizzleQueryExecutor,
} from "../../packages/provider-drizzle/src";
import { createKyselyProvider } from "../../packages/provider-kysely/src";
import {
  createObjectionProvider,
  type KnexLike,
  type KnexLikeQueryBuilder,
} from "../../packages/provider-objection/src";

function buildRel(): RelNode {
  return {
    id: "project_1",
    kind: "project",
    convention: "provider:warehouse",
    input: {
      id: "sort_1",
      kind: "sort",
      convention: "provider:warehouse",
      input: {
        id: "join_1",
        kind: "join",
        convention: "provider:warehouse",
        joinType: "inner",
        left: {
          id: "orders_scan",
          kind: "scan",
          convention: "provider:warehouse",
          table: "orders",
          alias: "o",
          select: ["id", "user_id", "total_cents"],
          output: [{ name: "o.id" }, { name: "o.user_id" }, { name: "o.total_cents" }],
        },
        right: {
          id: "users_scan",
          kind: "scan",
          convention: "provider:warehouse",
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

function createMockKyselyDb(
  rowsByTable: Map<string, QueryRow[]>,
  rowsByJoin: Map<string, QueryRow[]>,
): {
  selectFrom: (from: string) => any;
} {
  return {
    selectFrom(from: string) {
      const root = from;
      let rows = [...(rowsByTable.get(root) ?? [])];
      let projections: Array<{ source: string; output: string }> = [];

      const builder: any = {
        innerJoin(right: string) {
          rows = [...(rowsByJoin.get(`${root}->${right}`) ?? rows)];
          return builder;
        },
        leftJoin(right: string) {
          return builder.innerJoin(right);
        },
        rightJoin(right: string) {
          return builder.innerJoin(right);
        },
        fullJoin(right: string) {
          return builder.innerJoin(right);
        },
        where() {
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
        select(selectArg: unknown) {
          if (typeof selectArg === "function") {
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
                      return { kind: "count_all", output };
                    },
                  };
                },
              },
            };

            const expressions = selectArg(eb) as Array<Record<string, unknown>>;
            projections = expressions
              .filter((entry) => entry.kind === "ref")
              .map((entry) => ({
                source: entry.source as string,
                output: entry.output as string,
              }));
            return builder;
          }

          return builder;
        },
        async execute() {
          return rows.map((row) => {
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
}

function createMockDrizzleDb(
  rowsByTable: Map<object, QueryRow[]>,
  rowsByJoin: Map<string, QueryRow[]>,
): DrizzleQueryExecutor {
  const keyForTable = (table: unknown): string => {
    if (typeof table === "string") {
      return table;
    }
    if (table && typeof table === "object") {
      const named = (table as { name?: unknown }).name;
      if (typeof named === "string" && named.length > 0) {
        return named;
      }
    }
    return String(table);
  };

  return {
    select(...args: unknown[]) {
      const selection = (args[0] ?? {}) as Record<string, { name?: string }>;
      let rows: QueryRow[] = [];
      let sourceKey = "";
      const builder = {
        from(table: object) {
          rows = [...(rowsByTable.get(table) ?? [])];
          sourceKey = keyForTable(table);
          return builder;
        },
        innerJoin(table: unknown) {
          const rightKey = keyForTable(table);
          sourceKey = sourceKey ? `${sourceKey}->${rightKey}` : rightKey;
          rows = [...(rowsByJoin.get(sourceKey) ?? rows)];
          return builder;
        },
        leftJoin(table: unknown) {
          return builder.innerJoin(table);
        },
        rightJoin(table: unknown) {
          return builder.innerJoin(table);
        },
        fullJoin(table: unknown) {
          return builder.innerJoin(table);
        },
        where() {
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
        async execute() {
          return rows.map((row) => {
            const out: Record<string, unknown> = {};
            for (const [outputName, column] of Object.entries(selection)) {
              const source = column.name ?? outputName;
              out[outputName] = row[source] ?? null;
            }
            return out;
          });
        },
      };
      return builder;
    },
  };
}

function createMockObjectionKnex(rowsByJoin: Map<string, QueryRow[]>): KnexLike {
  const createBuilder = (sourceKey: string): KnexLikeQueryBuilder => {
    const keyParts = sourceKey.split(" as ");
    const tableName = keyParts[0] ?? sourceKey;

    let rows = [...(rowsByJoin.get(sourceKey) ?? [])];
    const projections: Array<{ output: string; source: string }> = [];
    let currentSourceKey = sourceKey;

    const executeRows = async (): Promise<QueryRow[]> => {
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
    };

    const builder: KnexLikeQueryBuilder & {
      __sourceKey?: string;
    } = {
      __sourceKey: currentSourceKey,
      clone() {
        return builder;
      },
      as(nextAlias: string) {
        currentSourceKey = `${tableName} as ${nextAlias}`;
        builder.__sourceKey = currentSourceKey;
        rows = [...(rowsByJoin.get(currentSourceKey) ?? rows)];
        return builder;
      },
      from(source: unknown) {
        if (
          source &&
          typeof source === "object" &&
          "__sourceKey" in (source as Record<string, unknown>)
        ) {
          currentSourceKey = stringifyUnknownValue(
            (source as { __sourceKey?: unknown }).__sourceKey,
            currentSourceKey,
          );
        } else if (typeof source === "string") {
          currentSourceKey = source;
        }
        builder.__sourceKey = currentSourceKey;
        rows = [...(rowsByJoin.get(currentSourceKey) ?? rows)];
        return builder;
      },
      innerJoin(table: unknown) {
        const rightKey =
          table && typeof table === "object" && "__sourceKey" in (table as Record<string, unknown>)
            ? stringifyUnknownValue((table as { __sourceKey?: unknown }).__sourceKey, "right")
            : stringifyUnknownValue(table, "right");
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
      where() {
        return builder;
      },
      whereIn() {
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
              source: stringifyUnknownValue(source, output),
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
      execute: executeRows,
    } as KnexLikeQueryBuilder & {
      __sourceKey?: string;
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
  } as KnexLike;
}

function buildScanFragment(providerName: string): Extract<ProviderFragment, { kind: "scan" }> {
  return {
    kind: "scan",
    provider: providerName,
    table: "orders",
    request: {
      table: "orders",
      select: ["id", "total_cents"],
    },
  };
}

function buildRelFragment(providerName: string): Extract<ProviderFragment, { kind: "rel" }> {
  return {
    kind: "rel",
    provider: providerName,
    rel: buildRel(),
  };
}

function registerConformanceSuite(title: string, cases: ProviderConformanceCase[]) {
  describe(title, () => {
    for (const testCase of cases) {
      it(testCase.name, () => testCase.run());
    }
  });
}

const expectedScanRows = [
  { id: "o1", total_cents: 1500 },
  { id: "o2", total_cents: 3000 },
  { id: "o3", total_cents: 700 },
];

const qualifiedOrdersScanRows = [
  { "orders.id": "o1", "orders.total_cents": 1500 },
  { "orders.id": "o2", "orders.total_cents": 3000 },
  { "orders.id": "o3", "orders.total_cents": 700 },
];

const expectedRelRows = [
  { id: "o2", email: "ada@example.com", total_cents: 3000 },
  { id: "o1", email: "ada@example.com", total_cents: 1500 },
  { id: "o3", email: "ben@example.com", total_cents: 700 },
];

describe("provider conformance", () => {
  const usersTable = { name: "users" };
  const ordersTable = { name: "orders" };

  registerConformanceSuite(
    "drizzle provider",
    createProviderConformanceCases({
      provider: createDrizzleProvider({
        db: createMockDrizzleDb(
          new Map<object, QueryRow[]>([
            [
              usersTable,
              [
                { id: "u1", email: "ada@example.com" },
                { id: "u2", email: "ben@example.com" },
              ],
            ],
            [
              ordersTable,
              [
                { id: "o1", user_id: "u1", total_cents: 1500 },
                { id: "o2", user_id: "u1", total_cents: 3000 },
                { id: "o3", user_id: "u2", total_cents: 700 },
              ],
            ],
          ]),
          new Map<string, QueryRow[]>([
            [
              "orders->users",
              [
                { id: "o2", user_id: "u1", total_cents: 3000, email: "ada@example.com" },
                { id: "o1", user_id: "u1", total_cents: 1500, email: "ada@example.com" },
                { id: "o3", user_id: "u2", total_cents: 700, email: "ben@example.com" },
              ],
            ],
          ]),
        ),
        tables: {
          users: {
            table: usersTable,
            columns: {
              id: { name: "id" } as any,
              email: { name: "email" } as any,
            },
          },
          orders: {
            table: ordersTable,
            columns: {
              id: { name: "id" } as any,
              user_id: { name: "user_id" } as any,
              total_cents: { name: "total_cents" } as any,
            },
          },
        },
      }),
      context: {},
      scan: {
        fragment: buildScanFragment("drizzle"),
        expectedRows: expectedScanRows,
      },
      rel: {
        fragment: buildRelFragment("drizzle"),
        expectedRows: expectedRelRows,
      },
    }),
  );

  registerConformanceSuite(
    "kysely provider",
    createProviderConformanceCases({
      provider: createKyselyProvider({
        db: createMockKyselyDb(
          new Map<string, QueryRow[]>([
            ["orders as orders", qualifiedOrdersScanRows],
            [
              "users as users",
              [
                { "users.id": "u1", "users.email": "ada@example.com" },
                { "users.id": "u2", "users.email": "ben@example.com" },
              ],
            ],
            [
              "orders as o",
              [
                { "o.id": "o1", "o.user_id": "u1", "o.total_cents": 1500 },
                { "o.id": "o2", "o.user_id": "u1", "o.total_cents": 3000 },
                { "o.id": "o3", "o.user_id": "u2", "o.total_cents": 700 },
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
                  "o.total_cents": 1500,
                  "u.id": "u1",
                  "u.email": "ada@example.com",
                },
                {
                  "o.id": "o3",
                  "o.user_id": "u2",
                  "o.total_cents": 700,
                  "u.id": "u2",
                  "u.email": "ben@example.com",
                },
              ],
            ],
          ]),
        ),
        entities: {
          orders: { table: "orders" },
          users: { table: "users" },
        },
      }),
      context: {},
      scan: {
        fragment: buildScanFragment("kysely"),
        expectedRows: expectedScanRows,
      },
      rel: {
        fragment: buildRelFragment("kysely"),
        expectedRows: expectedRelRows,
      },
    }),
  );

  registerConformanceSuite(
    "objection provider",
    createProviderConformanceCases({
      provider: (() => {
        const knex = createMockObjectionKnex(
          new Map<string, QueryRow[]>([
            ["orders", qualifiedOrdersScanRows],
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
                  "o.total_cents": 1500,
                  "u.id": "u1",
                  "u.email": "ada@example.com",
                },
                {
                  "o.id": "o3",
                  "o.user_id": "u2",
                  "o.total_cents": 700,
                  "u.id": "u2",
                  "u.email": "ben@example.com",
                },
              ],
            ],
          ]),
        );

        return createObjectionProvider({
          knex,
          entities: {
            orders: {
              table: "orders",
              base: () => knex.table("orders"),
            },
            users: {
              table: "users",
              base: () => knex.table("users"),
            },
          },
        });
      })(),
      context: {},
      scan: {
        fragment: buildScanFragment("objection"),
        expectedRows: expectedScanRows,
      },
      rel: {
        fragment: buildRelFragment("objection"),
        expectedRows: expectedRelRows,
      },
    }),
  );
});
