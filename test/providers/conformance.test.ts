import Database from "better-sqlite3";
import { describe, expect, it } from "vitest";

import type { ProviderAdapter, ProviderFragment, RelNode } from "../../src";
import { createDrizzleProvider } from "../../packages/drizzle/src";
import { createKyselyProvider } from "../../packages/kysely/src";
import { createObjectionProvider } from "../../packages/objection/src";

function seedDatabase(): Database.Database {
  const sqlite = new Database(":memory:");
  sqlite.exec(`
    CREATE TABLE users (
      id TEXT PRIMARY KEY NOT NULL,
      email TEXT NOT NULL
    );
    CREATE TABLE orders (
      id TEXT PRIMARY KEY NOT NULL,
      user_id TEXT NOT NULL,
      total_cents INTEGER NOT NULL
    );
    INSERT INTO users (id, email) VALUES
      ('u1', 'ada@example.com'),
      ('u2', 'ben@example.com');
    INSERT INTO orders (id, user_id, total_cents) VALUES
      ('o1', 'u1', 1500),
      ('o2', 'u1', 3000),
      ('o3', 'u2', 700);
  `);
  return sqlite;
}

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
          where: [{ op: "gte", column: "total_cents", value: 1000 }],
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

async function runRelFragment(
  providerName: string,
  provider: ProviderAdapter<object>,
): Promise<Array<Record<string, unknown>>> {
  const rel = buildRel();
  const fragment: ProviderFragment = {
    kind: "rel",
    provider: providerName,
    rel,
  };

  const canExecute = await provider.canExecute(fragment, {});
  expect(typeof canExecute === "boolean" ? canExecute : canExecute.supported).toBe(true);

  const compiled = await provider.compile(fragment, {});
  return provider.execute(compiled, {}) as Promise<Array<Record<string, unknown>>>;
}

describe("provider conformance (rel fragments)", () => {
  it("returns equivalent rows for drizzle, objection, and kysely providers", async () => {
    const expected = [
      { id: "o2", email: "ada@example.com", total_cents: 3000 },
      { id: "o1", email: "ada@example.com", total_cents: 1500 },
    ];

    {
      const sqlite = seedDatabase();
      try {
        const drizzleProvider = createDrizzleProvider({
          db: { select: () => ({}) },
          executeSql: async (sqlText) => sqlite.prepare(sqlText).all() as Array<Record<string, unknown>>,
          tables: {
            users: { table: {}, columns: {} as any },
            orders: { table: {}, columns: {} as any },
          },
        });

        const rows = await runRelFragment("drizzle", drizzleProvider);
        expect(rows).toEqual(expected);
      } finally {
        sqlite.close();
      }
    }

    {
      const sqlite = seedDatabase();
      try {
        const objectionProvider = createObjectionProvider({
          knex: {
            table() {
              throw new Error("table() is not used in rel conformance test.");
            },
            async raw(sqlText: string) {
              return {
                rows: sqlite.prepare(sqlText).all(),
              };
            },
          },
        });

        const rows = await runRelFragment("objection", objectionProvider);
        expect(rows).toEqual(expected);
      } finally {
        sqlite.close();
      }
    }

    {
      const sqlite = seedDatabase();
      try {
        const kyselyProvider = createKyselyProvider({
          executor: {
            async executeSql(args) {
              return sqlite.prepare(args.sql).all() as Array<Record<string, unknown>>;
            },
          },
        });

        const rows = await runRelFragment("kysely", kyselyProvider);
        expect(rows).toEqual(expected);
      } finally {
        sqlite.close();
      }
    }
  });
});
