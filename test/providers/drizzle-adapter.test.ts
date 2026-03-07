import { describe, expect, it } from "vitest";

import { unwrapProviderOperationResult, type ProviderFragment, type QueryRow, type RelNode } from "../../src";
import {
  createDrizzleProvider,
  impossibleCondition,
  runDrizzleScan,
  type DrizzleQueryExecutor,
} from "../../packages/drizzle/src";

type TestColumn = { name: string };
type TestRow = Record<string, unknown>;

interface RecordingDbCalls {
  whereConditions: unknown[];
  orderByClauses: unknown[][];
  limitValues: number[];
  offsetValues: number[];
  executeCount?: number;
}

function createRecordingDb(rowsByTable: Map<object, TestRow[]>): {
  db: DrizzleQueryExecutor;
  calls: RecordingDbCalls;
} {
  const calls: RecordingDbCalls = {
    whereConditions: [],
    orderByClauses: [],
    limitValues: [],
    offsetValues: [],
  };

  const db: DrizzleQueryExecutor = {
    select(...args: unknown[]) {
      const selection = (args[0] ?? {}) as Record<string, { name?: string }>;
      let rows: TestRow[] = [];

      const builder = {
        from(table: object) {
          rows = [...(rowsByTable.get(table) ?? [])];
          return builder;
        },
        where(condition: unknown) {
          calls.whereConditions.push(condition);
          return builder;
        },
        orderBy(...clauses: unknown[]) {
          calls.orderByClauses.push(clauses);
          return builder;
        },
        limit(value: number) {
          calls.limitValues.push(value);
          rows = rows.slice(0, value);
          return builder;
        },
        offset(value: number) {
          calls.offsetValues.push(value);
          rows = rows.slice(value);
          return builder;
        },
        async execute() {
          return rows.map((row) => {
            const projected: QueryRow = {};
            for (const [outputName, column] of Object.entries(selection)) {
              const sourceName = column.name ?? outputName;
              projected[outputName] = row[sourceName] ?? null;
            }
            return projected;
          });
        },
      };

      return builder;
    },
  };

  return {
    db,
    calls,
  };
}

function createJoinCapableDb(
  rowsByTable: Map<object, TestRow[]>,
  rowsByJoin: Map<string, TestRow[]>,
): {
  db: DrizzleQueryExecutor;
  calls: RecordingDbCalls;
} {
  const calls: RecordingDbCalls = {
    whereConditions: [],
    orderByClauses: [],
    limitValues: [],
    offsetValues: [],
    executeCount: 0,
  };

  const db: DrizzleQueryExecutor = {
    select(...args: unknown[]) {
      const selection = (args[0] ?? {}) as Record<string, { name?: string }>;
      let rows: TestRow[] = [];
      let rootKey = "";

      const builder = {
        from(table: object) {
          rootKey = String((table as { name?: string }).name ?? "root");
          rows = [...(rowsByTable.get(table) ?? [])];
          return builder;
        },
        innerJoin(table: object) {
          const right = String((table as { name?: string }).name ?? "right");
          rows = [...(rowsByJoin.get(`${rootKey}->${right}`) ?? rows)];
          return builder;
        },
        leftJoin(table: object) {
          return builder.innerJoin(table);
        },
        rightJoin(table: object) {
          return builder.innerJoin(table);
        },
        fullJoin(table: object) {
          return builder.innerJoin(table);
        },
        where(condition: unknown) {
          calls.whereConditions.push(condition);
          return builder;
        },
        groupBy() {
          return builder;
        },
        orderBy(...clauses: unknown[]) {
          calls.orderByClauses.push(clauses);
          return builder;
        },
        limit(value: number) {
          calls.limitValues.push(value);
          rows = rows.slice(0, value);
          return builder;
        },
        offset(value: number) {
          calls.offsetValues.push(value);
          rows = rows.slice(value);
          return builder;
        },
        async execute() {
          calls.executeCount = (calls.executeCount ?? 0) + 1;
          return rows.map((row) => {
            const projected: QueryRow = {};
            for (const [outputName, column] of Object.entries(selection)) {
              const sourceName = column.name ?? outputName;
              projected[outputName] = row[sourceName] ?? null;
            }
            return projected;
          });
        },
      };

      return builder;
    },
  };

  return { db, calls };
}

function flattenSqlTokens(value: unknown): unknown[] {
  const out: unknown[] = [];

  const visit = (current: unknown): void => {
    if (Array.isArray(current)) {
      for (const entry of current) {
        visit(entry);
      }
      return;
    }

    if (!current || typeof current !== "object") {
      out.push(current);
      return;
    }

    const queryChunks = (current as { queryChunks?: unknown[] }).queryChunks;
    if (Array.isArray(queryChunks)) {
      visit(queryChunks);
      return;
    }

    const nestedValues = (current as { value?: unknown[] }).value;
    if (Array.isArray(nestedValues)) {
      visit(nestedValues);
      return;
    }

    out.push(current);
  };

  visit(value);
  return out;
}

describe("drizzle adapter", () => {
  it("exposes entity handles and reports unsupported fragment kinds", async () => {
    const usersTable = { name: "users_table" };
    const { db } = createRecordingDb(new Map<object, TestRow[]>());
    const idColumn: TestColumn = { name: "id" };

    const provider = createDrizzleProvider({
      name: "warehouse",
      db,
      tables: {
        users: {
          table: usersTable,
          columns: { id: idColumn as never },
        },
      },
    });

    expect(provider.entities.users).toEqual({
      kind: "data_entity",
      entity: "users",
      provider: "warehouse",
    });

    const scanFragment: ProviderFragment = {
      kind: "scan",
      provider: "warehouse",
      table: "users",
      request: {
        table: "users",
        select: ["id"],
      },
    };
    expect(provider.canExecute(scanFragment, {})).toBe(true);

    const unknownScan: ProviderFragment = {
      ...scanFragment,
      table: "missing",
      request: {
        table: "missing",
        select: ["id"],
      },
    };
    expect(provider.canExecute(unknownScan, {})).toBe(false);

    const sqlRel: RelNode = {
      id: "sql_1",
      kind: "sql",
      convention: "provider:warehouse",
      sql: "SELECT 1",
      tables: ["users"],
      output: [],
    };
    const relFragment: ProviderFragment = {
      kind: "rel",
      provider: "warehouse",
      rel: sqlRel,
    };
    expect(provider.canExecute(relFragment, {})).toEqual(expect.objectContaining({
      supported: false,
      routeFamily: "rel-core",
      reason: "rel fragment must not contain sql nodes.",
    }));
    await expect(Promise.resolve(provider.compile(relFragment, {})).then(unwrapProviderOperationResult)).rejects.toThrow(
      "Unsupported relational fragment for drizzle provider.",
    );

  });

  it("derives columns from the table object when columns are omitted", async () => {
    const usersTable = {
      id: { name: "id" },
      userId: { name: "user_id" },
      email: { name: "email" },
    };
    const { db } = createRecordingDb(
      new Map<object, TestRow[]>([
        [
          usersTable,
          [
            {
              id: "u1",
              user_id: "ext_1",
              email: "ada@example.com",
            },
          ],
        ],
      ]),
    );

    const provider = createDrizzleProvider({
      db,
      tables: {
        users: {
          table: usersTable,
        },
      },
    });

    const plan = unwrapProviderOperationResult(
      await provider.compile(
        {
          kind: "scan",
          provider: "drizzle",
          table: "users",
          request: {
            table: "users",
            select: ["user_id", "email"],
          },
        },
        {},
      ),
    );
    const rows = unwrapProviderOperationResult(await provider.execute(plan, {}));

    expect(rows).toEqual([
      {
        user_id: "ext_1",
        email: "ada@example.com",
      },
    ]);
  });

  it("throws when columns cannot be derived from table config", async () => {
    const usersTable = {};
    const { db } = createRecordingDb(new Map<object, TestRow[]>([[usersTable, []]]));
    const provider = createDrizzleProvider({
      db,
      tables: {
        users: {
          table: usersTable,
        },
      },
    });

    const plan = unwrapProviderOperationResult(
      await provider.compile(
        {
          kind: "scan",
          provider: "drizzle",
          table: "users",
          request: {
            table: "users",
            select: ["id"],
          },
        },
        {},
      ),
    );

    await expect(Promise.resolve(provider.execute(plan, {})).then(unwrapProviderOperationResult)).rejects.toThrow(
      'Unable to derive columns for table "users". Provide an explicit columns map.',
    );
  });

  it("applies where/orderBy/limit/offset in runDrizzleScan", async () => {
    const ordersTable = { name: "orders_table" };
    const idColumn: TestColumn = { name: "id" };
    const totalCentsColumn: TestColumn = { name: "total_cents" };
    const orgIdColumn: TestColumn = { name: "org_id" };

    const { db, calls } = createRecordingDb(
      new Map<object, TestRow[]>([
        [
          ordersTable,
          [
            { id: "o1", total_cents: 100, org_id: "org_1" },
            { id: "o2", total_cents: 200, org_id: "org_1" },
            { id: "o3", total_cents: 300, org_id: "org_2" },
          ],
        ],
      ]),
    );

    const rows = await runDrizzleScan<"orders", string>({
      db,
      tableName: "orders",
      table: ordersTable,
      columns: {
        id: idColumn as never,
        total_cents: totalCentsColumn as never,
        org_id: orgIdColumn as never,
      },
      scope: [impossibleCondition()],
      request: {
        table: "orders",
        select: ["id", "total_cents"],
        where: [
          {
            op: "eq",
            column: "org_id",
            value: "org_1",
          },
        ],
        orderBy: [{ column: "total_cents", direction: "desc" }],
        limit: 2,
        offset: 1,
      },
    });

    expect(calls.whereConditions.length).toBe(1);
    expect(calls.orderByClauses.length).toBe(1);
    expect(calls.orderByClauses[0]?.length).toBe(1);
    expect(calls.limitValues).toEqual([2]);
    expect(calls.offsetValues).toEqual([1]);
    expect(rows).toEqual([{ id: "o2", total_cents: 200 }]);
  });

  it("throws helpful errors for unsupported scan columns", async () => {
    const usersTable = { name: "users_table" };
    const idColumn: TestColumn = { name: "id" };
    const { db } = createRecordingDb(new Map<object, TestRow[]>([[usersTable, []]]));

    await expect(
      runDrizzleScan<"users", string>({
        db,
        tableName: "users",
        table: usersTable,
        columns: {
          id: idColumn as never,
        },
        request: {
          table: "users",
          select: ["missing"],
        },
      }),
    ).rejects.toThrow('Unsupported column "missing" for table "users".');

    await expect(
      runDrizzleScan<"users", string>({
        db,
        tableName: "users",
        table: usersTable,
        columns: {
          id: idColumn as never,
        },
        request: {
          table: "users",
          select: ["id"],
          where: [{ op: "eq", column: "missing", value: "x" }],
        },
      }),
    ).rejects.toThrow('Unsupported filter column "missing" for table "users".');

    await expect(
      runDrizzleScan<"users", string>({
        db,
        tableName: "users",
        table: usersTable,
        columns: {
          id: idColumn as never,
        },
        request: {
          table: "users",
          select: ["id"],
          orderBy: [{ column: "missing", direction: "asc" }],
        },
      }),
    ).rejects.toThrow('Unsupported ORDER BY column "missing" for table "users".');
  });

  it("lookupMany appends key IN predicate and validates table config", async () => {
    const usersTable = { name: "users_table" };
    const idColumn: TestColumn = { name: "id" };
    const emailColumn: TestColumn = { name: "email" };
    const { db, calls } = createRecordingDb(
      new Map<object, TestRow[]>([
        [
          usersTable,
          [
            { id: "u1", email: "ada@example.com" },
            { id: "u2", email: "ben@example.com" },
          ],
        ],
      ]),
    );

    const provider = createDrizzleProvider({
      db,
      tables: {
        users: {
          table: usersTable,
          columns: {
            id: idColumn as never,
            email: emailColumn as never,
          },
        },
      },
    });

    const lookupMany = provider.lookupMany;
    if (!lookupMany) {
      throw new Error("Expected drizzle lookupMany to be defined.");
    }

    await lookupMany(
      {
        table: "users",
        key: "id",
        keys: ["u1", "u3"],
        select: ["id", "email"],
        where: [{ op: "is_not_null", column: "email" }],
      },
      {},
    );

    expect(calls.whereConditions.length).toBe(1);
    const tokens = flattenSqlTokens(calls.whereConditions[0]);
    expect(tokens).toContain(idColumn);
    expect(tokens).toContain("u1");
    expect(tokens).toContain("u3");

    await expect(
      Promise.resolve(
        lookupMany(
          {
            table: "missing",
            key: "id",
            keys: ["u1"],
            select: ["id"],
          },
          {},
        ),
      ).then(unwrapProviderOperationResult),
    ).rejects.toThrow("Unknown drizzle table config: missing");
  });

  it("infers dialect from table metadata and rejects mixed dialect providers", () => {
    const pgTable = { name: "orders", _: { config: { dialect: "pg" } } };
    const sqliteTable = { name: "users", _: { config: { dialect: "sqlite" } } };
    const { db } = createRecordingDb(new Map<object, TestRow[]>());

    expect(() =>
      createDrizzleProvider({
        db,
        tables: {
          orders: { table: pgTable },
        },
      }),
    ).not.toThrow();

    expect(() =>
      createDrizzleProvider({
        db,
        tables: {
          orders: { table: pgTable },
          users: { table: sqliteTable },
        },
      }),
    ).toThrow("mixed dialects");
  });

  it("executes join rel fragments as a single downstream query when supported", async () => {
    const usersTable = {
      name: "users",
      id: { name: "id" },
      email: { name: "email" },
      _: { config: { dialect: "pg" } },
    };
    const ordersTable = {
      name: "orders",
      id: { name: "id" },
      user_id: { name: "user_id" },
      total_cents: { name: "total_cents" },
      _: { config: { dialect: "pg" } },
    };

    const { db, calls } = createJoinCapableDb(
      new Map<object, TestRow[]>([
        [usersTable, []],
        [ordersTable, []],
      ]),
      new Map<string, TestRow[]>([
        [
          "orders->users",
          [
            { id: "o2", email: "ada@example.com", total_cents: 3000, user_id: "u1" },
            { id: "o1", email: "ada@example.com", total_cents: 1500, user_id: "u1" },
          ],
        ],
      ]),
    );

    const provider = createDrizzleProvider({
      db,
      tables: {
        users: { table: usersTable },
        orders: { table: ordersTable },
      },
    });

    const rel: RelNode = {
      id: "project_1",
      kind: "project",
      convention: "provider:drizzle",
      input: {
        id: "join_1",
        kind: "join",
        convention: "provider:drizzle",
        joinType: "inner",
        left: {
          id: "scan_orders",
          kind: "scan",
          convention: "provider:drizzle",
          table: "orders",
          alias: "o",
          select: ["id", "user_id", "total_cents"],
          output: [],
        },
        right: {
          id: "scan_users",
          kind: "scan",
          convention: "provider:drizzle",
          table: "users",
          alias: "u",
          select: ["id", "email"],
          output: [],
        },
        leftKey: { alias: "o", column: "user_id" },
        rightKey: { alias: "u", column: "id" },
        output: [],
      },
      columns: [
        { source: { alias: "o", column: "id" }, output: "id" },
        { source: { alias: "u", column: "email" }, output: "email" },
        { source: { alias: "o", column: "total_cents" }, output: "total_cents" },
      ],
      output: [],
    };

    const plan = unwrapProviderOperationResult(
      await provider.compile(
        {
          kind: "rel",
          provider: "drizzle",
          rel,
        },
        {},
      ),
    );
    const rows = unwrapProviderOperationResult(await provider.execute(plan, {}));

    expect(rows).toEqual([
      { id: "o2", email: "ada@example.com", total_cents: 3000 },
      { id: "o1", email: "ada@example.com", total_cents: 1500 },
    ]);
    expect(calls.executeCount).toBe(1);
  });

  it("pushes down calculated projection rel fragments with filter and sort", async () => {
    const ordersTable = {
      name: "orders",
      id: { name: "id" },
      total_cents: { name: "total_cents" },
      _: { config: { dialect: "pg" } },
    };

    const { db, calls } = createRecordingDb(
      new Map<object, TestRow[]>([
        [
          ordersTable,
          [
            {
              id: "o1",
              total_cents: 25000,
              total_dollars: 250,
              is_large_order: false,
            },
          ],
        ],
      ]),
    );

    const provider = createDrizzleProvider({
      db,
      tables: {
        orders: { table: ordersTable },
      },
    });

    const rel: RelNode = {
      id: "project_1",
      kind: "project",
      convention: "provider:drizzle",
      input: {
        id: "sort_1",
        kind: "sort",
        convention: "provider:drizzle",
        input: {
          id: "filter_1",
          kind: "filter",
          convention: "provider:drizzle",
          input: {
            id: "scan_orders",
            kind: "scan",
            convention: "provider:drizzle",
            table: "orders",
            alias: "o",
            select: ["id", "total_cents"],
            output: [],
          },
          where: [
            {
              column: "total_dollars",
              op: "gte",
              value: 200,
            },
          ],
          output: [],
        },
        orderBy: [
          {
            source: { column: "total_dollars" },
            direction: "desc",
          },
          {
            source: { column: "id" },
            direction: "asc",
          },
        ],
        output: [],
      },
      columns: [
        { source: { alias: "o", column: "id" }, output: "id" },
        { source: { alias: "o", column: "total_cents" }, output: "total_cents" },
        {
          kind: "expr",
          expr: {
            kind: "function",
            name: "divide",
            args: [
              { kind: "column", ref: { alias: "o", column: "total_cents" } },
              { kind: "literal", value: 100 },
            ],
          },
          output: "total_dollars",
        },
        {
          kind: "expr",
          expr: {
            kind: "function",
            name: "gte",
            args: [
              { kind: "column", ref: { alias: "o", column: "total_cents" } },
              { kind: "literal", value: 30000 },
            ],
          },
          output: "is_large_order",
        },
      ],
      output: [],
    };

    expect(provider.canExecute({ kind: "rel", provider: "drizzle", rel }, {})).toBe(true);

    const plan = unwrapProviderOperationResult(
      await provider.compile(
        {
          kind: "rel",
          provider: "drizzle",
          rel,
        },
        {},
      ),
    );
    const rows = unwrapProviderOperationResult(await provider.execute(plan, {}));

    expect(rows).toEqual([
      { id: "o1", total_cents: 25000, total_dollars: 250, is_large_order: false },
    ]);
    expect(calls.whereConditions).toHaveLength(1);
    expect(calls.orderByClauses).toHaveLength(1);
  });
});
