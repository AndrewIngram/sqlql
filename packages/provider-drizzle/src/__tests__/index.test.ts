import { describe, expect, it } from "vitest";

import { stringifyUnknownValue } from "@tupl/core";
import { type ProviderFragment } from "@tupl/core/provider";
import type { RelNode } from "@tupl/core/model/rel";
import type { QueryRow } from "@tupl/core/schema";
import {
  createDrizzleProvider,
  impossibleCondition,
  runDrizzleScan,
  type DrizzleQueryExecutor,
} from "../index";

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

function projectSelectionRows(selection: Record<string, unknown>, rows: TestRow[]): QueryRow[] {
  return rows.map((row) => {
    const projected: QueryRow = {};
    for (const [outputName, column] of Object.entries(selection)) {
      const sourceName =
        column &&
        typeof column === "object" &&
        "name" in column &&
        typeof (column as { name?: unknown }).name === "string"
          ? String((column as { name: string }).name)
          : outputName;
      projected[outputName] = row[sourceName] ?? null;
    }
    return projected;
  });
}

function stableTestRowKey(row: TestRow): string {
  return JSON.stringify(Object.entries(row).sort(([left], [right]) => left.localeCompare(right)));
}

function createSetOpCapableDb(rowsByTable: Map<object, TestRow[]>): {
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
      const selection = (args[0] ?? {}) as Record<string, unknown>;
      let rows: TestRow[] = [];

      const builder = {
        __rows: rows,
        from(table: object) {
          rows = [...(rowsByTable.get(table) ?? [])];
          builder.__rows = rows;
          return builder;
        },
        unionAll(right: { __rows?: TestRow[] }) {
          rows = [...rows, ...(right.__rows ?? [])];
          builder.__rows = rows;
          return builder;
        },
        union(right: { __rows?: TestRow[] }) {
          rows = [
            ...new Map(
              [...rows, ...(right.__rows ?? [])].map((row) => [stableTestRowKey(row), row]),
            ).values(),
          ];
          builder.__rows = rows;
          return builder;
        },
        intersect(right: { __rows?: TestRow[] }) {
          const rightKeys = new Set((right.__rows ?? []).map((row) => stableTestRowKey(row)));
          rows = rows.filter((row) => rightKeys.has(stableTestRowKey(row)));
          builder.__rows = rows;
          return builder;
        },
        except(right: { __rows?: TestRow[] }) {
          const rightKeys = new Set((right.__rows ?? []).map((row) => stableTestRowKey(row)));
          rows = rows.filter((row) => !rightKeys.has(stableTestRowKey(row)));
          builder.__rows = rows;
          return builder;
        },
        orderBy(...clauses: unknown[]) {
          calls.orderByClauses.push(clauses);
          return builder;
        },
        limit(value: number) {
          calls.limitValues.push(value);
          rows = rows.slice(0, value);
          builder.__rows = rows;
          return builder;
        },
        offset(value: number) {
          calls.offsetValues.push(value);
          rows = rows.slice(value);
          builder.__rows = rows;
          return builder;
        },
        async execute() {
          calls.executeCount = (calls.executeCount ?? 0) + 1;
          return projectSelectionRows(selection, rows);
        },
      };

      return builder;
    },
  };

  return { db, calls };
}

function createWithCapableDb(
  rowsByTable: Map<object, TestRow[]>,
  rowsByCte: Map<string, TestRow[]>,
): {
  db: DrizzleQueryExecutor & {
    $with: (name: string) => { as: (_query: unknown) => Record<string, unknown> };
    with: (...ctes: unknown[]) => {
      select: (selection: Record<string, unknown>) => {
        from: (source: unknown) => {
          where: (condition: unknown) => unknown;
          orderBy: (...clauses: unknown[]) => unknown;
          limit: (value: number) => unknown;
          offset: (value: number) => unknown;
          execute: () => Promise<QueryRow[]>;
        };
      };
    };
  };
  calls: RecordingDbCalls;
} {
  const calls: RecordingDbCalls = {
    whereConditions: [],
    orderByClauses: [],
    limitValues: [],
    offsetValues: [],
    executeCount: 0,
  };

  const db = {
    select(...args: unknown[]) {
      const selection = (args[0] ?? {}) as Record<string, unknown>;
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
          calls.executeCount = (calls.executeCount ?? 0) + 1;
          return projectSelectionRows(selection, rows);
        },
      };

      return builder;
    },
    $with(name: string) {
      return {
        as() {
          return {
            __sourceKey: name,
            id: { name: "id" },
            total_cents: { name: "total_cents" },
            spend_rank: { name: "spend_rank" },
          };
        },
      };
    },
    with(..._ctes: unknown[]) {
      return {
        select(selection: Record<string, unknown>) {
          return {
            from(source: unknown) {
              const sourceKey =
                source && typeof source === "object" && "__sourceKey" in (source as object)
                  ? stringifyUnknownValue((source as { __sourceKey?: unknown }).__sourceKey)
                  : stringifyUnknownValue(source);
              let rows = [...(rowsByCte.get(sourceKey) ?? [])];

              const builder = {
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
                  calls.executeCount = (calls.executeCount ?? 0) + 1;
                  return projectSelectionRows(selection, rows);
                },
              };

              return builder;
            },
          };
        },
      };
    },
  } satisfies DrizzleQueryExecutor & {
    $with: (name: string) => { as: (_query: unknown) => Record<string, unknown> };
    with: (...ctes: unknown[]) => {
      select: (selection: Record<string, unknown>) => {
        from: (source: unknown) => {
          where: (condition: unknown) => unknown;
          orderBy: (...clauses: unknown[]) => unknown;
          limit: (value: number) => unknown;
          offset: (value: number) => unknown;
          execute: () => Promise<QueryRow[]>;
        };
      };
    };
  };

  return { db, calls };
}

function createDbExecuteFallbackDb(rowsByTable: Map<object, TestRow[]>): {
  db: DrizzleQueryExecutor & { execute: (query: unknown) => Promise<QueryRow[]> };
  calls: RecordingDbCalls;
} {
  const calls: RecordingDbCalls = {
    whereConditions: [],
    orderByClauses: [],
    limitValues: [],
    offsetValues: [],
    executeCount: 0,
  };

  const db = {
    select(...args: unknown[]) {
      const selection = (args[0] ?? {}) as Record<string, unknown>;
      let rows: TestRow[] = [];

      const builder = {
        __rows: rows,
        __selection: selection,
        from(table: object) {
          rows = [...(rowsByTable.get(table) ?? [])];
          builder.__rows = rows;
          return builder;
        },
        getSQL() {
          return { queryChunks: ["select"] };
        },
      };

      return builder;
    },
    async execute(query: unknown) {
      calls.executeCount = (calls.executeCount ?? 0) + 1;
      const builder = query as {
        __rows?: TestRow[];
        __selection?: Record<string, unknown>;
      };
      return projectSelectionRows(builder.__selection ?? {}, builder.__rows ?? []);
    },
  } satisfies DrizzleQueryExecutor & { execute: (query: unknown) => Promise<QueryRow[]> };

  return { db, calls };
}

function buildAggregateRel(): RelNode {
  return {
    id: "project_aggregate",
    kind: "project",
    convention: "provider:drizzle",
    input: {
      id: "sort_aggregate",
      kind: "sort",
      convention: "provider:drizzle",
      orderBy: [{ source: { column: "total_spend" }, direction: "desc" }],
      input: {
        id: "aggregate_orders",
        kind: "aggregate",
        convention: "provider:drizzle",
        input: {
          id: "scan_orders",
          kind: "scan",
          convention: "provider:drizzle",
          table: "orders",
          alias: "o",
          select: ["user_id", "total_cents"],
          output: [],
        },
        groupBy: [{ alias: "o", column: "user_id" }],
        metrics: [
          { fn: "count", as: "order_count" },
          {
            fn: "sum",
            column: { alias: "o", column: "total_cents" },
            as: "total_spend",
            distinct: true,
          },
        ],
        output: [],
      },
      output: [],
    },
    columns: [
      { source: { column: "user_id" }, output: "user_id" },
      { source: { column: "order_count" }, output: "order_count" },
      { source: { column: "total_spend" }, output: "total_spend" },
    ],
    output: [],
  };
}

function buildSetOpRel(): RelNode {
  return {
    id: "limit_set_op",
    kind: "limit_offset",
    convention: "provider:drizzle",
    limit: 2,
    input: {
      id: "sort_set_op",
      kind: "sort",
      convention: "provider:drizzle",
      orderBy: [{ source: { column: "id" }, direction: "asc" }],
      input: {
        id: "union_orders",
        kind: "set_op",
        convention: "provider:drizzle",
        op: "union_all",
        left: {
          id: "project_left",
          kind: "project",
          convention: "provider:drizzle",
          input: {
            id: "scan_left",
            kind: "scan",
            convention: "provider:drizzle",
            table: "orders_west",
            alias: "ow",
            select: ["id"],
            output: [],
          },
          columns: [{ source: { alias: "ow", column: "id" }, output: "id" }],
          output: [],
        },
        right: {
          id: "project_right",
          kind: "project",
          convention: "provider:drizzle",
          input: {
            id: "scan_right",
            kind: "scan",
            convention: "provider:drizzle",
            table: "orders_east",
            alias: "oe",
            select: ["id"],
            output: [],
          },
          columns: [{ source: { alias: "oe", column: "id" }, output: "id" }],
          output: [],
        },
        output: [],
      },
      output: [],
    },
    output: [],
  };
}

function buildWithWindowRel(): RelNode {
  return {
    id: "with_ranked_orders",
    kind: "with",
    convention: "provider:drizzle",
    ctes: [
      {
        name: "ranked_orders",
        query: {
          id: "project_orders",
          kind: "project",
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
          columns: [
            { source: { alias: "o", column: "id" }, output: "id" },
            { source: { alias: "o", column: "total_cents" }, output: "total_cents" },
          ],
          output: [],
        },
      },
    ],
    body: {
      id: "limit_ranked_orders",
      kind: "limit_offset",
      convention: "provider:drizzle",
      limit: 1,
      input: {
        id: "sort_ranked_orders",
        kind: "sort",
        convention: "provider:drizzle",
        orderBy: [{ source: { column: "spend_rank" }, direction: "asc" }],
        input: {
          id: "project_ranked_orders",
          kind: "project",
          convention: "provider:drizzle",
          input: {
            id: "window_ranked_orders",
            kind: "window",
            convention: "provider:drizzle",
            input: {
              id: "scan_ranked_orders",
              kind: "scan",
              convention: "provider:drizzle",
              table: "ranked_orders",
              alias: "ro",
              select: ["id", "total_cents"],
              output: [],
            },
            functions: [
              {
                fn: "dense_rank",
                as: "spend_rank",
                partitionBy: [],
                orderBy: [{ source: { column: "total_cents" }, direction: "desc" }],
              },
            ],
            output: [],
          },
          columns: [
            { source: { column: "id" }, output: "id" },
            { source: { column: "spend_rank" }, output: "spend_rank" },
          ],
          output: [],
        },
        output: [],
      },
      output: [],
    },
    output: [],
  };
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
    expect(provider.canExecute(relFragment, {})).toEqual(
      expect.objectContaining({
        supported: false,
        routeFamily: "rel-core",
        reason: "rel fragment must not contain sql nodes.",
      }),
    );
    await expect(
      Promise.resolve(provider.compile(relFragment, {})).then((result) => result.unwrap()),
    ).rejects.toThrow("Unsupported relational fragment for drizzle provider.");
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

    const plan = (
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
      )
    ).unwrap();
    const rows = (await provider.execute(plan, {})).unwrap();

    expect(rows).toEqual([
      {
        user_id: "ext_1",
        email: "ada@example.com",
      },
    ]);
  });

  it("supports context-resolved db bindings for scan execution", async () => {
    const usersTable = { name: "users_table" };
    const idColumn: TestColumn = { name: "id" };
    const { db } = createRecordingDb(new Map<object, TestRow[]>([[usersTable, [{ id: "u1" }]]]));

    const provider = createDrizzleProvider<{ db: DrizzleQueryExecutor }>({
      dialect: "sqlite",
      db: (context) => context.db,
      tables: {
        users: {
          table: usersTable,
          columns: { id: idColumn as never },
        },
      },
    });

    const plan = (
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
        { db },
      )
    ).unwrap();
    const rows = (await provider.execute(plan, { db })).unwrap();

    expect(rows).toEqual([{ id: "u1" }]);
  });

  it("fails clearly when a context-resolved db binding is missing at runtime", async () => {
    const usersTable = { name: "users_table" };
    const idColumn: TestColumn = { name: "id" };

    const provider = createDrizzleProvider<{ db?: DrizzleQueryExecutor }>({
      dialect: "sqlite",
      db: (context) => context.db as DrizzleQueryExecutor,
      tables: {
        users: {
          table: usersTable,
          columns: { id: idColumn as never },
        },
      },
    });

    const plan = (
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
      )
    ).unwrap();

    await expect(
      Promise.resolve(provider.execute(plan, {})).then((result) => result.unwrap()),
    ).rejects.toThrow(
      "Drizzle provider runtime binding did not resolve to a valid database instance.",
    );
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

    const plan = (
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
      )
    ).unwrap();

    await expect(
      Promise.resolve(provider.execute(plan, {})).then((result) => result.unwrap()),
    ).rejects.toThrow(
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

    const lookupMany = provider.lookupMany?.bind(provider);
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
      ).then((result) => result.unwrap()),
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

  it("requires an explicit dialect when db is resolved from context and tables do not declare one", () => {
    expect(() =>
      createDrizzleProvider<{ db: DrizzleQueryExecutor }>({
        db: (context) => context.db,
        tables: {
          orders: { table: { name: "orders" } },
        },
      }),
    ).toThrow("context-resolved db binding");
  });

  it("checks WITH pushdown capabilities against the resolved db binding", async () => {
    const ordersTable = {
      name: "orders",
      id: { name: "id" },
      _: { config: { dialect: "pg" } },
    };
    const rel: RelNode = {
      id: "with_1",
      kind: "with",
      convention: "provider:drizzle",
      ctes: [
        {
          name: "scoped_orders",
          query: {
            id: "scan_1",
            kind: "scan",
            convention: "provider:drizzle",
            table: "orders",
            select: ["id"],
            output: [{ name: "id" }],
          },
        },
      ],
      body: {
        id: "scan_2",
        kind: "scan",
        convention: "provider:drizzle",
        table: "scoped_orders",
        select: ["id"],
        output: [{ name: "id" }],
      },
      output: [{ name: "id" }],
    };
    const provider = createDrizzleProvider<{ db: DrizzleQueryExecutor }>({
      db: (context) => context.db,
      tables: {
        orders: { table: ordersTable },
      },
    });
    const withCapableDb = {
      select() {
        return {
          from() {
            return {
              execute: async () => [],
            };
          },
        };
      },
      $with() {
        return {
          as(query: unknown) {
            return query;
          },
        };
      },
      with() {
        return {
          select() {
            return {
              from() {
                return {
                  execute: async () => [],
                };
              },
            };
          },
        };
      },
    } satisfies DrizzleQueryExecutor & {
      $with: (name: string) => { as: (query: unknown) => unknown };
      with: (...ctes: unknown[]) => {
        select: (selection: Record<string, unknown>) => {
          from: (source: unknown) => { execute: () => Promise<QueryRow[]> };
        };
      };
    };
    const withoutWithDb = {
      select: (...args: Parameters<typeof withCapableDb.select>) => withCapableDb.select(...args),
    } satisfies DrizzleQueryExecutor;

    expect(
      await Promise.resolve(
        provider.canExecute({ kind: "rel", provider: "drizzle", rel }, { db: withCapableDb }),
      ),
    ).toBe(true);
    await expect(
      Promise.resolve(
        provider.compile({ kind: "rel", provider: "drizzle", rel }, { db: withoutWithDb }),
      ).then((result) => result.unwrap()),
    ).rejects.toThrow(
      'Drizzle database instance does not support required APIs for "with" rel pushdown.',
    );
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

    const plan = (
      await provider.compile(
        {
          kind: "rel",
          provider: "drizzle",
          rel,
        },
        {},
      )
    ).unwrap();
    const rows = (await provider.execute(plan, {})).unwrap();

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

    const plan = (
      await provider.compile(
        {
          kind: "rel",
          provider: "drizzle",
          rel,
        },
        {},
      )
    ).unwrap();
    const rows = (await provider.execute(plan, {})).unwrap();

    expect(rows).toEqual([
      { id: "o1", total_cents: 25000, total_dollars: 250, is_large_order: false },
    ]);
    expect(calls.whereConditions).toHaveLength(1);
    expect(calls.orderByClauses).toHaveLength(1);
  });

  it("executes aggregate rel fragments with grouped metrics as a single downstream query", async () => {
    const ordersTable = {
      name: "orders",
      user_id: { name: "user_id" },
      total_cents: { name: "total_cents" },
      _: { config: { dialect: "pg" } },
    };
    const { db, calls } = createJoinCapableDb(
      new Map<object, TestRow[]>([
        [ordersTable, [{ user_id: "u1", order_count: 2, total_spend: 4500 }]],
      ]),
      new Map<string, TestRow[]>(),
    );

    const provider = createDrizzleProvider({
      db,
      tables: {
        orders: { table: ordersTable },
      },
    });

    const plan = (
      await provider.compile(
        {
          kind: "rel",
          provider: "drizzle",
          rel: buildAggregateRel(),
        },
        {},
      )
    ).unwrap();
    const rows = (await provider.execute(plan, {})).unwrap();

    expect(rows).toEqual([{ user_id: "u1", order_count: 2, total_spend: 4500 }]);
    expect(calls.executeCount).toBe(1);
    expect(calls.orderByClauses).toHaveLength(1);
  });

  it("executes set-op rel fragments through the single-query path", async () => {
    const westOrdersTable = {
      name: "orders_west",
      id: { name: "id" },
      _: { config: { dialect: "pg" } },
    };
    const eastOrdersTable = {
      name: "orders_east",
      id: { name: "id" },
      _: { config: { dialect: "pg" } },
    };
    const { db, calls } = createSetOpCapableDb(
      new Map<object, TestRow[]>([
        [westOrdersTable, [{ id: "o1" }]],
        [eastOrdersTable, [{ id: "o2" }]],
      ]),
    );

    const provider = createDrizzleProvider({
      db,
      tables: {
        orders_west: { table: westOrdersTable },
        orders_east: { table: eastOrdersTable },
      },
    });

    const plan = (
      await provider.compile(
        {
          kind: "rel",
          provider: "drizzle",
          rel: buildSetOpRel(),
        },
        {},
      )
    ).unwrap();
    const rows = (await provider.execute(plan, {})).unwrap();

    expect(rows).toEqual([{ id: "o1" }, { id: "o2" }]);
    expect(calls.executeCount).toBe(1);
    expect(calls.orderByClauses).toHaveLength(1);
    expect(calls.limitValues).toEqual([2]);
  });

  it("executes WITH rel fragments through the single-query path", async () => {
    const ordersTable = {
      name: "orders",
      id: { name: "id" },
      total_cents: { name: "total_cents" },
      _: { config: { dialect: "pg" } },
    };
    const { db, calls } = createWithCapableDb(
      new Map<object, TestRow[]>([[ordersTable, [{ id: "o2", total_cents: 3000 }]]]),
      new Map<string, TestRow[]>([
        [
          "ranked_orders",
          [
            { id: "o2", total_cents: 3000, spend_rank: 1 },
            { id: "o1", total_cents: 1500, spend_rank: 2 },
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

    const plan = (
      await provider.compile(
        {
          kind: "rel",
          provider: "drizzle",
          rel: buildWithWindowRel(),
        },
        {},
      )
    ).unwrap();
    const rows = (await provider.execute(plan, {})).unwrap();

    expect(rows).toEqual([{ id: "o2", spend_rank: 1 }]);
    expect(calls.executeCount).toBeGreaterThan(0);
    expect(calls.orderByClauses).toHaveLength(1);
    expect(calls.limitValues).toEqual([1]);
  });

  it("dispatches relational execution through db.execute when the builder lacks execute()", async () => {
    const ordersTable = {
      name: "orders",
      id: { name: "id" },
      _: { config: { dialect: "pg" } },
    };
    const { db, calls } = createDbExecuteFallbackDb(
      new Map<object, TestRow[]>([[ordersTable, [{ id: "o1" }]]]),
    );
    const provider = createDrizzleProvider({
      db,
      tables: {
        orders: { table: ordersTable },
      },
    });

    const plan = (
      await provider.compile(
        {
          kind: "rel",
          provider: "drizzle",
          rel: {
            id: "project_orders",
            kind: "project",
            convention: "provider:drizzle",
            input: {
              id: "scan_orders",
              kind: "scan",
              convention: "provider:drizzle",
              table: "orders",
              alias: "o",
              select: ["id"],
              output: [],
            },
            columns: [{ source: { alias: "o", column: "id" }, output: "id" }],
            output: [],
          },
        },
        {},
      )
    ).unwrap();
    const rows = (await provider.execute(plan, {})).unwrap();

    expect(rows).toEqual([{ id: "o1" }]);
    expect(calls.executeCount).toBe(1);
  });
});
