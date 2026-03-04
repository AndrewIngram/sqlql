import { describe, expect, it } from "vitest";
import { providersFromMethods } from "../support/methods-provider";
import { createArrayTableMethods, scanArrayRows } from "../../src/array-methods";

import {
  defineSchema,
  defineTableMethods,
  query,
  type TableMethodsForSchema,
  type TableScanRequest,
} from "../../src";

const EMPTY_CONTEXT = {} as const;

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

describe("query/parallel", () => {
  it("runs set-operation branches in parallel", async () => {
    const schema = defineSchema({
      tables: {
        a: {
          columns: {
            id: { type: "text", nullable: false },
          },
        },
        b: {
          columns: {
            id: { type: "text", nullable: false },
          },
        },
      },
    });

    let concurrentScans = 0;
    let maxConcurrentScans = 0;
    const runDelayedScan = async (
      rows: Array<Record<string, unknown>>,
      request: TableScanRequest,
    ): Promise<Array<Record<string, unknown>>> => {
      concurrentScans += 1;
      maxConcurrentScans = Math.max(maxConcurrentScans, concurrentScans);
      await sleep(25);
      const result = scanArrayRows(rows, request);
      concurrentScans -= 1;
      return result;
    };

    const methods = defineTableMethods(schema, {
      a: {
        ...createArrayTableMethods([{ id: "a1" }]),
        scan: (request) => runDelayedScan([{ id: "a1" }], request),
      },
      b: {
        ...createArrayTableMethods([{ id: "b1" }]),
        scan: (request) => runDelayedScan([{ id: "b1" }], request),
      },
    });

    const rows = await query({
      schema,
      providers: providersFromMethods(methods),
      context: EMPTY_CONTEXT,
      sql: `
        SELECT id FROM a
        UNION ALL
        SELECT id FROM b
      `,
    });

    expect(rows).toEqual([{ id: "a1" }, { id: "b1" }]);
    expect(maxConcurrentScans).toBeGreaterThan(1);
  });

  it("runs independent CTEs in parallel", async () => {
    const schema = defineSchema({
      tables: {
        a: {
          columns: {
            id: { type: "text", nullable: false },
          },
        },
        b: {
          columns: {
            id: { type: "text", nullable: false },
          },
        },
      },
    });

    let concurrentScans = 0;
    let maxConcurrentScans = 0;
    const delayedScan = async (
      rows: Array<Record<string, unknown>>,
      request: TableScanRequest,
    ): Promise<Array<Record<string, unknown>>> => {
      concurrentScans += 1;
      maxConcurrentScans = Math.max(maxConcurrentScans, concurrentScans);
      await sleep(25);
      const result = scanArrayRows(rows, request);
      concurrentScans -= 1;
      return result;
    };

    const methods = defineTableMethods(schema, {
      a: {
        ...createArrayTableMethods([{ id: "id_1" }]),
        scan: (request) => delayedScan([{ id: "id_1" }], request),
      },
      b: {
        ...createArrayTableMethods([{ id: "id_1" }]),
        scan: (request) => delayedScan([{ id: "id_1" }], request),
      },
    });

    const rows = await query({
      schema,
      providers: providersFromMethods(methods),
      context: EMPTY_CONTEXT,
      sql: `
        WITH a_cte AS (SELECT id FROM a),
             b_cte AS (SELECT id FROM b)
        SELECT a.id
        FROM a_cte a
        JOIN b_cte b ON a.id = b.id
      `,
    });

    expect(rows).toEqual([{ id: "id_1" }]);
    expect(maxConcurrentScans).toBeGreaterThan(1);
  });

  it("runs eligible join scans concurrently and keeps deterministic results", async () => {
    const schema = defineSchema({
      tables: {
        t1: {
          columns: {
            id: { type: "text", nullable: false },
            key: { type: "text", nullable: false },
          },
        },
        t2: {
          columns: {
            id: { type: "text", nullable: false },
            key: { type: "text", nullable: false },
          },
        },
        t3: {
          columns: {
            id: { type: "text", nullable: false },
            key: { type: "text", nullable: false },
          },
        },
      },
    });

    const rowsByTable = {
      t1: [{ id: "1", key: "k1" }],
      t2: [{ id: "2", key: "k1" }],
      t3: [{ id: "3", key: "k1" }],
    };

    let concurrentScans = 0;
    let maxConcurrentScans = 0;
    const withDelay = async (
      rows: Array<Record<string, unknown>>,
      request: TableScanRequest,
    ): Promise<Array<Record<string, unknown>>> => {
      concurrentScans += 1;
      maxConcurrentScans = Math.max(maxConcurrentScans, concurrentScans);
      await sleep(20);
      const result = scanArrayRows(rows, request);
      concurrentScans -= 1;
      return result;
    };

    const withDelayedLookup = async (
      rows: Array<Record<string, unknown>>,
      request: { key: string; values: unknown[]; select: string[] },
    ): Promise<Array<Record<string, unknown>>> => {
      concurrentScans += 1;
      maxConcurrentScans = Math.max(maxConcurrentScans, concurrentScans);
      await sleep(20);
      const result = rows
        .filter((row) => request.values.includes(row[request.key]))
        .map((row) =>
          Object.fromEntries(request.select.map((column) => [column, row[column] ?? null])),
        );
      concurrentScans -= 1;
      return result;
    };

    const methods = defineTableMethods(schema, {
      t1: {
        ...createArrayTableMethods(rowsByTable.t1),
        scan: (request) => withDelay(rowsByTable.t1, request),
        lookup: (request) =>
          withDelayedLookup(rowsByTable.t1, {
            key: request.key,
            values: request.values,
            select: request.select,
          }),
      },
      t2: {
        ...createArrayTableMethods(rowsByTable.t2),
        scan: (request) => withDelay(rowsByTable.t2, request),
        lookup: (request) =>
          withDelayedLookup(rowsByTable.t2, {
            key: request.key,
            values: request.values,
            select: request.select,
          }),
      },
      t3: {
        ...createArrayTableMethods(rowsByTable.t3),
        scan: (request) => withDelay(rowsByTable.t3, request),
        lookup: (request) =>
          withDelayedLookup(rowsByTable.t3, {
            key: request.key,
            values: request.values,
            select: request.select,
          }),
      },
    } satisfies TableMethodsForSchema<typeof schema, typeof EMPTY_CONTEXT>);

    const sql = `
      SELECT t1.id AS id1, t2.id AS id2, t3.id AS id3
      FROM t1
      JOIN t2 ON t1.key = t2.key
      JOIN t3 ON t1.key = t3.key
    `;

    const first = await query({
      schema,
      providers: providersFromMethods(methods),
      context: EMPTY_CONTEXT,
      sql,
    });
    const second = await query({
      schema,
      providers: providersFromMethods(methods),
      context: EMPTY_CONTEXT,
      sql,
    });

    expect(first).toEqual([{ id1: "1", id2: "2", id3: "3" }]);
    expect(second).toEqual(first);
    expect(maxConcurrentScans).toBeGreaterThan(1);
  });
});
