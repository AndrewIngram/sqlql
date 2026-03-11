import { describe, expect, it } from "vitest";
import { queryWithMethods } from "../../../../test/support/methods-provider";
import { createArrayTableMethods, scanArrayRows } from "../../../../test/support/array-methods";

import {
  defineTableMethods,
  type TableMethodsForSchema,
  type TableScanRequest,
} from "@tupl/schema";
import { buildEntitySchema } from "../../../../test/support/schema-builder";

const EMPTY_CONTEXT = {} as const;

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

describe("query/parallel", () => {
  it("returns deterministic results for set-operation branches", async () => {
    const schema = buildEntitySchema({
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
    });

    const delayedScan = async (
      rows: Array<Record<string, unknown>>,
      request: TableScanRequest,
    ): Promise<Array<Record<string, unknown>>> => {
      await sleep(10);
      return scanArrayRows(rows, request);
    };

    const methods = defineTableMethods(schema, {
      a: {
        ...createArrayTableMethods([{ id: "a1" }]),
        scan: (request) => delayedScan([{ id: "a1" }], request),
      },
      b: {
        ...createArrayTableMethods([{ id: "b1" }]),
        scan: (request) => delayedScan([{ id: "b1" }], request),
      },
    });

    const rows = await queryWithMethods({
      schema,
      methods,
      context: EMPTY_CONTEXT,
      sql: `
        SELECT id FROM a
        UNION ALL
        SELECT id FROM b
      `,
    });

    expect(rows).toEqual([{ id: "a1" }, { id: "b1" }]);
  });

  it("returns deterministic results for independent CTEs", async () => {
    const schema = buildEntitySchema({
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
    });

    const delayedScan = async (
      rows: Array<Record<string, unknown>>,
      request: TableScanRequest,
    ): Promise<Array<Record<string, unknown>>> => {
      await sleep(10);
      return scanArrayRows(rows, request);
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

    const rows = await queryWithMethods({
      schema,
      methods,
      context: EMPTY_CONTEXT,
      sql: `
        WITH a_cte AS (SELECT id FROM a),
             b_cte AS (SELECT id FROM b)
        SELECT id FROM a_cte
        UNION ALL
        SELECT id FROM b_cte
        ORDER BY id
      `,
    });

    expect(rows).toEqual([{ id: "id_1" }, { id: "id_1" }]);
  });

  it("keeps join results deterministic across repeated runs", async () => {
    const schema = buildEntitySchema({
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
    });

    const rowsByTable = {
      t1: [{ id: "1", key: "k1" }],
      t2: [{ id: "2", key: "k1" }],
      t3: [{ id: "3", key: "k1" }],
    };

    const withDelay = async (
      rows: Array<Record<string, unknown>>,
      request: TableScanRequest,
    ): Promise<Array<Record<string, unknown>>> => {
      await sleep(10);
      return scanArrayRows(rows, request);
    };

    const methods = defineTableMethods(schema, {
      t1: {
        ...createArrayTableMethods(rowsByTable.t1),
        scan: (request) => withDelay(rowsByTable.t1, request),
      },
      t2: {
        ...createArrayTableMethods(rowsByTable.t2),
        scan: (request) => withDelay(rowsByTable.t2, request),
      },
      t3: {
        ...createArrayTableMethods(rowsByTable.t3),
        scan: (request) => withDelay(rowsByTable.t3, request),
      },
    } satisfies TableMethodsForSchema<typeof schema, typeof EMPTY_CONTEXT>);

    const sql = `
      SELECT t1.id AS id1, t2.id AS id2, t3.id AS id3
      FROM t1
      JOIN t2 ON t1.key = t2.key
      JOIN t3 ON t1.key = t3.key
    `;

    const first = await queryWithMethods({
      schema,
      methods,
      context: EMPTY_CONTEXT,
      sql,
    });
    const second = await queryWithMethods({
      schema,
      methods,
      context: EMPTY_CONTEXT,
      sql,
    });

    expect(first).toEqual([{ id1: "1", id2: "2", id3: "3" }]);
    expect(second).toEqual(first);
  });
});
