import { describe, expect, it } from "vitest";
import { queryWithMethods } from "../../../../test/support/methods-provider";
import {
  aggregateArrayRows,
  createArrayTableMethods,
  scanArrayRows,
} from "../../../../test/support/array-methods";

import {
  defineTableMethods,
  type TableAggregateRequest,
  type TableLookupRequest,
  type TableScanRequest,
} from "@tupl/schema";
import { buildEntitySchema } from "../../../../test/support/schema-builder";

const EMPTY_CONTEXT = {} as const;

describe("query/planner-hooks", () => {
  it("applies ID-based planScan pushdown with local residual execution", async () => {
    const schema = buildEntitySchema({
      orders: {
        columns: {
          id: "text",
          status: "text",
          total_cents: "integer",
          created_at: "text",
        },
      },
    });

    const rows = [
      { id: "o1", status: "paid", total_cents: 1000, created_at: "2026-02-01" },
      { id: "o2", status: "paid", total_cents: 2500, created_at: "2026-02-03" },
      { id: "o3", status: "paid", total_cents: 1800, created_at: "2026-02-02" },
      { id: "o4", status: "draft", total_cents: 3000, created_at: "2026-02-04" },
    ];

    const remoteRequests: TableScanRequest[] = [];
    const methods = defineTableMethods(schema, {
      orders: {
        async scan(request) {
          remoteRequests.push(request);
          return scanArrayRows(rows, request);
        },
        planScan(request) {
          return {
            whereIds:
              request.where
                ?.filter((term) => term.clause.column === "status")
                .map((term) => term.id) ?? [],
            orderByIds: [],
            limitOffset: "residual",
          };
        },
      },
    });

    const result = await queryWithMethods({
      schema,
      methods,
      context: EMPTY_CONTEXT,
      sql: `
        SELECT id
        FROM orders
        WHERE status = 'paid' AND total_cents >= 1500
        ORDER BY created_at DESC
        LIMIT 1
      `,
    });

    expect(result).toEqual([{ id: "o2" }]);
    expect(remoteRequests).toHaveLength(1);
    expect(remoteRequests[0]?.where).toEqual([{ column: "status", op: "eq", value: "paid" }]);
    expect(remoteRequests[0]?.orderBy).toBeUndefined();
    expect(remoteRequests[0]?.limit).toBeUndefined();
  });

  it("supports simple ORDER BY ordinals in planner-hooked scan queries", async () => {
    const schema = buildEntitySchema({
      orders: {
        columns: {
          id: "text",
          created_at: "text",
        },
      },
    });

    const rows = [
      { id: "o1", created_at: "2026-02-01" },
      { id: "o2", created_at: "2026-02-03" },
      { id: "o3", created_at: "2026-02-02" },
    ];

    const remoteRequests: TableScanRequest[] = [];
    const methods = defineTableMethods(schema, {
      orders: {
        ...createArrayTableMethods(rows),
        async scan(request) {
          remoteRequests.push(request);
          return scanArrayRows(rows, request);
        },
      },
    });

    const result = await queryWithMethods({
      schema,
      methods,
      context: EMPTY_CONTEXT,
      sql: `
        SELECT id, created_at
        FROM orders
        ORDER BY 2 DESC
        LIMIT 1
      `,
    });

    expect(result).toEqual([{ id: "o2", created_at: "2026-02-03" }]);
    expect(remoteRequests).toHaveLength(1);
  });

  it("allows planScan residuals by default", async () => {
    const schema = buildEntitySchema({
      orders: {
        columns: {
          id: "text",
          status: "text",
        },
      },
    });

    const methods = defineTableMethods(schema, {
      orders: {
        ...createArrayTableMethods([{ id: "o1", status: "paid" }]),
        planScan() {
          return {
            whereIds: [],
          };
        },
      },
    });

    const result = await queryWithMethods({
      schema,
      methods,
      context: EMPTY_CONTEXT,
      sql: "SELECT id FROM orders WHERE status = 'paid'",
    });

    expect(result).toEqual([{ id: "o1" }]);
  });

  it("supports explicit remote/residual planScan mode", async () => {
    const schema = buildEntitySchema({
      orders: {
        columns: {
          id: "text",
          status: "text",
          total_cents: "integer",
        },
      },
    });

    const rows = [
      { id: "o1", status: "paid", total_cents: 1000 },
      { id: "o2", status: "paid", total_cents: 2400 },
      { id: "o3", status: "draft", total_cents: 4000 },
    ];

    const methods = defineTableMethods(schema, {
      orders: {
        async scan(request) {
          return scanArrayRows(rows, request);
        },
        planScan() {
          return {
            mode: "remote_residual",
            remote: {
              where: [{ op: "eq", column: "status", value: "paid" }],
            },
            residual: {
              where: [{ op: "gt", column: "total_cents", value: 1500 }],
            },
          };
        },
      },
    });

    const result = await queryWithMethods({
      schema,
      methods,
      context: EMPTY_CONTEXT,
      sql: "SELECT id FROM orders WHERE status = 'paid' AND total_cents > 1500 ORDER BY id ASC",
    });

    expect(result).toEqual([{ id: "o2" }]);
  });

  it("applies planLookup and local residual filters", async () => {
    const schema = buildEntitySchema({
      orders: {
        columns: {
          id: "text",
          user_id: "text",
        },
      },
      users: {
        columns: {
          id: "text",
          email: "text",
        },
      },
    });

    const lookupRequests: TableLookupRequest[] = [];
    const usersRows = [
      { id: "u1", email: "a@example.com" },
      { id: "u2", email: "b@example.com" },
    ];

    const methods = defineTableMethods(schema, {
      orders: createArrayTableMethods([
        { id: "o1", user_id: "u1" },
        { id: "o2", user_id: "u2" },
      ]),
      users: {
        ...createArrayTableMethods(usersRows),
        async lookup(request) {
          lookupRequests.push(request);
          return createArrayTableMethods(usersRows).lookup!(request, EMPTY_CONTEXT);
        },
        planLookup() {
          return {
            whereIds: [],
          };
        },
      },
    });

    const result = await queryWithMethods({
      schema,
      methods,
      context: EMPTY_CONTEXT,
      sql: `
        SELECT o.id, u.email
        FROM orders o
        JOIN users u ON u.id = o.user_id
        WHERE u.email = 'a@example.com'
      `,
    });

    expect(result).toEqual([{ id: "o1", email: "a@example.com" }]);
    expect(lookupRequests).toHaveLength(1);
    expect(lookupRequests[0]?.where).toBeUndefined();
  });

  it("falls back from aggregate handler when planAggregate leaves residual and policy allows it", async () => {
    const schema = buildEntitySchema({
      orders: {
        columns: {
          id: "text",
          status: "text",
          total_cents: "integer",
        },
      },
    });

    const rows = [
      { id: "o1", status: "paid", total_cents: 1000 },
      { id: "o2", status: "paid", total_cents: 2000 },
      { id: "o3", status: "draft", total_cents: 3000 },
    ];

    const aggregateRequests: TableAggregateRequest[] = [];
    const methods = defineTableMethods(schema, {
      orders: {
        ...createArrayTableMethods(rows),
        async aggregate(request) {
          aggregateRequests.push(request);
          return aggregateArrayRows(rows, request);
        },
        planAggregate() {
          return {
            metricIds: [],
          };
        },
      },
    });

    const result = await queryWithMethods({
      schema,
      methods,
      context: EMPTY_CONTEXT,
      sql: "SELECT SUM(total_cents) AS total FROM orders WHERE status = 'paid'",
    });

    expect(result).toEqual([{ total: 3000 }]);
    expect(aggregateRequests).toHaveLength(0);
  });
});
