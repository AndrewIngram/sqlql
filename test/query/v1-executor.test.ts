import { describe, expect, it } from "vitest";

import {

  defineSchema,
  executeRelWithProviders,
  type ProviderAdapter,
  type ProviderFragment,
  type QueryRow,
  type RelNode,
  type ScanFilterClause,
  type TableScanRequest,
} from "../../src";
import { finalizeProviders } from "../support/executable-schema";

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
        if (typeof value !== "string" || typeof clause.value !== "string" || !matchesLike(value, clause.value)) {
          return false;
        }
        break;
      case "not_like":
        if (typeof value !== "string" || typeof clause.value !== "string" || matchesLike(value, clause.value)) {
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

describe("query/v1 local executor", () => {
  it("executes filter + aggregate nodes locally over provider scans", async () => {
    const schema = defineSchema({
      tables: {
        orders: {
          provider: "memory",
          columns: {
            id: "text",
            org_id: "text",
            total_cents: "integer",
          },
        },
      },
    });

    const rows: QueryRow[] = [
      { id: "o1", org_id: "org_1", total_cents: 1000 },
      { id: "o2", org_id: "org_1", total_cents: 2000 },
      { id: "o3", org_id: "org_2", total_cents: 4000 },
    ];

    const providers = finalizeProviders({
      memory: {
        canExecute(fragment: ProviderFragment) {
          return fragment.kind === "scan";
        },
        async compile(fragment: ProviderFragment) {
          return {
            provider: "memory",
            kind: fragment.kind,
            payload: fragment,
          };
        },
        async execute(plan) {
          const fragment = plan.payload as ProviderFragment;
          if (fragment.kind !== "scan") {
            return [];
          }
          return scanRows(rows, fragment.request);
        },
      } satisfies Omit<ProviderAdapter, "name">,
    });

    const rel: RelNode = {
      id: "aggregate_1",
      kind: "aggregate",
      convention: "local",
      input: {
        id: "filter_1",
        kind: "filter",
        convention: "local",
        input: {
          id: "scan_1",
          kind: "scan",
          convention: "local",
          table: "orders",
          alias: "o",
          select: ["id", "org_id", "total_cents"],
          output: [{ name: "o.id" }, { name: "o.org_id" }, { name: "o.total_cents" }],
        },
        where: [{ op: "eq", column: "org_id", value: "org_1" }],
        output: [{ name: "o.id" }, { name: "o.org_id" }, { name: "o.total_cents" }],
      },
      groupBy: [{ alias: "o", column: "org_id" }],
      metrics: [
        { fn: "count", as: "order_count", column: { alias: "o", column: "id" } },
        { fn: "sum", as: "gross_cents", column: { alias: "o", column: "total_cents" } },
      ],
      output: [{ name: "org_id" }, { name: "order_count" }, { name: "gross_cents" }],
    };

    const result = await executeRelWithProviders(rel, schema, providers, {}, {
      maxExecutionRows: 1000,
      maxLookupKeysPerBatch: 1000,
      maxLookupBatches: 10,
    });

    expect(result).toEqual([{ org_id: "org_1", order_count: 2, gross_cents: 3000 }]);
  });

  it("executes set operations locally", async () => {
    const schema = defineSchema({
      tables: {
        left_items: {
          provider: "memory",
          columns: {
            id: "text",
          },
        },
        right_items: {
          provider: "memory",
          columns: {
            id: "text",
          },
        },
      },
    });

    const tableRows: Record<string, QueryRow[]> = {
      left_items: [{ id: "a" }, { id: "b" }],
      right_items: [{ id: "b" }, { id: "c" }],
    };

    const providers = finalizeProviders({
      memory: {
        canExecute(fragment: ProviderFragment) {
          return fragment.kind === "scan";
        },
        async compile(fragment: ProviderFragment) {
          return {
            provider: "memory",
            kind: fragment.kind,
            payload: fragment,
          };
        },
        async execute(plan) {
          const fragment = plan.payload as ProviderFragment;
          if (fragment.kind !== "scan") {
            return [];
          }
          return scanRows(tableRows[fragment.table] ?? [], fragment.request);
        },
      } satisfies Omit<ProviderAdapter, "name">,
    });

    const rel: RelNode = {
      id: "set_1",
      kind: "set_op",
      convention: "local",
      op: "union",
      left: {
        id: "left_project",
        kind: "project",
        convention: "local",
        input: {
          id: "left_scan",
          kind: "scan",
          convention: "local",
          table: "left_items",
          alias: "l",
          select: ["id"],
          output: [{ name: "l.id" }],
        },
        columns: [{ source: { alias: "l", column: "id" }, output: "id" }],
        output: [{ name: "id" }],
      },
      right: {
        id: "right_project",
        kind: "project",
        convention: "local",
        input: {
          id: "right_scan",
          kind: "scan",
          convention: "local",
          table: "right_items",
          alias: "r",
          select: ["id"],
          output: [{ name: "r.id" }],
        },
        columns: [{ source: { alias: "r", column: "id" }, output: "id" }],
        output: [{ name: "id" }],
      },
      output: [{ name: "id" }],
    };

    const result = await executeRelWithProviders(rel, schema, providers, {}, {
      maxExecutionRows: 1000,
      maxLookupKeysPerBatch: 1000,
      maxLookupBatches: 10,
    });

    expect(result).toEqual([{ id: "a" }, { id: "b" }, { id: "c" }]);
  });

  it("executes WITH nodes via local cte materialization", async () => {
    const schema = defineSchema({
      tables: {
        users: {
          provider: "memory",
          columns: {
            id: "text",
            email: "text",
            team: "text",
          },
        },
      },
    });

    const rows: QueryRow[] = [
      { id: "u1", email: "a@example.com", team: "enterprise" },
      { id: "u2", email: "b@example.com", team: "smb" },
      { id: "u3", email: "c@example.com", team: "enterprise" },
    ];

    const providers = finalizeProviders({
      memory: {
        canExecute(fragment: ProviderFragment) {
          return fragment.kind === "scan";
        },
        async compile(fragment: ProviderFragment) {
          return {
            provider: "memory",
            kind: fragment.kind,
            payload: fragment,
          };
        },
        async execute(plan) {
          const fragment = plan.payload as ProviderFragment;
          if (fragment.kind !== "scan") {
            return [];
          }
          return scanRows(rows, fragment.request);
        },
      } satisfies Omit<ProviderAdapter, "name">,
    });

    const rel: RelNode = {
      id: "with_1",
      kind: "with",
      convention: "local",
      ctes: [
        {
          name: "active_users",
          query: {
            id: "cte_project",
            kind: "project",
            convention: "local",
            input: {
              id: "cte_filter",
              kind: "filter",
              convention: "local",
              input: {
                id: "users_scan",
                kind: "scan",
                convention: "local",
                table: "users",
                alias: "u",
                select: ["id", "email", "team"],
                output: [{ name: "u.id" }, { name: "u.email" }, { name: "u.team" }],
              },
              where: [{ op: "eq", column: "team", value: "enterprise" }],
              output: [{ name: "u.id" }, { name: "u.email" }, { name: "u.team" }],
            },
            columns: [
              { source: { alias: "u", column: "id" }, output: "id" },
              { source: { alias: "u", column: "email" }, output: "email" },
            ],
            output: [{ name: "id" }, { name: "email" }],
          },
        },
      ],
      body: {
        id: "body_project",
        kind: "project",
        convention: "local",
        input: {
          id: "body_scan",
          kind: "scan",
          convention: "local",
          table: "active_users",
          alias: "au",
          select: ["id", "email"],
          output: [{ name: "au.id" }, { name: "au.email" }],
        },
        columns: [
          { source: { alias: "au", column: "id" }, output: "id" },
          { source: { alias: "au", column: "email" }, output: "email" },
        ],
        output: [{ name: "id" }, { name: "email" }],
      },
      output: [{ name: "id" }, { name: "email" }],
    };

    const result = await executeRelWithProviders(rel, schema, providers, {}, {
      maxExecutionRows: 1000,
      maxLookupKeysPerBatch: 1000,
      maxLookupBatches: 10,
    });

    expect(result).toEqual([
      { id: "u1", email: "a@example.com" },
      { id: "u3", email: "c@example.com" },
    ]);
  });

  it("executes semi joins for IN-subquery style membership filtering", async () => {
    const schema = defineSchema({
      tables: {
        my_orders: {
          provider: "memory",
          columns: {
            id: "text",
            vendor_id: "text",
          },
        },
        preferred_vendors: {
          provider: "memory",
          columns: {
            vendor_id: "text",
          },
        },
      },
    });

    const tableRows: Record<string, QueryRow[]> = {
      my_orders: [
        { id: "o1", vendor_id: "v1" },
        { id: "o2", vendor_id: "v2" },
        { id: "o3", vendor_id: "v3" },
      ],
      preferred_vendors: [
        { vendor_id: "v1" },
        { vendor_id: "v3" },
      ],
    };

    const providers = finalizeProviders({
      memory: {
        canExecute(fragment: ProviderFragment) {
          return fragment.kind === "scan";
        },
        async compile(fragment: ProviderFragment) {
          return {
            provider: "memory",
            kind: fragment.kind,
            payload: fragment,
          };
        },
        async execute(plan) {
          const fragment = plan.payload as ProviderFragment;
          if (fragment.kind !== "scan") {
            return [];
          }
          return scanRows(tableRows[fragment.table] ?? [], fragment.request);
        },
      } satisfies Omit<ProviderAdapter, "name">,
    });

    const rel: RelNode = {
      id: "semi_project",
      kind: "project",
      convention: "local",
      input: {
        id: "semi_join",
        kind: "join",
        convention: "local",
        joinType: "semi",
        left: {
          id: "orders_scan",
          kind: "scan",
          convention: "local",
          table: "my_orders",
          alias: "o",
          select: ["id", "vendor_id"],
          output: [{ name: "o.id" }, { name: "o.vendor_id" }],
        },
        right: {
          id: "preferred_scan",
          kind: "scan",
          convention: "local",
          table: "preferred_vendors",
          alias: "pv",
          select: ["vendor_id"],
          output: [{ name: "pv.vendor_id" }],
        },
        leftKey: { alias: "o", column: "vendor_id" },
        rightKey: { alias: "pv", column: "vendor_id" },
        output: [{ name: "o.id" }, { name: "o.vendor_id" }],
      },
      columns: [{ source: { alias: "o", column: "id" }, output: "id" }],
      output: [{ name: "id" }],
    };

    const result = await executeRelWithProviders(rel, schema, providers, {}, {
      maxExecutionRows: 1000,
      maxLookupKeysPerBatch: 1000,
      maxLookupBatches: 10,
    });

    expect(result).toEqual([{ id: "o1" }, { id: "o3" }]);
  });

  it("executes rank window functions locally", async () => {
    const schema = defineSchema({
      tables: {
        scores: {
          provider: "memory",
          columns: {
            id: "text",
            team: "text",
            score: "integer",
          },
        },
      },
    });

    const rows: QueryRow[] = [
      { id: "a", team: "red", score: 100 },
      { id: "b", team: "red", score: 90 },
      { id: "c", team: "red", score: 90 },
      { id: "d", team: "blue", score: 95 },
      { id: "e", team: "blue", score: 80 },
    ];

    const providers = finalizeProviders({
      memory: {
        canExecute(fragment: ProviderFragment) {
          return fragment.kind === "scan";
        },
        async compile(fragment: ProviderFragment) {
          return {
            provider: "memory",
            kind: fragment.kind,
            payload: fragment,
          };
        },
        async execute(plan) {
          const fragment = plan.payload as ProviderFragment;
          if (fragment.kind !== "scan") {
            return [];
          }
          return scanRows(rows, fragment.request);
        },
      } satisfies Omit<ProviderAdapter, "name">,
    });

    const rel: RelNode = {
      id: "rank_project",
      kind: "project",
      convention: "local",
      input: {
        id: "rank_window",
        kind: "window",
        convention: "local",
        input: {
          id: "scores_scan",
          kind: "scan",
          convention: "local",
          table: "scores",
          alias: "s",
          select: ["id", "team", "score"],
          output: [{ name: "s.id" }, { name: "s.team" }, { name: "s.score" }],
        },
        functions: [
          {
            fn: "dense_rank",
            as: "dense_rank",
            partitionBy: [{ alias: "s", column: "team" }],
            orderBy: [{ source: { alias: "s", column: "score" }, direction: "desc" }],
          },
          {
            fn: "rank",
            as: "rank",
            partitionBy: [{ alias: "s", column: "team" }],
            orderBy: [{ source: { alias: "s", column: "score" }, direction: "desc" }],
          },
          {
            fn: "row_number",
            as: "row_number",
            partitionBy: [{ alias: "s", column: "team" }],
            orderBy: [{ source: { alias: "s", column: "score" }, direction: "desc" }],
          },
        ],
        output: [
          { name: "s.id" },
          { name: "s.team" },
          { name: "s.score" },
          { name: "dense_rank" },
          { name: "rank" },
          { name: "row_number" },
        ],
      },
      columns: [
        { source: { alias: "s", column: "id" }, output: "id" },
        { source: { alias: "s", column: "team" }, output: "team" },
        { source: { column: "dense_rank" }, output: "dense_rank" },
        { source: { column: "rank" }, output: "rank" },
        { source: { column: "row_number" }, output: "row_number" },
      ],
      output: [
        { name: "id" },
        { name: "team" },
        { name: "dense_rank" },
        { name: "rank" },
        { name: "row_number" },
      ],
    };

    const result = await executeRelWithProviders(rel, schema, providers, {}, {
      maxExecutionRows: 1000,
      maxLookupKeysPerBatch: 1000,
      maxLookupBatches: 10,
    });

    expect(result).toContainEqual({ id: "a", team: "red", dense_rank: 1, rank: 1, row_number: 1 });
    expect(result).toContainEqual({ id: "b", team: "red", dense_rank: 2, rank: 2, row_number: 2 });
    expect(result).toContainEqual({ id: "c", team: "red", dense_rank: 2, rank: 2, row_number: 3 });
    expect(result).toContainEqual({ id: "d", team: "blue", dense_rank: 1, rank: 1, row_number: 1 });
    expect(result).toContainEqual({ id: "e", team: "blue", dense_rank: 2, rank: 2, row_number: 2 });
  });
});
