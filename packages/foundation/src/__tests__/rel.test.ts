import { describe, expect, it } from "vitest";

import { createSqlRel, relContainsSqlNode, type RelNode } from "@tupl/foundation";

function buildScan(table: string): Extract<RelNode, { kind: "scan" }> {
  return {
    id: `scan_${table}`,
    kind: "scan",
    convention: "local",
    table,
    select: ["id"],
    output: [{ name: "id" }],
  };
}

describe("relContainsSqlNode", () => {
  it("detects direct sql nodes", () => {
    expect(relContainsSqlNode(createSqlRel("select * from users", ["users"]))).toBe(true);
  });

  it("detects sql nodes nested inside projection expressions", () => {
    const rel: RelNode = {
      id: "project_1",
      kind: "project",
      convention: "local",
      input: buildScan("users"),
      columns: [
        { source: { column: "id" }, output: "id" },
        {
          kind: "expr",
          output: "exists_recent_order",
          expr: {
            kind: "subquery",
            id: "subquery_1",
            mode: "exists",
            rel: createSqlRel("select 1 from orders", ["orders"]),
          },
        },
      ],
      output: [{ name: "id" }, { name: "exists_recent_order" }],
    };

    expect(relContainsSqlNode(rel)).toBe(true);
  });

  it("detects sql nodes nested inside filter expressions and ctes", () => {
    const rel: RelNode = {
      id: "with_1",
      kind: "with",
      convention: "local",
      ctes: [
        {
          name: "recent_orders",
          query: {
            id: "filter_1",
            kind: "filter",
            convention: "local",
            input: buildScan("orders"),
            expr: {
              kind: "subquery",
              id: "subquery_2",
              mode: "scalar",
              rel: createSqlRel("select count(*) from line_items", ["line_items"]),
              outputColumn: "count",
            },
            output: [{ name: "id" }],
          },
        },
      ],
      body: buildScan("recent_orders"),
      output: [{ name: "id" }],
    };

    expect(relContainsSqlNode(rel)).toBe(true);
  });

  it("returns false for ordinary lowered relational trees", () => {
    const rel: RelNode = {
      id: "join_1",
      kind: "join",
      convention: "local",
      joinType: "inner",
      left: buildScan("orders"),
      right: buildScan("users"),
      leftKey: { column: "user_id" },
      rightKey: { column: "id" },
      output: [{ name: "id" }],
    };

    expect(relContainsSqlNode(rel)).toBe(false);
  });
});
