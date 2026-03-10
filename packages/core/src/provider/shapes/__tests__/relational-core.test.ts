import { describe, expect, it } from "vitest";

import { isRelProjectColumnMapping, type RelNode } from "@tupl/core/model/rel";
import {
  buildSingleQueryPlan,
  canCompileBasicRel,
  canCompileSetOpRel,
  canCompileWithRel,
  hasSqlNode,
  isSupportedRelationalPlan,
  resolveRelationalStrategy,
  type RelationalScanBindingBase,
} from "@tupl/core/provider/shapes";

interface Binding extends RelationalScanBindingBase {
  table: string;
}

function requireColumnProjectMapping(
  mapping: Extract<RelNode, { kind: "project" }>["columns"][number],
): { source: { alias?: string; table?: string; column: string }; output: string } {
  if (!isRelProjectColumnMapping(mapping)) {
    throw new Error("Expected a column projection mapping.");
  }
  return mapping;
}

function createBinding(scan: Extract<RelNode, { kind: "scan" }>): Binding {
  return {
    alias: scan.alias ?? scan.table,
    scan,
    table: scan.table,
  };
}

function buildJoinProjectRel(): RelNode {
  return {
    id: "project_1",
    kind: "project",
    convention: "provider:test",
    input: {
      id: "sort_1",
      kind: "sort",
      convention: "provider:test",
      input: {
        id: "join_1",
        kind: "join",
        convention: "provider:test",
        joinType: "inner",
        left: {
          id: "orders_scan",
          kind: "scan",
          convention: "provider:test",
          table: "orders",
          alias: "o",
          select: ["id", "user_id", "total_cents"],
          output: [{ name: "o.id" }, { name: "o.user_id" }, { name: "o.total_cents" }],
        },
        right: {
          id: "users_scan",
          kind: "scan",
          convention: "provider:test",
          table: "users",
          alias: "u",
          select: ["id", "email"],
          output: [{ name: "u.id" }, { name: "u.email" }],
        },
        leftKey: { alias: "o", column: "user_id" },
        rightKey: { alias: "u", column: "id" },
        output: [],
      },
      orderBy: [{ source: { alias: "o", column: "total_cents" }, direction: "desc" }],
      output: [],
    },
    columns: [
      { source: { alias: "o", column: "id" }, output: "id" },
      { source: { alias: "u", column: "email" }, output: "email" },
    ],
    output: [{ name: "id" }, { name: "email" }],
  };
}

function buildSetOpRel(): RelNode {
  return {
    id: "limit_1",
    kind: "limit_offset",
    convention: "provider:test",
    limit: 5,
    input: {
      id: "sort_1",
      kind: "sort",
      convention: "provider:test",
      orderBy: [{ source: { column: "id" }, direction: "asc" }],
      input: {
        id: "set_1",
        kind: "set_op",
        convention: "provider:test",
        op: "union_all",
        left: buildJoinProjectRel(),
        right: buildJoinProjectRel(),
        output: [{ name: "id" }, { name: "email" }],
      },
      output: [{ name: "id" }, { name: "email" }],
    },
    output: [{ name: "id" }, { name: "email" }],
  };
}

function buildWithRel(): RelNode {
  return {
    id: "with_1",
    kind: "with",
    convention: "provider:test",
    ctes: [
      {
        name: "recent_orders",
        query: buildJoinProjectRel(),
      },
    ],
    body: {
      id: "project_2",
      kind: "project",
      convention: "provider:test",
      input: {
        id: "window_1",
        kind: "window",
        convention: "provider:test",
        functions: [
          {
            fn: "row_number",
            as: "row_num",
            partitionBy: [],
            orderBy: [{ source: { alias: "r", column: "id" }, direction: "asc" }],
          },
        ],
        input: {
          id: "scan_1",
          kind: "scan",
          convention: "provider:test",
          table: "recent_orders",
          alias: "r",
          select: ["id", "email"],
          output: [{ name: "id" }, { name: "email" }],
        },
        output: [{ name: "id" }, { name: "email" }, { name: "row_num" }],
      },
      columns: [
        { source: { alias: "r", column: "id" }, output: "id" },
        { source: { column: "row_num" }, output: "row_num" },
      ],
      output: [{ name: "id" }, { name: "row_num" }],
    },
    output: [{ name: "id" }, { name: "row_num" }],
  };
}

describe("relational provider core", () => {
  it("normalizes a left-deep join plan once for provider emitters", () => {
    const plan = buildSingleQueryPlan(buildJoinProjectRel(), createBinding);

    expect(plan.joinPlan.root.alias).toBe("o");
    expect(plan.joinPlan.joins).toHaveLength(1);
    expect(plan.joinPlan.aliases.size).toBe(2);
    expect(plan.pipeline.project?.columns).toHaveLength(2);
    expect(plan.pipeline.sort?.orderBy).toHaveLength(1);
  });

  it("keeps set-op and with support decisions provider-owned", () => {
    const resolveBranchStrategy = (node: RelNode): "basic" | "set_op" | "with" | null =>
      resolveRelationalStrategy(node, {
        basicStrategy: "basic",
        setOpStrategy: "set_op",
        withStrategy: "with",
        canCompileBasic: (current) =>
          canCompileBasicRel(current, (table) => table === "orders" || table === "users", {
            requireColumnProjectMappings: true,
          }),
        validateBasic: (current) =>
          isSupportedRelationalPlan(() => {
            buildSingleQueryPlan(current, createBinding);
          }),
        canCompileSetOp: (current) =>
          canCompileSetOpRel(current, resolveBranchStrategy, requireColumnProjectMapping),
        canCompileWith: (current) => canCompileWithRel(current, resolveBranchStrategy),
      });

    expect(resolveBranchStrategy(buildJoinProjectRel())).toBe("basic");
    expect(resolveBranchStrategy(buildSetOpRel())).toBe("set_op");
    expect(resolveBranchStrategy(buildWithRel())).toBe("with");
    expect(hasSqlNode(buildJoinProjectRel())).toBe(false);
  });
});
