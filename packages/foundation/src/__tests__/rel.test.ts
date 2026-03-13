import { describe, expect, it } from "vitest";

import { collectRelTables, countRelNodes, type RelNode } from "@tupl/foundation";

function buildScan(table: string): Extract<RelNode, { kind: "scan" }> {
  return {
    id: `scan_${table}`,
    kind: "scan",
    convention: "logical",
    table,
    select: ["id"],
    output: [{ name: "id" }],
  };
}

describe("rel model", () => {
  it("counts repeat_union and nested cte scans as relational nodes", () => {
    const rel: RelNode = {
      id: "with_1",
      kind: "with",
      convention: "logical",
      ctes: [
        {
          name: "reachable",
          query: {
            id: "repeat_union_1",
            kind: "repeat_union",
            convention: "logical",
            cteName: "reachable",
            mode: "union_all",
            seed: buildScan("edges"),
            iterative: {
              id: "join_1",
              kind: "join",
              convention: "logical",
              joinType: "inner",
              left: {
                id: "scan_reachable",
                kind: "scan",
                convention: "logical",
                table: "reachable",
                alias: "r",
                select: ["id"],
                output: [{ name: "r.id" }],
              },
              right: buildScan("edges"),
              leftKey: { alias: "r", column: "id" },
              rightKey: { column: "source_id" },
              output: [{ name: "r.id" }, { name: "edges.id" }],
            },
            output: [{ name: "id" }],
          },
        },
      ],
      body: {
        id: "scan_cte",
        kind: "scan",
        convention: "logical",
        table: "reachable",
        select: ["id"],
        output: [{ name: "id" }],
      },
      output: [{ name: "id" }],
    };

    expect(countRelNodes(rel)).toBe(7);
  });

  it("collects physical tables while ignoring cte names", () => {
    const rel: RelNode = {
      id: "with_1",
      kind: "with",
      convention: "logical",
      ctes: [
        {
          name: "reachable",
          query: {
            id: "repeat_union_1",
            kind: "repeat_union",
            convention: "logical",
            cteName: "reachable",
            mode: "union_all",
            seed: buildScan("edges"),
            iterative: {
              id: "join_1",
              kind: "join",
              convention: "logical",
              joinType: "inner",
              left: {
                id: "scan_reachable",
                kind: "scan",
                convention: "logical",
                table: "reachable",
                alias: "r",
                select: ["id"],
                output: [{ name: "r.id" }],
              },
              right: buildScan("nodes"),
              leftKey: { alias: "r", column: "id" },
              rightKey: { column: "id" },
              output: [{ name: "r.id" }, { name: "nodes.id" }],
            },
            output: [{ name: "id" }],
          },
        },
      ],
      body: {
        id: "scan_cte",
        kind: "scan",
        convention: "logical",
        table: "reachable",
        select: ["id"],
        output: [{ name: "id" }],
      },
      output: [{ name: "id" }],
    };

    expect(collectRelTables(rel)).toEqual(["edges", "nodes"]);
  });
});
