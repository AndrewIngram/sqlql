import type { RelColumnRef, RelNode } from "@tupl/foundation";

import { nextRelId } from "../physical/planner-ids";
import { parseRelColumnRef } from "../select/select-from-lowering";
import { mapBinaryOperatorToRelFunction } from "../sql-expr-lowering";

/**
 * Decorrelation owns rewriting explicit correlate nodes into executable joins, filters, and projects.
 */
export function decorrelateRel(node: RelNode): RelNode {
  switch (node.kind) {
    case "scan":
    case "values":
    case "cte_ref":
      return node;
    case "filter":
      return {
        ...node,
        input: decorrelateRel(node.input),
      };
    case "project":
      return {
        ...node,
        input: decorrelateRel(node.input),
      };
    case "aggregate":
      return {
        ...node,
        input: decorrelateRel(node.input),
      };
    case "window":
      return {
        ...node,
        input: decorrelateRel(node.input),
      };
    case "sort":
      return {
        ...node,
        input: decorrelateRel(node.input),
      };
    case "limit_offset":
      return {
        ...node,
        input: decorrelateRel(node.input),
      };
    case "join":
      return {
        ...node,
        left: decorrelateRel(node.left),
        right: decorrelateRel(node.right),
      };
    case "set_op":
      return {
        ...node,
        left: decorrelateRel(node.left),
        right: decorrelateRel(node.right),
      };
    case "with":
      return {
        ...node,
        ctes: node.ctes.map((cte) => ({
          ...cte,
          query: decorrelateRel(cte.query),
        })),
        body: decorrelateRel(node.body),
      };
    case "repeat_union":
      return {
        ...node,
        seed: decorrelateRel(node.seed),
        iterative: decorrelateRel(node.iterative),
      };
    case "correlate":
      return decorrelateCorrelateNode(node);
  }
}

function decorrelateCorrelateNode(node: Extract<RelNode, { kind: "correlate" }>): RelNode {
  const left = decorrelateRel(node.left);
  const right = decorrelateRel(node.right);

  switch (node.apply.kind) {
    case "semi": {
      const keyedRight = ensureJoinKeyProjected(right, node.correlation.inner);
      const rightOutput = findProjectedJoinKeyOutput(keyedRight, node.correlation.inner);
      if (!rightOutput) {
        throw new Error("Correlate semi-join rewrite lost its projected join key.");
      }

      return {
        id: nextRelId("join"),
        kind: "join",
        convention: "local",
        joinType: "semi",
        left,
        right: keyedRight,
        leftKey: node.correlation.outer,
        rightKey: parseRelColumnRef(rightOutput),
        output: left.output,
      };
    }
    case "anti": {
      const keyedRight = ensureQualifiedJoinKeyProjected(right, node.correlation.inner);
      const rightOutput = `${node.correlation.inner.alias}.${node.correlation.inner.column}`;
      const antiJoin = {
        id: nextRelId("join"),
        kind: "join" as const,
        convention: "local" as const,
        joinType: "left" as const,
        left,
        right: keyedRight,
        leftKey: node.correlation.outer,
        rightKey: parseRelColumnRef(rightOutput),
        output: [...left.output, ...keyedRight.output],
      };
      const filtered = {
        id: nextRelId("filter"),
        kind: "filter" as const,
        convention: "local" as const,
        input: antiJoin,
        expr: {
          kind: "function" as const,
          name: "is_null",
          args: [
            {
              kind: "column" as const,
              ref: parseRelColumnRef(rightOutput),
            },
          ],
        },
        output: antiJoin.output,
      };

      return projectToOutputShape(filtered, left.output);
    }
    case "scalar_filter": {
      const mappedOperator = mapBinaryOperatorToRelFunction(node.apply.comparison.toUpperCase());
      if (!mappedOperator) {
        throw new Error(
          `Unsupported correlate scalar comparison operator: ${node.apply.comparison}`,
        );
      }

      const joined = {
        id: nextRelId("join"),
        kind: "join" as const,
        convention: "local" as const,
        joinType: "inner" as const,
        left,
        right,
        leftKey: node.correlation.outer,
        rightKey: parseRelColumnRef(node.apply.correlationColumn),
        output: [...left.output, ...right.output],
      };
      const filtered = {
        id: nextRelId("filter"),
        kind: "filter" as const,
        convention: "local" as const,
        input: joined,
        expr: {
          kind: "function" as const,
          name: mappedOperator,
          args: [
            {
              kind: "column" as const,
              ref: node.apply.outerCompare,
            },
            {
              kind: "column" as const,
              ref: {
                column: node.apply.metricColumn,
              },
            },
          ],
        },
        output: joined.output,
      };

      return projectToOutputShape(filtered, left.output);
    }
    case "scalar_project": {
      const joined = {
        id: nextRelId("join"),
        kind: "join" as const,
        convention: "local" as const,
        joinType: "left" as const,
        left,
        right,
        leftKey: node.correlation.outer,
        rightKey: parseRelColumnRef(node.apply.correlationColumn),
        output: [...left.output, ...right.output],
      };

      return {
        id: nextRelId("project"),
        kind: "project",
        convention: "local",
        input: joined,
        columns: [
          ...left.output.map((column) => ({
            kind: "column" as const,
            source: parseRelColumnRef(column.name),
            output: column.name,
          })),
          {
            kind: "column" as const,
            source: {
              column: node.apply.metricColumn,
            },
            output: node.apply.outputColumn,
          },
        ],
        output: [...left.output, { name: node.apply.outputColumn }],
      };
    }
  }
}

function projectToOutputShape(rel: RelNode, output: RelNode["output"]): RelNode {
  return {
    id: nextRelId("project"),
    kind: "project",
    convention: "local",
    input: rel,
    columns: output.map((column) => ({
      kind: "column" as const,
      source: parseRelColumnRef(column.name),
      output: column.name,
    })),
    output,
  };
}

function findProjectedJoinKeyOutput(rel: RelNode, ref: RelColumnRef): string | null {
  if (rel.kind === "project") {
    for (const mapping of rel.columns) {
      if (
        mapping.kind !== "expr" &&
        mapping.source.alias === ref.alias &&
        mapping.source.column === ref.column
      ) {
        return mapping.output;
      }
    }
  }

  for (const column of rel.output) {
    const parsed = parseRelColumnRef(column.name);
    if (parsed.alias === ref.alias && parsed.column === ref.column) {
      return column.name;
    }
  }
  return null;
}

function ensureJoinKeyProjected(rel: RelNode, ref: RelColumnRef): RelNode {
  const existing = findProjectedJoinKeyOutput(rel, ref);
  if (existing) {
    return rel;
  }

  const qualifiedRef = requireQualifiedRef(ref);
  const outputName = `${qualifiedRef.alias}.${qualifiedRef.column}`;

  if (rel.kind === "project") {
    return {
      ...rel,
      columns: [
        ...rel.columns,
        {
          kind: "column",
          source: qualifiedRef,
          output: outputName,
        },
      ],
      output: [...rel.output, { name: outputName }],
    };
  }

  if (rel.kind === "filter" || rel.kind === "sort" || rel.kind === "limit_offset") {
    const input = ensureJoinKeyProjected(rel.input, ref);
    if (input === rel.input) {
      return rel;
    }

    return {
      ...rel,
      input,
      output: input.output,
    };
  }

  return {
    id: nextRelId("project"),
    kind: "project",
    convention: "local",
    input: rel,
    columns: [
      ...rel.output.map((column) => ({
        kind: "column" as const,
        source: parseRelColumnRef(column.name),
        output: column.name,
      })),
      {
        kind: "column" as const,
        source: qualifiedRef,
        output: outputName,
      },
    ],
    output: [...rel.output, { name: outputName }],
  };
}

function ensureQualifiedJoinKeyProjected(rel: RelNode, ref: RelColumnRef): RelNode {
  const qualifiedRef = requireQualifiedRef(ref);
  const outputName = `${qualifiedRef.alias}.${qualifiedRef.column}`;
  if (rel.output.some((column) => column.name === outputName)) {
    return rel;
  }

  if (rel.kind === "project") {
    return {
      ...rel,
      columns: [
        ...rel.columns,
        {
          kind: "column",
          source: qualifiedRef,
          output: outputName,
        },
      ],
      output: [...rel.output, { name: outputName }],
    };
  }

  if (rel.kind === "filter" || rel.kind === "sort" || rel.kind === "limit_offset") {
    const input = ensureQualifiedJoinKeyProjected(rel.input, ref);
    if (input === rel.input) {
      return rel;
    }

    return {
      ...rel,
      input,
      output: input.output,
    };
  }

  return {
    id: nextRelId("project"),
    kind: "project",
    convention: "local",
    input: rel,
    columns: [
      ...rel.output.map((column) => ({
        kind: "column" as const,
        source: parseRelColumnRef(column.name),
        output: column.name,
      })),
      {
        kind: "column" as const,
        source: qualifiedRef,
        output: outputName,
      },
    ],
    output: [...rel.output, { name: outputName }],
  };
}

function requireQualifiedRef(ref: RelColumnRef): RelColumnRef {
  if (!ref.alias) {
    throw new Error("Decorrelated join keys must remain alias-qualified.");
  }

  return {
    alias: ref.alias,
    column: ref.column,
  };
}
