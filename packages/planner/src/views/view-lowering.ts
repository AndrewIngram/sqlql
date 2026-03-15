import { Result, type Result as BetterResult } from "better-result";

import { RelRewriteError, type RelColumnRef, type RelNode } from "@tupl/foundation";
import type { SchemaDefinition, SchemaViewRelNode } from "@tupl/schema-model";
import { createTableDefinitionFromEntity } from "@tupl/schema-model/normalization";

/**
 * View lowering owns translation from normalized schema-view definitions into generic relational IR.
 */
export function compileViewRelForPlanner(
  definition: unknown,
  schema: SchemaDefinition,
  nextRelId: (prefix: string) => string,
): BetterResult<RelNode, RelRewriteError> {
  if (
    definition &&
    typeof definition === "object" &&
    typeof (definition as { kind?: unknown }).kind === "string" &&
    typeof (definition as { convention?: unknown }).convention === "string"
  ) {
    return Result.ok(definition as RelNode);
  }

  if (
    !definition ||
    typeof definition !== "object" ||
    typeof (definition as { kind?: unknown }).kind !== "string"
  ) {
    return Result.err(
      new RelRewriteError({
        operation: "compile planner view rel",
        message: "View returned an unsupported rel definition.",
      }),
    );
  }

  return compileSchemaViewRelForPlanner(definition as SchemaViewRelNode, schema, nextRelId);
}

function compileSchemaViewRelForPlanner(
  node: SchemaViewRelNode,
  schema: SchemaDefinition,
  nextRelId: (prefix: string) => string,
): BetterResult<RelNode, RelRewriteError> {
  switch (node.kind) {
    case "scan": {
      const table =
        schema.tables[node.table] ??
        (node.entity ? createTableDefinitionFromEntity(node.entity) : undefined);
      if (!table || (node.entity && Object.keys(table.columns).length === 0)) {
        return Result.err(
          new RelRewriteError({
            operation: "compile planner view rel",
            message: `Unknown table in view rel scan: ${node.table}`,
          }),
        );
      }
      const select = Object.keys(table.columns);
      return Result.ok({
        id: nextRelId("view_scan"),
        kind: "scan",
        convention: "local",
        table: node.table,
        ...(node.entity ? { entity: node.entity } : {}),
        alias: node.table,
        select,
        output: select.map((column) => ({
          name: `${node.table}.${column}`,
        })),
      });
    }
    case "join": {
      return Result.gen(function* () {
        const left = yield* compileSchemaViewRelForPlanner(node.left, schema, nextRelId);
        const right = yield* compileSchemaViewRelForPlanner(node.right, schema, nextRelId);
        const joinNode: RelNode = {
          id: nextRelId("view_join"),
          kind: "join",
          convention: "local",
          joinType: node.type,
          left,
          right,
          leftKey: parseRelColumnRef(resolveViewRelRef(node.on.left)),
          rightKey: parseRelColumnRef(resolveViewRelRef(node.on.right)),
          output: [...left.output, ...right.output],
        };
        return Result.ok(joinNode);
      });
    }
    case "aggregate": {
      return Result.gen(function* () {
        const input = yield* compileSchemaViewRelForPlanner(node.from, schema, nextRelId);
        const groupBy = Object.entries(node.groupBy).map(([name, column]) => ({
          name,
          ref: parseRelColumnRef(resolveViewRelRef(column)),
        }));
        const metrics = Object.entries(node.measures).map(([output, metric]) => ({
          fn: metric.fn,
          as: output,
          ...(metric.column ? { column: parseRelColumnRef(resolveViewRelRef(metric.column)) } : {}),
        }));
        const aggregateNode: RelNode = {
          id: nextRelId("view_aggregate"),
          kind: "aggregate",
          convention: "local",
          input,
          groupBy: groupBy.map((entry) => entry.ref),
          metrics,
          output: [
            ...groupBy.map((column) => ({ name: column.name })),
            ...metrics.map((metric) => ({ name: metric.as })),
          ],
        };
        return Result.ok(aggregateNode);
      });
    }
  }
}

function parseRelColumnRef(ref: string): RelColumnRef {
  const idx = ref.lastIndexOf(".");
  if (idx < 0) {
    return {
      column: ref,
    };
  }
  return {
    alias: ref.slice(0, idx),
    column: ref.slice(idx + 1),
  };
}

function resolveViewRelRef(ref: { ref?: string }): string {
  if (!ref.ref) {
    throw new RelRewriteError({
      operation: "compile planner view rel",
      message: "View rel column reference was not normalized to a string reference.",
    });
  }
  return ref.ref;
}
