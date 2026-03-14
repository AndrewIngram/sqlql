import { Result, type Result as BetterResult } from "better-result";
import type { DataEntityHandle, RelNode, TuplSchemaNormalizationError } from "@tupl/foundation";

import { createSchemaNormalizationError } from "./schema-errors";
import type { SchemaDefinition } from "./types";

/**
 * Rel-schema validation owns checking that relational plans only reference schema-backed tables and columns.
 */
export function validateRelAgainstSchema(
  node: RelNode,
  schema: SchemaDefinition,
): BetterResult<void, TuplSchemaNormalizationError> {
  const validateScanColumn = (
    tableName: string,
    column: string,
    entity?: DataEntityHandle<string>,
  ): BetterResult<void, TuplSchemaNormalizationError> => {
    const logicalColumn = column.includes(".") ? column.slice(column.lastIndexOf(".") + 1) : column;

    if (entity?.columns) {
      if (!(logicalColumn in entity.columns)) {
        return Result.err(
          createSchemaNormalizationError({
            operation: "validate relational plan against schema",
            message: `Unknown column in relational plan: ${tableName}.${logicalColumn}`,
            table: tableName,
            column: logicalColumn,
          }),
        );
      }

      return Result.ok(undefined);
    }

    const table = schema.tables[tableName];
    if (!table) {
      return Result.ok(undefined);
    }

    if (!(logicalColumn in table.columns)) {
      return Result.err(
        createSchemaNormalizationError({
          operation: "validate relational plan against schema",
          message: `Unknown column in relational plan: ${tableName}.${logicalColumn}`,
          table: tableName,
          column: logicalColumn,
        }),
      );
    }

    return Result.ok(undefined);
  };

  const visit = (
    current: RelNode,
    cteNames: Set<string>,
  ): BetterResult<void, TuplSchemaNormalizationError> => {
    switch (current.kind) {
      case "scan":
        if (!cteNames.has(current.table) && !schema.tables[current.table] && !current.entity) {
          return Result.err(
            createSchemaNormalizationError({
              operation: "validate relational plan against schema",
              message: `Unknown table in relational plan: ${current.table}`,
              table: current.table,
            }),
          );
        }
        if (!cteNames.has(current.table) && (schema.tables[current.table] || current.entity)) {
          return Result.gen(function* () {
            for (const column of current.select) {
              yield* validateScanColumn(current.table, column, current.entity);
            }
            for (const clause of current.where ?? []) {
              yield* validateScanColumn(current.table, clause.column, current.entity);
            }
            for (const term of current.orderBy ?? []) {
              yield* validateScanColumn(current.table, term.column, current.entity);
            }
            return Result.ok(undefined);
          });
        }
        return Result.ok(undefined);
      case "values":
        return Result.ok(undefined);
      case "cte_ref":
        return Result.ok(undefined);
      case "filter":
      case "project":
      case "aggregate":
      case "window":
      case "sort":
      case "limit_offset":
        return visit(current.input, cteNames);
      case "join":
      case "set_op":
        return Result.gen(function* () {
          yield* visit(current.left, cteNames);
          yield* visit(current.right, cteNames);
          return Result.ok(undefined);
        });
      case "repeat_union":
        return Result.gen(function* () {
          const nextCteNames = new Set(cteNames);
          nextCteNames.add(current.cteName);
          yield* visit(current.seed, nextCteNames);
          yield* visit(current.iterative, nextCteNames);
          return Result.ok(undefined);
        });
      case "with": {
        const nextCteNames = new Set(cteNames);
        for (const cte of current.ctes) {
          nextCteNames.add(cte.name);
        }
        return Result.gen(function* () {
          for (const cte of current.ctes) {
            yield* visit(cte.query, nextCteNames);
          }
          yield* visit(current.body, nextCteNames);
          return Result.ok(undefined);
        });
      }
    }
  };

  return visit(node, new Set<string>());
}
