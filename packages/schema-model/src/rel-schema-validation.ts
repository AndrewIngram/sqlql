import { Result, type Result as BetterResult } from "better-result";

import { RelLoweringError, type DataEntityHandle, type RelNode } from "@tupl/foundation";

import type { SchemaDefinition } from "./types";

/**
 * Rel-schema validation owns checking that relational plans only reference schema-backed tables and columns.
 */
export function validateRelAgainstSchema(
  node: RelNode,
  schema: SchemaDefinition,
): BetterResult<void, RelLoweringError> {
  const validateScanColumn = (
    tableName: string,
    column: string,
    entity?: DataEntityHandle<string>,
  ): BetterResult<void, RelLoweringError> => {
    if (entity?.columns) {
      const logicalColumn = column.includes(".")
        ? column.slice(column.lastIndexOf(".") + 1)
        : column;
      if (!(logicalColumn in entity.columns)) {
        return Result.err(
          new RelLoweringError({
            operation: "validate relational plan against schema",
            message: `Unknown column in relational plan: ${tableName}.${logicalColumn}`,
          }),
        );
      }
      return Result.ok(undefined);
    }

    const table = schema.tables[tableName];
    if (!table) {
      return Result.ok(undefined);
    }
    const logicalColumn = column.includes(".") ? column.slice(column.lastIndexOf(".") + 1) : column;
    if (!(logicalColumn in table.columns)) {
      return Result.err(
        new RelLoweringError({
          operation: "validate relational plan against schema",
          message: `Unknown column in relational plan: ${tableName}.${logicalColumn}`,
        }),
      );
    }
    return Result.ok(undefined);
  };

  const visit = (current: RelNode): BetterResult<void, RelLoweringError> => {
    switch (current.kind) {
      case "scan":
        if (!schema.tables[current.table] && !current.entity) {
          return Result.err(
            new RelLoweringError({
              operation: "validate relational plan against schema",
              message: `Unknown table in relational plan: ${current.table}`,
            }),
          );
        }
        for (const column of current.select) {
          const result = validateScanColumn(current.table, column, current.entity);
          if (Result.isError(result)) {
            return result;
          }
        }
        for (const clause of current.where ?? []) {
          const result = validateScanColumn(current.table, clause.column, current.entity);
          if (Result.isError(result)) {
            return result;
          }
        }
        for (const term of current.orderBy ?? []) {
          const result = validateScanColumn(current.table, term.column, current.entity);
          if (Result.isError(result)) {
            return result;
          }
        }
        return Result.ok(undefined);
      case "values":
      case "cte_ref":
        return Result.ok(undefined);
      case "filter":
      case "project":
      case "aggregate":
      case "window":
      case "sort":
      case "limit_offset":
        return visit(current.input);
      case "correlate":
        return Result.gen(function* () {
          yield* visit(current.left);
          yield* visit(current.right);
          return Result.ok(undefined);
        });
      case "join":
      case "set_op":
        return Result.gen(function* () {
          yield* visit(current.left);
          yield* visit(current.right);
          return Result.ok(undefined);
        });
      case "repeat_union":
        return Result.gen(function* () {
          yield* visit(current.seed);
          yield* visit(current.iterative);
          return Result.ok(undefined);
        });
      case "with":
        return Result.gen(function* () {
          for (const cte of current.ctes) {
            yield* visit(cte.query);
          }
          yield* visit(current.body);
          return Result.ok(undefined);
        });
    }
  };

  return visit(node);
}
