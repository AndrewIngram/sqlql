import { isSchemaDataEntityHandle, toSchemaDslTableToken } from "./dsl-tokens";
import type { SchemaColRefToken } from "./types";

/**
 * DSL view helpers own scan/join/aggregate helper assembly for schema-defined views.
 */
export function buildSchemaDslViewRelHelpers() {
  return {
    col<TColumns extends string, TColumn extends TColumns>(
      tableOrEntity: unknown,
      column?: TColumn,
    ): SchemaColRefToken {
      if (typeof tableOrEntity === "string") {
        if (column != null) {
          throw new Error(
            "Schema DSL rel col(ref) does not accept a second argument for string refs.",
          );
        }
        return {
          kind: "dsl_col_ref",
          ref: tableOrEntity,
        };
      }

      if (column == null) {
        throw new Error("Schema DSL rel col(table, column) requires a column name.");
      }

      if (isSchemaDataEntityHandle(tableOrEntity)) {
        return {
          kind: "dsl_col_ref",
          entity: tableOrEntity,
          column,
        };
      }

      return {
        kind: "dsl_col_ref",
        table: toSchemaDslTableToken(tableOrEntity),
        column,
      };
    },
    expr: {
      eq(left: SchemaColRefToken, right: SchemaColRefToken) {
        return {
          kind: "eq",
          left,
          right,
        };
      },
    },
    agg: {
      count() {
        return { kind: "metric", fn: "count" };
      },
      countDistinct(column: SchemaColRefToken) {
        return { kind: "metric", fn: "count", column, distinct: true };
      },
      sum(column: SchemaColRefToken) {
        return { kind: "metric", fn: "sum", column };
      },
      sumDistinct(column: SchemaColRefToken) {
        return { kind: "metric", fn: "sum", column, distinct: true };
      },
      avg(column: SchemaColRefToken) {
        return { kind: "metric", fn: "avg", column };
      },
      avgDistinct(column: SchemaColRefToken) {
        return { kind: "metric", fn: "avg", column, distinct: true };
      },
      min(column: SchemaColRefToken) {
        return { kind: "metric", fn: "min", column };
      },
      max(column: SchemaColRefToken) {
        return { kind: "metric", fn: "max", column };
      },
    },
    scan(table: unknown) {
      return {
        kind: "scan",
        table:
          typeof table === "string" || isSchemaDataEntityHandle(table)
            ? table
            : toSchemaDslTableToken(table),
      };
    },
    join(input: {
      left: unknown;
      right: unknown;
      on: { kind: "eq"; left: SchemaColRefToken; right: SchemaColRefToken };
      type?: "inner" | "left" | "right" | "full";
    }) {
      return {
        kind: "join",
        left: input.left,
        right: input.right,
        on: input.on,
        type: input.type ?? "inner",
      };
    },
    aggregate(input: {
      from: unknown;
      groupBy: Record<string, SchemaColRefToken>;
      measures: Record<
        string,
        { kind: "metric"; fn: string; column?: SchemaColRefToken; distinct?: true }
      >;
    }) {
      return {
        kind: "aggregate",
        from: input.from,
        groupBy: input.groupBy,
        measures: input.measures,
      };
    },
  };
}
