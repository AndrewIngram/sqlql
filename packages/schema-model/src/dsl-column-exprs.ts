import type {
  DataEntityColumnMetadata,
  DataEntityReadMetadataMap,
  RelExpr,
} from "@tupl/foundation";

import { buildTypedColumnBuilder } from "./typed-column-builders";
import { toSchemaDslTableToken } from "./dsl-tokens";

/**
 * DSL column expressions own expression helper factories and typed column+expr column refs.
 */
export function buildColumnExprHelpers() {
  const fn = (name: string, ...args: RelExpr[]): RelExpr => ({
    kind: "function",
    name,
    args,
  });

  return {
    literal(value: string | number | boolean | null) {
      return { kind: "literal", value } satisfies RelExpr;
    },
    eq(left: RelExpr, right: RelExpr) {
      return fn("eq", left, right);
    },
    neq(left: RelExpr, right: RelExpr) {
      return fn("neq", left, right);
    },
    gt(left: RelExpr, right: RelExpr) {
      return fn("gt", left, right);
    },
    gte(left: RelExpr, right: RelExpr) {
      return fn("gte", left, right);
    },
    lt(left: RelExpr, right: RelExpr) {
      return fn("lt", left, right);
    },
    lte(left: RelExpr, right: RelExpr) {
      return fn("lte", left, right);
    },
    add(left: RelExpr, right: RelExpr) {
      return fn("add", left, right);
    },
    subtract(left: RelExpr, right: RelExpr) {
      return fn("subtract", left, right);
    },
    multiply(left: RelExpr, right: RelExpr) {
      return fn("multiply", left, right);
    },
    divide(left: RelExpr, right: RelExpr) {
      return fn("divide", left, right);
    },
    and(...args: RelExpr[]) {
      return fn("and", ...args);
    },
    or(...args: RelExpr[]) {
      return fn("or", ...args);
    },
    not(input: RelExpr) {
      return fn("not", input);
    },
  };
}

export function buildSchemaColumnsColHelper<
  TSourceColumns extends string,
  TColumnMetadata extends Partial<Record<TSourceColumns, DataEntityColumnMetadata<any>>> =
    DataEntityReadMetadataMap<TSourceColumns, Record<TSourceColumns, unknown>>,
>() {
  return Object.assign(function col<TColumns extends string, TColumn extends TColumns>(
    tableOrRef: unknown,
    column?: TColumn,
  ): RelExpr {
    if (typeof tableOrRef === "string") {
      if (column != null) {
        throw new Error(
          "Schema DSL column expr col(ref) does not accept a second argument for string refs.",
        );
      }
      return {
        kind: "column",
        ref: { column: tableOrRef },
      } satisfies RelExpr;
    }

    if (column == null) {
      throw new Error("Schema DSL column expr col(table, column) requires a column name.");
    }

    return {
      kind: "column",
      ref: {
        table: toSchemaDslTableToken(tableOrRef) as unknown as string,
        column,
      },
    } satisfies RelExpr;
  }, buildTypedColumnBuilder<TSourceColumns, TColumnMetadata>());
}
