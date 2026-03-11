import type {
  DataEntityColumnMetadata,
  DataEntityReadMetadataMap,
  RelExpr,
  RelNode,
} from "@tupl/foundation";

import type { SchemaDataEntityHandle, TableConstraints } from "./schema-contracts";
import type { DslTableDefinition, DslViewDefinition } from "./table-definition-contracts";
import type { SchemaDslViewRelHelpers, SchemaViewRelNodeInput } from "./schema-view-contracts";
import type { SchemaTypedColumnBuilder } from "./typed-column-builder-contracts";

/**
 * Schema builder contracts own the public table/view builder call signatures and helper callback types.
 */
type SchemaColumnsColHelper<
  TSourceColumns extends string,
  TColumnMetadata extends Partial<Record<TSourceColumns, DataEntityColumnMetadata<any>>> =
    DataEntityReadMetadataMap<TSourceColumns, Record<TSourceColumns, unknown>>,
> = SchemaTypedColumnBuilder<TSourceColumns, TColumnMetadata> & {
  (ref: string): RelExpr;
  <TColumns extends string, TColumn extends TColumns>(
    table: import("./table-definition-contracts").SchemaDslRelationRef<TColumns>,
    column: TColumn,
  ): RelExpr;
};

interface SchemaColumnExprHelpers {
  literal: (value: string | number | boolean | null) => RelExpr;
  eq: (left: RelExpr, right: RelExpr) => RelExpr;
  neq: (left: RelExpr, right: RelExpr) => RelExpr;
  gt: (left: RelExpr, right: RelExpr) => RelExpr;
  gte: (left: RelExpr, right: RelExpr) => RelExpr;
  lt: (left: RelExpr, right: RelExpr) => RelExpr;
  lte: (left: RelExpr, right: RelExpr) => RelExpr;
  add: (left: RelExpr, right: RelExpr) => RelExpr;
  subtract: (left: RelExpr, right: RelExpr) => RelExpr;
  multiply: (left: RelExpr, right: RelExpr) => RelExpr;
  divide: (left: RelExpr, right: RelExpr) => RelExpr;
  and: (...args: RelExpr[]) => RelExpr;
  or: (...args: RelExpr[]) => RelExpr;
  not: (input: RelExpr) => RelExpr;
}

type SchemaBuilderTableMethods = {
  <
    TSourceColumns extends string,
    TMappedColumns extends string,
    TRow extends Partial<Record<TSourceColumns, unknown>> = Record<TSourceColumns, unknown>,
    TColumnMetadata extends Partial<Record<TSourceColumns, DataEntityColumnMetadata<any>>> =
      DataEntityReadMetadataMap<TSourceColumns, TRow>,
  >(
    name: string,
    from: SchemaDataEntityHandle<TSourceColumns, TRow, TColumnMetadata>,
    input: {
      columns:
        | Record<
            TMappedColumns,
            | import("./table-definition-contracts").SchemaColumnLensDefinition
            | import("./table-definition-contracts").SchemaTypedColumnDefinition<TSourceColumns>
            | import("./table-definition-contracts").SchemaCalculatedColumnDefinition
            | import("./schema-contracts").TableColumnDefinition
            | import("./schema-contracts").SchemaColRefToken
          >
        | ((helpers: {
            col: SchemaColumnsColHelper<TSourceColumns, TColumnMetadata>;
            expr: SchemaColumnExprHelpers;
          }) => Record<
            TMappedColumns,
            | import("./table-definition-contracts").SchemaColumnLensDefinition
            | import("./table-definition-contracts").SchemaTypedColumnDefinition<TSourceColumns>
            | import("./table-definition-contracts").SchemaCalculatedColumnDefinition
            | import("./schema-contracts").TableColumnDefinition
            | import("./schema-contracts").SchemaColRefToken
          >);
      constraints?: TableConstraints;
    },
  ): DslTableDefinition<TMappedColumns, TSourceColumns>;
};

type SchemaBuilderViewMethods<TContext> = {
  <TRelColumns extends string, TColumns extends string>(
    name: string,
    rel: (
      helpers: SchemaDslViewRelHelpers,
      context: TContext,
    ) => SchemaViewRelNodeInput<TRelColumns> | RelNode,
    input: {
      columns:
        | ((helpers: {
            col: SchemaColumnsColHelper<
              TRelColumns,
              DataEntityReadMetadataMap<TRelColumns, Record<TRelColumns, unknown>>
            >;
            expr: SchemaColumnExprHelpers;
          }) => Record<
            TColumns,
            | import("./table-definition-contracts").SchemaColumnLensDefinition
            | import("./table-definition-contracts").SchemaTypedColumnDefinition<TRelColumns>
            | import("./table-definition-contracts").SchemaCalculatedColumnDefinition
            | import("./schema-contracts").SchemaColRefToken
          >)
        | Record<
            TColumns,
            | import("./table-definition-contracts").SchemaColumnLensDefinition
            | import("./table-definition-contracts").SchemaTypedColumnDefinition<TRelColumns>
            | import("./table-definition-contracts").SchemaCalculatedColumnDefinition
            | import("./schema-contracts").SchemaColRefToken
          >;
      constraints?: TableConstraints;
    },
  ): DslViewDefinition<TContext, TColumns, TRelColumns>;
  <TColumns extends string>(
    name: string,
    rel: (context: TContext) => SchemaViewRelNodeInput<string> | RelNode,
    input: {
      columns: Record<
        TColumns,
        | import("./table-definition-contracts").SchemaColumnLensDefinition
        | import("./table-definition-contracts").SchemaTypedColumnDefinition<string>
        | import("./table-definition-contracts").SchemaCalculatedColumnDefinition
        | import("./schema-contracts").SchemaColRefToken
      >;
      constraints?: TableConstraints;
    },
  ): DslViewDefinition<TContext, TColumns, string>;
};

export interface SchemaBuilder<TContext> {
  table: SchemaBuilderTableMethods;
  view: SchemaBuilderViewMethods<TContext>;
  build(): import("./schema-contracts").SchemaDefinition;
}
