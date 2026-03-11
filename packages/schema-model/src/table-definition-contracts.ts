import type { RelExpr, RelNode } from "@tupl/foundation";

import type {
  ColumnForeignKeyReference,
  PhysicalDialect,
  SchemaColRefToken,
  SchemaDataEntityHandle,
  SchemaValueCoercion,
  SqlScalarType,
  TableColumnDefinition,
  TableConstraints,
} from "./schema-contracts";
import type { SchemaDslViewRelHelpers, SchemaViewRelNodeInput } from "./schema-view-contracts";

/**
 * Table definition contracts own the logical table/view declaration shapes used by the schema DSL.
 */
export interface SchemaColumnLensDefinition {
  source: string | SchemaColRefToken;
  type?: SqlScalarType;
  nullable?: boolean;
  primaryKey?: boolean;
  unique?: boolean;
  enum?: readonly string[];
  enumFrom?: SchemaColRefToken | string;
  enumMap?: Record<string, string>;
  physicalType?: string;
  physicalDialect?: PhysicalDialect;
  foreignKey?: ColumnForeignKeyReference;
  description?: string;
  coerce?: SchemaValueCoercion;
}

export interface SchemaTypedColumnDefinition<TSourceColumn extends string = string> {
  kind: "dsl_typed_column";
  sourceColumn: TSourceColumn;
  definition: TableColumnDefinition;
  coerce?: SchemaValueCoercion;
}

export interface SchemaCalculatedColumnDefinition {
  kind: "dsl_calculated_column";
  expr: RelExpr;
  definition: TableColumnDefinition;
  coerce?: SchemaValueCoercion;
}

type DslTableColumnInput<TSourceColumns extends string = string> =
  | TableColumnDefinition
  | SchemaColumnLensDefinition
  | SchemaColRefToken
  | SchemaTypedColumnDefinition<TSourceColumns>
  | SchemaCalculatedColumnDefinition;

type DslViewColumnInput<TSourceColumns extends string = string> =
  | SchemaColumnLensDefinition
  | SchemaColRefToken
  | SchemaTypedColumnDefinition<TSourceColumns>
  | SchemaCalculatedColumnDefinition;

export interface DslTableDefinition<
  TMappedColumns extends string = string,
  TSourceColumns extends string = string,
> {
  kind: "dsl_table";
  tableToken: import("./schema-contracts").SchemaDslTableToken<TMappedColumns>;
  from: SchemaDataEntityHandle<TSourceColumns>;
  columns: Record<TMappedColumns, DslTableColumnInput<TSourceColumns>>;
  constraints?: TableConstraints;
}

export interface DslViewDefinition<
  TContext,
  TColumns extends string = string,
  TRelColumns extends string = string,
> {
  kind: "dsl_view";
  tableToken: import("./schema-contracts").SchemaDslTableToken<TColumns>;
  rel: (
    context: TContext,
    helpers: SchemaDslViewRelHelpers,
  ) => SchemaViewRelNodeInput<TRelColumns> | RelNode;
  columns: Record<TColumns, DslViewColumnInput<TRelColumns>>;
  constraints?: TableConstraints;
}

export type SchemaDslRelationRef<TColumns extends string> =
  | import("./schema-contracts").SchemaDslTableToken<TColumns>
  | DslTableDefinition<TColumns, string>
  | DslViewDefinition<any, TColumns, string>;
