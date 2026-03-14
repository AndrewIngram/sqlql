import type { ProviderAdapter } from "@tupl/provider-kit";
import type { RelExpr } from "@tupl/foundation";

import type {
  SchemaDefinition,
  SchemaValueCoercion,
  TableColumnDefinition,
} from "./schema-contracts";

/**
 * Normalized contracts define schema bindings after DSL resolution.
 */
export interface NormalizedPhysicalTableBinding {
  kind: "physical";
  provider?: string;
  entity: string;
  columnBindings: Record<string, NormalizedColumnBinding>;
  columnToSource: Record<string, string>;
  providerInstance?: ProviderAdapter<unknown>;
}

export interface NormalizedViewTableBinding<TContext = unknown> {
  kind: "view";
  rel: (context: TContext) => unknown;
  columnBindings: Record<string, NormalizedColumnBinding>;
  columnToSource: Record<string, string>;
}

export interface NormalizedSourceColumnBinding {
  kind: "source";
  source: string;
  definition?: TableColumnDefinition;
  coerce?: SchemaValueCoercion;
}

export interface NormalizedCalculatedColumnBinding {
  kind: "expr";
  expr: RelExpr;
  definition?: TableColumnDefinition;
  coerce?: SchemaValueCoercion;
}

export type NormalizedColumnBinding =
  | NormalizedSourceColumnBinding
  | NormalizedCalculatedColumnBinding;

export type NormalizedTableBinding<TContext = unknown> =
  | NormalizedPhysicalTableBinding
  | NormalizedViewTableBinding<TContext>;

export type TableName<TSchema extends SchemaDefinition> = Extract<keyof TSchema["tables"], string>;

export type TableColumnName<
  TSchema extends SchemaDefinition,
  TTableName extends TableName<TSchema>,
> = Extract<keyof TSchema["tables"][TTableName]["columns"], string>;
