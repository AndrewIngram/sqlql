import type { DslTableDefinition, DslViewDefinition } from "./dsl-contracts";

/**
 * Builder state owns the hidden registration map behind createSchemaBuilder().
 */
export type RegisteredSchemaDefinition<TContext> =
  | DslTableDefinition<string, string>
  | DslViewDefinition<TContext, string, string>;

export interface SchemaBuilderState<TContext> {
  definitions: Map<string, RegisteredSchemaDefinition<TContext>>;
}

export const schemaBuilderState = new WeakMap<object, SchemaBuilderState<any>>();
