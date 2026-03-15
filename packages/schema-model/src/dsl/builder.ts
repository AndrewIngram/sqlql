import type { RelNode } from "@tupl/foundation";

import type {
  SchemaBuilder,
  SchemaDefinition,
  TableMethodsForSchema,
  TableMethodsMap,
} from "../types";
import type {
  TablePlanningMethodsForSchema,
  TablePlanningMethodsMap,
} from "../contracts/table-planning-contracts";
import {
  buildColumnExprHelpers as buildColumnExprHelpersInternal,
  buildSchemaColumnsColHelper as buildSchemaColumnsColHelperInternal,
  createSchemaDslTableToken,
  isSchemaDataEntityHandle as isSchemaDataEntityHandleInternal,
} from "./builder-helpers";
import {
  schemaBuilderState,
  type RegisteredSchemaDefinition,
  type SchemaBuilderState,
} from "./builder-state";
import type { SchemaDslViewRelHelpers, SchemaViewRelNodeInput } from "../contracts/dsl-contracts";
import { buildRegisteredSchemaDefinition } from "../normalization";

/**
 * Builder owns schema authoring entrypoints and registration flow.
 * It intentionally stops at collecting table/view definitions; normalization semantics live downstream.
 */
export function createSchemaBuilder<TContext>(): SchemaBuilder<TContext> {
  const state: SchemaBuilderState<TContext> = {
    definitions: new Map(),
  };

  const registerDefinition = <TDefinition extends RegisteredSchemaDefinition<TContext>>(
    name: string,
    definition: TDefinition,
  ): TDefinition => {
    if (typeof name !== "string" || name.trim().length === 0) {
      throw new Error("Schema builder table/view name must be a non-empty string.");
    }
    if (state.definitions.has(name)) {
      throw new Error(`Schema builder already contains a table or view named ${name}.`);
    }
    state.definitions.set(name, definition);
    return definition;
  };

  const table = ((name: unknown, from: unknown, input: any) => {
    if (
      typeof name !== "string" ||
      name.trim().length === 0 ||
      !isSchemaDataEntityHandleInternal(from) ||
      !input
    ) {
      throw new Error(
        "Schema builder table(name, source, config) requires a non-empty name, a data entity handle, and a config object.",
      );
    }

    const columns =
      typeof input.columns === "function"
        ? input.columns({
            col: buildSchemaColumnsColHelperInternal(),
            expr: buildColumnExprHelpersInternal(),
          })
        : input.columns;

    return registerDefinition(name, {
      kind: "dsl_table",
      tableToken: createSchemaDslTableToken(),
      from,
      columns,
      ...(input.constraints ? { constraints: input.constraints } : {}),
    });
  }) as SchemaBuilder<TContext>["table"];

  const view = ((name: unknown, relFactory: unknown, input: any) => {
    if (
      typeof name !== "string" ||
      name.trim().length === 0 ||
      typeof relFactory !== "function" ||
      !input
    ) {
      throw new Error(
        "Schema builder view(name, source, config) requires a non-empty name, a rel function, and a config object.",
      );
    }
    const rel = (context: TContext, helpers: SchemaDslViewRelHelpers) =>
      (relFactory as Function).length === 0
        ? (relFactory as () => SchemaViewRelNodeInput<string> | RelNode)()
        : (
            relFactory as (
              helpers: SchemaDslViewRelHelpers,
              context: TContext,
            ) => SchemaViewRelNodeInput<string> | RelNode
          )(helpers, context);
    const columns =
      typeof input.columns === "function"
        ? input.columns({
            col: buildSchemaColumnsColHelperInternal(),
            expr: buildColumnExprHelpersInternal(),
          })
        : input.columns;

    return registerDefinition(name, {
      kind: "dsl_view",
      tableToken: createSchemaDslTableToken(),
      rel,
      columns,
      ...(input.constraints ? { constraints: input.constraints } : {}),
    });
  }) as SchemaBuilder<TContext>["view"];

  const builder: SchemaBuilder<TContext> = {
    table,
    view,
    build() {
      return buildRegisteredSchemaDefinition(state);
    },
  };

  schemaBuilderState.set(builder as object, state);
  return builder;
}

export function isSchemaBuilder<TContext = unknown>(
  value: unknown,
): value is SchemaBuilder<TContext> {
  return !!value && typeof value === "object" && schemaBuilderState.has(value as object);
}

/**
 * `defineTableMethods(...)` preserves whichever contract the caller opts into:
 * root-visible scan/lookup/aggregate behavior for ordinary schema code, or the explicit
 * table-planning extension contract for runtimes and advanced tests.
 */
export function defineTableMethods<TContext, TMethods extends TableMethodsMap<TContext>>(
  methods: TMethods,
): TMethods;
export function defineTableMethods<TContext, TMethods extends TablePlanningMethodsMap<TContext>>(
  methods: TMethods,
): TMethods;

export function defineTableMethods<TSchema extends SchemaDefinition, TContext>(
  schema: TSchema,
  methods: TableMethodsForSchema<TSchema, TContext>,
): TableMethodsForSchema<TSchema, TContext>;
export function defineTableMethods<TSchema extends SchemaDefinition, TContext>(
  schema: TSchema,
  methods: TablePlanningMethodsForSchema<TSchema, TContext>,
): TablePlanningMethodsForSchema<TSchema, TContext>;

export function defineTableMethods(...args: unknown[]): unknown {
  if (args.length === 1) {
    return args[0];
  }

  if (args.length === 2) {
    return args[1];
  }

  throw new Error("defineTableMethods expects either (methods) or (schema, methods).");
}
