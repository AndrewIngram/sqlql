import { createDataEntityHandle } from "@tupl/provider-kit";
import {
  createSchemaBuilder,
  type SchemaBuilder,
  type SchemaDefinition,
  type TableColumnDefinition,
  type TableConstraints,
} from "@tupl/schema-model";

/**
 * Schema fixtures own the simplest shared builders for test-only logical schemas.
 */
export function buildSchema<TContext = Record<string, never>>(
  register: (builder: SchemaBuilder<TContext>) => void,
): SchemaDefinition {
  const builder = createSchemaBuilder<TContext>();
  register(builder);
  return builder.build();
}

type EntityTableDefinition<TColumns extends Record<string, TableColumnDefinition>> = {
  provider?: string;
  columns: TColumns;
  constraints?: TableConstraints;
};

type EntitySchemaInput = Record<
  string,
  EntityTableDefinition<Record<string, TableColumnDefinition>>
>;

type EntitySchemaDefinition<TTables extends EntitySchemaInput> = {
  tables: {
    [TTableName in keyof TTables]: {
      provider?: string;
      columns: TTables[TTableName]["columns"];
      constraints?: TableConstraints;
    };
  };
};

export function buildEntitySchema<const TTables extends EntitySchemaInput>(
  tables: TTables,
): EntitySchemaDefinition<TTables> {
  const builder = createSchemaBuilder<Record<string, never>>();
  for (const [name, table] of Object.entries(tables)) {
    const entity = createDataEntityHandle({
      entity: name,
      provider: table.provider ?? "memory",
    });
    builder.table(name, entity, {
      columns: table.columns,
      ...(table.constraints ? { constraints: table.constraints } : {}),
    });
  }
  return builder.build() as EntitySchemaDefinition<TTables>;
}
