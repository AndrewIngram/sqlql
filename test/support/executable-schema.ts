import {
  bindAdapterEntities,
  createDataEntityHandle,
  createExecutableSchema,
  getNormalizedTableBinding,
  isNormalizedSourceColumnBinding,
  type DataEntityColumnMap,
  type NormalizedColumnBinding,
  type ProviderAdapter,
  type SchemaColumnLensDefinition,
  type SchemaDefinition,
  type TableColumnDefinition,
} from "../../src";

export function finalizeProviders<TContext>(
  providers: Record<string, Omit<ProviderAdapter<TContext>, "name"> | ProviderAdapter<TContext>>,
): Record<string, ProviderAdapter<TContext>> {
  for (const [providerName, adapter] of Object.entries(providers)) {
    const boundAdapter = adapter as ProviderAdapter<TContext>;
    if (!boundAdapter.name) {
      boundAdapter.name = providerName;
    }
    bindAdapterEntities(boundAdapter);
  }

  return providers as Record<string, ProviderAdapter<TContext>>;
}

function toEntityColumns(
  columns: Record<string, TableColumnDefinition>,
): DataEntityColumnMap<string> {
  return Object.fromEntries(
    Object.entries(columns).map(([columnName, definition]) => [
      columnName,
      toEntityColumnMetadata(columnName, definition),
    ]),
  );
}

function toEntityColumnsFromBindings(
  bindings: Record<string, NormalizedColumnBinding>,
  fallbackDefinitions?: Record<string, TableColumnDefinition>,
): DataEntityColumnMap<string> {
  return Object.fromEntries(
    Object.entries(bindings).flatMap(([columnName, binding]) =>
      isNormalizedSourceColumnBinding(binding)
        ? [[
            columnName,
            toEntityColumnMetadata(
              columnName,
              binding.definition ?? fallbackDefinitions?.[columnName],
              binding.source,
            ),
          ] as const]
        : [],
    ),
  );
}

function toEntityColumnMetadata(
  columnName: string,
  definition?: TableColumnDefinition,
  source = columnName,
) {
  if (!definition) {
    return { source };
  }
  if (typeof definition === "string") {
    return {
      source,
      type: definition,
    };
  }
  return {
    source,
    type: definition.type,
    ...(definition.nullable != null ? { nullable: definition.nullable } : {}),
    ...(definition.primaryKey != null ? { primaryKey: definition.primaryKey } : {}),
    ...(definition.unique != null ? { unique: definition.unique } : {}),
    ...(definition.enum ? { enum: definition.enum } : {}),
    ...(definition.physicalType ? { physicalType: definition.physicalType } : {}),
    ...(definition.physicalDialect ? { physicalDialect: definition.physicalDialect } : {}),
  };
}

function toLensDefinition(
  columnName: string,
  definition: TableColumnDefinition,
): SchemaColumnLensDefinition {
  return toLensDefinitionFromSource(columnName, definition, columnName);
}

function toLensDefinitionFromSource(
  columnName: string,
  definition: TableColumnDefinition,
  source: string,
): SchemaColumnLensDefinition {
  if (typeof definition === "string") {
    return {
      source,
      type: definition,
    };
  }

  const lens: SchemaColumnLensDefinition = {
    source,
  };
  lens.type = definition.type;
  if (definition.nullable != null) {
    lens.nullable = definition.nullable;
  }
  if (definition.primaryKey != null) {
    lens.primaryKey = definition.primaryKey;
  }
  if (definition.unique != null) {
    lens.unique = definition.unique;
  }
  if (definition.enum) {
    lens.enum = definition.enum;
  }
  if (definition.enumFrom) {
    lens.enumFrom = definition.enumFrom;
  }
  if (definition.enumMap) {
    lens.enumMap = definition.enumMap;
  }
  if (definition.physicalType) {
    lens.physicalType = definition.physicalType;
  }
  if (definition.physicalDialect) {
    lens.physicalDialect = definition.physicalDialect;
  }
  if (definition.foreignKey) {
    lens.foreignKey = definition.foreignKey;
  }
  if (definition.description) {
    lens.description = definition.description;
  }
  return lens;
}

export function createExecutableSchemaFromProviders<
  TContext,
  TSchema extends SchemaDefinition,
>(
  schema: TSchema,
  providers: Record<string, Omit<ProviderAdapter<TContext>, "name"> | ProviderAdapter<TContext>>,
) {
  const providerEntries = Object.entries(providers);
  const singleProviderName = providerEntries.length === 1 ? providerEntries[0]?.[0] : undefined;

  for (const [providerName, adapter] of providerEntries) {
    const boundAdapter = adapter as ProviderAdapter<TContext>;
    if (!boundAdapter.name) {
      boundAdapter.name = providerName;
    }
    boundAdapter.entities ??= {};
  }

  return createExecutableSchema(({ table, view }) => ({
    tables: Object.fromEntries(
      Object.entries(schema.tables).map(([tableName, tableDefinition]) => {
        const binding = getNormalizedTableBinding(schema, tableName);
        if (binding?.kind === "view") {
          return [
            tableName,
            view({
              rel: (_helpers: unknown, context: TContext) => binding.rel(context),
              columns: () =>
                Object.fromEntries(
                  Object.entries(binding.columnBindings).flatMap(([columnName, columnBinding]) =>
                    isNormalizedSourceColumnBinding(columnBinding)
                      ? [[
                          columnName,
                          {
                            source: columnBinding.source,
                            ...(typeof columnBinding.definition === "string"
                              ? { type: columnBinding.definition }
                              : columnBinding.definition ?? {}),
                            ...(columnBinding.coerce ? { coerce: columnBinding.coerce } : {}),
                          },
                        ] as const]
                      : [],
                  ),
                ),
              ...(("constraints" in tableDefinition && tableDefinition.constraints)
                ? { constraints: tableDefinition.constraints }
                : {}),
            } as any),
          ];
        }

        const providerName = binding?.provider ?? tableDefinition.provider ?? singleProviderName;
        if (!providerName) {
          throw new Error(
            `Table ${tableName} must declare table.provider when more than one provider is involved.`,
          );
        }

        const adapter = providers[providerName] as ProviderAdapter<TContext> | undefined;
        if (!adapter) {
          throw new Error(`No provider registered for table ${tableName}: ${providerName}`);
        }

        if (!adapter.entities?.[tableName]) {
          adapter.entities ??= {};
          adapter.entities[tableName] = createDataEntityHandle({
            entity: binding?.kind === "physical" ? binding.entity : tableName,
            provider: providerName,
            adapter,
            columns: binding?.kind === "physical"
              ? toEntityColumnsFromBindings(binding.columnBindings, tableDefinition.columns)
              : toEntityColumns(tableDefinition.columns),
          });
        }

        bindAdapterEntities(adapter);

        return [
          tableName,
          table(adapter.entities[tableName], {
            columns: () =>
              Object.fromEntries(
                binding?.kind === "physical"
                  ? Object.entries(binding.columnBindings).flatMap(([columnName, columnBinding]) =>
                      isNormalizedSourceColumnBinding(columnBinding)
                        ? [[
                            columnName,
                            {
                              ...toLensDefinitionFromSource(
                                columnName,
                                columnBinding.definition ?? tableDefinition.columns[columnName] ?? "text",
                                columnBinding.source,
                              ),
                              ...(columnBinding.coerce ? { coerce: columnBinding.coerce } : {}),
                            },
                          ] as const]
                        : [],
                    )
                  : Object.entries(tableDefinition.columns).map(([columnName, definition]) => [
                      columnName,
                      toLensDefinition(columnName, definition),
                    ]),
              ),
            ...(tableDefinition.constraints ? { constraints: tableDefinition.constraints } : {}),
          }),
        ];
      }),
    ),
  }));
}
