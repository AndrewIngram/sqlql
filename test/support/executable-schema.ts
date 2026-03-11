import { type DataEntityColumnMap } from "@tupl/foundation";
import {
  bindAdapterEntities,
  createDataEntityHandle,
  type ProviderAdapter,
} from "@tupl/provider-kit";
import {
  createSchemaBuilder,
  getNormalizedTableBinding,
  isNormalizedSourceColumnBinding,
  type NormalizedColumnBinding,
  type SchemaColumnLensDefinition,
  type SchemaDefinition,
  type TableColumnDefinition,
} from "@tupl/schema";
import { createExecutableSchema } from "@tupl/runtime";

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
        ? [
            [
              columnName,
              toEntityColumnMetadata(
                columnName,
                binding.definition ?? fallbackDefinitions?.[columnName],
                binding.source,
              ),
            ] as const,
          ]
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
  return toLensDefinitionFromSource(definition, columnName);
}

function toLensDefinitionFromSource(
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

function getCalculatedColumnMethodName(definition: TableColumnDefinition): string {
  const type = typeof definition === "string" ? definition : definition.type;
  return type === "text" ? "string" : type;
}

function getCalculatedColumnOptions(
  binding: Extract<NormalizedColumnBinding, { kind: "expr" }>,
  definition: TableColumnDefinition,
) {
  if (typeof definition === "string") {
    return binding.coerce ? { coerce: binding.coerce } : {};
  }

  return {
    ...(definition.nullable != null ? { nullable: definition.nullable } : {}),
    ...(definition.physicalType ? { physicalType: definition.physicalType } : {}),
    ...(definition.physicalDialect ? { physicalDialect: definition.physicalDialect } : {}),
    ...(definition.foreignKey ? { foreignKey: definition.foreignKey } : {}),
    ...(definition.description ? { description: definition.description } : {}),
    ...(binding.coerce ? { coerce: binding.coerce } : {}),
  };
}

export function createExecutableSchemaFromProviders<TContext, TSchema extends SchemaDefinition>(
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

  const builder = createSchemaBuilder<TContext>();

  for (const [tableName, tableDefinition] of Object.entries(schema.tables)) {
    const binding = getNormalizedTableBinding(schema, tableName);
    if (binding?.kind === "view") {
      builder.view(
        tableName,
        ((context: TContext) => binding.rel(context)) as any,
        {
          columns: () =>
            Object.fromEntries(
              Object.entries(binding.columnBindings).flatMap(([columnName, columnBinding]) =>
                isNormalizedSourceColumnBinding(columnBinding)
                  ? [
                      [
                        columnName,
                        {
                          source: columnBinding.source,
                          ...(typeof columnBinding.definition === "string"
                            ? { type: columnBinding.definition }
                            : (columnBinding.definition ?? {})),
                          ...(columnBinding.coerce ? { coerce: columnBinding.coerce } : {}),
                        },
                      ] as const,
                    ]
                  : [],
              ),
            ),
          ...("constraints" in tableDefinition && tableDefinition.constraints
            ? { constraints: tableDefinition.constraints }
            : {}),
        } as any,
      );
      continue;
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
        columns:
          binding?.kind === "physical"
            ? toEntityColumnsFromBindings(binding.columnBindings, tableDefinition.columns)
            : toEntityColumns(tableDefinition.columns),
      });
    }

    bindAdapterEntities(adapter);

    builder.table(tableName, adapter.entities[tableName], {
      columns: ({ col }) =>
        Object.fromEntries(
          binding?.kind === "physical"
            ? Object.entries(binding.columnBindings).map(([columnName, columnBinding]) => {
                if (isNormalizedSourceColumnBinding(columnBinding)) {
                  return [
                    columnName,
                    {
                      ...toLensDefinitionFromSource(
                        columnBinding.definition ?? tableDefinition.columns[columnName] ?? "text",
                        columnBinding.source,
                      ),
                      ...(columnBinding.coerce ? { coerce: columnBinding.coerce } : {}),
                    },
                  ] as const;
                }

                if (columnBinding.kind !== "expr") {
                  throw new Error(`Unsupported column binding kind for ${tableName}.${columnName}`);
                }

                const definition =
                  columnBinding.definition ?? tableDefinition.columns[columnName] ?? "text";
                const methodName = getCalculatedColumnMethodName(definition);
                const calcMethod = (
                  col as unknown as Record<string, (expr: any, options?: any) => unknown>
                )[methodName];
                if (typeof calcMethod !== "function") {
                  throw new Error(
                    `Unsupported calculated column type for ${tableName}.${columnName}`,
                  );
                }

                return [
                  columnName,
                  calcMethod(
                    columnBinding.expr,
                    getCalculatedColumnOptions(columnBinding, definition),
                  ),
                ] as const;
              })
            : Object.entries(tableDefinition.columns).map(([columnName, definition]) => [
                columnName,
                toLensDefinition(columnName, definition),
              ]),
        ) as Record<string, any>,
      ...(tableDefinition.constraints ? { constraints: tableDefinition.constraints } : {}),
    });
  }

  return createExecutableSchema(builder);
}
