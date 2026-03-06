import {
  bindAdapterEntities,
  type ConstraintValidationOptions,
  createDataEntityHandle,
  createExecutableSchema,
  type QueryGuardrails,
  type QuerySessionOptions,
  type ProviderAdapter,
  type ProviderFragment,
  type QueryRow,
  type SchemaDefinition,
  type TableMethodsMap,
} from "../../src";

function toEntityColumns(schema: SchemaDefinition, tableName: string) {
  const table = schema.tables[tableName];
  if (!table) {
    throw new Error(`Unknown table: ${tableName}`);
  }

  return Object.fromEntries(
    Object.entries(table.columns).map(([columnName, definition]) => [
      columnName,
      typeof definition === "string"
        ? {
            source: columnName,
            type: definition,
          }
        : {
            source: columnName,
            type: definition.type,
            ...(definition.nullable != null ? { nullable: definition.nullable } : {}),
            ...(definition.primaryKey != null ? { primaryKey: definition.primaryKey } : {}),
            ...(definition.unique != null ? { unique: definition.unique } : {}),
            ...(definition.enum ? { enum: definition.enum } : {}),
            ...(definition.physicalType ? { physicalType: definition.physicalType } : {}),
            ...(definition.physicalDialect ? { physicalDialect: definition.physicalDialect } : {}),
          },
    ]),
  );
}

export function createMethodsProvider<TContext>(
  schema: SchemaDefinition,
  methods: TableMethodsMap<TContext>,
  providerName = "memory",
): ProviderAdapter<TContext> {
  const adapter: ProviderAdapter<TContext> = {
    name: providerName,
    entities: {},
    canExecute(fragment) {
      switch (fragment.kind) {
        case "scan":
          return !!methods[fragment.table]?.scan;
        case "aggregate":
          return !!methods[fragment.table]?.aggregate;
        case "rel":
          return false;
        default:
          return false;
      }
    },
    async compile(fragment) {
      return {
        provider: providerName,
        kind: fragment.kind,
        payload: fragment,
      };
    },
    async execute(plan, context) {
      const fragment = plan.payload as ProviderFragment;
      switch (fragment.kind) {
        case "scan": {
          const method = methods[fragment.table];
          if (!method?.scan) {
            throw new Error(`No table methods registered for table: ${fragment.table}`);
          }
          return method.scan(fragment.request, context);
        }
        case "aggregate": {
          const method = methods[fragment.table];
          if (!method?.aggregate) {
            throw new Error(`No aggregate method registered for table: ${fragment.table}`);
          }
          return method.aggregate(fragment.request, context);
        }
        case "rel":
          throw new Error("Methods-based provider does not support rel fragments.");
      }
    },
    async lookupMany(request, context): Promise<QueryRow[]> {
      const method = methods[request.table];
      if (!method?.lookup) {
        return [];
      }

      return method.lookup(
        {
          table: request.table,
          key: request.key,
          values: request.keys,
          select: request.select,
          ...(request.where ? { where: request.where } : {}),
        },
        context,
      );
    },
  };

  for (const tableName of Object.keys(schema.tables)) {
    adapter.entities![tableName] = createDataEntityHandle({
      entity: tableName,
      provider: providerName,
      adapter,
      columns: toEntityColumns(schema, tableName),
    });
  }

  return bindAdapterEntities(adapter);
}

export function createExecutableMethodsSchema<TContext, TSchema extends SchemaDefinition>(
  schema: TSchema,
  methods: TableMethodsMap<TContext>,
  providerName = "memory",
) {
  const provider = createMethodsProvider(schema, methods, providerName);

  return createExecutableSchema(({ table }) => ({
    tables: Object.fromEntries(
      Object.entries(schema.tables).map(([tableName, tableDefinition]) => [
        tableName,
        table(provider.entities![tableName]!, {
          columns: () => Object.fromEntries(
            Object.entries(tableDefinition.columns).map(([columnName, definition]) => [
              columnName,
              typeof definition === "string"
                ? { source: columnName, type: definition }
                : { source: columnName, ...definition },
            ]),
          ),
          ...(tableDefinition.constraints ? { constraints: tableDefinition.constraints } : {}),
        }),
      ]),
    ),
  }));
}

export function queryWithMethods<TContext>(
  input: {
    schema: SchemaDefinition;
    methods: TableMethodsMap<TContext>;
    context: TContext;
    sql: string;
    queryGuardrails?: Partial<QueryGuardrails>;
    constraintValidation?: ConstraintValidationOptions;
  },
): Promise<QueryRow[]> {
  return createExecutableMethodsSchema(
    input.schema,
    input.methods,
  ).query({
    context: input.context,
    sql: input.sql,
    ...(input.queryGuardrails ? { queryGuardrails: input.queryGuardrails } : {}),
    ...(input.constraintValidation ? { constraintValidation: input.constraintValidation } : {}),
  });
}

export function createMethodsSession<TContext>(
  input: {
    schema: SchemaDefinition;
    methods: TableMethodsMap<TContext>;
    context: TContext;
    sql: string;
    queryGuardrails?: Partial<QueryGuardrails>;
    constraintValidation?: ConstraintValidationOptions;
    options?: QuerySessionOptions;
  },
) {
  return createExecutableMethodsSchema(
    input.schema,
    input.methods,
  ).createSession({
    context: input.context,
    sql: input.sql,
    ...(input.queryGuardrails ? { queryGuardrails: input.queryGuardrails } : {}),
    ...(input.constraintValidation ? { constraintValidation: input.constraintValidation } : {}),
    ...(input.options ? { options: input.options } : {}),
  });
}
