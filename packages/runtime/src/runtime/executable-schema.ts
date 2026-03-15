import { Result } from "better-result";

import { TuplRuntimeError, type TuplResult } from "@tupl/foundation";
import { type ProviderAdapter, type ProvidersMap } from "@tupl/provider-kit";
import { isSchemaBuilder, type SchemaBuilder, type SchemaDefinition } from "@tupl/schema-model";
import {
  finalizeSchemaDefinition,
  getNormalizedTableBinding,
} from "@tupl/schema-model/normalization";

import type { ExecutableSchema } from "./contracts";
import { bindExecutableSchemaSessionAccess } from "./executable-schema-runtime";
import { explainInternalResult, queryInternalResult } from "./query-runner";
import { createQuerySession } from "./session/query-session-factory";

/**
 * Executable schema owns schema-to-runtime binding and the public executable facade constructors.
 */
function collectExecutableProvidersResult<TContext>(schema: SchemaDefinition) {
  const providers: ProvidersMap<TContext> = {};

  for (const [tableName] of Object.entries(schema.tables)) {
    const binding = getNormalizedTableBinding(schema, tableName);
    if (!binding || binding.kind === "view") {
      continue;
    }

    const provider = binding.providerInstance as ProviderAdapter<TContext> | undefined;
    if (!provider) {
      return Result.err(
        new TuplRuntimeError({
          operation: "collect executable providers",
          message: `Table ${tableName} must be declared from a provider-owned entity via table(name, provider.entities.someTable, config).`,
        }),
      );
    }

    const existing = providers[provider.name];
    if (existing && existing !== provider) {
      return Result.err(
        new TuplRuntimeError({
          operation: "collect executable providers",
          message: `Duplicate provider name detected in executable schema: ${provider.name}.`,
        }),
      );
    }
    providers[provider.name] = provider;

    if (!binding.provider || binding.provider !== provider.name) {
      return Result.err(
        new TuplRuntimeError({
          operation: "collect executable providers",
          message: `Table ${tableName} is bound to provider ${binding.provider ?? "<missing>"}, but the attached provider is named ${provider.name}.`,
        }),
      );
    }
  }

  return Result.ok(providers);
}

function createExecutableSchemaInternal<TContext, TSchema extends SchemaDefinition>(
  input: TSchema | SchemaBuilder<TContext>,
): TuplResult<ExecutableSchema<TContext, TSchema | SchemaDefinition>> {
  const schemaResult = isSchemaBuilder<TContext>(input)
    ? input.build()
    : finalizeSchemaDefinition(input as TSchema);
  if (Result.isError(schemaResult)) {
    return schemaResult as TuplResult<ExecutableSchema<TContext, TSchema | SchemaDefinition>>;
  }

  const schema = schemaResult.value;
  const providersResult = collectExecutableProvidersResult<TContext>(schema);
  if (Result.isError(providersResult)) {
    return providersResult as TuplResult<ExecutableSchema<TContext, TSchema | SchemaDefinition>>;
  }

  const providers = providersResult.value;
  const runtime = {
    schema,
    providers,
  };

  const executableSchema = {
    schema,
    query(input) {
      return queryInternalResult({
        schema: runtime.schema,
        providers: runtime.providers,
        ...input,
      });
    },
    explain(input) {
      return explainInternalResult({
        schema: runtime.schema,
        providers: runtime.providers,
        ...input,
      });
    },
  } satisfies ExecutableSchema<TContext, TSchema | SchemaDefinition>;

  bindExecutableSchemaSessionAccess(executableSchema, {
    createSession(input) {
      return createQuerySession({
        schema: runtime.schema,
        providers: runtime.providers,
        ...input,
      });
    },
  });
  return Result.ok(executableSchema);
}

export function createExecutableSchema<TContext>(
  builder: SchemaBuilder<TContext>,
): TuplResult<ExecutableSchema<TContext, SchemaDefinition>>;
export function createExecutableSchema<TContext, TSchema extends SchemaDefinition>(
  schema: TSchema,
): TuplResult<ExecutableSchema<TContext, TSchema>>;
export function createExecutableSchema<TContext, TSchema extends SchemaDefinition>(
  input: TSchema | SchemaBuilder<TContext>,
): TuplResult<ExecutableSchema<TContext, TSchema | SchemaDefinition>> {
  return createExecutableSchemaInternal(input);
}
