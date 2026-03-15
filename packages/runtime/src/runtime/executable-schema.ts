import { Result } from "better-result";

import { TuplRuntimeError, type TuplResult } from "@tupl/foundation";
import { type ProviderAdapter, type ProvidersMap } from "@tupl/provider-kit";
import { isSchemaBuilder, type SchemaBuilder, type SchemaDefinition } from "@tupl/schema-model";
import { resolveSchemaLinkedEnums } from "@tupl/schema-model/enums";
import {
  finalizeSchemaDefinition,
  getNormalizedTableBinding,
  validateProviderBindings,
} from "@tupl/schema-model/normalization";

import type { ExecutableSchema, PreparedRuntimeSchema } from "./contracts";
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

function finalizeRuntimeSchemaResult<TContext, TSchema extends SchemaDefinition>(
  input: TSchema | SchemaBuilder<TContext>,
): TuplResult<TSchema | SchemaDefinition> {
  const schemaResult = isSchemaBuilder<TContext>(input)
    ? input.build()
    : finalizeSchemaDefinition(input as TSchema);
  if (Result.isError(schemaResult)) {
    return schemaResult;
  }

  return resolveSchemaLinkedEnums(schemaResult.value);
}

/**
 * Runtime schema preparation owns the last-mile schema work required before query/explain
 * execution: finalization, linked-enum materialization, and provider-binding validation.
 */
export function prepareRuntimeSchemaResult<TContext>(
  builder: SchemaBuilder<TContext>,
): TuplResult<PreparedRuntimeSchema<TContext, SchemaDefinition>>;
export function prepareRuntimeSchemaResult<TContext, TSchema extends SchemaDefinition>(
  schema: TSchema,
): TuplResult<PreparedRuntimeSchema<TContext, TSchema>>;
export function prepareRuntimeSchemaResult<TContext, TSchema extends SchemaDefinition>(input: {
  schema: TSchema;
  providers: ProvidersMap<TContext>;
}): TuplResult<PreparedRuntimeSchema<TContext, TSchema>>;
export function prepareRuntimeSchemaResult<TContext, TSchema extends SchemaDefinition>(
  input: TSchema | SchemaBuilder<TContext> | { schema: TSchema; providers: ProvidersMap<TContext> },
): TuplResult<PreparedRuntimeSchema<TContext, TSchema | SchemaDefinition>> {
  return prepareRuntimeSchemaResultImpl(input);
}

function prepareRuntimeSchemaResultImpl<TContext, TSchema extends SchemaDefinition>(
  input: TSchema | SchemaBuilder<TContext> | { schema: TSchema; providers: ProvidersMap<TContext> },
): TuplResult<PreparedRuntimeSchema<TContext, TSchema | SchemaDefinition>> {
  const explicitProviders =
    typeof input === "object" && input !== null && "schema" in input && "providers" in input
      ? input.providers
      : undefined;
  const schemaInput =
    typeof input === "object" && input !== null && "schema" in input && "providers" in input
      ? input.schema
      : input;

  const schemaResult = finalizeRuntimeSchemaResult<TContext, TSchema>(schemaInput);
  if (Result.isError(schemaResult)) {
    return schemaResult as TuplResult<PreparedRuntimeSchema<TContext, TSchema | SchemaDefinition>>;
  }

  const schema = schemaResult.value;
  const providersResult = explicitProviders
    ? Result.ok(explicitProviders)
    : collectExecutableProvidersResult<TContext>(schema);
  if (Result.isError(providersResult)) {
    return providersResult as TuplResult<
      PreparedRuntimeSchema<TContext, TSchema | SchemaDefinition>
    >;
  }

  const validationResult = validateProviderBindings(schema, providersResult.value);
  if (Result.isError(validationResult)) {
    return validationResult as TuplResult<
      PreparedRuntimeSchema<TContext, TSchema | SchemaDefinition>
    >;
  }

  return Result.ok({
    schema,
    providers: providersResult.value,
  });
}

function prepareRuntimeSchemaInternal<TContext, TSchema extends SchemaDefinition>(
  input: TSchema | SchemaBuilder<TContext>,
): TuplResult<PreparedRuntimeSchema<TContext, TSchema | SchemaDefinition>> {
  return prepareRuntimeSchemaResultImpl(input);
}

function createExecutableSchemaInternal<TContext, TSchema extends SchemaDefinition>(
  input: TSchema | SchemaBuilder<TContext>,
): TuplResult<ExecutableSchema<TContext, TSchema | SchemaDefinition>> {
  const preparedSchemaResult = prepareRuntimeSchemaInternal<TContext, TSchema>(input);
  if (Result.isError(preparedSchemaResult)) {
    return preparedSchemaResult as TuplResult<
      ExecutableSchema<TContext, TSchema | SchemaDefinition>
    >;
  }

  const runtime = preparedSchemaResult.value;

  const executableSchema = {
    schema: runtime.schema,
    query(input) {
      return queryInternalResult({
        preparedSchema: runtime,
        ...input,
      });
    },
    explain(input) {
      return explainInternalResult({
        preparedSchema: runtime,
        ...input,
      });
    },
  } satisfies ExecutableSchema<TContext, TSchema | SchemaDefinition>;

  bindExecutableSchemaSessionAccess(executableSchema, {
    createSession(input) {
      return createQuerySession({
        preparedSchema: runtime,
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
