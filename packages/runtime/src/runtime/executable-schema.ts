import { Result } from "better-result";

import { TuplRuntimeError, type TuplResult } from "@tupl/foundation";
import { type ProviderAdapter, type ProvidersMap } from "@tupl/provider-kit";
import {
  finalizeSchemaDefinition,
  getNormalizedTableBinding,
  isSchemaBuilder,
  type SchemaBuilder,
  type SchemaDefinition,
} from "@tupl/schema-model";

import type { ExecutableSchema, ExecutableSchemaRuntime } from "./contracts";
import { unwrapQueryResult, tryQueryStep } from "./diagnostics";
import { explainInternal, queryInternal, queryInternalResult } from "./query-runner";
import { createQuerySessionInternal, createQuerySessionResult } from "./session";

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

    const provider = binding.adapter as ProviderAdapter<TContext> | undefined;
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
          message: `Table ${tableName} is bound to provider ${binding.provider ?? "<missing>"}, but the attached adapter is named ${provider.name}.`,
        }),
      );
    }
  }

  return Result.ok(providers);
}

function createExecutableSchemaResultInternal<TContext, TSchema extends SchemaDefinition>(
  input: TSchema | SchemaBuilder<TContext>,
): TuplResult<ExecutableSchema<TContext, TSchema | SchemaDefinition>> {
  const schemaResult = tryQueryStep("create executable schema", () =>
    isSchemaBuilder<TContext>(input) ? input.build() : finalizeSchemaDefinition(input as TSchema),
  );
  if (Result.isError(schemaResult)) {
    return schemaResult as TuplResult<ExecutableSchema<TContext, TSchema | SchemaDefinition>>;
  }

  const schema = schemaResult.value;
  const providersResult = collectExecutableProvidersResult<TContext>(schema);
  if (Result.isError(providersResult)) {
    return providersResult as TuplResult<ExecutableSchema<TContext, TSchema | SchemaDefinition>>;
  }

  const providers = providersResult.value;
  const runtime: ExecutableSchemaRuntime<TContext> = {
    schema,
    providers,
  };

  return Result.ok({
    schema,
    query(input) {
      return queryInternal({
        schema: runtime.schema,
        providers: runtime.providers,
        ...input,
      });
    },
    queryResult(input) {
      return queryInternalResult({
        schema: runtime.schema,
        providers: runtime.providers,
        ...input,
      });
    },
    createSession(input) {
      return createQuerySessionInternal({
        schema: runtime.schema,
        providers: runtime.providers,
        ...input,
      });
    },
    createSessionResult(input) {
      return createQuerySessionResult({
        schema: runtime.schema,
        providers: runtime.providers,
        ...input,
      });
    },
    explain(input) {
      return explainInternal({
        schema: runtime.schema,
        providers: runtime.providers,
        ...input,
      });
    },
  });
}

export function createExecutableSchemaResult<TContext>(
  builder: SchemaBuilder<TContext>,
): TuplResult<ExecutableSchema<TContext, SchemaDefinition>>;
export function createExecutableSchemaResult<TContext, TSchema extends SchemaDefinition>(
  schema: TSchema,
): TuplResult<ExecutableSchema<TContext, TSchema>>;
export function createExecutableSchemaResult<TContext, TSchema extends SchemaDefinition>(
  input: TSchema | SchemaBuilder<TContext>,
): TuplResult<ExecutableSchema<TContext, TSchema | SchemaDefinition>> {
  return createExecutableSchemaResultInternal(input);
}

export function createExecutableSchema<TContext>(
  builder: SchemaBuilder<TContext>,
): ExecutableSchema<TContext, SchemaDefinition>;
export function createExecutableSchema<TContext, TSchema extends SchemaDefinition>(
  schema: TSchema,
): ExecutableSchema<TContext, TSchema>;
export function createExecutableSchema<TContext, TSchema extends SchemaDefinition>(
  input: TSchema | SchemaBuilder<TContext>,
): ExecutableSchema<TContext, TSchema | SchemaDefinition> {
  return unwrapQueryResult(createExecutableSchemaResultInternal(input));
}
