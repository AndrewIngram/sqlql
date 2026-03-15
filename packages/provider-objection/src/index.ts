import {
  createSqlRelationalProviderAdapter,
  type FragmentProviderAdapter,
} from "@tupl/provider-kit";
import type { LookupManyCapableProviderAdapter } from "@tupl/provider-kit/shapes";

import { executeLookupManyResult } from "./execution/lookup-execution";
import { objectionQueryTranslationBackend } from "./planning/rel-builder";
import { type ScanBinding } from "./planning/rel-strategy";
import { resolveKnex } from "./backend/runtime-checks";
import type {
  CreateObjectionProviderOptions,
  KnexLike,
  KnexLikeQueryBuilder,
  ObjectionProviderEntities,
  ObjectionProviderEntityConfig,
  ResolvedEntityConfig,
} from "./types";

export type {
  CreateObjectionProviderOptions,
  KnexLike,
  KnexLikeQueryBuilder,
  ObjectionProviderEntityConfig,
  ObjectionProviderShape,
} from "./types";

/**
 * Objection provider entrypoints own runtime binding validation and helper wiring.
 * Query-builder planning and execution details live in the internal planning/execution/backend families.
 */
export function createObjectionProvider<
  TContext,
  TEntities extends Record<string, ObjectionProviderEntityConfig<TContext, any, string>> = Record<
    string,
    ObjectionProviderEntityConfig<TContext, any, string>
  >,
>(
  options: CreateObjectionProviderOptions<TContext, TEntities>,
): FragmentProviderAdapter<TContext> & {
  lookupMany: LookupManyCapableProviderAdapter<TContext>["lookupMany"];
  entities: ObjectionProviderEntities<TEntities>;
} {
  const providerName = options.name ?? "objection";
  const entityOptions = (options.entities ?? {}) as TEntities;

  return createSqlRelationalProviderAdapter<
    TContext,
    TEntities,
    ResolvedEntityConfig<TContext>,
    ScanBinding<TContext>,
    KnexLike,
    KnexLikeQueryBuilder,
    ObjectionProviderEntities<TEntities>
  >({
    name: providerName,
    entities: entityOptions,
    resolveRuntime(context) {
      return resolveKnex(options, context);
    },
    unsupportedRelCompileMessage: "Unsupported relational fragment for Objection provider.",
    unsupportedRelReasonMessage:
      "Rel fragment is not supported for single-query Objection pushdown.",
    queryBackend: objectionQueryTranslationBackend,
    async lookupMany({ request, context, resolvedEntities, runtime }) {
      return executeLookupManyResult(runtime, resolvedEntities, request, context);
    },
  });
}
