import type { ScanFilterClause } from "@tupl/provider-kit";
import { resolveColumnFromFilterColumn, resolveColumnRef } from "@tupl/provider-kit/shapes";

import type {
  KyselyDatabaseLike,
  KyselyProviderEntityConfig,
  KyselyQueryBuilderLike,
  ResolvedEntityConfig,
  ScanBinding,
} from "../types";

type BaseBinding<TContext> =
  | {
      entity: string;
      config: KyselyProviderEntityConfig<TContext>;
    }
  | {
      entity: string;
      resolved: ResolvedEntityConfig<TContext>;
    };

export function toRef(
  alias: string | undefined,
  column: string,
): { alias?: string; column: string } {
  if (alias) {
    return { alias, column };
  }
  return { column };
}

export async function applyBase<TContext>(
  query: KyselyQueryBuilderLike,
  db: KyselyDatabaseLike,
  binding: BaseBinding<TContext>,
  context: TContext,
  alias: string,
): Promise<KyselyQueryBuilderLike> {
  const config = "resolved" in binding ? binding.resolved.config : binding.config;
  if (!config.base) {
    return query;
  }

  return config.base({
    db,
    query,
    context,
    entity: binding.entity,
    alias,
  });
}

export function applyWhereClause<TContext>(
  query: KyselyQueryBuilderLike,
  clause: ScanFilterClause,
  aliases: Map<string, ScanBinding<TContext>>,
): KyselyQueryBuilderLike {
  const column = resolveFilterColumn(aliases, clause.column);

  switch (clause.op) {
    case "eq":
      return query.where(column, "=", clause.value);
    case "neq":
      return query.where(column, "!=", clause.value);
    case "gt":
      return query.where(column, ">", clause.value);
    case "gte":
      return query.where(column, ">=", clause.value);
    case "lt":
      return query.where(column, "<", clause.value);
    case "lte":
      return query.where(column, "<=", clause.value);
    case "in":
      return query.where(column, "in", clause.values.length > 0 ? clause.values : [null]);
    case "not_in":
      return query.where(column, "not in", clause.values.length > 0 ? clause.values : [null]);
    case "like":
      return query.where(column, "like", clause.value);
    case "not_like":
      return query.where(column, "not like", clause.value);
    case "is_distinct_from":
      return query.where(column, "is distinct from", clause.value);
    case "is_not_distinct_from":
      return query.where(column, "is not distinct from", clause.value);
    case "is_null":
      return query.where(column, "is", null);
    case "is_not_null":
      return query.where(column, "is not", null);
  }
}

export function resolveFilterColumn<TContext>(
  aliases: Map<string, ScanBinding<TContext>>,
  column: string,
): string {
  return resolveColumnFromFilterColumn(aliases, column);
}

export function resolveQualifiedColumnRef<TContext>(
  aliases: Map<string, ScanBinding<TContext>>,
  ref: { alias?: string; column: string },
): string {
  return resolveColumnRef(aliases, ref);
}
