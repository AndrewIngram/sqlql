import type { QueryRow, ScanFilterClause } from "@tupl/provider-kit";
import { resolveColumnFromFilterColumn, resolveColumnRef } from "@tupl/provider-kit/shapes";
import type { RelNode } from "@tupl/foundation";

import type {
  KnexLike,
  KnexLikeQueryBuilder,
  ObjectionProviderEntityConfig,
  ResolvedEntityConfig,
} from "../types";
import type { ScanBinding } from "../planning/rel-strategy";
import { UnsupportedSingleQueryPlanError } from "../planning/rel-strategy";

export function toRef(
  alias: string | undefined,
  column: string,
): { alias?: string; column: string } {
  if (alias) {
    return { alias, column };
  }
  return { column };
}

export function isKnexLikeQueryBuilder(value: unknown): value is KnexLikeQueryBuilder {
  if (!value || typeof value !== "object") {
    return false;
  }

  const candidate = value as Partial<KnexLikeQueryBuilder>;
  return typeof candidate.select === "function" && typeof candidate.where === "function";
}

export function resolveBaseQueryBuilder<TContext>(
  base: NonNullable<ObjectionProviderEntityConfig<TContext>["base"]>,
  context: TContext,
): KnexLikeQueryBuilder {
  const scoped = base(context);
  if (isKnexLikeQueryBuilder(scoped)) {
    return scoped;
  }
  throw new Error(
    "Objection entity base(context) must return a Knex/Objection query builder synchronously.",
  );
}

export async function executeQuery(query: KnexLikeQueryBuilder): Promise<QueryRow[]> {
  if (typeof query.execute === "function") {
    return (await query.execute()) ?? [];
  }
  return (await (query as unknown as Promise<QueryRow[]>)) ?? [];
}

export function createJoinSource<TContext>(
  binding: ScanBinding<TContext>,
  context: TContext,
): unknown {
  if (!binding.resolved.config.base) {
    return { [binding.alias]: binding.table };
  }

  const base = resolveBaseQueryBuilder(binding.resolved.config.base, context);
  const cloned = base.clone?.() ?? base;
  return (cloned.as?.(binding.alias) ?? cloned) as unknown;
}

export function createBaseQuery<TContext>(
  knex: KnexLike,
  binding: ResolvedEntityConfig<TContext>,
  context: TContext,
  alias?: string,
): KnexLikeQueryBuilder {
  if (!binding.config.base) {
    return knex.queryBuilder().from(alias ? { [alias]: binding.table } : binding.table);
  }

  const base = resolveBaseQueryBuilder(binding.config.base, context);
  const query = base.clone?.() ?? base;
  if (alias && query.as) {
    return knex.queryBuilder().from(query.as(alias));
  }
  return query;
}

export function applyWhereClause<TContext>(
  query: KnexLikeQueryBuilder,
  clause: ScanFilterClause,
  aliases: Map<string, ScanBinding<TContext>>,
): KnexLikeQueryBuilder {
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
      return query.whereIn(column, clause.values);
    case "not_in":
      return query.where(column, "not in", clause.values);
    case "like":
      return query.where(column, "like", clause.value);
    case "not_like":
      return query.where(column, "not like", clause.value);
    case "is_distinct_from":
      return query.where(column, "is distinct from", clause.value);
    case "is_not_distinct_from":
      return query.where(column, "is not distinct from", clause.value);
    case "is_null":
      return query.whereNull(column);
    case "is_not_null":
      return query.whereNotNull(column);
  }
}

export function applyWindowFunction(
  query: KnexLikeQueryBuilder,
  fn: Extract<RelNode, { kind: "window" }>["functions"][number],
  scanAlias: string,
): KnexLikeQueryBuilder {
  const methodName = fn.fn === "dense_rank" ? "denseRank" : fn.fn === "rank" ? "rank" : "rowNumber";

  const method = (query as unknown as Record<string, unknown>)[methodName];
  if (typeof method !== "function") {
    throw new UnsupportedSingleQueryPlanError(
      `Knex query builder does not support ${methodName} window functions in this dialect.`,
    );
  }

  const orderBy =
    fn.orderBy.length === 1
      ? (() => {
          const firstOrder = fn.orderBy[0];
          if (!firstOrder) {
            throw new UnsupportedSingleQueryPlanError(
              `${methodName} window function requires at least one ORDER BY column.`,
            );
          }
          return {
            column: resolveWithBodyColumnRef(firstOrder.source, scanAlias),
            order: firstOrder.direction,
          };
        })()
      : fn.orderBy.map((term) => ({
          column: resolveWithBodyColumnRef(term.source, scanAlias),
          order: term.direction,
        }));

  const partitionBy =
    fn.partitionBy.length === 0
      ? undefined
      : fn.partitionBy.length === 1
        ? (() => {
            const firstPartition = fn.partitionBy[0];
            if (!firstPartition) {
              return undefined;
            }
            return resolveWithBodyColumnRef(firstPartition, scanAlias);
          })()
        : fn.partitionBy.map((ref) => resolveWithBodyColumnRef(ref, scanAlias));

  return (method as (...args: unknown[]) => KnexLikeQueryBuilder).call(
    query,
    fn.as,
    orderBy,
    partitionBy,
  );
}

export function resolveWithBodyColumnRef(
  ref: { alias?: string; table?: string; column: string },
  scanAlias: string,
): string {
  const refAlias = ref.alias ?? ref.table;
  if (refAlias && refAlias !== scanAlias) {
    throw new UnsupportedSingleQueryPlanError(
      `WITH body column "${refAlias}.${ref.column}" must reference alias "${scanAlias}".`,
    );
  }

  return `${scanAlias}.${ref.column}`;
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
