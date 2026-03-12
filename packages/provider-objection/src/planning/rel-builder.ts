import type {
  SqlRelationalBackend,
  SqlRelationalOrderTerm,
  SqlRelationalWithSelection,
} from "@tupl/provider-kit";
import type { RelNode } from "@tupl/foundation";

import {
  applyWhereClause,
  applyWindowFunction,
  createJoinSource,
  resolveQualifiedColumnRef,
  resolveWithBodyColumnRef,
  toRef,
} from "../backend/query-helpers";
import type { KnexLike, KnexLikeQueryBuilder, ResolvedEntityConfig } from "../types";
import {
  createScanBinding,
  type ScanBinding,
  UnsupportedSingleQueryPlanError,
} from "./rel-strategy";

/**
 * Objection backend hooks only translate shared rel semantics into knex query-builder calls.
 */
export const objectionSqlRelationalBackend: SqlRelationalBackend<
  any,
  ResolvedEntityConfig<any>,
  ScanBinding<any>,
  KnexLike,
  KnexLikeQueryBuilder
> = {
  createScanBinding,
  createRootQuery({ runtime, root, context }) {
    const source = createJoinSource(root, context);
    return runtime.queryBuilder().from(source);
  },
  applyRegularJoin({ query, join, context }) {
    const joinMethod =
      join.joinType === "inner"
        ? "innerJoin"
        : join.joinType === "left"
          ? "leftJoin"
          : join.joinType === "right"
            ? "rightJoin"
            : "fullJoin";

    const fn = (query as unknown as Record<string, unknown>)[joinMethod];
    if (typeof fn !== "function") {
      throw new UnsupportedSingleQueryPlanError(
        `Knex query builder does not support ${joinMethod} in this dialect.`,
      );
    }

    const rightSource = createJoinSource(join.right, context);
    return (fn as (...args: unknown[]) => KnexLikeQueryBuilder).call(
      query,
      rightSource,
      `${join.leftKey.alias}.${join.leftKey.column}`,
      `${join.rightKey.alias}.${join.rightKey.column}`,
    );
  },
  applySemiJoin({ query, leftKey, subquery }) {
    return query.whereIn(`${leftKey.alias}.${leftKey.column}`, subquery);
  },
  applyWhereClause({ query, clause, aliases }) {
    return applyWhereClause(query, clause, aliases);
  },
  applySelection({ query, selection, aliases }) {
    const cleared = query.clearSelect?.() ?? query;
    for (const entry of selection) {
      if (entry.kind === "metric") {
        applyMetricSelection(cleared, aliases, entry.metric, entry.output);
        continue;
      }
      if (entry.kind === "expr") {
        throw new UnsupportedSingleQueryPlanError(
          "Computed projections are not supported in Objection single-query pushdown.",
        );
      }

      const source = resolveQualifiedColumnRef(aliases, {
        ...toRef(entry.source.alias ?? entry.source.table, entry.source.column),
      });
      cleared.select({ [entry.output]: source });
    }
    return cleared;
  },
  applyGroupBy({ query, groupBy, aliases }) {
    return query.groupBy(
      ...groupBy.map((ref) =>
        resolveQualifiedColumnRef(aliases, {
          ...toRef(ref.alias ?? ref.table, ref.column),
        }),
      ),
    );
  },
  applyOrderBy({ query, orderBy, aliases }) {
    let out = query;
    for (const term of orderBy) {
      out = out.orderBy(resolveOrderTerm(term, aliases), term.direction);
    }
    return out;
  },
  applyLimit({ query, limit }) {
    return query.limit(limit);
  },
  applyOffset({ query, offset }) {
    return query.offset(offset);
  },
  applySetOp({ left, right, wrapper }) {
    const methodName =
      wrapper.setOp.op === "union_all"
        ? "unionAll"
        : wrapper.setOp.op === "union"
          ? "union"
          : wrapper.setOp.op === "intersect"
            ? "intersect"
            : "except";

    const applySetOp = (left as unknown as Record<string, unknown>)[methodName];
    if (typeof applySetOp !== "function") {
      throw new UnsupportedSingleQueryPlanError(
        `Knex query builder does not support ${methodName} for single-query pushdown.`,
      );
    }

    return applySetOp.call(left, [right]) as KnexLikeQueryBuilder;
  },
  buildWithQuery({ body, ctes, projection, orderBy, runtime }) {
    let query = runtime.queryBuilder();

    for (const cte of ctes) {
      const withFn = (query as { with?: unknown }).with;
      if (typeof withFn !== "function") {
        throw new UnsupportedSingleQueryPlanError(
          "Knex query builder does not support CTE builders required for WITH pushdown.",
        );
      }

      query = withFn.call(query, cte.name, cte.query) as KnexLikeQueryBuilder;
    }

    const scanAlias = body.cteScan.alias ?? body.cteScan.table;
    const fromSource = body.cteScan.alias
      ? ({ [body.cteScan.alias]: body.cteScan.table } as Record<string, string>)
      : body.cteScan.table;
    query = query.from(fromSource);

    const aliases = new Map<string, ScanBinding<any>>([
      [
        scanAlias,
        {
          alias: scanAlias,
          entity: body.cteScan.table,
          table: body.cteScan.table,
          scan: body.cteScan,
          resolved: {
            entity: body.cteScan.table,
            table: body.cteScan.table,
            config: {},
          },
        },
      ],
    ]);

    for (const clause of body.cteScan.where ?? []) {
      query = applyWhereClause(query, clause, aliases);
    }
    for (const filter of body.filters) {
      for (const clause of filter.where ?? []) {
        query = applyWhereClause(query, clause, aliases);
      }
    }

    for (const fn of body.window?.functions ?? []) {
      query = applyWindowFunction(query, fn, scanAlias);
    }

    query = query.clearSelect?.() ?? query;
    applyWithSelection(query, projection, scanAlias);

    for (const term of orderBy) {
      query = query.orderBy(resolveWithOrderTerm(term, scanAlias), term.direction);
    }

    return query;
  },
  async executeQuery({ query }) {
    if (typeof query.execute === "function") {
      return (await query.execute()) ?? [];
    }
    return (await (query as unknown as Promise<import("@tupl/provider-kit").QueryRow[]>)) ?? [];
  },
};

function applyWithSelection(
  query: KnexLikeQueryBuilder,
  projection: SqlRelationalWithSelection[],
  scanAlias: string,
): void {
  for (const entry of projection) {
    if (entry.kind === "window") {
      query = query.select({ [entry.output]: entry.window.as });
      continue;
    }

    const source = resolveWithBodyColumnRef(entry.source, scanAlias);
    query = query.select({ [entry.output]: source });
  }
}

function applyMetricSelection<TContext>(
  query: KnexLikeQueryBuilder,
  aliases: Map<string, ScanBinding<TContext>>,
  metric: Extract<RelNode, { kind: "aggregate" }>["metrics"][number],
  output: string,
): void {
  if (metric.fn === "count" && !metric.column) {
    query.count({ [output]: "*" });
    return;
  }

  if (!metric.column) {
    throw new UnsupportedSingleQueryPlanError(`Aggregate ${metric.fn} requires a column.`);
  }

  const source = resolveQualifiedColumnRef(aliases, {
    ...toRef(metric.column.alias ?? metric.column.table, metric.column.column),
  });

  if (metric.fn === "count" && metric.distinct) {
    query.countDistinct({ [output]: source });
    return;
  }

  if (metric.fn === "sum" && metric.distinct) {
    throw new UnsupportedSingleQueryPlanError(
      "Knex sum(distinct ...) is not supported in this adapter yet.",
    );
  }

  if (metric.fn === "avg" && metric.distinct) {
    throw new UnsupportedSingleQueryPlanError(
      "Knex avg(distinct ...) is not supported in this adapter yet.",
    );
  }

  switch (metric.fn) {
    case "count":
      query.count({ [output]: source });
      break;
    case "sum":
      query.sum({ [output]: source });
      break;
    case "avg":
      query.avg({ [output]: source });
      break;
    case "min":
      query.min({ [output]: source });
      break;
    case "max":
      query.max({ [output]: source });
      break;
  }
}

function resolveOrderTerm<TContext>(
  term: SqlRelationalOrderTerm,
  aliases: Map<string, ScanBinding<TContext>>,
): string {
  if (term.kind === "output") {
    return term.column;
  }

  return resolveQualifiedColumnRef(aliases, {
    ...toRef(term.source.alias ?? term.source.table, term.source.column),
  });
}

function resolveWithOrderTerm(term: SqlRelationalOrderTerm, scanAlias: string): string {
  if (term.kind === "output") {
    return term.column;
  }

  return resolveWithBodyColumnRef(term.source, scanAlias);
}
