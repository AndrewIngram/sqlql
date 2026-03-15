import type { RelNode } from "@tupl/foundation";
import {
  SqlRelationalOrderTerm,
  SqlRelationalQueryTranslationBackend,
  SqlRelationalSelection,
  UnsupportedSqlRelationalPlanError,
} from "@tupl/provider-kit/relational-sql";

import {
  applyWhereClause,
  applyWindowFunction,
  createJoinSource,
  resolveQualifiedColumnRef,
  resolveWithBodyColumnRef,
  toRef,
  executeQuery,
} from "../backend/query-helpers";
import type { KnexLike, KnexLikeQueryBuilder, ResolvedEntityConfig, ScanBinding } from "../types";

/**
 * Objection/Knex query translation owns only Knex-specific query-builder primitives.
 * Provider-kit owns recursive rel lowering, set-op/CTE traversal, and filter replay.
 */
export const objectionQueryTranslationBackend: SqlRelationalQueryTranslationBackend<
  unknown,
  ResolvedEntityConfig<unknown>,
  ScanBinding<unknown>,
  KnexLike,
  KnexLikeQueryBuilder
> = {
  createRootQuery({ runtime, root, context }) {
    return runtime.queryBuilder().from(createJoinSource(root, context));
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
      throw new UnsupportedSqlRelationalPlanError(
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
    const next = query.clearSelect?.() ?? query;
    applySelection(next, selection, aliases);
    return next;
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
    let next = query;
    for (const term of orderBy) {
      next = next.orderBy(resolveOrderTerm(term, aliases), term.direction);
    }
    return next;
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
      throw new UnsupportedSqlRelationalPlanError(
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
        throw new UnsupportedSqlRelationalPlanError(
          "Knex query builder does not support CTE builders required for WITH pushdown.",
        );
      }

      query = withFn.call(query, cte.name, cte.query) as KnexLikeQueryBuilder;
    }

    const scanAlias = body.cteRef.alias ?? body.cteRef.name;
    const fromSource = body.cteRef.alias
      ? ({ [body.cteRef.alias]: body.cteRef.name } as Record<string, string>)
      : body.cteRef.name;
    query = query.from(fromSource);

    const aliases = new Map<string, ScanBinding<unknown>>([
      [
        scanAlias,
        {
          alias: scanAlias,
          entity: body.cteRef.name,
          table: body.cteRef.name,
          scan: {
            ...body.cteRef,
            kind: "scan",
            table: body.cteRef.name,
          },
          resolved: {
            entity: body.cteRef.name,
            table: body.cteRef.name,
            config: {},
          },
        },
      ],
    ]);

    for (const clause of body.cteRef.where ?? []) {
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

    const windowAliases = new Set((body.window?.functions ?? []).map((fn) => fn.as));
    for (const entry of projection) {
      if (entry.kind === "window") {
        query = query.select({ [entry.output]: entry.window.as });
        continue;
      }

      if (!entry.source.alias && !entry.source.table && windowAliases.has(entry.source.column)) {
        query = query.select({ [entry.output]: entry.source.column });
        continue;
      }

      const source = resolveWithBodyColumnRef(entry.source, scanAlias);
      query = query.select({ [entry.output]: source });
    }

    for (const term of orderBy) {
      const source =
        term.kind === "qualified" ? resolveWithBodyColumnRef(term.source, scanAlias) : term.column;
      query = query.orderBy(source, term.direction);
    }

    return query;
  },
  executeQuery({ query }) {
    return executeQuery(query);
  },
};

function applySelection<TContext>(
  query: KnexLikeQueryBuilder,
  selection: SqlRelationalSelection[],
  aliases: Map<string, ScanBinding<TContext>>,
): void {
  for (const entry of selection) {
    switch (entry.kind) {
      case "column": {
        const source = resolveQualifiedColumnRef(aliases, {
          ...toRef(entry.source.alias ?? entry.source.table, entry.source.column),
        });
        query.select({ [entry.output]: source });
        break;
      }
      case "metric":
        applyMetricSelection(query, aliases, entry.metric, entry.output);
        break;
      case "expr":
        throw new UnsupportedSqlRelationalPlanError(
          "Computed projections are not supported in Objection single-query pushdown.",
        );
    }
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
    throw new UnsupportedSqlRelationalPlanError(`Aggregate ${metric.fn} requires a column.`);
  }

  const source = resolveQualifiedColumnRef(aliases, {
    ...toRef(metric.column.alias ?? metric.column.table, metric.column.column),
  });

  if (metric.fn === "count" && metric.distinct) {
    query.countDistinct({ [output]: source });
    return;
  }

  if (metric.fn === "sum" && metric.distinct) {
    throw new UnsupportedSqlRelationalPlanError(
      "Knex sum(distinct ...) is not supported in this adapter yet.",
    );
  }

  if (metric.fn === "avg" && metric.distinct) {
    throw new UnsupportedSqlRelationalPlanError(
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
  if (term.kind === "qualified") {
    return resolveQualifiedColumnRef(aliases, {
      ...toRef(term.source.alias ?? term.source.table, term.source.column),
    });
  }

  return term.column;
}
