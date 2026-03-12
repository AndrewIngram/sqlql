import type { SqlRelationalBackend, SqlRelationalOrderTerm } from "@tupl/provider-kit";
import type { RelNode } from "@tupl/foundation";

import {
  applyBase,
  applyWhereClause,
  resolveQualifiedColumnRef,
  toRef,
} from "../backend/query-helpers";
import type { KyselyDatabaseLike, KyselyQueryBuilderLike, ResolvedEntityConfig } from "../types";
import {
  createScanBinding,
  type ScanBinding,
  UnsupportedSingleQueryPlanError,
} from "./rel-strategy";

/**
 * Kysely backend hooks describe only query-builder differences.
 * Shared rel compilation now lives in provider-kit.
 */
export const kyselySqlRelationalBackend: SqlRelationalBackend<
  any,
  ResolvedEntityConfig<any>,
  ScanBinding<any>,
  KyselyDatabaseLike,
  KyselyQueryBuilderLike
> = {
  createScanBinding,
  async createRootQuery({ runtime, root, context }) {
    const from = `${root.table} as ${root.alias}`;
    const query = runtime.selectFrom(from);
    return applyBase(query, runtime, root.resolved, context, root.alias);
  },
  async applyRegularJoin({ query, join, context, runtime }) {
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
        `Kysely query builder does not support ${joinMethod} in this dialect.`,
      );
    }

    const joined = (fn as (...args: unknown[]) => KyselyQueryBuilderLike).call(
      query,
      `${join.right.table} as ${join.right.alias}`,
      `${join.leftKey.alias}.${join.leftKey.column}`,
      `${join.rightKey.alias}.${join.rightKey.column}`,
    );

    return applyBase(joined, runtime, join.right.resolved, context, join.right.alias);
  },
  applySemiJoin({ query, leftKey, subquery }) {
    return query.where(`${leftKey.alias}.${leftKey.column}`, "in", subquery);
  },
  applyWhereClause({ query, clause, aliases }) {
    return applyWhereClause(query, clause, aliases);
  },
  applySelection({ query, selection, aliases }) {
    return query.select((eb: any) =>
      selection.map((entry) => {
        if (entry.kind === "metric") {
          return buildMetricExpression(eb, entry.metric, aliases).as(entry.output);
        }
        if (entry.kind === "expr") {
          throw new UnsupportedSingleQueryPlanError(
            "Computed projections are not supported in Kysely single-query pushdown.",
          );
        }

        const source = resolveQualifiedColumnRef(aliases, {
          ...toRef(entry.source.alias ?? entry.source.table, entry.source.column),
        });
        return eb.ref(source).as(entry.output);
      }),
    );
  },
  applyGroupBy({ query, groupBy, aliases }) {
    return (
      query.groupBy?.(
        groupBy.map((ref) =>
          resolveQualifiedColumnRef(aliases, {
            ...toRef(ref.alias ?? ref.table, ref.column),
          }),
        ),
      ) ?? query
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
        `Kysely query builder does not support ${methodName} for single-query pushdown.`,
      );
    }

    return applySetOp.call(left, right) as KyselyQueryBuilderLike;
  },
  buildWithQuery({ body, ctes, projection, orderBy, runtime }) {
    if (typeof runtime.with !== "function") {
      throw new UnsupportedSingleQueryPlanError(
        "Kysely database instance does not support CTE builders required for WITH pushdown.",
      );
    }

    let withDb = runtime;
    for (const cte of ctes) {
      withDb = withDb.with!(cte.name, () => cte.query);
    }

    const scanAlias = body.cteScan.alias ?? body.cteScan.table;
    const from = body.cteScan.alias
      ? `${body.cteScan.table} as ${body.cteScan.alias}`
      : body.cteScan.table;
    let query = withDb.selectFrom(from);

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

    query = query.select((eb: any) =>
      projection.map((entry) => {
        if (entry.kind === "window") {
          return buildWindowExpression(eb, entry.window, scanAlias).as(entry.output);
        }

        const source = resolveWithBodyColumnRef(entry.source, scanAlias);
        return eb.ref(source).as(entry.output);
      }),
    );

    for (const term of orderBy) {
      query = query.orderBy(resolveWithOrderTerm(term, scanAlias), term.direction);
    }

    return query;
  },
  async executeQuery({ query }) {
    return query.execute();
  },
};

function buildMetricExpression<TContext>(
  eb: any,
  metric: Extract<RelNode, { kind: "aggregate" }>["metrics"][number],
  aliases: Map<string, ScanBinding<TContext>>,
): any {
  if (metric.fn === "count" && !metric.column) {
    return eb.fn.countAll();
  }

  if (!metric.column) {
    throw new UnsupportedSingleQueryPlanError(`Aggregate ${metric.fn} requires a column.`);
  }

  const ref = resolveQualifiedColumnRef(aliases, {
    ...toRef(metric.column.alias ?? metric.column.table, metric.column.column),
  });

  const fn = (eb as { fn?: Record<string, (value: unknown) => any> }).fn;
  if (!fn) {
    throw new UnsupportedSingleQueryPlanError(
      "Kysely expression builder does not expose fn helpers.",
    );
  }

  const fnImpl = fn[metric.fn];
  if (typeof fnImpl !== "function") {
    throw new UnsupportedSingleQueryPlanError(`Unsupported aggregate function: ${metric.fn}.`);
  }

  let expression = fnImpl(eb.ref(ref));
  if (metric.distinct && expression && typeof expression.distinct === "function") {
    expression = expression.distinct();
  }

  return expression;
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

function buildWindowExpression(
  eb: any,
  fn: Extract<RelNode, { kind: "window" }>["functions"][number],
  scanAlias: string,
): any {
  const aggregate = eb.fn.agg(fn.fn, []);

  return aggregate.over((ob: any) => {
    let over = ob;

    if (fn.partitionBy.length > 0) {
      over = over.partitionBy(
        fn.partitionBy.map((ref) => resolveWithBodyColumnRef(ref, scanAlias)),
      );
    }

    for (const term of fn.orderBy) {
      over = over.orderBy(resolveWithBodyColumnRef(term.source, scanAlias), term.direction);
    }

    return over;
  });
}

function resolveWithOrderTerm(term: SqlRelationalOrderTerm, scanAlias: string): string {
  if (term.kind === "output") {
    return term.column;
  }

  return resolveWithBodyColumnRef(term.source, scanAlias);
}

function resolveWithBodyColumnRef(
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
