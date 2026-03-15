import type { RelNode } from "@tupl/foundation";
import type {
  SqlRelationalOrderTerm,
  SqlRelationalQueryTranslationBackend,
  SqlRelationalSelection,
} from "@tupl/provider-kit/relational-sql";

import {
  applyBase,
  applyWhereClause,
  resolveQualifiedColumnRef,
  toRef,
} from "../backend/query-helpers";
import type { KyselyDatabaseLike, KyselyQueryBuilderLike, ResolvedEntityConfig } from "../types";
import { type ScanBinding, UnsupportedSingleQueryPlanError } from "./rel-strategy";

type SelectionEntry = {
  output: string;
  toExpression: (eb: any) => unknown;
};

/**
 * Kysely query translation owns only Kysely-specific query-builder operations.
 * Provider-kit owns strategy selection, recursion, set-op/CTE traversal, and filter replay.
 */
export const kyselyQueryTranslationBackend: SqlRelationalQueryTranslationBackend<
  unknown,
  ResolvedEntityConfig<unknown>,
  ScanBinding<unknown>,
  KyselyDatabaseLike,
  KyselyQueryBuilderLike
> = {
  async createRootQuery({ runtime, root, context }) {
    const rootFrom = `${root.table} as ${root.alias}`;
    let query = runtime.selectFrom(rootFrom);
    query = await applyBase(query, runtime, root, context, root.alias);
    return query;
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

    const next = (fn as (...args: unknown[]) => KyselyQueryBuilderLike).call(
      query,
      `${join.right.table} as ${join.right.alias}`,
      `${join.leftKey.alias}.${join.leftKey.column}`,
      `${join.rightKey.alias}.${join.rightKey.column}`,
    );

    return applyBase(next, runtime, join.right, context, join.right.alias);
  },
  applySemiJoin({ query, leftKey, subquery }) {
    return query.where(`${leftKey.alias}.${leftKey.column}`, "in", subquery);
  },
  applyWhereClause({ query, clause, aliases }) {
    return applyWhereClause(query, clause, aliases);
  },
  applySelection({ query, selection, aliases }) {
    return query.select((eb: any) =>
      buildSelectionEntries(selection, aliases).map((entry) => entry.toExpression(eb)),
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

    const scanAlias = body.cteRef.alias ?? body.cteRef.name;
    const from = body.cteRef.alias
      ? `${body.cteRef.name} as ${body.cteRef.alias}`
      : body.cteRef.name;
    let query = withDb.selectFrom(from);

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

    const windowByAlias = new Map((body.window?.functions ?? []).map((fn) => [fn.as, fn] as const));
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
      query = query.orderBy(
        resolveWithBodyOrderTerm(term, scanAlias, windowByAlias),
        term.direction,
      );
    }

    return query;
  },
  async executeQuery({ query }) {
    return query.execute();
  },
};

function buildSelectionEntries<TContext>(
  selection: SqlRelationalSelection[],
  aliases: Map<string, ScanBinding<TContext>>,
): SelectionEntry[] {
  return selection.map((entry) => {
    switch (entry.kind) {
      case "column": {
        const source = resolveQualifiedColumnRef(aliases, {
          ...toRef(entry.source.alias ?? entry.source.table, entry.source.column),
        });
        return {
          output: entry.output,
          toExpression: (eb: any) => eb.ref(source).as(entry.output),
        };
      }
      case "metric":
        return {
          output: entry.output,
          toExpression: (eb: any) =>
            buildMetricExpression(eb, entry.metric, aliases).as(entry.output),
        };
      case "expr":
        throw new UnsupportedSingleQueryPlanError(
          "Computed projections are not supported in Kysely single-query pushdown.",
        );
    }
  });
}

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

function resolveWithBodyOrderTerm(
  term: SqlRelationalOrderTerm,
  scanAlias: string,
  windowByAlias: Map<string, Extract<RelNode, { kind: "window" }>["functions"][number]>,
): string {
  if (term.kind === "output" && windowByAlias.has(term.column)) {
    return term.column;
  }

  if (term.kind === "qualified") {
    return resolveWithBodyColumnRef(term.source, scanAlias);
  }

  return term.column;
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
