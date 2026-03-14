import type { RelColumnRef, RelProjectExprMapping } from "@tupl/foundation";
import type { OrderByTermAst } from "../sqlite-parser/ast";

import type {
  Binding,
  ParsedAggregateProjection,
  ParsedOrderByTerm,
  ResolvedOrderTerm,
  SelectExprProjection,
  SelectProjection,
} from "../planner-types";
import { nextSyntheticColumnName } from "../physical/planner-ids";
import {
  parsePositiveOrdinalLiteral,
  resolveColumnRef,
  toRawColumnRef,
} from "../sql-expr-lowering";

/**
 * Aggregate order resolution owns ORDER BY parsing plus projection/output mapping for aggregate and non-aggregate queries.
 */
export function parseOrderBy(
  rawOrderBy: unknown,
  bindings: Binding[],
  aliasToBinding: Map<string, Binding>,
  outputs: Set<string>,
): ParsedOrderByTerm[] | null {
  const orderBy = Array.isArray(rawOrderBy) ? (rawOrderBy as OrderByTermAst[]) : [];
  const out: ParsedOrderByTerm[] = [];

  for (const term of orderBy) {
    const ordinal = parsePositiveOrdinalLiteral(term.expr, "ORDER BY");
    if (ordinal != null) {
      out.push({
        kind: "ordinal",
        position: ordinal,
        direction: term.type === "DESC" ? "desc" : "asc",
      });
      continue;
    }

    const rawRef = toRawColumnRef(term.expr);
    if (rawRef && !rawRef.table && outputs.has(rawRef.column)) {
      out.push({
        kind: "output",
        output: rawRef.column,
        direction: term.type === "DESC" ? "desc" : "asc",
      });
      continue;
    }

    const resolved = resolveColumnRef(term.expr, bindings, aliasToBinding);
    if (!resolved?.alias) {
      return null;
    }

    out.push({
      kind: "ref",
      source: {
        alias: resolved.alias,
        column: resolved.column,
      },
      direction: term.type === "DESC" ? "desc" : "asc",
    });
  }

  return out;
}

export function resolveNonAggregateOrderBy(
  orderByTerms: ParsedOrderByTerm[],
  projections: SelectProjection[],
  toParsedOrderSource: (
    ref: RelColumnRef | null | undefined,
    fallbackColumn: string,
  ) => ResolvedOrderTerm["source"],
): {
  orderBy: ResolvedOrderTerm[];
  materializations: RelProjectExprMapping[];
} {
  const projectionsByOutput = new Map(
    projections.map((projection) => [projection.output, projection] as const),
  );
  const materializations: RelProjectExprMapping[] = [];
  const orderBy: ResolvedOrderTerm[] = [];

  const resolveProjectionSource = (
    projection: SelectProjection,
    ordinal?: number,
  ): ResolvedOrderTerm["source"] => {
    if (projection.kind === "column") {
      return toParsedOrderSource(projection.source, projection.output);
    }
    if (projection.kind === "window") {
      return { column: projection.function.as };
    }
    if (projection.kind === "correlated_scalar") {
      return { column: projection.output };
    }

    const materialization = materializeSelectExprProjection(projection, "order_by");
    if (materialization) {
      materializations.push(materialization);
    }
    if (!projection.source) {
      throw new Error(
        ordinal != null
          ? `ORDER BY ordinal ${ordinal} could not be resolved.`
          : `ORDER BY expression "${projection.output}" could not be resolved.`,
      );
    }
    return { column: projection.source.column };
  };

  for (const term of orderByTerms) {
    if (term.kind === "ref") {
      orderBy.push({
        source: term.source,
        direction: term.direction,
      });
      continue;
    }

    const projection =
      term.kind === "ordinal"
        ? projections[term.position - 1]
        : projectionsByOutput.get(term.output);
    if (!projection) {
      if (term.kind === "ordinal") {
        throw new Error(`ORDER BY ordinal ${term.position} is out of range.`);
      }
      throw new Error(`Unknown ORDER BY output "${term.output}".`);
    }

    orderBy.push({
      source: resolveProjectionSource(
        projection,
        term.kind === "ordinal" ? term.position : undefined,
      ),
      direction: term.direction,
    });
  }

  return { orderBy, materializations };
}

export function resolveAggregateOrderBy(
  orderByTerms: ParsedOrderByTerm[],
  projections: ParsedAggregateProjection[],
): ResolvedOrderTerm[] {
  const projectionsByOutput = new Map(
    projections.map((projection) => [projection.output, projection] as const),
  );
  const groupOutputsBySource = new Map<string, string>();

  for (const projection of projections) {
    if (projection.kind !== "group" || !projection.source) {
      continue;
    }
    groupOutputsBySource.set(
      `${projection.source.alias ?? ""}.${projection.source.column}`,
      projection.source.column,
    );
  }

  const resolveProjectionSource = (
    projection: ParsedAggregateProjection,
    ordinal?: number,
  ): ResolvedOrderTerm["source"] => {
    if (projection.kind === "metric") {
      return { column: projection.metric.as };
    }
    if (!projection.source) {
      throw new Error(
        ordinal != null
          ? `ORDER BY ordinal ${ordinal} could not be resolved.`
          : `ORDER BY expression "${projection.output}" could not be resolved.`,
      );
    }
    return { column: projection.source.column };
  };

  return orderByTerms.map((term) => {
    if (term.kind === "ref") {
      const key = `${term.source.alias ?? ""}.${term.source.column}`;
      return {
        source: { column: groupOutputsBySource.get(key) ?? term.source.column },
        direction: term.direction,
      };
    }

    const projection =
      term.kind === "ordinal"
        ? projections[term.position - 1]
        : projectionsByOutput.get(term.output);
    if (!projection) {
      if (term.kind === "ordinal") {
        throw new Error(`ORDER BY ordinal ${term.position} is out of range.`);
      }
      throw new Error(`Unknown ORDER BY output "${term.output}".`);
    }

    return {
      source: resolveProjectionSource(
        projection,
        term.kind === "ordinal" ? term.position : undefined,
      ),
      direction: term.direction,
    };
  });
}

function materializeSelectExprProjection(
  projection: SelectExprProjection,
  prefix: string,
): RelProjectExprMapping | null {
  if (projection.source) {
    return null;
  }

  const column = nextSyntheticColumnName(prefix);
  projection.source = { column };
  return {
    kind: "expr",
    expr: projection.expr,
    output: column,
  };
}
