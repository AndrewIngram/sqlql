import { Result, type Result as BetterResult } from "better-result";

import { RelLoweringError, type RelColumnRef, type RelProjectExprMapping } from "@tupl/foundation";

import type {
  ParsedAggregateGroupProjection,
  ParsedAggregateProjection,
  ParsedGroupByTerm,
} from "../planner-types";
import { nextSyntheticColumnName } from "../physical/planner-ids";

/**
 * Group-by resolution owns aggregate group validation and ordinal/ref materialization.
 */
export function validateAggregateProjectionGroupBy(
  projections: ParsedAggregateProjection[],
  groupBy: RelColumnRef[],
): boolean {
  const groupBySet = new Set(groupBy.map((ref) => `${ref.alias ?? ""}.${ref.column}`));

  for (const projection of projections) {
    if (projection.kind !== "group") {
      continue;
    }
    if (!projection.source) {
      return false;
    }

    const key = `${projection.source.alias ?? ""}.${projection.source.column}`;
    if (!groupBySet.has(key)) {
      return false;
    }
  }

  return true;
}

export function resolveAggregateGroupBy(
  groupByTerms: ParsedGroupByTerm[],
  projections: ParsedAggregateProjection[],
): BetterResult<
  {
    groupBy: RelColumnRef[];
    materializations: RelProjectExprMapping[];
  },
  RelLoweringError
> {
  const groupBy: RelColumnRef[] = [];
  const materializations: RelProjectExprMapping[] = [];

  for (const term of groupByTerms) {
    if (term.kind === "ref") {
      groupBy.push(term.ref);
      continue;
    }

    const projection = projections[term.position - 1];
    if (!projection) {
      return Result.err(
        new RelLoweringError({
          operation: "resolve aggregate GROUP BY",
          message: `GROUP BY ordinal ${term.position} is out of range.`,
        }),
      );
    }
    if (projection.kind === "metric") {
      return Result.err(
        new RelLoweringError({
          operation: "resolve aggregate GROUP BY",
          message: `GROUP BY ordinal ${term.position} cannot reference an aggregate output.`,
        }),
      );
    }

    const materialization = materializeAggregateGroupProjection(projection, "group_by");
    if (materialization) {
      materializations.push(materialization);
    }
    if (!projection.source) {
      return Result.err(
        new RelLoweringError({
          operation: "resolve aggregate GROUP BY",
          message: `GROUP BY ordinal ${term.position} could not be resolved.`,
        }),
      );
    }

    groupBy.push(projection.source);
  }

  return Result.ok({ groupBy, materializations });
}

function materializeAggregateGroupProjection(
  projection: ParsedAggregateGroupProjection,
  prefix: string,
): RelProjectExprMapping | null {
  if (projection.source || !projection.expr) {
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
