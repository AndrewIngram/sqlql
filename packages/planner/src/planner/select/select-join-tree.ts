import type { RelColumnRef, RelNode } from "@tupl/foundation";
import type { SchemaDefinition, ScanFilterClause } from "@tupl/schema-model";

import { nextRelId } from "../physical/planner-ids";
import type { PreparedSimpleSelect } from "./select-shape";
import { appearsInRel, parseRelColumnRef } from "./select-from-lowering";
import {
  collectRelExprRefs,
  isCorrelatedSubquery,
  mapBinaryOperatorToRelFunction,
} from "../sql-expr-lowering";
import {
  combineAndExprs,
  getPushableWhereAliases,
  literalFilterToRelExpr,
} from "../where-lowering";

/**
 * Select join-tree lowering owns scan construction, join assembly, pushed filters,
 * and subquery semi-join attachment before projection/aggregate shaping.
 */
export function buildSimpleSelectJoinTree(
  shape: PreparedSimpleSelect,
  schema: SchemaDefinition,
  tryLowerSelect: (ast: import("../sqlite-parser/ast").SelectAst) => RelNode | null,
): RelNode | null {
  // Push only literal predicates that attach cleanly to one scan alias. Everything else stays as
  // a residual rel expression so later provider/runtime layers can make one explicit fallback
  // decision instead of each lowering step inventing its own partial rule.
  const pushableWhereAliases = shape.rootBinding
    ? getPushableWhereAliases(shape.rootBinding.alias, shape.joins)
    : new Set<string>();
  const pushableLiteralFilters = shape.whereFilters.literals.filter((filter) =>
    pushableWhereAliases.has(filter.alias),
  );
  const residualExpr = combineAndExprs([
    ...shape.whereFilters.literals
      .filter((filter) => !pushableWhereAliases.has(filter.alias))
      .map(literalFilterToRelExpr),
    ...(shape.whereFilters.residualExpr ? [shape.whereFilters.residualExpr] : []),
  ]);

  const columnsByAlias = collectRequiredColumns(
    shape,
    schema,
    pushableLiteralFilters,
    residualExpr ?? null,
  );
  const scansByAlias = buildScansByAlias(shape, columnsByAlias, pushableLiteralFilters);

  let current: RelNode = shape.rootBinding
    ? scansByAlias.get(shape.rootBinding.alias)!
    : {
        id: nextRelId("values"),
        kind: "values",
        convention: "local",
        rows: [[]],
        output: [],
      };

  for (const join of shape.joins) {
    const right = scansByAlias.get(join.alias);
    if (!right) {
      return null;
    }

    const joinLeftOnCurrent = appearsInRel(current, join.leftAlias);
    const leftKey: RelColumnRef = joinLeftOnCurrent
      ? { alias: join.leftAlias, column: join.leftColumn }
      : { alias: join.rightAlias, column: join.rightColumn };
    const rightKey: RelColumnRef = joinLeftOnCurrent
      ? { alias: join.rightAlias, column: join.rightColumn }
      : { alias: join.leftAlias, column: join.leftColumn };

    current = {
      id: nextRelId("join"),
      kind: "join",
      convention: "local",
      joinType: join.joinType,
      left: current,
      right,
      leftKey,
      rightKey,
      output: [...current.output, ...right.output],
    };
  }

  for (const inFilter of shape.whereFilters.inSubqueries) {
    // IN-subqueries become semi-joins only when the subquery is uncorrelated and shape-compatible.
    // Returning null here intentionally punts the whole select back to the broader lowering path
    // rather than producing a half-lowered tree with hidden execution constraints.
    const outerAliases = new Set(shape.bindings.map((binding) => binding.alias));
    if (isCorrelatedSubquery(inFilter.subquery, outerAliases)) {
      return null;
    }

    const subqueryRel = tryLowerSelect(inFilter.subquery);
    if (!subqueryRel || subqueryRel.output.length !== 1) {
      return null;
    }
    const rightOutput = subqueryRel.output[0]?.name;
    if (!rightOutput) {
      return null;
    }

    current = {
      id: nextRelId("join"),
      kind: "join",
      convention: "local",
      joinType: "semi",
      left: current,
      right: subqueryRel,
      leftKey: {
        alias: inFilter.alias,
        column: inFilter.column,
      },
      rightKey: parseRelColumnRef(rightOutput),
      output: current.output,
    };
  }

  for (const inFilter of shape.whereFilters.correlatedInSubqueries) {
    const subqueryRel = tryLowerSelect(inFilter.subquery);
    if (!subqueryRel || subqueryRel.output.length !== 1) {
      return null;
    }

    current = {
      id: nextRelId("join"),
      kind: "join",
      convention: "local",
      joinType: "semi",
      left: current,
      right: subqueryRel,
      leftKey: {
        alias: inFilter.outer.alias,
        column: inFilter.outer.column,
      },
      rightKey: parseRelColumnRef(subqueryRel.output[0]!.name),
      output: current.output,
    };
  }

  for (const existsFilter of shape.whereFilters.existsSubqueries) {
    const subqueryRel = tryLowerSelect(existsFilter.subquery);
    if (!subqueryRel) {
      return null;
    }

    if (existsFilter.negated) {
      const keyedSubqueryRel = ensureQualifiedJoinKeyProjected(subqueryRel, existsFilter.inner);
      const rightOutput = `${existsFilter.inner.alias}.${existsFilter.inner.column}`;
      const leftOutput = current.output;
      const antiJoin = {
        id: nextRelId("join"),
        kind: "join" as const,
        convention: "local" as const,
        joinType: "left" as const,
        left: current,
        right: keyedSubqueryRel,
        leftKey: {
          alias: existsFilter.outer.alias,
          column: existsFilter.outer.column,
        },
        rightKey: parseRelColumnRef(rightOutput),
        output: [...current.output, ...keyedSubqueryRel.output],
      };
      const filtered = {
        id: nextRelId("filter"),
        kind: "filter" as const,
        convention: "local" as const,
        input: antiJoin,
        expr: {
          kind: "function" as const,
          name: "is_null",
          args: [
            {
              kind: "column" as const,
              ref: parseRelColumnRef(rightOutput),
            },
          ],
        },
        output: antiJoin.output,
      };

      current = projectToOutputShape(filtered, leftOutput);
      continue;
    }

    const keyedSubqueryRel = ensureJoinKeyProjected(subqueryRel, existsFilter.inner);
    const rightOutput = findProjectedJoinKeyOutput(keyedSubqueryRel, existsFilter.inner);
    if (!rightOutput) {
      return null;
    }

    current = {
      id: nextRelId("join"),
      kind: "join",
      convention: "local",
      joinType: "semi",
      left: current,
      right: keyedSubqueryRel,
      leftKey: {
        alias: existsFilter.outer.alias,
        column: existsFilter.outer.column,
      },
      rightKey: parseRelColumnRef(rightOutput),
      output: current.output,
    };
  }

  for (const scalarFilter of shape.whereFilters.correlatedScalarAggregates) {
    const subqueryRel = tryLowerSelect(scalarFilter.subquery);
    if (!subqueryRel || subqueryRel.output.length !== 2) {
      return null;
    }

    const mappedOperator = mapBinaryOperatorToRelFunction(scalarFilter.operator.toUpperCase());
    if (!mappedOperator) {
      return null;
    }

    current = {
      id: nextRelId("join"),
      kind: "join",
      convention: "local",
      joinType: "inner",
      left: current,
      right: subqueryRel,
      leftKey: {
        alias: scalarFilter.outerKey.alias,
        column: scalarFilter.outerKey.column,
      },
      rightKey: parseRelColumnRef(scalarFilter.correlationOutput),
      output: [...current.output, ...subqueryRel.output],
    };

    current = {
      id: nextRelId("filter"),
      kind: "filter",
      convention: "local",
      input: current,
      expr: {
        kind: "function",
        name: mappedOperator,
        args: [
          {
            kind: "column",
            ref: {
              alias: scalarFilter.outerCompare.alias,
              column: scalarFilter.outerCompare.column,
            },
          },
          {
            kind: "column",
            ref: {
              column: scalarFilter.metricOutput,
            },
          },
        ],
      },
      output: current.output,
    };
  }

  if (residualExpr) {
    current = {
      id: nextRelId("filter"),
      kind: "filter",
      convention: "local",
      input: current,
      expr: residualExpr,
      output: current.output,
    };
  }

  return current;
}

function projectToOutputShape(rel: RelNode, output: RelNode["output"]): RelNode {
  return {
    id: nextRelId("project"),
    kind: "project",
    convention: "local",
    input: rel,
    columns: output.map((column) => ({
      kind: "column" as const,
      source: parseRelColumnRef(column.name),
      output: column.name,
    })),
    output,
  };
}

function findProjectedJoinKeyOutput(
  rel: RelNode,
  ref: { alias: string; column: string },
): string | null {
  if (rel.kind === "project") {
    for (const mapping of rel.columns) {
      if (
        mapping.kind !== "expr" &&
        mapping.source.alias === ref.alias &&
        mapping.source.column === ref.column
      ) {
        return mapping.output;
      }
    }
  }

  for (const column of rel.output) {
    const parsed = parseRelColumnRef(column.name);
    if (parsed.alias === ref.alias && parsed.column === ref.column) {
      return column.name;
    }
  }
  return null;
}

function ensureJoinKeyProjected(rel: RelNode, ref: { alias: string; column: string }): RelNode {
  const existing = findProjectedJoinKeyOutput(rel, ref);
  if (existing) {
    return rel;
  }

  if (rel.kind === "project") {
    const outputName = `${ref.alias}.${ref.column}`;
    return {
      ...rel,
      columns: [
        ...rel.columns,
        {
          kind: "column",
          source: {
            alias: ref.alias,
            column: ref.column,
          },
          output: outputName,
        },
      ],
      output: [...rel.output, { name: outputName }],
    };
  }

  if (rel.kind === "filter" || rel.kind === "sort" || rel.kind === "limit_offset") {
    const input = ensureJoinKeyProjected(rel.input, ref);
    if (input === rel.input) {
      return rel;
    }

    return {
      ...rel,
      input,
      output: input.output,
    };
  }

  const outputName = `${ref.alias}.${ref.column}`;
  return {
    id: nextRelId("project"),
    kind: "project",
    convention: "local",
    input: rel,
    columns: [
      ...rel.output.map((column) => ({
        kind: "column" as const,
        source: parseRelColumnRef(column.name),
        output: column.name,
      })),
      {
        kind: "column" as const,
        source: {
          alias: ref.alias,
          column: ref.column,
        },
        output: outputName,
      },
    ],
    output: [...rel.output, { name: outputName }],
  };
}

function ensureQualifiedJoinKeyProjected(
  rel: RelNode,
  ref: { alias: string; column: string },
): RelNode {
  const outputName = `${ref.alias}.${ref.column}`;
  if (rel.output.some((column) => column.name === outputName)) {
    return rel;
  }

  if (rel.kind === "project") {
    return {
      ...rel,
      columns: [
        ...rel.columns,
        {
          kind: "column",
          source: {
            alias: ref.alias,
            column: ref.column,
          },
          output: outputName,
        },
      ],
      output: [...rel.output, { name: outputName }],
    };
  }

  if (rel.kind === "filter" || rel.kind === "sort" || rel.kind === "limit_offset") {
    const input = ensureQualifiedJoinKeyProjected(rel.input, ref);
    if (input === rel.input) {
      return rel;
    }

    return {
      ...rel,
      input,
      output: input.output,
    };
  }

  return {
    id: nextRelId("project"),
    kind: "project",
    convention: "local",
    input: rel,
    columns: [
      ...rel.output.map((column) => ({
        kind: "column" as const,
        source: parseRelColumnRef(column.name),
        output: column.name,
      })),
      {
        kind: "column" as const,
        source: {
          alias: ref.alias,
          column: ref.column,
        },
        output: outputName,
      },
    ],
    output: [...rel.output, { name: outputName }],
  };
}

function collectRequiredColumns(
  shape: PreparedSimpleSelect,
  schema: SchemaDefinition,
  pushableLiteralFilters: Array<{ alias: string; clause: ScanFilterClause }>,
  residualExpr: import("@tupl/foundation").RelExpr | null,
): Map<string, Set<string>> {
  const columnsByAlias = new Map<string, Set<string>>();
  for (const binding of shape.bindings) {
    columnsByAlias.set(binding.alias, new Set<string>());
  }

  if (shape.aggregateMode) {
    // Aggregate lowering owns the invariant that every grouped expression, metric input, and
    // post-aggregate ordering reference is already present on the scan rows feeding the aggregate.
    // That keeps provider/runtime stages from having to rediscover which pre-aggregate columns are
    // semantically required to preserve the logical result.
    for (const projection of shape.safeAggregateProjections) {
      if (projection.kind !== "group") {
        continue;
      }
      if (projection.source?.alias) {
        columnsByAlias.get(projection.source.alias)?.add(projection.source.column);
      }
      if (projection.expr) {
        for (const ref of collectRelExprRefs(projection.expr)) {
          if (ref.alias) {
            columnsByAlias.get(ref.alias)?.add(ref.column);
          }
        }
      }
    }

    for (const metric of shape.allAggregateMetrics) {
      if (!metric.column) {
        continue;
      }
      const alias = metric.column.alias ?? metric.column.table;
      if (alias) {
        columnsByAlias.get(alias)?.add(metric.column.column);
      }
    }

    for (const ref of shape.effectiveGroupBy) {
      if (ref.alias) {
        columnsByAlias.get(ref.alias)?.add(ref.column);
      }
    }
  } else {
    // Non-aggregate select lowering follows the same rule for ordinary projections and window
    // functions: scans must surface every referenced base column before later shaping rewrites the
    // row. This keeps join-tree construction responsible for physical row shape, while later
    // project/window stages only decide logical output naming.
    for (const projection of shape.safeProjections) {
      if (projection.kind === "column") {
        if (projection.source.alias) {
          columnsByAlias.get(projection.source.alias)?.add(projection.source.column);
        }
        continue;
      }
      if (projection.kind === "expr") {
        for (const ref of collectRelExprRefs(projection.expr)) {
          if (ref.alias) {
            columnsByAlias.get(ref.alias)?.add(ref.column);
          }
        }
        continue;
      }
      for (const partition of projection.function.partitionBy) {
        if (partition.alias) {
          columnsByAlias.get(partition.alias)?.add(partition.column);
        }
      }
      if ("column" in projection.function && projection.function.column?.alias) {
        columnsByAlias
          .get(projection.function.column.alias)
          ?.add(projection.function.column.column);
      }
      for (const orderTerm of projection.function.orderBy) {
        if (orderTerm.source.alias) {
          columnsByAlias.get(orderTerm.source.alias)?.add(orderTerm.source.column);
        }
      }
    }
  }

  for (const join of shape.joins) {
    columnsByAlias.get(join.leftAlias)?.add(join.leftColumn);
    columnsByAlias.get(join.rightAlias)?.add(join.rightColumn);
  }
  for (const filter of pushableLiteralFilters) {
    columnsByAlias.get(filter.alias)?.add(filter.clause.column);
  }
  for (const filter of shape.whereFilters.inSubqueries) {
    columnsByAlias.get(filter.alias)?.add(filter.column);
  }
  for (const filter of shape.whereFilters.existsSubqueries) {
    columnsByAlias.get(filter.inner.alias)?.add(filter.inner.column);
    columnsByAlias.get(filter.outer.alias)?.add(filter.outer.column);
  }
  for (const filter of shape.whereFilters.correlatedInSubqueries) {
    columnsByAlias.get(filter.inner.alias)?.add(filter.inner.column);
    columnsByAlias.get(filter.outer.alias)?.add(filter.outer.column);
  }
  for (const filter of shape.whereFilters.correlatedScalarAggregates) {
    columnsByAlias.get(filter.outerCompare.alias)?.add(filter.outerCompare.column);
    columnsByAlias.get(filter.outerKey.alias)?.add(filter.outerKey.column);
    columnsByAlias.get(filter.innerKey.alias)?.add(filter.innerKey.column);
  }
  if (residualExpr) {
    for (const ref of collectRelExprRefs(residualExpr)) {
      if (ref.alias) {
        columnsByAlias.get(ref.alias)?.add(ref.column);
      }
    }
  }
  for (const term of shape.orderBy) {
    if (term.source.alias) {
      columnsByAlias.get(term.source.alias)?.add(term.source.column);
    }
  }

  for (const binding of shape.bindings) {
    const columns = columnsByAlias.get(binding.alias);
    if (!columns || columns.size > 0) {
      continue;
    }
    // Even a scan whose columns are not referenced later still needs one stable physical column so
    // join/filter execution can materialize a row shape. Using the first declared schema column is
    // intentionally arbitrary but local: it avoids leaking a "synthetic empty row" convention into
    // provider/runtime execution semantics.
    if (schema.tables[binding.table]) {
      const schemaColumns = Object.keys(schema.tables[binding.table]?.columns ?? {});
      const first = schemaColumns[0];
      if (first) {
        columns.add(first);
      }
    }
  }

  return columnsByAlias;
}

function buildScansByAlias(
  shape: PreparedSimpleSelect,
  columnsByAlias: Map<string, Set<string>>,
  pushableLiteralFilters: Array<{ alias: string; clause: ScanFilterClause }>,
): Map<string, Extract<RelNode, { kind: "scan" }>> {
  const filtersByAlias = new Map<string, ScanFilterClause[]>();
  for (const filter of pushableLiteralFilters) {
    const current = filtersByAlias.get(filter.alias) ?? [];
    current.push(filter.clause);
    filtersByAlias.set(filter.alias, current);
  }

  const scansByAlias = new Map<string, Extract<RelNode, { kind: "scan" }>>();
  for (const binding of shape.bindings) {
    const select = [...(columnsByAlias.get(binding.alias) ?? new Set<string>())];
    const scanWhere = filtersByAlias.get(binding.alias);

    scansByAlias.set(binding.alias, {
      id: nextRelId("scan"),
      kind: "scan",
      convention: "local",
      table: binding.table,
      alias: binding.alias,
      select,
      ...(scanWhere && scanWhere.length > 0 ? { where: scanWhere } : {}),
      output: select.map((column) => ({
        name: `${binding.alias}.${column}`,
      })),
    });
  }

  return scansByAlias;
}
