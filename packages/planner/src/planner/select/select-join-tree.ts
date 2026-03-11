import type { RelColumnRef, RelNode } from "@tupl/foundation";
import type { SchemaDefinition, ScanFilterClause } from "@tupl/schema-model";

import { nextRelId } from "../planner-ids";
import type { PreparedSimpleSelect } from "./select-shape";
import { appearsInRel, parseRelColumnRef } from "./select-from-lowering";
import { collectRelExprRefs, isCorrelatedSubquery } from "../sql-expr-lowering";
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
  const pushableWhereAliases = getPushableWhereAliases(shape.rootBinding.alias, shape.joins);
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

  let current: RelNode = scansByAlias.get(shape.rootBinding.alias)!;

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
