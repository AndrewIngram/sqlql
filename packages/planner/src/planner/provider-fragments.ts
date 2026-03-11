import { Result } from "better-result";

import {
  isRelProjectColumnMapping,
  type RelColumnRef,
  type RelExpr,
  type RelNode,
  type RelProjectNode,
  type RelScanNode,
  type ScanFilterClause,
} from "@tupl/foundation";
import {
  type ColumnDefinition,
  createPhysicalBindingFromEntity,
  createTableDefinitionFromEntity,
  getNormalizedColumnSourceMap,
  getNormalizedTableBinding,
  resolveNormalizedColumnSource,
  type SchemaDefinition,
} from "@tupl/schema-model";
import type { ProviderFragment } from "@tupl/provider-kit";
import { toTuplPlanningError } from "./planner-errors";
import { resolveSingleProvider } from "./conventions";
import { expandRelViewsResult } from "./view-expansion";
import type { AliasToSourceMap } from "./planner-types";

/**
 * Provider fragments own normalization of planner nodes into provider-facing fragment requests.
 */
export function buildProviderFragmentForRel<TContext = unknown>(
  node: RelNode,
  schema: SchemaDefinition,
  context?: TContext,
): ProviderFragment | null {
  const result = buildProviderFragmentForRelResult(node, schema, context);
  if (Result.isError(result)) {
    throw result.error;
  }
  return result.value;
}

export function buildProviderFragmentForRelResult<TContext = unknown>(
  node: RelNode,
  schema: SchemaDefinition,
  context?: TContext,
) {
  return Result.gen(function* () {
    const expanded = yield* expandRelViewsResult(node, schema, context);
    const provider = resolveSingleProvider(expanded, schema);
    if (!provider) {
      return Result.ok(null);
    }

    return buildProviderFragmentForNodeResult(expanded, schema, provider);
  });
}

export function buildProviderFragmentForNodeResult(
  node: RelNode,
  schema: SchemaDefinition,
  provider: string,
) {
  return Result.try({
    try: () => buildProviderFragmentForNode(node, schema, provider),
    catch: (error) => toTuplPlanningError(error, "build provider fragment"),
  });
}

function buildProviderFragmentForNode(
  node: RelNode,
  schema: SchemaDefinition,
  provider: string,
): ProviderFragment {
  if (node.kind === "scan") {
    const normalizedScan = normalizeScanForProvider(node, schema);
    return {
      kind: "scan",
      provider,
      table: normalizedScan.table,
      request: {
        table: normalizedScan.table,
        ...(normalizedScan.alias ? { alias: normalizedScan.alias } : {}),
        select: normalizedScan.select,
        ...(normalizedScan.where ? { where: normalizedScan.where } : {}),
        ...(normalizedScan.orderBy ? { orderBy: normalizedScan.orderBy } : {}),
        ...(normalizedScan.limit != null ? { limit: normalizedScan.limit } : {}),
        ...(normalizedScan.offset != null ? { offset: normalizedScan.offset } : {}),
      },
    };
  }

  if (node.kind === "aggregate") {
    const aggregateFragment = buildAggregateProviderFragment(node, schema, provider);
    if (aggregateFragment) {
      return aggregateFragment;
    }
  }

  return {
    kind: "rel",
    provider,
    rel: normalizeRelForProvider(node, schema),
  };
}

function buildAggregateProviderFragment(
  node: Extract<RelNode, { kind: "aggregate" }>,
  schema: SchemaDefinition,
  provider: string,
): ProviderFragment | null {
  const extracted = extractAggregateProviderInput(node.input);
  if (!extracted) {
    return null;
  }

  const mergedScan: RelScanNode = {
    ...extracted.scan,
    ...(extracted.where.length > 0
      ? {
          where: [...(extracted.scan.where ?? []), ...extracted.where],
        }
      : {}),
  };
  const normalizedScan = normalizeScanForProvider(mergedScan, schema);
  const aliasToSource = collectAliasToSourceMappings(mergedScan, schema);

  return {
    kind: "aggregate",
    provider,
    table: normalizedScan.table,
    request: {
      table: normalizedScan.table,
      ...(normalizedScan.alias ? { alias: normalizedScan.alias } : {}),
      ...(normalizedScan.where?.length ? { where: normalizedScan.where } : {}),
      ...(node.groupBy.length
        ? {
            groupBy: node.groupBy.map(
              (column) => mapColumnRefForAlias(column, aliasToSource).column,
            ),
          }
        : {}),
      metrics: node.metrics.map((metric) => ({
        fn: metric.fn,
        as: metric.as,
        ...(metric.distinct ? { distinct: true } : {}),
        ...(metric.column
          ? {
              column: mapColumnRefForAlias(metric.column, aliasToSource).column,
            }
          : {}),
      })),
    },
  };
}

function extractAggregateProviderInput(node: RelNode): {
  scan: RelScanNode;
  where: ScanFilterClause[];
} | null {
  const where: ScanFilterClause[] = [];
  let current = node;

  while (current.kind === "filter") {
    if (current.expr) {
      return null;
    }
    if (current.where) {
      where.push(...current.where);
    }
    current = current.input;
  }

  if (current.kind !== "scan") {
    return null;
  }

  if (current.orderBy?.length || current.limit != null || current.offset != null) {
    return null;
  }

  return {
    scan: current,
    where,
  };
}

function normalizeRelForProvider(node: RelNode, schema: SchemaDefinition): RelNode {
  const aliasToSource = collectAliasToSourceMappings(node, schema);

  const visit = (current: RelNode): RelNode => {
    switch (current.kind) {
      case "scan":
        return normalizeScanForProvider(current, schema);
      case "filter":
        return {
          ...current,
          input: visit(current.input),
          ...(current.where
            ? {
                where: current.where.map((clause) => ({
                  ...clause,
                  column: mapColumnNameForAlias(clause.column, aliasToSource),
                })),
              }
            : {}),
          ...(current.expr
            ? {
                expr: mapRelExprRefsForAliasSource(current.expr, aliasToSource),
              }
            : {}),
        };
      case "project":
        return {
          ...current,
          input: visit(current.input),
          columns: current.columns.map((column) =>
            isRelProjectColumnMapping(column)
              ? {
                  ...column,
                  source: mapColumnRefForAlias(column.source, aliasToSource),
                }
              : {
                  ...column,
                  expr: mapRelExprRefsForAliasSource(column.expr, aliasToSource),
                },
          ),
        };
      case "join":
        return {
          ...current,
          left: visit(current.left),
          right: visit(current.right),
          leftKey: mapColumnRefForAlias(current.leftKey, aliasToSource),
          rightKey: mapColumnRefForAlias(current.rightKey, aliasToSource),
        };
      case "aggregate":
        return {
          ...current,
          input: visit(current.input),
          groupBy: current.groupBy.map((column) => mapColumnRefForAlias(column, aliasToSource)),
          metrics: current.metrics.map((metric) => ({
            ...metric,
            ...(metric.column
              ? { column: mapColumnRefForAlias(metric.column, aliasToSource) }
              : {}),
          })),
        };
      case "window":
        return {
          ...current,
          input: visit(current.input),
          functions: current.functions.map((fn) => ({
            ...fn,
            partitionBy: fn.partitionBy.map((column) =>
              mapColumnRefForAlias(column, aliasToSource),
            ),
            orderBy: fn.orderBy.map((term) => ({
              ...term,
              source: mapColumnRefForAlias(term.source, aliasToSource),
            })),
          })),
        };
      case "sort":
        return {
          ...current,
          input: visit(current.input),
          orderBy: current.orderBy.map((term) => ({
            ...term,
            source: mapColumnRefForAlias(term.source, aliasToSource),
          })),
        };
      case "limit_offset":
        return {
          ...current,
          input: visit(current.input),
        };
      case "set_op":
        return {
          ...current,
          left: visit(current.left),
          right: visit(current.right),
        };
      case "with":
        return {
          ...current,
          ctes: current.ctes.map((cte) => ({
            ...cte,
            query: visit(cte.query),
          })),
          body: visit(current.body),
        };
      case "sql":
        return current;
    }
  };

  return simplifyProviderProjects(visit(node));
}

function simplifyProviderProjects(node: RelNode): RelNode {
  switch (node.kind) {
    case "scan":
    case "sql":
      return node;
    case "filter":
      return { ...node, input: simplifyProviderProjects(node.input) };
    case "project":
      return hoistProjectAcrossUnaryChain({
        ...node,
        input: simplifyProviderProjects(node.input),
      });
    case "aggregate":
    case "window":
    case "sort":
    case "limit_offset":
      return {
        ...node,
        input: simplifyProviderProjects(node.input),
      };
    case "join":
    case "set_op":
      return {
        ...node,
        left: simplifyProviderProjects(node.left),
        right: simplifyProviderProjects(node.right),
      };
    case "with":
      return {
        ...node,
        ctes: node.ctes.map((cte) => ({
          ...cte,
          query: simplifyProviderProjects(cte.query),
        })),
        body: simplifyProviderProjects(node.body),
      };
  }
}

function hoistProjectAcrossUnaryChain(project: RelProjectNode): RelNode {
  const unaryChain: Array<Extract<RelNode, { kind: "filter" | "sort" | "limit_offset" }>> = [];
  let current = project.input;

  while (current.kind === "filter" || current.kind === "sort" || current.kind === "limit_offset") {
    unaryChain.push(current);
    current = current.input;
  }

  if (current.kind !== "project") {
    return project;
  }

  const mergedColumns = composeProjectMappings(project.columns, current.columns);
  if (!mergedColumns) {
    return project;
  }

  let rebuiltInput: RelNode = current.input;
  for (let index = unaryChain.length - 1; index >= 0; index -= 1) {
    const unary = unaryChain[index];
    if (!unary) {
      continue;
    }
    rebuiltInput = {
      ...unary,
      input: rebuiltInput,
    };
  }

  return {
    ...project,
    input: rebuiltInput,
    columns: mergedColumns,
  };
}

function composeProjectMappings(
  outer: RelProjectNode["columns"],
  inner: RelProjectNode["columns"],
): RelProjectNode["columns"] | null {
  const innerByOutput = new Map(inner.map((mapping) => [mapping.output, mapping] as const));
  const merged: RelProjectNode["columns"] = [];

  for (const mapping of outer) {
    if (!isRelProjectColumnMapping(mapping) || mapping.source.alias || mapping.source.table) {
      return null;
    }

    const innerMapping = innerByOutput.get(mapping.source.column);
    if (!innerMapping) {
      return null;
    }

    if (isRelProjectColumnMapping(innerMapping)) {
      merged.push({
        kind: "column",
        source: innerMapping.source,
        output: mapping.output,
      });
      continue;
    }

    merged.push({
      kind: "expr",
      expr: innerMapping.expr,
      output: mapping.output,
    });
  }

  return merged;
}

function normalizeScanForProvider(node: RelScanNode, schema: SchemaDefinition): RelScanNode {
  const binding =
    getNormalizedTableBinding(schema, node.table) ??
    (node.entity ? createPhysicalBindingFromEntity(node.entity) : undefined);
  if (!binding || binding.kind !== "physical") {
    return node;
  }
  const table =
    schema.tables[node.table] ??
    (node.entity ? createTableDefinitionFromEntity(node.entity) : undefined);

  const mapColumn = (column: string): string => resolveNormalizedColumnSource(binding, column);
  const mapClause = (clause: ScanFilterClause): ScanFilterClause => {
    const mapped = mapEnumFilterForProvider(table?.columns[clause.column], clause);
    return {
      ...mapped,
      column: mapColumn(mapped.column),
    };
  };

  return {
    ...node,
    table: binding.entity,
    ...(node.entity ? { entity: node.entity } : {}),
    select: node.select.map(mapColumn),
    ...(node.where
      ? {
          where: node.where.map(mapClause),
        }
      : {}),
    ...(node.orderBy
      ? {
          orderBy: node.orderBy.map((term) => ({
            ...term,
            column: mapColumn(term.column),
          })),
        }
      : {}),
  };
}

function mapEnumFilterForProvider(definition: unknown, clause: ScanFilterClause): ScanFilterClause {
  if (!definition || typeof definition === "string") {
    return clause;
  }

  const column = definition as ColumnDefinition;
  if (!column.enumMap || Object.keys(column.enumMap).length === 0) {
    return clause;
  }

  const mapFacadeValueToSource = (value: unknown): string[] => {
    if (typeof value !== "string") {
      return [];
    }
    const out: string[] = [];
    for (const [sourceValue, facadeValue] of Object.entries(column.enumMap ?? {})) {
      if (facadeValue === value) {
        out.push(sourceValue);
      }
    }
    return out;
  };

  if (clause.op === "eq") {
    const mappedValues = mapFacadeValueToSource(clause.value);
    if (mappedValues.length === 0) {
      throw new Error(
        `No upstream enum mapping for value ${JSON.stringify(clause.value)} on ${clause.column}.`,
      );
    }
    if (mappedValues.length === 1) {
      return {
        ...clause,
        value: mappedValues[0],
      };
    }
    return {
      op: "in",
      column: clause.column,
      values: mappedValues,
    };
  }

  if (clause.op === "in") {
    const mapped = [...new Set(clause.values.flatMap((value) => mapFacadeValueToSource(value)))];
    if (mapped.length === 0) {
      throw new Error(`No upstream enum mappings for IN predicate on ${clause.column}.`);
    }
    return {
      ...clause,
      values: mapped,
    };
  }

  return clause;
}

function collectAliasToSourceMappings(node: RelNode, schema: SchemaDefinition): AliasToSourceMap {
  const mappings: AliasToSourceMap = new Map();

  const visit = (current: RelNode): void => {
    switch (current.kind) {
      case "scan": {
        const binding =
          getNormalizedTableBinding(schema, current.table) ??
          (current.entity ? createPhysicalBindingFromEntity(current.entity) : undefined);
        if (binding?.kind !== "physical") {
          return;
        }
        const alias = current.alias ?? current.table;
        mappings.set(alias, getNormalizedColumnSourceMap(binding));
        return;
      }
      case "filter":
      case "project":
      case "aggregate":
      case "window":
      case "sort":
      case "limit_offset":
        visit(current.input);
        return;
      case "join":
      case "set_op":
        visit(current.left);
        visit(current.right);
        return;
      case "with":
        for (const cte of current.ctes) {
          visit(cte.query);
        }
        visit(current.body);
        return;
      case "sql":
        return;
    }
  };

  visit(node);
  return mappings;
}

function mapColumnRefForAlias(ref: RelColumnRef, aliasToSource: AliasToSourceMap): RelColumnRef {
  const alias = ref.alias ?? ref.table;
  if (alias) {
    const mapping = aliasToSource.get(alias);
    if (!mapping) {
      return ref;
    }
    return {
      ...ref,
      column: mapping[ref.column] ?? ref.column,
    };
  }

  return {
    ...ref,
    column: mapColumnNameForAlias(ref.column, aliasToSource),
  };
}

function mapColumnNameForAlias(column: string, aliasToSource: AliasToSourceMap): string {
  const idx = column.lastIndexOf(".");
  if (idx > 0) {
    const alias = column.slice(0, idx);
    const name = column.slice(idx + 1);
    const mapping = aliasToSource.get(alias);
    if (!mapping) {
      return column;
    }
    const mapped = mapping[name] ?? name;
    return `${alias}.${mapped}`;
  }

  let mappedColumn: string | null = null;
  for (const mapping of aliasToSource.values()) {
    if (!(column in mapping)) {
      continue;
    }
    const candidate = mapping[column] ?? column;
    if (mappedColumn && mappedColumn !== candidate) {
      return column;
    }
    mappedColumn = candidate;
  }

  return mappedColumn ?? column;
}

function mapRelExprRefsForAliasSource(expr: RelExpr, aliasToSource: AliasToSourceMap): RelExpr {
  switch (expr.kind) {
    case "literal":
      return expr;
    case "column":
      return {
        kind: "column",
        ref: mapColumnRefForAlias(expr.ref, aliasToSource),
      };
    case "function":
      return {
        kind: "function",
        name: expr.name,
        args: expr.args.map((arg) => mapRelExprRefsForAliasSource(arg, aliasToSource)),
      };
    case "subquery":
      return expr;
  }
}
