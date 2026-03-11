import { isRelProjectColumnMapping, type RelNode, type RelProjectNode } from "@tupl/foundation";
import type { SchemaDefinition } from "@tupl/schema-model";

import {
  collectAliasToSourceMappings,
  mapColumnNameForAlias,
  mapColumnRefForAlias,
  mapRelExprRefsForAliasSource,
} from "./provider-alias-mapping";
import { normalizeScanForProvider } from "./provider-scan-normalization";

/**
 * Provider rel normalization owns full rel-tree rewriting into provider-facing column/source space.
 */
export function normalizeRelForProvider(node: RelNode, schema: SchemaDefinition): RelNode {
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
