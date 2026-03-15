import type { RelColumnRef, RelExpr, RelNode } from "@tupl/foundation";
import { type SchemaDefinition } from "@tupl/schema-model";
import {
  createPhysicalBindingFromEntity,
  getNormalizedColumnSourceMap,
  getNormalizedTableBinding,
} from "@tupl/schema-model/normalization";

import type { AliasToSourceMap } from "../planner-types";

/**
 * Provider alias mapping owns facade-to-source column remapping for provider-facing rel fragments.
 */
export function collectAliasToSourceMappings(
  node: RelNode,
  schema: SchemaDefinition,
): AliasToSourceMap {
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
      case "values":
      case "cte_ref":
        return;
      case "filter":
      case "project":
      case "aggregate":
      case "window":
      case "sort":
      case "limit_offset":
        visit(current.input);
        return;
      case "correlate":
        visit(current.left);
        visit(current.right);
        return;
      case "join":
      case "set_op":
        visit(current.left);
        visit(current.right);
        return;
      case "repeat_union":
        visit(current.seed);
        visit(current.iterative);
        return;
      case "with":
        for (const cte of current.ctes) {
          visit(cte.query);
        }
        visit(current.body);
        return;
    }
  };

  visit(node);
  return mappings;
}

export function mapColumnRefForAlias(
  ref: RelColumnRef,
  aliasToSource: AliasToSourceMap,
): RelColumnRef {
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

export function mapColumnNameForAlias(column: string, aliasToSource: AliasToSourceMap): string {
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

export function mapRelExprRefsForAliasSource(
  expr: RelExpr,
  aliasToSource: AliasToSourceMap,
): RelExpr {
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
