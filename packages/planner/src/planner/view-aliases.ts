import type { RelColumnRef, RelExpr } from "@tupl/foundation";

import type { ViewAliasColumnMap } from "./planner-types";

/**
 * View alias helpers own alias-map merging and ref rewriting after planner view expansion.
 */
export function mergeAliasMaps(
  ...maps: Array<Map<string, ViewAliasColumnMap>>
): Map<string, ViewAliasColumnMap> {
  const out = new Map<string, ViewAliasColumnMap>();
  for (const aliases of maps) {
    for (const [alias, mapping] of aliases.entries()) {
      out.set(alias, mapping);
    }
  }
  return out;
}

export function resolveViewSourceRef(
  source: string,
  aliases: Map<string, ViewAliasColumnMap>,
): RelColumnRef {
  const ref = parseRelColumnRef(source);
  return ref.alias || ref.table ? resolveMappedColumnRef(ref, aliases) : ref;
}

export function mapViewColumnName(
  column: string,
  viewAliasMapping: ViewAliasColumnMap,
  aliases: Map<string, ViewAliasColumnMap>,
): string {
  const idx = column.lastIndexOf(".");
  if (idx > 0) {
    const name = column.slice(idx + 1);
    if (name in viewAliasMapping) {
      return toColumnName(
        resolveMappedColumnRef(viewAliasMapping[name] ?? parseRelColumnRef(name), aliases),
      );
    }
    return rewriteColumnNameWithAliases(column, aliases);
  }

  const mapped = viewAliasMapping[column];
  if (mapped) {
    return toColumnName(resolveMappedColumnRef(mapped, aliases));
  }
  return rewriteColumnNameWithAliases(column, aliases);
}

export function rewriteColumnNameWithAliases(
  column: string,
  aliases: Map<string, ViewAliasColumnMap>,
): string {
  const ref = parseRelColumnRef(column);
  return toColumnName(resolveMappedColumnRef(ref, aliases));
}

export function resolveMappedColumnRef(
  ref: RelColumnRef,
  aliases: Map<string, ViewAliasColumnMap>,
): RelColumnRef {
  const seen = new Set<string>();
  let current = ref;

  while (true) {
    const alias = current.alias ?? current.table;
    if (!alias) {
      let candidate: RelColumnRef | null = null;
      for (const mapping of aliases.values()) {
        const mapped = mapping[current.column];
        if (!mapped) {
          continue;
        }
        const resolved =
          mapped.alias || mapped.table ? resolveMappedColumnRef(mapped, aliases) : mapped;
        const key = toColumnName(resolved);
        if (!candidate) {
          candidate = resolved;
          continue;
        }
        if (toColumnName(candidate) !== key) {
          return current;
        }
      }
      return candidate ?? current;
    }

    const key = `${alias}.${current.column}`;
    if (seen.has(key)) {
      return current;
    }
    seen.add(key);

    const mapping = aliases.get(alias);
    if (!mapping) {
      return current;
    }
    const next = mapping[current.column];
    if (!next) {
      return {
        column: current.column,
      };
    }
    current = next;
  }
}

export function mapRelExprRefs(expr: RelExpr, aliases: Map<string, ViewAliasColumnMap>): RelExpr {
  switch (expr.kind) {
    case "literal":
      return expr;
    case "column":
      return {
        kind: "column",
        ref: resolveMappedColumnRef(expr.ref, aliases),
      };
    case "function":
      return {
        kind: "function",
        name: expr.name,
        args: expr.args.map((arg) => mapRelExprRefs(arg, aliases)),
      };
    case "subquery":
      return expr;
  }
}

function toColumnName(ref: RelColumnRef): string {
  const alias = ref.alias ?? ref.table;
  return alias ? `${alias}.${ref.column}` : ref.column;
}

export function parseRelColumnRef(ref: string): RelColumnRef {
  const idx = ref.lastIndexOf(".");
  if (idx < 0) {
    return {
      column: ref,
    };
  }
  return {
    alias: ref.slice(0, idx),
    column: ref.slice(idx + 1),
  };
}
