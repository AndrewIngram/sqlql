import type { RelExpr } from "@tupl/foundation";

import { isSchemaDataEntityHandle } from "./dsl-tokens";
import type {
  SchemaColRefToken,
  SchemaDataEntityHandle,
  SchemaDslTableToken,
} from "./schema-contracts";
import type { SchemaViewRelNode, SchemaViewRelNodeInput } from "./types";

/**
 * View normalization owns normalization of DSL view definitions into schema-facing view contracts.
 */
export function resolveColRefToken(
  token: SchemaColRefToken,
  resolveTableToken: (token: SchemaDslTableToken<string>) => string,
  resolveEntityToken: (entity: SchemaDataEntityHandle<string>) => string,
): string {
  if (token.ref) {
    return token.ref;
  }

  if (token.table && token.column) {
    return `${resolveTableToken(token.table)}.${token.column}`;
  }

  if (token.entity && token.column) {
    return `${resolveEntityToken(token.entity)}.${token.column}`;
  }

  throw new Error("Invalid schema column reference token.");
}

export function resolveEnumRef(
  enumFrom: SchemaColRefToken | string,
  resolveTableToken: (token: SchemaDslTableToken<string>) => string,
  resolveEntityToken: (entity: SchemaDataEntityHandle<string>) => string,
): string {
  if (typeof enumFrom === "string") {
    return enumFrom;
  }

  return resolveColRefToken(enumFrom, resolveTableToken, resolveEntityToken);
}

export function resolveViewRelDefinition(
  definition: unknown,
  resolveTableToken: (token: SchemaDslTableToken<string>) => string,
  resolveEntityToken: (entity: SchemaDataEntityHandle<string>) => string,
): unknown {
  if (
    definition &&
    typeof definition === "object" &&
    typeof (definition as { convention?: unknown }).convention === "string"
  ) {
    return definition;
  }

  if (
    !definition ||
    typeof definition !== "object" ||
    typeof (definition as { kind?: unknown }).kind !== "string"
  ) {
    return definition;
  }

  const asRef = (token: SchemaColRefToken): SchemaColRefToken => ({
    kind: "dsl_col_ref",
    ref: resolveColRefToken(token, resolveTableToken, resolveEntityToken),
  });

  const resolveNode = (node: SchemaViewRelNodeInput): SchemaViewRelNode => {
    switch (node.kind) {
      case "scan":
        if (isSchemaDataEntityHandle((node as { entity?: unknown }).entity)) {
          const entity = (node as unknown as { entity: SchemaDataEntityHandle<string> }).entity;
          return {
            kind: "scan",
            table: typeof node.table === "string" ? node.table : resolveEntityToken(entity),
            entity,
          };
        }
        if (isSchemaDataEntityHandle(node.table)) {
          return {
            kind: "scan",
            table: resolveEntityToken(node.table),
            entity: node.table,
          };
        }
        return {
          kind: "scan",
          table: typeof node.table === "string" ? node.table : resolveTableToken(node.table),
        };
      case "join":
        return {
          kind: "join",
          left: resolveNode(node.left),
          right: resolveNode(node.right),
          on: {
            kind: "eq",
            left: asRef(node.on.left),
            right: asRef(node.on.right),
          },
          type: node.type,
        };
      case "aggregate":
        return {
          kind: "aggregate",
          from: resolveNode(node.from),
          groupBy: Object.fromEntries(
            Object.entries(node.groupBy).map(([name, token]) => [name, asRef(token)]),
          ),
          measures: Object.fromEntries(
            Object.entries(node.measures).map(([name, metric]) => [
              name,
              metric.column
                ? {
                    ...metric,
                    column: asRef(metric.column),
                  }
                : metric,
            ]),
          ),
        };
    }
  };

  return resolveNode(definition as SchemaViewRelNodeInput);
}

export function parseColumnSource(ref: string): string {
  const idx = ref.lastIndexOf(".");
  return idx >= 0 ? ref.slice(idx + 1) : ref;
}

export function collectUnqualifiedExprColumns(expr: RelExpr): Set<string> {
  const out = new Set<string>();

  const visit = (current: RelExpr): void => {
    switch (current.kind) {
      case "literal":
      case "subquery":
        return;
      case "function":
        current.args.forEach(visit);
        return;
      case "column":
        if (!current.ref.table && !current.ref.alias) {
          out.add(current.ref.column);
        }
        return;
    }
  };

  visit(expr);
  return out;
}
