import type { RelExpr } from "@tupl/foundation";

import {
  isColumnLensDefinition,
  isSchemaCalculatedColumnDefinition,
  isSchemaColRefToken,
  isSchemaDataEntityHandle,
  isSchemaDslTableToken,
  isSchemaTypedColumnDefinition,
} from "./dsl-tokens";
import { assertColumnCompatibility, resolveEntityColumnSource } from "./entity-bindings";
import type {
  NormalizedColumnBinding,
  NormalizedPhysicalTableBinding,
  SchemaColRefToken,
  SchemaDataEntityHandle,
  SchemaDslTableToken,
  TableColumnDefinition,
} from "./types";
import {
  collectUnqualifiedExprColumns,
  parseColumnSource,
  resolveColRefToken,
  resolveEnumRef,
} from "./view-normalization";

/**
 * Binding normalization owns logical-column binding resolution and normalized source maps.
 */
export function getNormalizedColumnBindings(
  binding: Pick<
    | NormalizedPhysicalTableBinding
    | Extract<import("./types").NormalizedTableBinding, { kind: "view" }>,
    "columnBindings" | "columnToSource"
  >,
): Record<string, NormalizedColumnBinding> {
  if (binding.columnBindings && Object.keys(binding.columnBindings).length > 0) {
    return binding.columnBindings;
  }

  return Object.fromEntries(
    Object.entries(binding.columnToSource).map(([column, source]) => [
      column,
      { kind: "source", source },
    ]),
  );
}

export function getNormalizedColumnSourceMap(
  binding: Pick<
    | NormalizedPhysicalTableBinding
    | Extract<import("./types").NormalizedTableBinding, { kind: "view" }>,
    "columnBindings" | "columnToSource"
  >,
): Record<string, string> {
  const entries = Object.entries(getNormalizedColumnBindings(binding)).flatMap(
    ([column, columnBinding]) =>
      columnBinding.kind === "source" ? [[column, columnBinding] as const] : [],
  );
  return Object.fromEntries(
    entries.map(([column, columnBinding]) => [column, columnBinding.source]),
  );
}

export function resolveNormalizedColumnSource(
  binding: Pick<
    | NormalizedPhysicalTableBinding
    | Extract<import("./types").NormalizedTableBinding, { kind: "view" }>,
    "columnBindings" | "columnToSource"
  >,
  logicalColumn: string,
): string {
  const bindingByColumn = getNormalizedColumnBindings(binding)[logicalColumn];
  return bindingByColumn?.kind === "source" ? bindingByColumn.source : logicalColumn;
}

export function buildColumnSourceMapFromBindings(
  columnBindings: Record<string, NormalizedColumnBinding>,
): Record<string, string> {
  return Object.fromEntries(
    Object.entries(columnBindings).flatMap(([column, binding]) =>
      binding.kind === "source" ? [[column, binding.source] as const] : [],
    ),
  );
}

export function normalizeColumnBinding(
  columnName: string,
  rawColumn: unknown,
  options: {
    preserveQualifiedRef: boolean;
    resolveTableToken: (token: SchemaDslTableToken<string>) => string;
    resolveEntityToken: (entity: SchemaDataEntityHandle<string>) => string;
    entity?: SchemaDataEntityHandle<string>;
  },
): {
  definition: TableColumnDefinition;
  binding: NormalizedColumnBinding;
} {
  if (isSchemaCalculatedColumnDefinition(rawColumn)) {
    return {
      definition: rawColumn.definition,
      binding: {
        kind: "expr",
        expr: resolveColumnExpr(
          rawColumn.expr,
          options.resolveTableToken,
          options.resolveEntityToken,
        ),
        definition: rawColumn.definition,
        ...(rawColumn.coerce ? { coerce: rawColumn.coerce } : {}),
      },
    };
  }

  if (isSchemaTypedColumnDefinition(rawColumn)) {
    const source = options.entity
      ? resolveEntityColumnSource(rawColumn.sourceColumn, options.entity)
      : rawColumn.sourceColumn;
    assertColumnCompatibility(
      rawColumn.sourceColumn,
      rawColumn.definition,
      rawColumn.coerce,
      options.entity,
    );
    return {
      definition: rawColumn.definition,
      binding: {
        kind: "source",
        source,
        definition: rawColumn.definition,
        ...(rawColumn.coerce ? { coerce: rawColumn.coerce } : {}),
      },
    };
  }

  if (isSchemaColRefToken(rawColumn)) {
    const ref = resolveColRefToken(
      rawColumn,
      options.resolveTableToken,
      options.resolveEntityToken,
    );
    return {
      definition: "text",
      binding: {
        kind: "source",
        source: options.preserveQualifiedRef ? ref : parseColumnSource(ref),
        definition: "text",
      },
    };
  }

  if (isColumnLensDefinition(rawColumn)) {
    const sourceRef = isSchemaColRefToken(rawColumn.source)
      ? resolveColRefToken(rawColumn.source, options.resolveTableToken, options.resolveEntityToken)
      : rawColumn.source;
    const enumFromRef = rawColumn.enumFrom
      ? resolveEnumRef(rawColumn.enumFrom, options.resolveTableToken, options.resolveEntityToken)
      : undefined;

    const definition = {
      type: rawColumn.type ?? "text",
      ...(rawColumn.nullable != null ? { nullable: rawColumn.nullable } : {}),
      ...(rawColumn.primaryKey === true
        ? { primaryKey: true as const }
        : rawColumn.primaryKey === false
          ? { primaryKey: false as const }
          : {}),
      ...(rawColumn.unique === true
        ? { unique: true as const }
        : rawColumn.unique === false
          ? { unique: false as const }
          : {}),
      ...(rawColumn.enum ? { enum: rawColumn.enum } : {}),
      ...(enumFromRef ? { enumFrom: enumFromRef } : {}),
      ...(rawColumn.enumMap ? { enumMap: rawColumn.enumMap } : {}),
      ...(rawColumn.physicalType ? { physicalType: rawColumn.physicalType } : {}),
      ...(rawColumn.physicalDialect ? { physicalDialect: rawColumn.physicalDialect } : {}),
      ...(rawColumn.foreignKey ? { foreignKey: rawColumn.foreignKey } : {}),
      ...(rawColumn.description ? { description: rawColumn.description } : {}),
    } as TableColumnDefinition;

    return {
      definition,
      binding: {
        kind: "source",
        source: options.preserveQualifiedRef ? sourceRef : parseColumnSource(sourceRef),
        definition,
        ...(rawColumn.coerce ? { coerce: rawColumn.coerce } : {}),
      },
    };
  }

  if (typeof rawColumn !== "string") {
    const definitionInput = rawColumn as Exclude<TableColumnDefinition, string> & {
      enumFrom?: SchemaColRefToken | string;
    };
    const enumFromRef = definitionInput.enumFrom
      ? resolveEnumRef(
          definitionInput.enumFrom,
          options.resolveTableToken,
          options.resolveEntityToken,
        )
      : undefined;
    const definition = {
      ...definitionInput,
      ...(enumFromRef ? { enumFrom: enumFromRef } : {}),
    } satisfies TableColumnDefinition;
    return {
      definition,
      binding: {
        kind: "source",
        source: columnName,
        definition,
      },
    };
  }

  return {
    definition: rawColumn as TableColumnDefinition,
    binding: {
      kind: "source",
      source: columnName,
      definition: rawColumn as TableColumnDefinition,
    },
  };
}

export function resolveColumnExpr(
  expr: RelExpr,
  resolveTableToken: (token: SchemaDslTableToken<string>) => string,
  resolveEntityToken: (entity: SchemaDataEntityHandle<string>) => string,
): RelExpr {
  switch (expr.kind) {
    case "literal":
      return expr;
    case "function":
      return {
        kind: "function",
        name: expr.name,
        args: expr.args.map((arg) => resolveColumnExpr(arg, resolveTableToken, resolveEntityToken)),
      };
    case "column": {
      const tableOrAlias = (expr.ref as { table?: unknown; alias?: unknown }).table;
      if (isSchemaDslTableToken(tableOrAlias)) {
        return {
          kind: "column",
          ref: {
            table: resolveTableToken(tableOrAlias),
            column: expr.ref.column,
          },
        };
      }
      if (isSchemaDataEntityHandle(tableOrAlias)) {
        return {
          kind: "column",
          ref: {
            table: resolveEntityToken(tableOrAlias),
            column: expr.ref.column,
          },
        };
      }
      return expr;
    }
    case "subquery":
      return expr;
  }
}

export function validateCalculatedColumnDependencies(
  tableName: string,
  columnBindings: Record<string, NormalizedColumnBinding>,
): void {
  const exprColumns = new Set(
    Object.entries(columnBindings)
      .filter(([, binding]) => binding.kind === "expr")
      .map(([column]) => column),
  );

  for (const [columnName, binding] of Object.entries(columnBindings)) {
    if (binding.kind !== "expr") {
      continue;
    }

    for (const dependency of collectUnqualifiedExprColumns(binding.expr)) {
      if (!exprColumns.has(dependency)) {
        continue;
      }
      throw new Error(
        `Calculated column ${tableName}.${columnName} cannot reference calculated sibling ${tableName}.${dependency} in the same columns block.`,
      );
    }
  }
}
