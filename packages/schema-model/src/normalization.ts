import { Result } from "better-result";

import type { DataEntityColumnMetadata, RelExpr } from "@tupl/foundation";
import { TuplProviderBindingError } from "@tupl/foundation";
import type { ProvidersMap } from "@tupl/provider-kit";
import { getDataEntityAdapter } from "@tupl/provider-kit";

import {
  buildSchemaDslViewRelHelpers,
  isColumnLensDefinition,
  isDslTableDefinition,
  isDslViewDefinition,
  isSchemaCalculatedColumnDefinition,
  isSchemaColRefToken,
  isSchemaDataEntityHandle,
  isSchemaDslTableToken,
  isSchemaTypedColumnDefinition,
  type SchemaBuilderState,
} from "./builder";
import { validateSchemaConstraints } from "./constraints";
import { resolveColumnDefinition } from "./definition";
import type {
  NormalizedColumnBinding,
  NormalizedPhysicalTableBinding,
  NormalizedSourceColumnBinding,
  NormalizedTableBinding,
  SchemaColRefToken,
  SchemaDataEntityHandle,
  SchemaDefinition,
  SchemaDslTableToken,
  SchemaValueCoercion,
  SchemaViewRelNode,
  SchemaViewRelNodeInput,
  TableColumnDefinition,
  TableColumns,
  TableDefinition,
} from "./types";

/**
 * Normalization owns schema finalization, normalized bindings, and provider-binding resolution.
 * Callers should rely on the normalized schema contract, not on the backing normalization state.
 */
const normalizedSchemaState = new WeakMap<
  SchemaDefinition,
  {
    tables: Record<string, NormalizedTableBinding>;
  }
>();

export function copyNormalizedSchemaBindings(from: SchemaDefinition, to: SchemaDefinition): void {
  const existingBindings = normalizedSchemaState.get(from);
  if (!existingBindings) {
    return;
  }

  normalizedSchemaState.set(to, {
    tables: { ...existingBindings.tables },
  });
}

export function finalizeSchemaDefinition<TSchema extends SchemaDefinition>(
  schema: TSchema,
): TSchema {
  validateNormalizedTableBindings(schema);
  validateTableProviders(schema);
  validateSchemaConstraints(schema);
  return schema;
}

export function getNormalizedTableBinding(
  schema: SchemaDefinition,
  tableName: string,
): NormalizedTableBinding | undefined {
  return normalizedSchemaState.get(schema)?.tables[tableName];
}

export function getNormalizedColumnBindings(
  binding: Pick<
    NormalizedPhysicalTableBinding | Extract<NormalizedTableBinding, { kind: "view" }>,
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
    NormalizedPhysicalTableBinding | Extract<NormalizedTableBinding, { kind: "view" }>,
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
    NormalizedPhysicalTableBinding | Extract<NormalizedTableBinding, { kind: "view" }>,
    "columnBindings" | "columnToSource"
  >,
  logicalColumn: string,
): string {
  const bindingByColumn = getNormalizedColumnBindings(binding)[logicalColumn];
  return bindingByColumn?.kind === "source" ? bindingByColumn.source : logicalColumn;
}

function buildColumnSourceMapFromBindings(
  columnBindings: Record<string, NormalizedColumnBinding>,
): Record<string, string> {
  return Object.fromEntries(
    Object.entries(columnBindings).flatMap(([column, binding]) =>
      binding.kind === "source" ? [[column, binding.source] as const] : [],
    ),
  );
}

export function buildRegisteredSchemaDefinition<TContext>(
  state: SchemaBuilderState<TContext>,
): SchemaDefinition {
  const tables: Record<string, TableDefinition> = {};
  const bindings: Record<string, NormalizedTableBinding> = {};
  const tableTokenToName = new Map<symbol, string>();
  const entries = [...state.definitions.entries()];

  for (const [tableName, rawTable] of entries) {
    if (isDslTableDefinition(rawTable) || isDslViewDefinition(rawTable)) {
      tableTokenToName.set(rawTable.tableToken.__id, tableName);
    }
  }

  const resolveTableToken = (token: SchemaDslTableToken<string>): string => {
    const tableName = tableTokenToName.get(token.__id);
    if (!tableName) {
      throw new Error("Schema DSL table token could not be resolved to a table name.");
    }
    return tableName;
  };
  const resolveEntityToken = (entity: SchemaDataEntityHandle<string>): string => {
    if (!entity.entity || entity.entity.length === 0) {
      throw new Error("Schema DSL data entity handle is missing entity name.");
    }
    return entity.entity;
  };
  const viewRelHelpers = buildSchemaDslViewRelHelpers();

  for (const [tableName, rawTable] of entries) {
    if (isDslTableDefinition(rawTable)) {
      const normalizedColumns: TableColumns = {};
      const columnBindings: Record<string, NormalizedColumnBinding> = {};
      for (const [columnName, rawColumn] of Object.entries(rawTable.columns)) {
        const normalized = normalizeColumnBinding(columnName, rawColumn, {
          preserveQualifiedRef: false,
          resolveTableToken,
          resolveEntityToken,
          entity: rawTable.from,
        });
        normalizedColumns[columnName] = normalized.definition;
        columnBindings[columnName] = normalized.binding;
      }
      validateCalculatedColumnDependencies(tableName, columnBindings);

      tables[tableName] = {
        provider: rawTable.from.provider,
        columns: normalizedColumns,
        ...(rawTable.constraints ? { constraints: rawTable.constraints } : {}),
      };
      const adapter = getDataEntityAdapter(rawTable.from);

      bindings[tableName] = {
        kind: "physical",
        provider: rawTable.from.provider,
        entity: rawTable.from.entity,
        columnBindings,
        columnToSource: buildColumnSourceMapFromBindings(columnBindings),
        ...(adapter ? { adapter } : {}),
      };
      continue;
    }

    if (isDslViewDefinition(rawTable)) {
      const normalizedColumns: TableColumns = {};
      const columnBindings: Record<string, NormalizedColumnBinding> = {};
      for (const [columnName, rawColumn] of Object.entries(rawTable.columns)) {
        const normalized = normalizeColumnBinding(columnName, rawColumn, {
          preserveQualifiedRef: true,
          resolveTableToken,
          resolveEntityToken,
        });
        normalizedColumns[columnName] = normalized.definition;
        columnBindings[columnName] = normalized.binding;
      }
      validateCalculatedColumnDependencies(tableName, columnBindings);

      tables[tableName] = {
        provider: "__view__",
        columns: normalizedColumns,
        ...(rawTable.constraints ? { constraints: rawTable.constraints } : {}),
      };

      bindings[tableName] = {
        kind: "view",
        rel: (context: unknown) => {
          const definition = rawTable.rel(context as TContext, viewRelHelpers);
          return resolveViewRelDefinition(definition, resolveTableToken, resolveEntityToken);
        },
        columnBindings,
        columnToSource: buildColumnSourceMapFromBindings(columnBindings),
      };
      continue;
    }

    tables[tableName] = rawTable as never;
  }

  const schema: SchemaDefinition = { tables };
  normalizedSchemaState.set(schema, { tables: bindings });
  return finalizeSchemaDefinition(schema);
}

function normalizeColumnBinding(
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

function resolveColumnExpr(
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

function validateCalculatedColumnDependencies(
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

function resolveColRefToken(
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

function resolveEnumRef(
  enumFrom: SchemaColRefToken | string,
  resolveTableToken: (token: SchemaDslTableToken<string>) => string,
  resolveEntityToken: (entity: SchemaDataEntityHandle<string>) => string,
): string {
  if (typeof enumFrom === "string") {
    return enumFrom;
  }

  return resolveColRefToken(enumFrom, resolveTableToken, resolveEntityToken);
}

function resolveViewRelDefinition(
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

function parseColumnSource(ref: string): string {
  const idx = ref.lastIndexOf(".");
  return idx >= 0 ? ref.slice(idx + 1) : ref;
}

function collectUnqualifiedExprColumns(expr: RelExpr): Set<string> {
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

function resolveEntityColumnSource(column: string, entity: SchemaDataEntityHandle<string>): string {
  return entity.columns?.[column]?.source ?? column;
}

export function createTableDefinitionFromEntity(
  entity: SchemaDataEntityHandle<string>,
): TableDefinition {
  const columns = entity.columns
    ? Object.fromEntries(
        Object.entries(entity.columns).map(([columnName, metadata]) => [
          columnName,
          buildEntityColumnDefinition(metadata),
        ]),
      )
    : {};

  return {
    provider: entity.provider,
    columns,
  };
}

export function createPhysicalBindingFromEntity(
  entity: SchemaDataEntityHandle<string>,
): NormalizedPhysicalTableBinding {
  const tableDefinition = createTableDefinitionFromEntity(entity);
  const adapter = getDataEntityAdapter(entity);
  return {
    kind: "physical",
    provider: entity.provider,
    entity: entity.entity,
    columnBindings: Object.fromEntries(
      Object.entries(tableDefinition.columns).map(([columnName, definition]) => [
        columnName,
        {
          kind: "source",
          source: resolveEntityColumnSource(columnName, entity),
          definition,
        } satisfies NormalizedSourceColumnBinding,
      ]),
    ),
    columnToSource: Object.fromEntries(
      Object.keys(tableDefinition.columns).map((columnName) => [
        columnName,
        resolveEntityColumnSource(columnName, entity),
      ]),
    ),
    ...(adapter ? { adapter } : {}),
  };
}

function buildEntityColumnDefinition(
  metadata: DataEntityColumnMetadata<any>,
): TableColumnDefinition {
  const base = {
    type: metadata.type ?? "text",
    ...(metadata.nullable != null ? { nullable: metadata.nullable } : {}),
    ...(metadata.enum ? { enum: metadata.enum } : {}),
    ...(metadata.physicalType ? { physicalType: metadata.physicalType } : {}),
    ...(metadata.physicalDialect ? { physicalDialect: metadata.physicalDialect } : {}),
  };

  if (metadata.primaryKey) {
    return {
      ...base,
      primaryKey: true,
    } satisfies TableColumnDefinition;
  }

  if (metadata.unique) {
    return {
      ...base,
      unique: true,
    } satisfies TableColumnDefinition;
  }

  return base satisfies TableColumnDefinition;
}

function sourceTypeMatchesTargetType(
  sourceType: TableColumnDefinition extends infer _ ? string | undefined : never,
  targetType: string,
): boolean {
  if (!sourceType) {
    return true;
  }
  switch (targetType) {
    case "real":
      return sourceType === "real" || sourceType === "integer";
    default:
      return sourceType === targetType;
  }
}

function assertColumnCompatibility(
  logicalColumn: string,
  definition: TableColumnDefinition,
  coerce: SchemaValueCoercion | undefined,
  entity: SchemaDataEntityHandle<string> | undefined,
): void {
  if (!entity || coerce) {
    return;
  }

  const sourceMetadata = entity.columns?.[logicalColumn];
  if (!sourceMetadata?.type) {
    return;
  }

  const targetType = resolveColumnDefinition(definition).type;
  if (!sourceTypeMatchesTargetType(sourceMetadata.type, targetType)) {
    throw new Error(
      `Column ${entity.entity}.${sourceMetadata.source} is exposed as ${sourceMetadata.type}, but the schema declared ${targetType}. Add a coerce function or align the declared type.`,
    );
  }
}

function validateTableProviders(schema: SchemaDefinition): void {
  for (const [tableName, table] of Object.entries(schema.tables)) {
    const normalized = getNormalizedTableBinding(schema, tableName);
    if (normalized?.kind === "view") {
      continue;
    }

    if (table.provider == null) {
      continue;
    }

    if (typeof table.provider !== "string" || table.provider.trim().length === 0) {
      throw new Error(
        `Table ${tableName} must define a non-empty provider binding (table.provider).`,
      );
    }
  }
}

function validateNormalizedTableBindings(schema: SchemaDefinition): void {
  const normalized = normalizedSchemaState.get(schema);
  if (!normalized) {
    throw new Error(
      "Physical tables must be declared via createSchemaBuilder().table(name, provider.entities.someTable, config).",
    );
  }

  for (const [tableName, table] of Object.entries(schema.tables)) {
    const binding = normalized.tables[tableName];
    if (!binding) {
      throw new Error(
        `Table ${tableName} must be declared via createSchemaBuilder().table(name, provider.entities.someTable, config).`,
      );
    }

    if (binding.kind === "view") {
      continue;
    }

    if (typeof binding.entity !== "string" || binding.entity.trim().length === 0) {
      throw new Error(`Table ${tableName} is missing an entity-backed physical binding.`);
    }

    if (typeof binding.provider !== "string" || binding.provider.trim().length === 0) {
      throw new Error(`Table ${tableName} is missing a provider-backed physical binding.`);
    }

    if (table.provider !== binding.provider) {
      throw new Error(
        `Table ${tableName} must define provider ${binding.provider} to match its entity-backed physical binding.`,
      );
    }
  }
}

export function resolveTableProvider(schema: SchemaDefinition, table: string): string {
  const result = resolveTableProviderResult(schema, table);
  if (Result.isError(result)) {
    throw result.error;
  }

  return result.value;
}

export function resolveTableProviderResult(schema: SchemaDefinition, table: string) {
  const normalized = getNormalizedTableBinding(schema, table);
  if (normalized?.kind === "physical" && normalized.provider) {
    return Result.ok(normalized.provider);
  }

  if (normalized?.kind === "view") {
    return Result.err(
      new TuplProviderBindingError({
        table,
        message: `View table ${table} does not have a direct provider binding.`,
      }),
    );
  }

  const tableDefinition = schema.tables[table];
  if (!tableDefinition) {
    return Result.err(
      new TuplProviderBindingError({
        table,
        message: `Unknown table: ${table}`,
      }),
    );
  }

  if (!tableDefinition.provider || tableDefinition.provider.length === 0) {
    return Result.err(
      new TuplProviderBindingError({
        table,
        message: `Table ${table} is missing required provider mapping.`,
      }),
    );
  }

  return Result.ok(tableDefinition.provider);
}

export function validateProviderBindings<TContext>(
  schema: SchemaDefinition,
  providers: ProvidersMap<TContext>,
): void {
  const result = validateProviderBindingsResult(schema, providers);
  if (Result.isError(result)) {
    throw result.error;
  }
}

export function validateProviderBindingsResult<TContext>(
  schema: SchemaDefinition,
  providers: ProvidersMap<TContext>,
) {
  for (const tableName of Object.keys(schema.tables)) {
    const normalized = getNormalizedTableBinding(schema, tableName);
    if (normalized?.kind === "view") {
      continue;
    }

    const providerNameResult =
      normalized?.kind === "physical" && normalized.provider
        ? Result.ok(normalized.provider)
        : resolveTableProviderResult(schema, tableName);
    if (Result.isError(providerNameResult)) {
      return providerNameResult;
    }

    const providerName = providerNameResult.value;
    if (!providers[providerName]) {
      return Result.err(
        new TuplProviderBindingError({
          table: tableName,
          provider: providerName,
          message: `Table ${tableName} is bound to provider ${providerName}, but no such provider is registered.`,
        }),
      );
    }
  }

  return Result.ok(undefined);
}
