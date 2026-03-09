import type { DataEntityColumnMetadata } from "../model/data-entity";
import type { RelColumnRef, RelExpr, RelNode } from "../model/rel";
import { getDataEntityAdapter } from "../provider";
import type { SchemaViewRelNode } from "./dsl";
import type {
  QueryRow,
  SchemaDataEntityHandle,
  SchemaDefinition,
  SchemaValueCoercion,
  SqlScalarType,
  TableColumnDefinition,
  TableColumns,
  TableDefinition,
} from "./definition";
import { resolveColumnDefinition } from "./definition";

export interface NormalizedPhysicalTableBinding {
  kind: "physical";
  provider?: string;
  entity: string;
  columnBindings: Record<string, NormalizedColumnBinding>;
  columnToSource: Record<string, string>;
  adapter?: unknown;
}

export interface NormalizedViewTableBinding<TContext = unknown> {
  kind: "view";
  rel: (context: TContext) => SchemaViewRelNode | unknown;
  columnBindings: Record<string, NormalizedColumnBinding>;
  columnToSource: Record<string, string>;
}

export interface NormalizedSourceColumnBinding {
  kind: "source";
  source: string;
  definition?: TableColumnDefinition;
  coerce?: SchemaValueCoercion;
}

export interface NormalizedCalculatedColumnBinding {
  kind: "expr";
  expr: RelExpr;
  definition?: TableColumnDefinition;
  coerce?: SchemaValueCoercion;
}

export type NormalizedColumnBinding =
  | NormalizedSourceColumnBinding
  | NormalizedCalculatedColumnBinding;

export type NormalizedTableBinding<TContext = unknown> =
  | NormalizedPhysicalTableBinding
  | NormalizedViewTableBinding<TContext>;

interface SchemaNormalizationState {
  tables: Record<string, NormalizedTableBinding>;
}

const normalizedSchemaState = new WeakMap<SchemaDefinition, SchemaNormalizationState>();

export function registerNormalizedSchema(
  schema: SchemaDefinition,
  tables: Record<string, NormalizedTableBinding>,
): void {
  normalizedSchemaState.set(schema, { tables });
}

export function getNormalizedTableBinding(
  schema: SchemaDefinition,
  tableName: string,
): NormalizedTableBinding | undefined {
  return normalizedSchemaState.get(schema)?.tables[tableName];
}

export function getNormalizedColumnBindings(
  binding: Pick<
    NormalizedPhysicalTableBinding | NormalizedViewTableBinding,
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
    NormalizedPhysicalTableBinding | NormalizedViewTableBinding,
    "columnBindings" | "columnToSource"
  >,
): Record<string, string> {
  const entries = Object.entries(getNormalizedColumnBindings(binding)).flatMap(
    ([column, columnBinding]) =>
      isNormalizedSourceColumnBinding(columnBinding) ? [[column, columnBinding] as const] : [],
  );
  return Object.fromEntries(
    entries.map(([column, columnBinding]) => [column, columnBinding.source]),
  );
}

export function resolveNormalizedColumnSource(
  binding: Pick<
    NormalizedPhysicalTableBinding | NormalizedViewTableBinding,
    "columnBindings" | "columnToSource"
  >,
  logicalColumn: string,
): string {
  const bindingByColumn = getNormalizedColumnBindings(binding)[logicalColumn];
  return isNormalizedSourceColumnBinding(bindingByColumn) ? bindingByColumn.source : logicalColumn;
}

export function coerceValue(value: unknown, coerce: SchemaValueCoercion): unknown {
  if (typeof coerce === "function") {
    return coerce(value);
  }

  switch (coerce) {
    case "isoTimestamp":
      if (value == null) {
        return value;
      }
      if (value instanceof Date) {
        return value.toISOString();
      }
      if (typeof value === "string") {
        return value;
      }
      throw new Error(`Built-in coercion "${coerce}" only supports Date or string values.`);
  }
}

export function normalizeProviderRowValue(
  value: unknown,
  binding: NormalizedColumnBinding | undefined,
  fallbackDefinition?: TableColumnDefinition,
  options: {
    enforceNotNull?: boolean;
    enforceEnum?: boolean;
  } = {},
): unknown {
  if (!binding) {
    return value;
  }

  const definition = resolveColumnDefinition(binding.definition ?? fallbackDefinition ?? "text");
  const coerced = binding.coerce ? coerceValue(value, binding.coerce) : value;
  const enforceNotNull = options.enforceNotNull ?? true;
  const enforceEnum = options.enforceEnum ?? true;

  if (coerced == null) {
    if (enforceNotNull && definition.nullable === false) {
      throw new Error(
        `Column ${describeNormalizedColumnBinding(binding)} is non-nullable but provider returned null.`,
      );
    }
    return null;
  }

  switch (definition.type) {
    case "text":
      if (typeof coerced !== "string") {
        throw new Error(`Column ${describeNormalizedColumnBinding(binding)} must be a string.`);
      }
      if (enforceEnum && definition.enum && !definition.enum.includes(coerced)) {
        throw new Error(
          `Column ${describeNormalizedColumnBinding(binding)} must be one of ${definition.enum.join(", ")}.`,
        );
      }
      return coerced;
    case "integer":
      if (typeof coerced !== "number" || !Number.isFinite(coerced) || !Number.isInteger(coerced)) {
        throw new Error(`Column ${describeNormalizedColumnBinding(binding)} must be an integer.`);
      }
      return coerced;
    case "real":
      if (typeof coerced !== "number" || !Number.isFinite(coerced)) {
        throw new Error(
          `Column ${describeNormalizedColumnBinding(binding)} must be a finite number.`,
        );
      }
      return coerced;
    case "blob":
      if (!(coerced instanceof Uint8Array)) {
        throw new Error(`Column ${describeNormalizedColumnBinding(binding)} must be a Uint8Array.`);
      }
      return coerced;
    case "boolean":
      if (typeof coerced !== "boolean") {
        throw new Error(`Column ${describeNormalizedColumnBinding(binding)} must be a boolean.`);
      }
      return coerced;
    case "timestamp":
    case "date":
    case "datetime":
      if (!(typeof coerced === "string" || coerced instanceof Date)) {
        throw new Error(
          `Column ${describeNormalizedColumnBinding(binding)} must be a ${definition.type} string or Date.`,
        );
      }
      return coerced instanceof Date ? coerced.toISOString() : coerced;
    case "json":
      return coerced;
  }
}

export function mapProviderRowsToLogical(
  rows: QueryRow[],
  selectedLogicalColumns: string[],
  binding: NormalizedPhysicalTableBinding | null,
  tableDefinition?: TableDefinition,
  options: {
    enforceNotNull?: boolean;
    enforceEnum?: boolean;
  } = {},
): QueryRow[] {
  if (!binding) {
    return rows;
  }

  return rows.map((row) => {
    const out: QueryRow = {};
    for (const logical of selectedLogicalColumns) {
      const columnBinding = getNormalizedColumnBindings(binding)[logical];
      const source = isNormalizedSourceColumnBinding(columnBinding)
        ? columnBinding.source
        : logical;
      const fallbackDefinition = tableDefinition?.columns[logical];
      out[logical] = normalizeProviderRowValue(
        row[source] ?? null,
        columnBinding,
        fallbackDefinition,
        options,
      );
    }
    return out;
  });
}

export function mapProviderRowsToRelOutput(
  rows: QueryRow[],
  rel: RelNode,
  schema: SchemaDefinition,
): QueryRow[] {
  if (rel.output.length === 0) {
    return rows;
  }

  const outputDefinitions = inferRelOutputDefinitions(rel, schema);
  return rows.map((row) => {
    const out: QueryRow = {};
    for (const output of rel.output) {
      out[output.name] = normalizeProviderRelOutputValue(
        row[output.name] ?? null,
        output.name,
        outputDefinitions[output.name],
      );
    }
    return out;
  });
}

export function isNormalizedSourceColumnBinding(
  binding: NormalizedColumnBinding | undefined,
): binding is NormalizedSourceColumnBinding {
  return !!binding && binding.kind === "source";
}

function describeNormalizedColumnBinding(binding: NormalizedColumnBinding): string {
  return binding.kind === "source" ? binding.source : "<expr>";
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

export interface EnumLinkReference {
  table: string;
  column: string;
}

export interface ResolveSchemaLinkedEnumsOptions {
  resolveEnumValues?: (
    ref: EnumLinkReference,
    schema: SchemaDefinition,
  ) => readonly string[] | undefined;
  onUnresolved?: "throw" | "ignore";
  strictUnmapped?: boolean;
}

export function resolveSchemaLinkedEnums(
  schema: SchemaDefinition,
  options: ResolveSchemaLinkedEnumsOptions = {},
): SchemaDefinition {
  const resolveEnumValues = options.resolveEnumValues ?? defaultResolveLinkedEnumValues;
  const onUnresolved = options.onUnresolved ?? "throw";
  const strictUnmapped = options.strictUnmapped ?? true;

  let changed = false;
  const tables: Record<string, TableDefinition> = {};

  for (const [tableName, table] of Object.entries(schema.tables)) {
    const columns: TableColumns = {};

    for (const [columnName, columnDefinition] of Object.entries(table.columns)) {
      if (typeof columnDefinition === "string") {
        columns[columnName] = columnDefinition;
        continue;
      }

      const resolved = resolveColumnDefinition(columnDefinition);
      if (!resolved.enumFrom) {
        columns[columnName] = columnDefinition;
        continue;
      }

      const ref = parseEnumLinkReference(resolved.enumFrom, tableName, columnName);
      const upstreamEnum = resolveEnumValues(ref, schema);
      if (!upstreamEnum || upstreamEnum.length === 0) {
        if (onUnresolved === "throw") {
          throw new Error(
            `Unable to resolve enumFrom for ${tableName}.${columnName} from ${ref.table}.${ref.column}.`,
          );
        }
        columns[columnName] = columnDefinition;
        continue;
      }

      const mappedValues: string[] = [];
      const seenMappedValues = new Set<string>();
      for (const upstreamValue of upstreamEnum) {
        if (resolved.enumMap) {
          const mapped = resolved.enumMap[upstreamValue];
          if (!mapped) {
            if (strictUnmapped) {
              throw new Error(
                `Unmapped enumFrom value "${upstreamValue}" for ${tableName}.${columnName}.`,
              );
            }
            continue;
          }
          if (!seenMappedValues.has(mapped)) {
            mappedValues.push(mapped);
            seenMappedValues.add(mapped);
          }
          continue;
        }
        if (!seenMappedValues.has(upstreamValue)) {
          mappedValues.push(upstreamValue);
          seenMappedValues.add(upstreamValue);
        }
      }

      columns[columnName] = {
        ...columnDefinition,
        enum: mappedValues,
        enumFrom: resolved.enumFrom,
      };
      changed = true;
    }

    tables[tableName] = {
      ...table,
      columns,
    };
  }

  if (!changed) {
    return schema;
  }

  const nextSchema: SchemaDefinition = { tables };
  const normalized = normalizedSchemaState.get(schema);
  if (normalized) {
    registerNormalizedSchema(nextSchema, normalized.tables);
  }
  return nextSchema;
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

function resolveEntityColumnSource(column: string, entity: SchemaDataEntityHandle<string>): string {
  return entity.columns?.[column]?.source ?? column;
}

function inferRelOutputDefinitions(
  rel: RelNode,
  schema: SchemaDefinition,
  cteDefinitions: Map<string, Record<string, TableColumnDefinition | undefined>> = new Map(),
): Record<string, TableColumnDefinition | undefined> {
  switch (rel.kind) {
    case "scan":
      return inferScanOutputDefinitions(rel, schema, cteDefinitions);
    case "filter":
      return inferRelOutputDefinitions(rel.input, schema, cteDefinitions);
    case "project": {
      const inputDefinitions = inferRelOutputDefinitions(rel.input, schema, cteDefinitions);
      return Object.fromEntries(
        rel.columns.map((column) => [
          column.output,
          isProjectExpr(column)
            ? inferRelExprDefinition(column.expr, inputDefinitions)
            : resolveRelRefOutputDefinition(inputDefinitions, column.source),
        ]),
      );
    }
    case "join": {
      const leftDefinitions = inferRelOutputDefinitions(rel.left, schema, cteDefinitions);
      const rightDefinitions = inferRelOutputDefinitions(rel.right, schema, cteDefinitions);
      return {
        ...applyJoinNullability(
          leftDefinitions,
          rel.joinType === "right" || rel.joinType === "full",
        ),
        ...applyJoinNullability(
          rightDefinitions,
          rel.joinType === "left" || rel.joinType === "full",
        ),
      };
    }
    case "aggregate": {
      const inputDefinitions = inferRelOutputDefinitions(rel.input, schema, cteDefinitions);
      const out: Record<string, TableColumnDefinition | undefined> = {};

      for (let index = 0; index < rel.groupBy.length; index += 1) {
        const groupRef = rel.groupBy[index];
        const output = rel.output[index];
        if (!groupRef || !output) {
          continue;
        }
        out[output.name] = resolveRelRefOutputDefinition(inputDefinitions, groupRef);
      }

      for (let index = 0; index < rel.metrics.length; index += 1) {
        const metric = rel.metrics[index];
        const output = rel.output[rel.groupBy.length + index];
        if (!metric || !output) {
          continue;
        }
        out[output.name] = inferAggregateMetricDefinition(metric, inputDefinitions);
      }

      return out;
    }
    case "window": {
      const out = {
        ...inferRelOutputDefinitions(rel.input, schema, cteDefinitions),
      };
      for (const fn of rel.functions) {
        out[fn.as] = buildInferredColumnDefinition("integer", false);
      }
      return out;
    }
    case "sort":
    case "limit_offset":
      return inferRelOutputDefinitions(rel.input, schema, cteDefinitions);
    case "set_op": {
      const leftDefinitions = inferRelOutputDefinitions(rel.left, schema, cteDefinitions);
      const rightDefinitions = inferRelOutputDefinitions(rel.right, schema, cteDefinitions);
      const out: Record<string, TableColumnDefinition | undefined> = {};
      for (let index = 0; index < rel.output.length; index += 1) {
        const output = rel.output[index];
        const leftOutput = rel.left.output[index];
        const rightOutput = rel.right.output[index];
        if (!output) {
          continue;
        }
        out[output.name] =
          (leftOutput && leftDefinitions[leftOutput.name]) ||
          (rightOutput && rightDefinitions[rightOutput.name]);
      }
      return out;
    }
    case "with": {
      const nextCtes = new Map(cteDefinitions);
      for (const cte of rel.ctes) {
        nextCtes.set(cte.name, inferRelOutputDefinitions(cte.query, schema, nextCtes));
      }
      return inferRelOutputDefinitions(rel.body, schema, nextCtes);
    }
    case "sql":
      return {};
  }
}

function inferScanOutputDefinitions(
  rel: Extract<RelNode, { kind: "scan" }>,
  schema: SchemaDefinition,
  cteDefinitions: Map<string, Record<string, TableColumnDefinition | undefined>>,
): Record<string, TableColumnDefinition | undefined> {
  const cteDefinition = cteDefinitions.get(rel.table);
  if (cteDefinition) {
    return Object.fromEntries(
      rel.output.map((output, index) => [
        output.name,
        cteDefinition[rel.select[index] ?? output.name],
      ]),
    );
  }

  const table = schema.tables[rel.table];
  if (!table && rel.entity) {
    const entityTable = createTableDefinitionFromEntity(rel.entity);
    return Object.fromEntries(
      rel.output.map((output, index) => {
        const selected = rel.select[index] ?? output.name;
        const logicalColumn = selected.includes(".")
          ? selected.slice(selected.lastIndexOf(".") + 1)
          : selected;
        return [output.name, entityTable.columns[logicalColumn]];
      }),
    );
  }
  if (!table) {
    return {};
  }

  return Object.fromEntries(
    rel.output.map((output, index) => {
      const selected = rel.select[index] ?? output.name;
      const logicalColumn = selected.includes(".")
        ? selected.slice(selected.lastIndexOf(".") + 1)
        : selected;
      return [output.name, table.columns[logicalColumn]];
    }),
  );
}

function inferAggregateMetricDefinition(
  metric: Extract<RelNode, { kind: "aggregate" }>["metrics"][number],
  inputDefinitions: Record<string, TableColumnDefinition | undefined>,
): TableColumnDefinition | undefined {
  switch (metric.fn) {
    case "count":
      return buildInferredColumnDefinition("integer", false);
    case "avg":
      return buildInferredColumnDefinition("real", true);
    case "sum": {
      const sourceType = metric.column
        ? resolveColumnDefinition(
            resolveRelRefOutputDefinition(inputDefinitions, metric.column) ??
              buildInferredColumnDefinition("real", true),
          ).type
        : "real";
      return buildInferredColumnDefinition(sourceType === "integer" ? "integer" : "real", true);
    }
    case "min":
    case "max": {
      const sourceDefinition = metric.column
        ? resolveRelRefOutputDefinition(inputDefinitions, metric.column)
        : undefined;
      return sourceDefinition ? withColumnNullability(sourceDefinition, true) : undefined;
    }
  }
}

function inferRelExprDefinition(
  expr: RelExpr,
  inputDefinitions: Record<string, TableColumnDefinition | undefined>,
): TableColumnDefinition | undefined {
  switch (expr.kind) {
    case "literal":
      return inferLiteralDefinition(expr.value);
    case "column":
      return resolveRelRefOutputDefinition(inputDefinitions, expr.ref);
    case "subquery":
      return expr.mode === "exists" ? buildInferredColumnDefinition("boolean", false) : undefined;
    case "function": {
      const args = expr.args.map((arg) => inferRelExprDefinition(arg, inputDefinitions));
      switch (expr.name) {
        case "eq":
        case "neq":
        case "gt":
        case "gte":
        case "lt":
        case "lte":
        case "and":
        case "or":
        case "not":
        case "like":
        case "not_like":
        case "in":
        case "not_in":
        case "is_null":
        case "is_not_null":
        case "is_distinct_from":
        case "is_not_distinct_from":
        case "between":
          return buildInferredColumnDefinition("boolean", true);
        case "add":
        case "subtract":
        case "multiply":
        case "mod":
        case "abs":
        case "round":
          return buildInferredColumnDefinition(resolveNumericExprType(args), true);
        case "divide":
          return buildInferredColumnDefinition("real", true);
        case "concat":
        case "lower":
        case "upper":
        case "trim":
        case "substr":
          return buildInferredColumnDefinition("text", true);
        case "length":
          return buildInferredColumnDefinition("integer", true);
        case "coalesce":
          return args.find((definition) => definition != null);
        case "nullif":
          return args[0] ? withColumnNullability(args[0], true) : undefined;
        case "case":
          return args.find((_, index) => index % 2 === 1);
        case "cast": {
          const target = expr.args[1];
          if (target?.kind !== "literal" || typeof target.value !== "string") {
            return undefined;
          }
          switch (target.value.toLowerCase()) {
            case "integer":
            case "int":
              return buildInferredColumnDefinition("integer", true);
            case "real":
            case "numeric":
            case "float":
              return buildInferredColumnDefinition("real", true);
            case "boolean":
              return buildInferredColumnDefinition("boolean", true);
            case "text":
              return buildInferredColumnDefinition("text", true);
            default:
              return undefined;
          }
        }
        default:
          return undefined;
      }
    }
  }
}

function inferLiteralDefinition(
  value: string | number | boolean | null,
): TableColumnDefinition | undefined {
  if (value == null) {
    return undefined;
  }
  switch (typeof value) {
    case "string":
      return buildInferredColumnDefinition("text", true);
    case "boolean":
      return buildInferredColumnDefinition("boolean", true);
    case "number":
      return buildInferredColumnDefinition(Number.isInteger(value) ? "integer" : "real", true);
    default:
      return undefined;
  }
}

function resolveNumericExprType(
  definitions: Array<TableColumnDefinition | undefined>,
): SqlScalarType {
  return definitions.some(
    (definition) => definition && resolveColumnDefinition(definition).type === "real",
  )
    ? "real"
    : "integer";
}

function resolveRelRefOutputDefinition(
  definitions: Record<string, TableColumnDefinition | undefined>,
  ref: RelColumnRef,
): TableColumnDefinition | undefined {
  const qualified = toRelOutputKey(ref);
  if (qualified && qualified in definitions) {
    return definitions[qualified];
  }
  if (!ref.alias && !ref.table && ref.column in definitions) {
    return definitions[ref.column];
  }

  const matches = Object.entries(definitions)
    .filter(([name]) => name === ref.column || name.endsWith(`.${ref.column}`))
    .map(([, definition]) => definition);
  return matches.length === 1 ? matches[0] : undefined;
}

function applyJoinNullability(
  definitions: Record<string, TableColumnDefinition | undefined>,
  nullable: boolean,
): Record<string, TableColumnDefinition | undefined> {
  if (!nullable) {
    return definitions;
  }

  return Object.fromEntries(
    Object.entries(definitions).map(([name, definition]) => [
      name,
      definition ? withColumnNullability(definition, true) : undefined,
    ]),
  );
}

function withColumnNullability(
  definition: TableColumnDefinition,
  nullable: boolean,
): TableColumnDefinition {
  const resolved = resolveColumnDefinition(definition);
  if (nullable && resolved.nullable) {
    return definition;
  }

  return {
    type: resolved.type,
    nullable,
    ...(resolved.enum ? { enum: resolved.enum } : {}),
    ...(resolved.enumFrom ? { enumFrom: resolved.enumFrom } : {}),
    ...(resolved.enumMap ? { enumMap: resolved.enumMap } : {}),
    ...(resolved.physicalType ? { physicalType: resolved.physicalType } : {}),
    ...(resolved.physicalDialect ? { physicalDialect: resolved.physicalDialect } : {}),
    ...(resolved.foreignKey ? { foreignKey: resolved.foreignKey } : {}),
    ...(resolved.description ? { description: resolved.description } : {}),
  };
}

function buildInferredColumnDefinition(
  type: SqlScalarType,
  nullable: boolean,
): TableColumnDefinition {
  return {
    type,
    nullable,
  };
}

function normalizeProviderRelOutputValue(
  value: unknown,
  outputName: string,
  definition?: TableColumnDefinition,
): unknown {
  if (!definition) {
    return value;
  }

  const coerce = buildRelOutputCoercion(definition);

  return normalizeProviderRowValue(
    value,
    {
      kind: "source",
      source: outputName,
      definition,
      ...(coerce ? { coerce } : {}),
    },
    definition,
  );
}

function buildRelOutputCoercion(
  definition: TableColumnDefinition,
): SchemaValueCoercion | undefined {
  const resolved = resolveColumnDefinition(definition);
  switch (resolved.type) {
    case "integer":
      return (value) => {
        if (typeof value === "string" || typeof value === "bigint") {
          return Number(value);
        }
        return value;
      };
    case "real":
      return (value) => {
        if (typeof value === "string" || typeof value === "bigint") {
          return Number(value);
        }
        return value;
      };
    case "boolean":
      return (value) => {
        if (typeof value === "string") {
          if (value === "true" || value === "t") {
            return true;
          }
          if (value === "false" || value === "f") {
            return false;
          }
        }
        if (value === 1) {
          return true;
        }
        if (value === 0) {
          return false;
        }
        return value;
      };
    default:
      return undefined;
  }
}

function toRelOutputKey(ref: RelColumnRef): string | null {
  const alias = ref.alias ?? ref.table;
  return alias ? `${alias}.${ref.column}` : null;
}

function parseEnumLinkReference(
  ref: string,
  tableName: string,
  columnName: string,
): EnumLinkReference {
  const parts = ref.split(".");
  if (parts.length !== 2) {
    throw new Error(
      `enumFrom reference for ${tableName}.${columnName} must be in "table.column" format.`,
    );
  }

  const [table, column] = parts;
  if (!table || !column) {
    throw new Error(
      `enumFrom reference for ${tableName}.${columnName} must be in "table.column" format.`,
    );
  }

  return { table, column };
}

function defaultResolveLinkedEnumValues(
  ref: EnumLinkReference,
  schema: SchemaDefinition,
): readonly string[] | undefined {
  const table = schema.tables[ref.table];
  const column = table?.columns[ref.column];
  if (!column || typeof column === "string") {
    return undefined;
  }
  return resolveColumnDefinition(column).enum;
}

function isProjectExpr(
  column: Extract<RelNode, { kind: "project" }>["columns"][number],
): column is Extract<RelNode, { kind: "project" }>["columns"][number] & { kind: "expr" } {
  return "expr" in column;
}
