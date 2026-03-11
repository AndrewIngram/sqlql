import { getDataEntityAdapter } from "@tupl/provider-kit";

import { type SchemaBuilderState } from "./builder-state";
import {
  buildSchemaDslViewRelHelpers,
  isDslTableDefinition,
  isDslViewDefinition,
} from "./builder-helpers";
import { validateSchemaConstraints } from "./constraints";
import {
  buildColumnSourceMapFromBindings,
  normalizeColumnBinding,
  validateCalculatedColumnDependencies,
} from "./binding-normalization";
import { resolveViewRelDefinition } from "./view-normalization";
import type {
  NormalizedTableBinding,
  SchemaDataEntityHandle,
  SchemaDefinition,
  SchemaDslTableToken,
  SchemaDslViewRelHelpers,
  TableColumns,
  TableDefinition,
} from "./types";

/**
 * Schema finalization owns schema assembly and the hidden normalized binding state.
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
  const viewRelHelpers = buildSchemaDslViewRelHelpers() as SchemaDslViewRelHelpers;

  for (const [tableName, rawTable] of entries) {
    if (isDslTableDefinition(rawTable)) {
      const normalizedColumns: TableColumns = {};
      const columnBindings: Record<string, import("./types").NormalizedColumnBinding> = {};
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
      const columnBindings: Record<string, import("./types").NormalizedColumnBinding> = {};
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
