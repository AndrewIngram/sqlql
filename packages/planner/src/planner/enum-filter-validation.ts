import type { SchemaDefinition } from "@tupl/schema-model";
import { resolveColumnDefinition } from "@tupl/schema-model/definition";

import type { Binding, LiteralFilter } from "./planner-types";

/**
 * Enum filter validation owns facade enum-value checks for literal scan filters.
 */
export function validateEnumLiteralFilters(
  filters: LiteralFilter[],
  bindings: Binding[],
  schema: SchemaDefinition,
): void {
  const tableByAlias = new Map(bindings.map((binding) => [binding.alias, binding.table]));

  for (const filter of filters) {
    const tableName = tableByAlias.get(filter.alias);
    if (!tableName) {
      continue;
    }
    const definition = schema.tables[tableName]?.columns[filter.clause.column];
    if (!definition || typeof definition === "string") {
      continue;
    }
    const resolved = resolveColumnDefinition(definition);
    if (!resolved.enum) {
      continue;
    }

    if (filter.clause.op === "eq") {
      if (typeof filter.clause.value === "string" && !resolved.enum.includes(filter.clause.value)) {
        throw new Error(`Invalid enum value for ${tableName}.${filter.clause.column}`);
      }
      continue;
    }

    if (filter.clause.op === "in") {
      for (const value of filter.clause.values) {
        if (value == null) {
          continue;
        }
        if (typeof value !== "string" || !resolved.enum.includes(value)) {
          throw new Error(`Invalid enum value for ${tableName}.${filter.clause.column}`);
        }
      }
    }
  }
}
