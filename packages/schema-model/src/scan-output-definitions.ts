import type { RelNode } from "@tupl/foundation";

import { createTableDefinitionFromEntity } from "./normalization";
import type { SchemaDefinition, TableColumnDefinition } from "./types";

/**
 * Scan output definitions own output-column inference for scans and CTE scan lookups.
 */
export function inferScanOutputDefinitions(
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
