import type { DataEntityHandle, RelNode } from "@tupl/foundation";

import type { SchemaDefinition } from "./types";

/**
 * Rel-schema validation owns checking that relational plans only reference schema-backed tables and columns.
 */
export function validateRelAgainstSchema(node: RelNode, schema: SchemaDefinition): void {
  const validateScanColumn = (
    tableName: string,
    column: string,
    entity?: DataEntityHandle<string>,
  ): void => {
    if (entity?.columns) {
      const logicalColumn = column.includes(".")
        ? column.slice(column.lastIndexOf(".") + 1)
        : column;
      if (!(logicalColumn in entity.columns)) {
        throw new Error(`Unknown column in relational plan: ${tableName}.${logicalColumn}`);
      }
      return;
    }

    const table = schema.tables[tableName];
    if (!table) {
      return;
    }
    const logicalColumn = column.includes(".") ? column.slice(column.lastIndexOf(".") + 1) : column;
    if (!(logicalColumn in table.columns)) {
      throw new Error(`Unknown column in relational plan: ${tableName}.${logicalColumn}`);
    }
  };

  const visit = (current: RelNode, cteNames: Set<string>): void => {
    switch (current.kind) {
      case "scan":
        if (!cteNames.has(current.table) && !schema.tables[current.table] && !current.entity) {
          throw new Error(`Unknown table in relational plan: ${current.table}`);
        }
        if (!cteNames.has(current.table) && (schema.tables[current.table] || current.entity)) {
          for (const column of current.select) {
            validateScanColumn(current.table, column, current.entity);
          }
          for (const clause of current.where ?? []) {
            validateScanColumn(current.table, clause.column, current.entity);
          }
          for (const term of current.orderBy ?? []) {
            validateScanColumn(current.table, term.column, current.entity);
          }
        }
        return;
      case "sql":
        for (const table of current.tables) {
          if (!cteNames.has(table) && !schema.tables[table]) {
            throw new Error(`Unknown table in relational plan: ${table}`);
          }
        }
        return;
      case "filter":
      case "project":
      case "aggregate":
      case "window":
      case "sort":
      case "limit_offset":
        visit(current.input, cteNames);
        return;
      case "join":
      case "set_op":
        visit(current.left, cteNames);
        visit(current.right, cteNames);
        return;
      case "with": {
        const nextCteNames = new Set(cteNames);
        for (const cte of current.ctes) {
          nextCteNames.add(cte.name);
        }
        for (const cte of current.ctes) {
          visit(cte.query, nextCteNames);
        }
        visit(current.body, nextCteNames);
        return;
      }
    }
  };

  visit(node, new Set<string>());
}
