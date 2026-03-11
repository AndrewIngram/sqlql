import type { RelScanNode, ScanFilterClause } from "@tupl/foundation";
import {
  type ColumnDefinition,
  createPhysicalBindingFromEntity,
  createTableDefinitionFromEntity,
  getNormalizedTableBinding,
  resolveNormalizedColumnSource,
  type SchemaDefinition,
} from "@tupl/schema-model";

/**
 * Provider scan normalization owns physical-entity scan rewriting and enum filter translation.
 */
export function normalizeScanForProvider(node: RelScanNode, schema: SchemaDefinition): RelScanNode {
  const binding =
    getNormalizedTableBinding(schema, node.table) ??
    (node.entity ? createPhysicalBindingFromEntity(node.entity) : undefined);
  if (!binding || binding.kind !== "physical") {
    return node;
  }
  const table =
    schema.tables[node.table] ??
    (node.entity ? createTableDefinitionFromEntity(node.entity) : undefined);

  const mapColumn = (column: string): string => resolveNormalizedColumnSource(binding, column);
  const mapClause = (clause: ScanFilterClause): ScanFilterClause => {
    const mapped = mapEnumFilterForProvider(table?.columns[clause.column], clause);
    return {
      ...mapped,
      column: mapColumn(mapped.column),
    };
  };

  return {
    ...node,
    table: binding.entity,
    ...(node.entity ? { entity: node.entity } : {}),
    select: node.select.map(mapColumn),
    ...(node.where
      ? {
          where: node.where.map(mapClause),
        }
      : {}),
    ...(node.orderBy
      ? {
          orderBy: node.orderBy.map((term) => ({
            ...term,
            column: mapColumn(term.column),
          })),
        }
      : {}),
  };
}

function mapEnumFilterForProvider(definition: unknown, clause: ScanFilterClause): ScanFilterClause {
  if (!definition || typeof definition === "string") {
    return clause;
  }

  const column = definition as ColumnDefinition;
  if (!column.enumMap || Object.keys(column.enumMap).length === 0) {
    return clause;
  }

  const mapFacadeValueToSource = (value: unknown): string[] => {
    if (typeof value !== "string") {
      return [];
    }
    const out: string[] = [];
    for (const [sourceValue, facadeValue] of Object.entries(column.enumMap ?? {})) {
      if (facadeValue === value) {
        out.push(sourceValue);
      }
    }
    return out;
  };

  if (clause.op === "eq") {
    const mappedValues = mapFacadeValueToSource(clause.value);
    if (mappedValues.length === 0) {
      throw new Error(
        `No upstream enum mapping for value ${JSON.stringify(clause.value)} on ${clause.column}.`,
      );
    }
    if (mappedValues.length === 1) {
      return {
        ...clause,
        value: mappedValues[0],
      };
    }
    return {
      op: "in",
      column: clause.column,
      values: mappedValues,
    };
  }

  if (clause.op === "in") {
    const mapped = [...new Set(clause.values.flatMap((value) => mapFacadeValueToSource(value)))];
    if (mapped.length === 0) {
      throw new Error(`No upstream enum mappings for IN predicate on ${clause.column}.`);
    }
    return {
      ...clause,
      values: mapped,
    };
  }

  return clause;
}
