import type { RelNode } from "@tupl/foundation";

import {
  mapProviderRowsToLogical as mapProviderRowsToLogicalInternal,
  isNormalizedSourceColumnBinding as isNormalizedSourceColumnBindingInternal,
} from "./logical-row-mapping";
import { inferAndMapRelOutputRows } from "./rel-output-mapping";
import {
  coerceValue as coerceValueInternal,
  normalizeProviderRowValue as normalizeProviderRowValueInternal,
} from "./row-coercion";
import type {
  NormalizedColumnBinding,
  NormalizedPhysicalTableBinding,
  NormalizedSourceColumnBinding,
  QueryRow,
  SchemaDefinition,
  SchemaValueCoercion,
  TableColumnDefinition,
  TableDefinition,
} from "./types";

/**
 * Mapping owns provider-row coercion and logical-row projection against normalized bindings.
 */
export function coerceValue(value: unknown, coerce: SchemaValueCoercion): unknown {
  return coerceValueInternal(value, coerce);
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
  return normalizeProviderRowValueInternal(value, binding, fallbackDefinition, options);
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
  return mapProviderRowsToLogicalInternal(
    rows,
    selectedLogicalColumns,
    binding,
    tableDefinition,
    options,
  );
}

export function mapProviderRowsToRelOutput(
  rows: QueryRow[],
  rel: RelNode,
  schema: SchemaDefinition,
): QueryRow[] {
  return inferAndMapRelOutputRows(rows, rel, schema, (value, outputName, definition, coerce) =>
    normalizeProviderRowValue(
      value,
      definition
        ? {
            kind: "source",
            source: outputName,
            definition,
            ...(coerce ? { coerce } : {}),
          }
        : undefined,
      definition,
    ),
  );
}

export function isNormalizedSourceColumnBinding(
  binding: NormalizedColumnBinding | undefined,
): binding is NormalizedSourceColumnBinding {
  return isNormalizedSourceColumnBindingInternal(binding);
}
