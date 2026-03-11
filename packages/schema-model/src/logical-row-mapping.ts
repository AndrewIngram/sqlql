import { getNormalizedColumnBindings } from "./normalization";
import { normalizeProviderRowValue } from "./row-coercion";
import type {
  NormalizedColumnBinding,
  NormalizedPhysicalTableBinding,
  NormalizedSourceColumnBinding,
  QueryRow,
  TableDefinition,
} from "./types";

/**
 * Logical row mapping owns logical-column projection against normalized physical bindings.
 */
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

export function isNormalizedSourceColumnBinding(
  binding: NormalizedColumnBinding | undefined,
): binding is NormalizedSourceColumnBinding {
  return !!binding && binding.kind === "source";
}
