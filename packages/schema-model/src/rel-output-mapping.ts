import type { QueryRow, SchemaDefinition } from "./types";

import { inferRelOutputDefinitions, buildRelOutputCoercion } from "./output-inference";

/**
 * Rel-output mapping owns row shaping against inferred relational output definitions.
 */
export function inferAndMapRelOutputRows(
  rows: QueryRow[],
  rel: import("@tupl/foundation").RelNode,
  schema: SchemaDefinition,
  normalizeRowValue: (
    value: unknown,
    outputName: string,
    definition?: import("./types").TableColumnDefinition,
    coerce?: import("./types").SchemaValueCoercion,
  ) => unknown,
): QueryRow[] {
  if (rel.output.length === 0) {
    return rows;
  }

  const outputDefinitions = inferRelOutputDefinitions(rel, schema);
  return rows.map((row) => {
    const out: QueryRow = {};
    for (const output of rel.output) {
      const definition = outputDefinitions[output.name];
      out[output.name] = normalizeRowValue(
        row[output.name] ?? null,
        output.name,
        definition,
        definition ? buildRelOutputCoercion(definition) : undefined,
      );
    }
    return out;
  });
}
