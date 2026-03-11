import { resolveColumnDefinition } from "./definition";
import type { NormalizedColumnBinding, SchemaValueCoercion, TableColumnDefinition } from "./types";

/**
 * Row coercion owns provider scalar normalization and normalized-binding validation.
 */
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

function describeNormalizedColumnBinding(binding: NormalizedColumnBinding): string {
  return binding.kind === "source" ? binding.source : "<expr>";
}
