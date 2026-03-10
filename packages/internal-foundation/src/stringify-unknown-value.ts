export function stringifyUnknownValue(value: unknown, fallback = ""): string {
  if (value == null) {
    return fallback;
  }
  if (typeof value === "string") {
    return value;
  }
  if (typeof value === "number" || typeof value === "boolean" || typeof value === "bigint") {
    return value.toString();
  }
  if (value instanceof Date) {
    return value.toISOString();
  }

  try {
    const json = JSON.stringify(value);
    return json ?? (fallback || Object.prototype.toString.call(value));
  } catch {
    return fallback || Object.prototype.toString.call(value);
  }
}
