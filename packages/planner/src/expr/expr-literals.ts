/**
 * Expr literals own parser-AST literal, ordinal, and limit/offset helpers.
 */
export function parsePositiveOrdinalLiteral(
  raw: unknown,
  clause: "GROUP BY" | "ORDER BY",
): number | undefined {
  const expr = raw as { type?: unknown; value?: unknown };
  if (expr?.type !== "number") {
    return undefined;
  }

  if (typeof expr.value !== "number" || !Number.isInteger(expr.value) || expr.value <= 0) {
    throw new Error(`${clause} ordinal must be a positive integer.`);
  }

  return expr.value;
}

export function parseLiteral(raw: unknown): unknown {
  const expr = raw as { type?: unknown; value?: unknown };

  switch (expr?.type) {
    case "single_quote_string":
    case "double_quote_string":
    case "string":
      return typeof expr.value === "string" ? expr.value : "";
    case "number": {
      const value = expr.value;
      if (typeof value === "number") {
        return value;
      }
      if (typeof value === "string") {
        const parsed = Number(value);
        return Number.isFinite(parsed) ? parsed : undefined;
      }
      return undefined;
    }
    case "bool":
      return Boolean(expr.value);
    case "null":
      return null;
    default:
      return undefined;
  }
}

export function tryParseLiteralExpressionList(raw: unknown): unknown[] | undefined {
  const expr = raw as { type?: unknown; value?: unknown };
  if (expr?.type !== "expr_list" || !Array.isArray(expr.value)) {
    return undefined;
  }

  const values = expr.value.map((entry) => parseLiteral(entry));
  if (values.some((value) => value === undefined)) {
    return undefined;
  }

  return values;
}

export function parseLimitAndOffset(rawLimit: unknown): { limit?: number; offset?: number } {
  if (!rawLimit || typeof rawLimit !== "object") {
    return {};
  }

  const limitNode = rawLimit as {
    value?: Array<{ value?: unknown }>;
    seperator?: unknown;
  };

  if (!Array.isArray(limitNode.value) || limitNode.value.length === 0) {
    return {};
  }

  const first = parseNumericLiteral(limitNode.value[0]?.value);
  const second = parseNumericLiteral(limitNode.value[1]?.value);
  const separator = limitNode.seperator;

  if (first == null) {
    throw new Error("Unable to parse LIMIT value.");
  }

  if (separator === "offset") {
    return {
      limit: first,
      ...(second != null ? { offset: second } : {}),
    };
  }

  if (separator === ",") {
    return {
      ...(second != null ? { limit: second } : {}),
      offset: first,
    };
  }

  return { limit: first };
}

function parseNumericLiteral(value: unknown): number | undefined {
  if (typeof value === "number") {
    return Number.isFinite(value) ? value : undefined;
  }

  if (typeof value === "string") {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : undefined;
  }

  return undefined;
}
