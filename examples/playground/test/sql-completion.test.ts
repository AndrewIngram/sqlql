import { describe, expect, it } from "vitest";

import { EXAMPLE_PACKS } from "../src/examples";
import { getSqlSuggestionLabels } from "../src/sql-completion";

describe("playground/sql-completion", () => {
  const schema = EXAMPLE_PACKS[0]?.schema;

  it("suggests table names after FROM", () => {
    if (!schema) {
      throw new Error("Missing example schema.");
    }

    const sql = "SELECT * FROM ";
    const suggestions = getSqlSuggestionLabels(sql, sql.length, schema);

    expect(suggestions.context).toBe("table");
    expect(suggestions.labels).toContain("orders");
    expect(suggestions.labels).toContain("customers");
  });

  it("suggests columns for alias-qualified prefixes", () => {
    if (!schema) {
      throw new Error("Missing example schema.");
    }

    const sql = "SELECT o. FROM orders o";
    const suggestions = getSqlSuggestionLabels(sql, "SELECT o.".length, schema);

    expect(suggestions.context).toBe("alias_column");
    expect(suggestions.labels).toContain("id");
    expect(suggestions.labels).toContain("total_cents");
  });

  it("includes SQL functions in general context", () => {
    if (!schema) {
      throw new Error("Missing example schema.");
    }

    const sql = "SELECT ";
    const suggestions = getSqlSuggestionLabels(sql, sql.length, schema);

    expect(suggestions.labels).toContain("SUM");
    expect(suggestions.labels).toContain("ROW_NUMBER");
  });
});
