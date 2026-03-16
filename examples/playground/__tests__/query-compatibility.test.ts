import { describe, expect, it } from "vite-plus/test";

import { buildQueryCatalog, FACADE_SCHEMA, QUERY_PRESETS } from "../src/examples";
import { buildQueryCompatibilityMap, checkQueryCompatibility } from "../src/query-compatibility";
import type { SchemaParseResult } from "../src/types";

describe("playground/query-compatibility", () => {
  it("marks all shipped presets as compatible", () => {
    for (const query of QUERY_PRESETS) {
      const result = checkQueryCompatibility(FACADE_SCHEMA, query.sql);
      expect(result.compatible, `${query.label} compatibility mismatch`).toBe(true);
    }
  });

  it("marks compatible queries as compatible", () => {
    const query = QUERY_PRESETS[0]?.sql;
    if (!query) {
      throw new Error("Expected a sample pack query.");
    }

    const result = checkQueryCompatibility(FACADE_SCHEMA, query);
    expect(result.compatible).toBe(true);
  });

  it("marks missing table references as incompatible", () => {
    const result = checkQueryCompatibility(FACADE_SCHEMA, "SELECT * FROM missing_table;");
    expect(result.compatible).toBe(false);
    expect(result.reason).toBeTruthy();
  });

  it("marks unsupported SQL statements as incompatible", () => {
    const result = checkQueryCompatibility(FACADE_SCHEMA, "UPDATE my_orders SET status = 'paid';");
    expect(result.compatible).toBe(false);
    expect(result.reason).toContain("SELECT");
  });

  it("returns disabled-all compatibility when schema is invalid", () => {
    const catalog = buildQueryCatalog(QUERY_PRESETS).slice(0, 3);
    const invalidSchemaResult: SchemaParseResult = {
      ok: false,
      issues: [{ path: "$", message: "invalid schema" }],
    };

    const map = buildQueryCompatibilityMap(invalidSchemaResult, catalog);
    expect(Object.values(map).every((entry) => entry.compatible === false)).toBe(true);
    expect(
      Object.values(map).every((entry) => entry.reason === "Fix schema TypeScript first."),
    ).toBe(true);
  });
});
