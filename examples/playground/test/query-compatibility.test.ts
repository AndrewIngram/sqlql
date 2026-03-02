import { describe, expect, it } from "vitest";

import { buildQueryCatalog, EXAMPLE_PACKS } from "../src/examples";
import {
  buildQueryCompatibilityMap,
  checkQueryCompatibility,
} from "../src/query-compatibility";
import type { SchemaParseResult } from "../src/types";

describe("playground/query-compatibility", () => {
  it("marks every preset query as compatible with its own schema", () => {
    for (const pack of EXAMPLE_PACKS) {
      for (const query of pack.queries) {
        const result = checkQueryCompatibility(pack.schema, query.sql);
        expect(
          result.compatible,
          `[${pack.id}] ${query.label} should be compatible`,
        ).toBe(true);
      }
    }
  });

  it("marks compatible queries as compatible", () => {
    const pack = EXAMPLE_PACKS[0];
    const query = pack?.queries[0]?.sql;
    if (!pack || !query) {
      throw new Error("Expected a sample pack query.");
    }

    const result = checkQueryCompatibility(pack.schema, query);
    expect(result.compatible).toBe(true);
  });

  it("marks missing table references as incompatible", () => {
    const pack = EXAMPLE_PACKS[0];
    if (!pack) {
      throw new Error("Expected a sample pack.");
    }

    const result = checkQueryCompatibility(pack.schema, "SELECT * FROM missing_table;");
    expect(result.compatible).toBe(false);
    expect(result.reason).toBeTruthy();
  });

  it("marks unsupported SQL statements as incompatible", () => {
    const pack = EXAMPLE_PACKS[0];
    if (!pack) {
      throw new Error("Expected a sample pack.");
    }

    const result = checkQueryCompatibility(pack.schema, "UPDATE orders SET status = 'paid';");
    expect(result.compatible).toBe(false);
    expect(result.reason).toContain("SELECT");
  });

  it("returns disabled-all compatibility when schema is invalid", () => {
    const catalog = buildQueryCatalog(EXAMPLE_PACKS).slice(0, 3);
    const invalidSchemaResult: SchemaParseResult = {
      ok: false,
      issues: [{ path: "$", message: "invalid schema" }],
    };

    const map = buildQueryCompatibilityMap(invalidSchemaResult, catalog);
    expect(Object.values(map).every((entry) => entry.compatible === false)).toBe(true);
    expect(Object.values(map).every((entry) => entry.reason === "Fix schema JSON first.")).toBe(
      true,
    );
  });
});
