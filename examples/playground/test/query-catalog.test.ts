import { describe, expect, it } from "vitest";

import { buildQueryCatalog, QUERY_PRESETS } from "../src/examples";

describe("playground/query-catalog", () => {
  it("contains all configured query presets", () => {
    const catalog = buildQueryCatalog(QUERY_PRESETS);
    const expectedCount = QUERY_PRESETS.length;

    expect(catalog).toHaveLength(expectedCount);
  });

  it("builds deterministic stable query ids", () => {
    const first = buildQueryCatalog(QUERY_PRESETS);
    const second = buildQueryCatalog(QUERY_PRESETS);

    expect(first.map((entry) => entry.id)).toEqual(second.map((entry) => entry.id));
    expect(first[0]?.id).toBe("orders_calculated_columns");
  });

  it("includes a logical-only category for providerless showcase queries", () => {
    const catalog = buildQueryCatalog(QUERY_PRESETS);
    const logicalOnly = catalog.filter((entry) => entry.category === "Logical only");

    expect(logicalOnly.map((entry) => entry.id)).toEqual([
      "math_fibonacci",
      "math_powers_of_two",
      "math_triangular_window",
      "logical_derived_table",
      "logical_correlated_exists",
      "logical_correlated_scalar_max",
      "logical_named_window",
    ]);
  });

  it("groups data-backed presets into single-provider and multi-provider buckets", () => {
    const catalog = buildQueryCatalog(QUERY_PRESETS);

    expect(catalog.filter((entry) => entry.category === "Single-provider").length).toBeGreaterThan(
      1,
    );
    expect(
      catalog.filter((entry) => entry.category === "Multi-provider").map((entry) => entry.id),
    ).toEqual(["product_engagement"]);
  });

  it("preserves preset highlights for UI display", () => {
    const catalog = buildQueryCatalog(QUERY_PRESETS);
    const fibonacci = catalog.find((entry) => entry.id === "math_fibonacci");

    expect(fibonacci?.highlights).toEqual(["Recursive CTE", "Arithmetic"]);
  });
});
