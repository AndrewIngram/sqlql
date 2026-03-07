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
});
