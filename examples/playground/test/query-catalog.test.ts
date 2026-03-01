import { describe, expect, it } from "vitest";

import { buildQueryCatalog, EXAMPLE_PACKS } from "../src/examples";

describe("playground/query-catalog", () => {
  it("contains all queries from all packs", () => {
    const catalog = buildQueryCatalog(EXAMPLE_PACKS);
    const expectedCount = EXAMPLE_PACKS.reduce((sum, pack) => sum + pack.queries.length, 0);

    expect(catalog).toHaveLength(expectedCount);
  });

  it("builds deterministic stable query ids", () => {
    const first = buildQueryCatalog(EXAMPLE_PACKS);
    const second = buildQueryCatalog(EXAMPLE_PACKS);

    expect(first.map((entry) => entry.id)).toEqual(second.map((entry) => entry.id));
    expect(first[0]?.id).toBe("commerce:0");
  });
});

