import { describe, expect, it } from "vitest";
import { Result } from "better-result";

import type { ProviderFragment } from "@tupl/provider-kit";
import {
  buildLookupOnlyUnsupportedReport,
  filterLookupRows,
  projectLookupRow,
  validateLookupRequest,
} from "@tupl/provider-kit/shapes";

describe("lookup provider core", () => {
  it("builds a standard unsupported report for non-lookup fragments", () => {
    const fragment: ProviderFragment = {
      kind: "scan",
      provider: "redisProvider",
      table: "product_view_counts",
      request: {
        table: "product_view_counts",
        select: ["product_id", "view_count"],
      },
    };

    const report = buildLookupOnlyUnsupportedReport(
      fragment,
      "Lookup-only providers do not support scan pushdown.",
    );

    expect(report.supported).toBe(false);
    expect(report.routeFamily).toBe("scan");
    expect(report.missingAtoms).toContain("scan.project");
  });

  it("shares lookup validation, filtering, and projection", () => {
    const validation = validateLookupRequest(
      {
        table: "product_view_counts",
        key: "product_id",
        keys: ["p1", "p2"],
        select: ["product_id", "view_count"],
      },
      {
        lookupKey: "product_id",
        columns: ["product_id", "view_count"] as const,
      },
    );

    expect(Result.isOk(validation)).toBe(true);

    const rows = filterLookupRows(
      [
        { product_id: "p1", view_count: 8 },
        { product_id: "p2", view_count: 3 },
      ],
      [{ op: "gt", column: "view_count", value: 5 }],
    );

    expect(rows).toEqual([{ product_id: "p1", view_count: 8 }]);
    expect(projectLookupRow(rows[0] ?? {}, ["product_id"])).toEqual({ product_id: "p1" });
  });
});
