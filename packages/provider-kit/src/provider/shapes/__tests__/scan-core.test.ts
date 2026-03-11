import { describe, expect, it } from "vitest";

import type { ProviderFragment } from "@tupl/provider-kit";
import { buildScanUnsupportedReport } from "@tupl/provider-kit/shapes";

describe("scan provider core", () => {
  it("builds a standard unsupported report for scan fragments", () => {
    const fragment: ProviderFragment = {
      kind: "scan",
      provider: "redisProvider",
      table: "product_view_counts",
      request: {
        table: "product_view_counts",
        select: ["product_id", "view_count"],
        where: [{ op: "eq", column: "product_id", value: "p1" }],
        orderBy: [{ column: "view_count", direction: "desc" }],
        limit: 10,
      },
    };

    const report = buildScanUnsupportedReport(
      fragment,
      ["scan.project", "scan.filter.basic"],
      "Scan providers do not support sort pushdown.",
    );

    expect(report).toMatchObject({
      supported: false,
      reason: "Scan providers do not support sort pushdown.",
      routeFamily: "scan",
    });
    expect(report.requiredAtoms).toEqual([
      "scan.project",
      "scan.filter.basic",
      "expr.compare_basic",
      "scan.sort",
      "scan.limit_offset",
    ]);
    expect(report.missingAtoms).toEqual(["expr.compare_basic", "scan.sort", "scan.limit_offset"]);
  });
});
