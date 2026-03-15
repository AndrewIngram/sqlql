import { describe, expect, it } from "vitest";

import type { RelNode } from "@tupl/foundation";
import { buildScanUnsupportedReport } from "@tupl/provider-kit/shapes";

describe("scan provider core", () => {
  it("builds a standard unsupported report for scan fragments", () => {
    const rel: RelNode = {
      id: "redis:product_view_counts",
      kind: "scan",
      convention: "provider:redisProvider",
      table: "product_view_counts",
      select: ["product_id", "view_count"],
      output: [{ name: "product_id" }, { name: "view_count" }],
      where: [{ op: "eq", column: "product_id", value: "p1" }],
      orderBy: [{ column: "view_count", direction: "desc" }],
      limit: 10,
    };

    const report = buildScanUnsupportedReport(rel, "Scan providers do not support sort pushdown.");

    expect(report).toMatchObject({
      supported: false,
      reason: "Scan providers do not support sort pushdown.",
      routeFamily: "scan",
    });
  });
});
