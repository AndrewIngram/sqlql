import { describe, expect, it } from "vite-plus/test";

import { buildScanUnsupportedReport } from "@tupl/provider-kit/shapes";

describe("scan provider core", () => {
  it("builds a standard unsupported report for scan fragments", () => {
    const report = buildScanUnsupportedReport("Scan providers do not support sort pushdown.");

    expect(report).toMatchObject({
      supported: false,
      reason: "Scan providers do not support sort pushdown.",
    });
  });
});
