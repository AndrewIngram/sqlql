import { describe, expect, it } from "vite-plus/test";

import { normalizeSqlPreview, truncateReason } from "../src/query-preview";

describe("playground/query-preview", () => {
  it("normalizes whitespace into a single line", () => {
    const preview = normalizeSqlPreview(`
      SELECT  *
      FROM orders
      WHERE status = 'paid'
    `);

    expect(preview).toBe("SELECT * FROM orders WHERE status = 'paid'");
  });

  it("truncates long reasons", () => {
    const reason = "x".repeat(140);
    expect(truncateReason(reason, 32)).toHaveLength(32);
    expect(truncateReason(reason, 32).endsWith("…")).toBe(true);
  });
});
