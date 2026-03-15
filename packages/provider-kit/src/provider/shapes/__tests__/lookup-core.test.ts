import { describe, expect, it } from "vitest";
import { Result } from "better-result";

import type { RelNode } from "@tupl/foundation";
import {
  buildLookupOnlyUnsupportedReport,
  filterLookupRows,
  prepareKeyedSimpleRelScan,
  projectLookupRow,
  validateLookupRequest,
} from "@tupl/provider-kit/shapes";

describe("lookup provider core", () => {
  it("builds a standard unsupported report for non-lookup fragments", () => {
    const report = buildLookupOnlyUnsupportedReport(
      "Lookup-only providers do not support scan pushdown.",
    );

    expect(report.supported).toBe(false);
    expect(report.reason).toBe("Lookup-only providers do not support scan pushdown.");
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

  it("prepares keyed simple scans in one pass", () => {
    const rel: RelNode = {
      id: "redis:product_view_counts",
      kind: "scan",
      convention: "provider:redisProvider",
      table: "product_view_counts",
      select: ["product_id", "view_count"],
      where: [{ op: "eq", column: "product_id", value: "p1" }],
      orderBy: [{ column: "view_count", direction: "desc" }],
      output: [{ name: "product_id" }, { name: "view_count" }],
    };

    const prepared = prepareKeyedSimpleRelScan(rel, {
      entity: {
        lookupKey: "product_id",
        columns: ["product_id", "view_count"] as const,
      },
      policy: {
        supportsSelectColumn(column) {
          return column === "product_id" || column === "view_count";
        },
        supportsFilterClause(clause) {
          return clause.column === "product_id" || clause.column === "view_count";
        },
        supportsSortTerm(term) {
          return term.column === "view_count";
        },
      },
    });

    expect(Result.isOk(prepared)).toBe(true);
    expect(Result.isOk(prepared) ? prepared.value : null).toEqual({
      request: {
        table: "product_view_counts",
        select: ["product_id", "view_count"],
        where: [{ op: "eq", column: "product_id", value: "p1" }],
        orderBy: [{ column: "view_count", direction: "desc" }],
      },
      key: "product_id",
      keys: ["p1"],
      fetchColumns: ["product_id", "view_count"],
    });
  });

  it("reports keyed scan failures with capability diagnostics", () => {
    const rel: RelNode = {
      id: "redis:product_view_counts",
      kind: "scan",
      convention: "provider:redisProvider",
      table: "product_view_counts",
      select: ["product_id", "view_count"],
      output: [{ name: "product_id" }, { name: "view_count" }],
    };

    const prepared = prepareKeyedSimpleRelScan(rel, {
      entity: {
        lookupKey: "product_id",
        columns: ["product_id", "view_count"] as const,
      },
    });

    expect(Result.isError(prepared)).toBe(true);
    expect(Result.isError(prepared) ? prepared.error : null).toMatchObject({
      supported: false,
      reason: "Provider requires an equality or IN predicate on product_view_counts.product_id.",
    });
  });
});
