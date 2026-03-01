import { describe, expect, it } from "vitest";

import {
  canSelectCatalogQuery,
  CUSTOM_QUERY_ID,
  selectionAfterManualSqlEdit,
  selectionAfterSchemaChange,
} from "../src/query-selection-state";
import type { QueryCompatibilityMap } from "../src/types";

describe("playground/query-selection-state", () => {
  it("switches to custom on manual SQL edits", () => {
    expect(selectionAfterManualSqlEdit()).toBe(CUSTOM_QUERY_ID);
  });

  it("switches to custom when selected query becomes incompatible", () => {
    const map: QueryCompatibilityMap = {
      "commerce:0": { compatible: false, reason: "missing table" },
    };

    expect(selectionAfterSchemaChange("commerce:0", map)).toBe(CUSTOM_QUERY_ID);
  });

  it("keeps compatible selections", () => {
    const map: QueryCompatibilityMap = {
      "commerce:0": { compatible: true },
    };

    expect(selectionAfterSchemaChange("commerce:0", map)).toBe("commerce:0");
    expect(canSelectCatalogQuery("commerce:0", map)).toBe(true);
  });
});

