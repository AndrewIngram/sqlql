import { describe, expect, it } from "vitest";

import { EXAMPLE_PACKS, serializeJson } from "../src/examples";
import { compilePlaygroundInput } from "../src/session-runtime";

describe("playground/sql-diagnostics", () => {
  it("reports unknown table references during compile", () => {
    const pack = EXAMPLE_PACKS[0];
    if (!pack) {
      throw new Error("Expected default example pack.");
    }

    const compiled = compilePlaygroundInput(
      serializeJson(pack.schema),
      serializeJson(pack.rows),
      "SELECT * FROM missing_table",
    );

    expect(compiled.ok).toBe(false);
    if (compiled.ok) {
      throw new Error("Expected compile failure.");
    }
    expect(compiled.issues[0]).toBe("Unknown table: missing_table");
  });

  it("reports unknown column references during compile", () => {
    const pack = EXAMPLE_PACKS[0];
    if (!pack) {
      throw new Error("Expected default example pack.");
    }

    const compiled = compilePlaygroundInput(
      serializeJson(pack.schema),
      serializeJson(pack.rows),
      "SELECT c.missing_column FROM customers c",
    );

    expect(compiled.ok).toBe(false);
    if (compiled.ok) {
      throw new Error("Expected compile failure.");
    }
    expect(compiled.issues[0]).toBe("Unknown column: c.missing_column");
  });

});
