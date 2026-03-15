import { describe, expect, it } from "vitest";

import { DEFAULT_FACADE_SCHEMA_CODE, SCENARIO_PRESETS, serializeJson } from "../src/examples";
import { compilePlaygroundInput } from "../src/session-runtime";

describe("playground/sql-diagnostics", () => {
  it("reports unknown table references during compile", { timeout: 15_000 }, async () => {
    const scenario = SCENARIO_PRESETS[0];
    if (!scenario) {
      throw new Error("Expected default scenario.");
    }

    const compiled = await compilePlaygroundInput(
      DEFAULT_FACADE_SCHEMA_CODE,
      serializeJson(scenario.rows),
      "SELECT * FROM missing_table",
    );

    expect(compiled.ok).toBe(false);
    if (compiled.ok) {
      throw new Error("Expected compile failure.");
    }
    expect(compiled.issues[0]).toBe("Unknown table: missing_table");
  });

  it("reports unknown column references during compile", { timeout: 15_000 }, async () => {
    const scenario = SCENARIO_PRESETS[0];
    if (!scenario) {
      throw new Error("Expected default scenario.");
    }

    const compiled = await compilePlaygroundInput(
      DEFAULT_FACADE_SCHEMA_CODE,
      serializeJson(scenario.rows),
      "SELECT o.missing_column FROM my_orders o",
    );

    expect(compiled.ok).toBe(false);
    if (compiled.ok) {
      throw new Error("Expected compile failure.");
    }
    expect(compiled.issues[0]).toBe("Unknown column in relational plan: my_orders.missing_column");
  });
});
