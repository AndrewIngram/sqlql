import { describe, expect, it } from "vitest";

import {
  DEFAULT_FACADE_SCHEMA_CODE,
  QUERY_PRESETS,
  SCENARIO_PRESETS,
  serializeJson,
} from "../src/examples";
import { compilePlaygroundInput, createSession } from "../src/session-runtime";

describe("playground/preset-queries", () => {
  it("compiles and executes every query preset against every scenario", async () => {
    for (const scenario of SCENARIO_PRESETS) {
      for (const query of QUERY_PRESETS) {
        const compiled = await compilePlaygroundInput(
          DEFAULT_FACADE_SCHEMA_CODE,
          serializeJson(scenario.rows),
          query.sql,
        );

        expect(compiled.ok, `[${scenario.id}] ${query.label} compile mismatch`).toBe(true);
        if (!compiled.ok) {
          continue;
        }

        const bundle = await createSession(compiled, scenario.context);
        const rows = await bundle.session.runToCompletion();
        expect(
          Array.isArray(rows),
          `[${scenario.id}] ${query.label} should return rows`,
        ).toBe(true);
      }
    }
  });
});
