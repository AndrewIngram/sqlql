import { beforeAll, describe, expect, it } from "vitest";

import {
  DEFAULT_FACADE_SCHEMA_CODE,
  QUERY_PRESETS,
  SCENARIO_PRESETS,
  serializeJson,
} from "../src/examples";
import {
  compilePreparedPlaygroundQuery,
  createSession,
  preparePlaygroundInput,
  type PlaygroundPreparedInputSuccess,
} from "../src/session-runtime";

describe.each(SCENARIO_PRESETS)("playground/preset-queries [$id]", (scenario) => {
  let prepared: PlaygroundPreparedInputSuccess | null = null;

  beforeAll(async () => {
    const result = await preparePlaygroundInput(
      DEFAULT_FACADE_SCHEMA_CODE,
      serializeJson(scenario.rows),
    );
    expect(result.ok, `[${scenario.id}] scenario preparation failed`).toBe(true);
    if (!result.ok) {
      return;
    }
    prepared = result;
  });

  it.each(QUERY_PRESETS)(
    "compiles and executes preset \"$label\" for scenario",
    { timeout: 15_000 },
    async (query) => {
      expect(prepared, `[${scenario.id}] scenario preparation missing`).not.toBeNull();
      if (!prepared) {
        return;
      }

      const compiled = compilePreparedPlaygroundQuery(prepared, query.sql);
      expect(compiled.ok, `[${scenario.id}] ${query.label} compile mismatch`).toBe(true);
      if (!compiled.ok) {
        return;
      }

      const bundle = await createSession(compiled, scenario.context);
      const rows = await bundle.session.runToCompletion();
      expect(
        Array.isArray(rows),
        `[${scenario.id}] ${query.label} should return rows`,
      ).toBe(true);
    },
  );
});
