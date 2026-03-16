import { beforeAll, describe, expect, it } from "vite-plus/test";

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

const LOGICAL_PRESET_EXPECTATIONS = [
  {
    id: "math_fibonacci",
    expectedRows: [
      { n: 0 },
      { n: 1 },
      { n: 1 },
      { n: 2 },
      { n: 3 },
      { n: 5 },
      { n: 8 },
      { n: 13 },
      { n: 21 },
      { n: 34 },
    ],
  },
  {
    id: "math_powers_of_two",
    expectedRows: [
      { exponent: 0, value: 1 },
      { exponent: 1, value: 2 },
      { exponent: 2, value: 4 },
      { exponent: 3, value: 8 },
      { exponent: 4, value: 16 },
      { exponent: 5, value: 32 },
      { exponent: 6, value: 64 },
      { exponent: 7, value: 128 },
      { exponent: 8, value: 256 },
    ],
  },
  {
    id: "math_triangular_window",
    expectedRows: [
      { n: 1, triangular: 1 },
      { n: 2, triangular: 3 },
      { n: 3, triangular: 6 },
      { n: 4, triangular: 10 },
      { n: 5, triangular: 15 },
      { n: 6, triangular: 21 },
      { n: 7, triangular: 28 },
      { n: 8, triangular: 36 },
    ],
  },
  {
    id: "logical_derived_table",
    expectedRows: [
      { parity: "even", n_squared: 4 },
      { parity: "odd", n_squared: 9 },
      { parity: "even", n_squared: 16 },
    ],
  },
  {
    id: "logical_correlated_exists",
    expectedRows: [
      { id: 1, grp: "alpha", val: 2 },
      { id: 4, grp: "beta", val: 4 },
    ],
  },
  {
    id: "logical_correlated_scalar_max",
    expectedRows: [
      { id: 2, grp: "alpha", val: 5 },
      { id: 4, grp: "beta", val: 4 },
      { id: 5, grp: "gamma", val: 3 },
    ],
  },
  {
    id: "logical_named_window",
    expectedRows: [
      { id: 1, grp: "alpha", val: 2, running_total: 2, grp_rank: 2 },
      { id: 2, grp: "alpha", val: 5, running_total: 7, grp_rank: 1 },
      { id: 3, grp: "beta", val: 1, running_total: 1, grp_rank: 2 },
      { id: 4, grp: "beta", val: 4, running_total: 5, grp_rank: 1 },
      { id: 5, grp: "gamma", val: 3, running_total: 3, grp_rank: 1 },
    ],
  },
] as const;

describe("playground/logical-query-presets", () => {
  let prepared: PlaygroundPreparedInputSuccess | null = null;

  beforeAll(async () => {
    const scenario = SCENARIO_PRESETS[0];
    if (!scenario) {
      throw new Error("Missing default scenario preset.");
    }

    const result = await preparePlaygroundInput(
      DEFAULT_FACADE_SCHEMA_CODE,
      serializeJson(scenario.rows),
    );
    expect(result.ok, "scenario preparation failed").toBe(true);
    if (!result.ok) {
      return;
    }
    prepared = result;
  }, 20_000);

  it.each(LOGICAL_PRESET_EXPECTATIONS)(
    'returns the expected rows for "$id"',
    { timeout: 25_000 },
    async ({ id, expectedRows }) => {
      expect(prepared, "scenario preparation missing").not.toBeNull();
      if (!prepared) {
        return;
      }

      const query = QUERY_PRESETS.find((preset) => preset.id === id);
      if (!query) {
        throw new Error(`Missing query preset: ${id}`);
      }

      const compiled = compilePreparedPlaygroundQuery(prepared, query.sql);
      expect(compiled.ok, `${id} should compile`).toBe(true);
      if (!compiled.ok) {
        return;
      }

      const bundle = await createSession(compiled, SCENARIO_PRESETS[0]!.context);
      const rows = await bundle.session.runToCompletion();
      expect(rows).toEqual(expectedRows);
    },
  );
});
