import { describe, expect, it } from "vitest";

import {
  DB_PROVIDER_MODULE_ID,
  DEFAULT_DB_PROVIDER_CODE,
  DEFAULT_FACADE_SCHEMA_CODE,
  DEFAULT_GENERATED_DB_FILE_CODE,
  QUERY_PRESETS,
  SCENARIO_PRESETS,
  GENERATED_DB_MODULE_ID,
  KV_PROVIDER_MODULE_ID,
  serializeJson,
} from "../src/examples";
import {
  compilePlaygroundInput,
  compilePreparedPlaygroundQuery,
  createSession,
  preparePlaygroundInput,
  runSessionToCompletion,
} from "../src/session-runtime";

describe("playground/provider-pushdown", () => {
  it("executes simple same-provider joins and grouped aggregates as a single downstream query", { timeout: 15_000 }, async () => {
    const scenario = SCENARIO_PRESETS[0];
    if (!scenario) {
      throw new Error("Missing scenario preset.");
    }

    const pushdownPresetIds = [
      "orders_calculated_columns",
      "orders_with_vendors",
      "vendor_spend",
      "status_distinct",
      "paid_orders",
      "preferred_vendor_orders",
      "activity_union",
      "vendor_rank",
    ] as const;

    const prepared = await preparePlaygroundInput(
      DEFAULT_FACADE_SCHEMA_CODE,
      serializeJson(scenario.rows),
    );
    expect(prepared.ok).toBe(true);
    if (!prepared.ok) {
      return;
    }

    let reseed = true;
    for (const presetId of pushdownPresetIds) {
      const preset = QUERY_PRESETS.find((query) => query.id === presetId);
      if (!preset) {
        throw new Error(`Missing query preset: ${presetId}`);
      }

      const compiled = compilePreparedPlaygroundQuery(prepared, preset.sql);

      expect(compiled.ok).toBe(true);
      if (!compiled.ok) {
        continue;
      }

      const bundle = await createSession(compiled, scenario.context, { reseed });
      reseed = false;
      const plan = bundle.session.getPlan();

      expect(plan.steps).toHaveLength(1);
      expect(plan.steps[0]?.kind).toBe("remote_fragment");
      expect(plan.steps[0]?.request).toEqual({
        fragment: "rel",
      });

      const snapshot = await runSessionToCompletion(bundle.session, []);
      expect(
        snapshot.executedOperations.length,
        `${presetId} should execute as a single provider operation`,
      ).toBe(1);
      expect(snapshot.executedOperations[0]?.kind).toBe("sql_query");
      expect(snapshot.executedOperations[0]?.provider).toBe("dbProvider");
      const sqlText = snapshot.executedOperations[0]?.kind === "sql_query"
        ? snapshot.executedOperations[0].sql.toLowerCase()
        : "";
      if (
        presetId === "orders_calculated_columns"
      ) {
        expect(sqlText).toContain("/");
        expect(sqlText).toContain("order by");
      }
      if (
        presetId === "orders_with_vendors" ||
        presetId === "vendor_spend"
      ) {
        expect(sqlText).toContain(" join ");
      }
      if (presetId === "vendor_spend") {
        expect(sqlText).toContain("group by");
      }
      if (presetId === "status_distinct") {
        expect(sqlText).toContain("select distinct");
        expect(sqlText).toContain("order by");
      }
      if (presetId === "activity_union") {
        expect(sqlText).toContain("union all");
      }
      if (presetId === "vendor_rank") {
        expect(sqlText).toContain("with");
        expect(sqlText).toContain("dense_rank");
      }
    }
  });

  it("uses kv-provider module mapping code at runtime", async () => {
    const scenario = SCENARIO_PRESETS[0];
    if (!scenario) {
      throw new Error("Missing scenario preset.");
    }

    const customKvProviderCode = `
import { createKvProvider, playgroundKvRuntime, type KvProviderFactoryRuntime } from "@playground/kv-provider-core";

type QueryContext = { orgId: string; userId: string };

function parseViewCounterKey(raw: string): { userId: string; productId: string } | null {
  const separator = raw.indexOf(":");
  if (separator <= 0 || separator >= raw.length - 1) {
    return null;
  }

  const userId = raw.slice(0, separator).trim();
  const productId = raw.slice(separator + 1).trim();
  if (userId.length === 0 || productId.length === 0) {
    return null;
  }

  return { userId, productId };
}

export function createProvider(runtime: KvProviderFactoryRuntime) {
  return createKvProvider<QueryContext>({
    name: "kvProvider",
    rows: runtime.rows,
    recordOperation: runtime.recordOperation,
    entities: {
      product_view_counts: {
        entity: "product_view_counts",
        columns: ["product_id", "view_count"] as const,
        mapRow({ key, value, context }: { key: string; value: unknown; context: QueryContext }) {
          const parsed = parseViewCounterKey(key);
          if (!parsed || parsed.userId !== context.userId) {
            return null;
          }
          if (typeof value !== "number") {
            return null;
          }
          return {
            product_id: parsed.productId,
            view_count: value * 10,
          };
        },
      },
    },
  });
}

export const kvProvider = createProvider(playgroundKvRuntime);
    `.trim();

    const compiled = await compilePlaygroundInput(
      DEFAULT_FACADE_SCHEMA_CODE,
      serializeJson(scenario.rows),
      `
SELECT product_id, view_count
FROM product_view_counts
ORDER BY view_count DESC;
      `.trim(),
      {
        modules: {
          [DB_PROVIDER_MODULE_ID]: DEFAULT_DB_PROVIDER_CODE,
          [GENERATED_DB_MODULE_ID]: DEFAULT_GENERATED_DB_FILE_CODE,
          [KV_PROVIDER_MODULE_ID]: customKvProviderCode,
        },
      },
    );

    expect(compiled.ok).toBe(true);
    if (!compiled.ok) {
      return;
    }

    const bundle = await createSession(compiled, scenario.context);
    const snapshot = await runSessionToCompletion(bundle.session, []);
    expect(snapshot.executedOperations).toHaveLength(1);
    expect(snapshot.executedOperations[0]?.kind).toBe("kv_lookup");

    const rows = snapshot.result ?? [];
    expect(rows.length).toBeGreaterThan(0);
    for (const row of rows) {
      expect(typeof row.view_count).toBe("number");
      expect((row.view_count as number) % 10).toBe(0);
    }
  });
});
