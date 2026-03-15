import { describe, expect, it } from "vitest";

import {
  DB_PROVIDER_MODULE_ID,
  DEFAULT_DB_PROVIDER_CODE,
  DEFAULT_FACADE_SCHEMA_CODE,
  DEFAULT_GENERATED_DB_FILE_CODE,
  GENERATED_DB_MODULE_ID,
  QUERY_PRESETS,
  REDIS_PROVIDER_MODULE_ID,
  SCENARIO_PRESETS,
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
  it(
    "executes simple same-provider joins and grouped aggregates as a single downstream query",
    { timeout: 35_000 },
    async () => {
      const scenario = SCENARIO_PRESETS[0];
      if (!scenario) {
        throw new Error("Missing scenario preset.");
      }

      const pushdownPresetIds = [
        "orders_calculated_columns",
        "orders_with_vendors",
        "vendor_spend",
        "items_with_products",
        "status_distinct",
        "paid_orders",
        "preferred_vendor_orders",
        "activity_union",
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
        expect(plan.steps[0]?.kind).toBe("remote_fragment");
        expect(plan.steps[0]?.request).toMatchObject({
          relKind: expect.any(String),
        });
        expect(plan.steps.filter((step) => step.kind === "remote_fragment")).toHaveLength(1);

        const snapshot = await runSessionToCompletion(bundle.session, []);
        expect(
          snapshot.executedOperations.length,
          `${presetId} should execute as a single provider operation`,
        ).toBe(1);
        expect(snapshot.executedOperations[0]?.kind).toBe("sql_query");
        expect(snapshot.executedOperations[0]?.provider).toBe("dbProvider");
        const sqlText =
          snapshot.executedOperations[0]?.kind === "sql_query"
            ? snapshot.executedOperations[0].sql.toLowerCase()
            : "";
        if (presetId === "orders_calculated_columns") {
          expect(sqlText).toContain("/");
          expect(sqlText).toContain("order by");
        }
        if (presetId === "orders_with_vendors" || presetId === "vendor_spend") {
          expect(sqlText).toContain(" join ");
        }
        if (presetId === "items_with_products") {
          expect(sqlText).toContain(" join ");
          expect(sqlText).toContain("order by");
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
      }
    },
  );

  it("uses redis-provider module mapping code at runtime", async () => {
    const scenario = SCENARIO_PRESETS[0];
    if (!scenario) {
      throw new Error("Missing scenario preset.");
    }

    const customRedisProviderCode = `
import type { RedisLike } from "@tupl/provider-ioredis";
import { createIoredisProvider, playgroundIoredisRuntime } from "@playground/provider-ioredis-provider-core";

type QueryContext = {
  orgId: string;
  userId: string;
  redis: RedisLike;
};

export const redisProvider = createIoredisProvider<QueryContext>({
  name: "redisProvider",
  redis: (ctx: QueryContext) => ctx.redis,
  recordOperation: playgroundIoredisRuntime.recordOperation,
  entities: {
    product_view_counts: {
      entity: "product_view_counts",
      lookupKey: "product_id",
      columns: ["product_id", "view_count"] as const,
      buildRedisKey({ key, context }) {
        return \`product_view_counts:\${context.userId}:\${String(key)}\`;
      },
      decodeRow({ hash }) {
        if (typeof hash.product_id !== "string" || typeof hash.view_count !== "string") {
          return null;
        }
        const viewCount = Number(hash.view_count);
        if (!Number.isFinite(viewCount)) {
          return null;
        }
        return {
          product_id: hash.product_id,
          view_count: viewCount * 10,
        };
      },
    },
  },
});
    `.trim();

    const compiled = await compilePlaygroundInput(
      DEFAULT_FACADE_SCHEMA_CODE,
      serializeJson(scenario.rows),
      `
SELECT p.id AS product_id, v.view_count
FROM active_products p
LEFT JOIN product_view_counts v ON v.product_id = p.id
ORDER BY view_count DESC;
      `.trim(),
      {
        modules: {
          [DB_PROVIDER_MODULE_ID]: DEFAULT_DB_PROVIDER_CODE,
          [GENERATED_DB_MODULE_ID]: DEFAULT_GENERATED_DB_FILE_CODE,
          [REDIS_PROVIDER_MODULE_ID]: customRedisProviderCode,
        },
      },
    );

    expect(compiled.ok).toBe(true);
    if (!compiled.ok) {
      return;
    }

    const bundle = await createSession(compiled, scenario.context);
    const snapshot = await runSessionToCompletion(bundle.session, []);
    expect(snapshot.executedOperations.map((operation) => operation.kind)).toEqual([
      "sql_query",
      "redis_lookup",
    ]);

    const rows = snapshot.result ?? [];
    expect(rows.length).toBeGreaterThan(0);
    const rowsWithCounts = rows.filter(
      (row): row is typeof row & { view_count: number } => typeof row.view_count === "number",
    );
    expect(rowsWithCounts.length).toBeGreaterThan(0);
    for (const row of rowsWithCounts) {
      expect(typeof row.view_count).toBe("number");
      expect((row.view_count as number) % 10).toBe(0);
    }
  });

  it(
    "executes active_products to product_view_counts as one sql query plus one redis lookup",
    { timeout: 15_000 },
    async () => {
      const scenario = SCENARIO_PRESETS[0];
      if (!scenario) {
        throw new Error("Missing scenario preset.");
      }

      const prepared = await preparePlaygroundInput(
        DEFAULT_FACADE_SCHEMA_CODE,
        serializeJson(scenario.rows),
      );
      expect(prepared.ok).toBe(true);
      if (!prepared.ok) {
        return;
      }

      const compiled = compilePreparedPlaygroundQuery(
        prepared,
        `
SELECT p.name, v.view_count
FROM active_products p
LEFT JOIN product_view_counts v ON v.product_id = p.id
ORDER BY v.view_count DESC, p.name;
      `.trim(),
      );

      expect(compiled.ok).toBe(true);
      if (!compiled.ok) {
        return;
      }

      const bundle = await createSession(compiled, scenario.context);
      const plan = bundle.session.getPlan();
      expect(plan.steps.filter((step) => step.kind === "remote_fragment")).toHaveLength(1);
      expect(plan.steps.some((step) => step.kind === "lookup_join")).toBe(true);
      expect(plan.steps.some((step) => step.kind === "scan")).toBe(false);

      const snapshot = await runSessionToCompletion(bundle.session, []);

      expect(snapshot.executedOperations).toHaveLength(2);
      expect(snapshot.executedOperations.map((operation) => operation.kind)).toEqual([
        "sql_query",
        "redis_lookup",
      ]);
    },
  );
});
