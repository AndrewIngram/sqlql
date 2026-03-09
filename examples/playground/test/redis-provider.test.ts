import { describe, expect, it } from "vitest";

import { createIoredisProvider } from "../../../packages/provider-ioredis/src";
import { SCENARIO_PRESETS } from "../src/examples";
import { getPlaygroundRedisRuntime, reseedDownstreamDatabase } from "../src/pglite-runtime";

describe("playground/redis-provider", () => {
  it("reads seeded Redis hashes through lookupMany", async () => {
    const scenario = SCENARIO_PRESETS[0];
    if (!scenario) {
      throw new Error("Missing scenario preset.");
    }

    await reseedDownstreamDatabase(scenario.rows);
    const runtime = await getPlaygroundRedisRuntime();

    const provider = createIoredisProvider<{ userId: string }>({
      name: "redisProvider",
      redis: runtime.redis,
      entities: {
        product_view_counts: {
          entity: "product_view_counts",
          lookupKey: "product_id",
          columns: ["product_id", "view_count"] as const,
          buildRedisKey: ({ key, context }) =>
            `product_view_counts:${context.userId}:${String(key)}`,
          decodeRow: ({ hash }) => {
            if (typeof hash.product_id !== "string" || typeof hash.view_count !== "string") {
              return null;
            }
            const viewCount = Number(hash.view_count);
            if (!Number.isFinite(viewCount)) {
              return null;
            }
            return {
              product_id: hash.product_id,
              view_count: Math.trunc(viewCount),
            };
          },
        },
      },
    });

    const rows = (
      await provider.lookupMany!(
        {
          table: "product_view_counts",
          key: "product_id",
          keys: ["p_router", "p_support", "p_missing"],
          select: ["product_id", "view_count"],
          where: [{ op: "gt", column: "view_count", value: 30 }],
        },
        { userId: "u_alex" },
      )
    ).unwrap();

    expect(rows).toEqual([{ product_id: "p_router", view_count: 94 }]);
  });
});
