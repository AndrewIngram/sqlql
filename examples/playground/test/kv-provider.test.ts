import { describe, expect, it } from "vitest";

import { createKvProvider } from "../src/kv-provider";

describe("playground/kv-provider", () => {
  it("supports multiple entities with independent mapRow transforms", async () => {
    const provider = createKvProvider<{ userId: string }>({
      name: "kvProvider",
      rows: [
        { key: "views:u1:p1", value: 7 },
        { key: "views:u2:p1", value: 9 },
        { key: "flags:p1", value: 1 },
        { key: "flags:p2", value: 0 },
      ],
      entities: {
        product_view_counts: {
          entity: "product_view_counts",
          columns: ["product_id", "view_count"] as const,
          mapRow({ key, value, context }) {
            const parts = key.split(":");
            if (parts.length !== 3 || parts[0] !== "views") {
              return null;
            }
            const [, userId, productId] = parts;
            if (!userId || !productId || userId !== context.userId || typeof value !== "number") {
              return null;
            }
            return {
              product_id: productId,
              view_count: value,
            };
          },
        },
        product_flags: {
          entity: "product_flags",
          columns: ["product_id", "is_hot"] as const,
          mapRow({ key, value }) {
            const parts = key.split(":");
            if (parts.length !== 2 || parts[0] !== "flags" || typeof value !== "number") {
              return null;
            }
            const productId = parts[1];
            if (!productId) {
              return null;
            }
            return {
              product_id: productId,
              is_hot: value === 1,
            };
          },
        },
      },
    });

    const context = { userId: "u1" };
    const viewsPlan = await provider.compile(
      {
        kind: "scan",
        provider: "kvProvider",
        table: "product_view_counts",
        request: {
          table: "product_view_counts",
          select: ["product_id", "view_count"],
        },
      },
      context,
    );
    const viewsRows = await provider.execute(viewsPlan, context);

    const flagsPlan = await provider.compile(
      {
        kind: "scan",
        provider: "kvProvider",
        table: "product_flags",
        request: {
          table: "product_flags",
          select: ["product_id", "is_hot"],
          orderBy: [{ column: "product_id", direction: "asc" }],
        },
      },
      context,
    );
    const flagsRows = await provider.execute(flagsPlan, context);

    expect(viewsRows).toEqual([{ product_id: "p1", view_count: 7 }]);
    expect(flagsRows).toEqual([
      { product_id: "p1", is_hot: true },
      { product_id: "p2", is_hot: false },
    ]);
  });

  it("applies lookupMany and residual filters over mapped rows", async () => {
    const provider = createKvProvider<{ userId: string }>({
      name: "kvProvider",
      rows: [
        { key: "views:u1:p1", value: 7 },
        { key: "views:u1:p2", value: 3 },
        { key: "views:u1:p3", value: 11 },
      ],
      entities: {
        product_view_counts: {
          entity: "product_view_counts",
          columns: ["product_id", "view_count"] as const,
          mapRow({ key, value, context }) {
            const parts = key.split(":");
            if (parts.length !== 3 || parts[0] !== "views" || parts[1] !== context.userId) {
              return null;
            }
            if (typeof value !== "number") {
              return null;
            }
            return {
              product_id: parts[2],
              view_count: value,
            };
          },
        },
      },
    });

    const rows = await provider.lookupMany?.(
      {
        table: "product_view_counts",
        key: "product_id",
        keys: ["p1", "p2", "p4"],
        select: ["product_id", "view_count"],
        where: [{ op: "gt", column: "view_count", value: 5 }],
      },
      { userId: "u1" },
    );

    expect(rows).toEqual([{ product_id: "p1", view_count: 7 }]);
  });
});
