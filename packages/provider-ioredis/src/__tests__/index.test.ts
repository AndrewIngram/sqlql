import { describe, expect, it } from "vite-plus/test";

import type { RelNode } from "@tupl/foundation";
import { AdapterResult } from "@tupl/provider-kit";
import { createIoredisProvider, type RedisLike, type RedisPipelineLike } from "../index";

class StubPipeline implements RedisPipelineLike {
  private readonly keys: string[] = [];

  constructor(private readonly hashes: Map<string, Record<string, string>>) {}

  hgetall(key: string): RedisPipelineLike {
    this.keys.push(key);
    return this;
  }

  async exec() {
    return this.keys.map(
      (key) => [null, this.hashes.get(key) ?? {}] as [Error | null, Record<string, string>],
    );
  }
}

class StubRedis implements RedisLike {
  constructor(private readonly hashes: Map<string, Record<string, string>>) {}

  pipeline(): RedisPipelineLike {
    return new StubPipeline(this.hashes);
  }
}

describe("ioredis adapter", () => {
  it("exposes typed entity handles and executes keyed relational scan fragments", async () => {
    const provider = createIoredisProvider<{ tenant: string }>({
      name: "redisProvider",
      redis: new StubRedis(
        new Map([
          [
            "product_view_counts:acme:p1",
            {
              product_id: "p1",
              view_count: "8",
            },
          ],
        ]),
      ),
      entities: {
        product_view_counts: {
          entity: "product_view_counts",
          lookupKey: "product_id",
          columns: ["product_id", "view_count"] as const,
          shape: {
            product_id: { type: "text", nullable: false },
            view_count: { type: "integer", nullable: false },
          },
          buildRedisKey: ({ key, context }) =>
            `product_view_counts:${context.tenant}:${String(key)}`,
          decodeRow: ({ hash }) => ({
            product_id: hash.product_id ?? "",
            view_count: Number(hash.view_count ?? 0),
          }),
        },
      },
    });

    expect(provider.entities.product_view_counts).toEqual({
      kind: "data_entity",
      entity: "product_view_counts",
      provider: "redisProvider",
      columns: {
        product_id: {
          source: "product_id",
          type: "text",
          nullable: false,
        },
        view_count: {
          source: "view_count",
          type: "integer",
          nullable: false,
        },
      },
    });

    const scanRel: RelNode = {
      id: "redis:product_view_counts",
      kind: "scan",
      convention: "provider:redisProvider",
      table: "product_view_counts",
      select: ["product_id", "view_count"],
      where: [{ op: "eq", column: "product_id", value: "p1" }],
      output: [{ name: "product_id" }, { name: "view_count" }],
    };
    const scanCapability = await provider.canExecute(scanRel, { tenant: "acme" });
    expect(scanCapability).toBe(true);

    const plan = (await provider.compile(scanRel, { tenant: "acme" })).unwrap();
    expect(await provider.describeCompiledPlan?.(plan, { tenant: "acme" })).toMatchObject({
      kind: "redis_lookup_scan",
      summary: "redisProvider keyed hash lookup",
    });
    expect((await provider.execute(plan, { tenant: "acme" })).unwrap()).toEqual([
      { product_id: "p1", view_count: 8 },
    ]);
  });

  it("reports unsupported unconstrained scans", async () => {
    const provider = createIoredisProvider<{ tenant: string }>({
      name: "redisProvider",
      redis: new StubRedis(new Map()),
      entities: {
        product_view_counts: {
          entity: "product_view_counts",
          lookupKey: "product_id",
          columns: ["product_id", "view_count"] as const,
          buildRedisKey: ({ key }) => `product_view_counts:${String(key)}`,
          decodeRow: ({ hash }) => ({
            product_id: hash.product_id ?? "",
            view_count: Number(hash.view_count ?? 0),
          }),
        },
      },
    });

    const scanCapability = await provider.canExecute(
      {
        id: "redis:product_view_counts",
        kind: "scan",
        convention: "provider:redisProvider",
        table: "product_view_counts",
        select: ["product_id", "view_count"],
        output: [{ name: "product_id" }, { name: "view_count" }],
      },
      { tenant: "acme" },
    );
    if (typeof scanCapability === "boolean") {
      throw new Error("Expected a capability report for unconstrained scans.");
    }
    expect(scanCapability.supported).toBe(false);
    expect(scanCapability.reason).toBe(
      "Provider requires an equality or IN predicate on product_view_counts.product_id.",
    );
  });

  it("resolves lookupMany against Redis hashes with residual filtering and projection", async () => {
    const provider = createIoredisProvider<{ tenant: string }>({
      name: "redisProvider",
      redis: new StubRedis(
        new Map([
          [
            "product_view_counts:acme:p1",
            {
              product_id: "p1",
              view_count: "8",
            },
          ],
          [
            "product_view_counts:acme:p2",
            {
              product_id: "p2",
              view_count: "3",
            },
          ],
          [
            "product_view_counts:globex:p1",
            {
              product_id: "p1",
              view_count: "19",
            },
          ],
        ]),
      ),
      entities: {
        product_view_counts: {
          entity: "product_view_counts",
          lookupKey: "product_id",
          columns: ["product_id", "view_count"] as const,
          buildRedisKey: ({ key, context }) =>
            `product_view_counts:${context.tenant}:${String(key)}`,
          decodeRow: ({ hash }) => {
            if (!hash.product_id || !hash.view_count) {
              return null;
            }
            return {
              product_id: hash.product_id,
              view_count: Number(hash.view_count),
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
          keys: ["p1", "p2", "p3"],
          select: ["product_id", "view_count"],
          where: [{ op: "gt", column: "view_count", value: 5 }],
        },
        { tenant: "acme" },
      )
    ).unwrap();

    expect(rows).toEqual([{ product_id: "p1", view_count: 8 }]);
  });

  it("fails unknown entities and invalid lookup columns", async () => {
    const provider = createIoredisProvider<{ tenant: string }>({
      name: "redisProvider",
      redis: new StubRedis(new Map()),
      entities: {
        product_view_counts: {
          entity: "product_view_counts",
          lookupKey: "product_id",
          columns: ["product_id", "view_count"] as const,
          buildRedisKey: ({ key }) => `product_view_counts:${String(key)}`,
          decodeRow: ({ hash }) => ({
            product_id: hash.product_id ?? "",
            view_count: Number(hash.view_count ?? 0),
          }),
        },
      },
    });

    const missingEntity = await provider.lookupMany!(
      {
        table: "missing",
        key: "product_id",
        keys: ["p1"],
        select: ["product_id"],
      },
      { tenant: "acme" },
    );
    expect(AdapterResult.isError(missingEntity)).toBe(true);
    if (AdapterResult.isError(missingEntity)) {
      expect(missingEntity.error.message).toBe("Unknown Redis entity missing.");
    }

    const wrongKey = await provider.lookupMany!(
      {
        table: "product_view_counts",
        key: "view_count",
        keys: [1],
        select: ["product_id"],
      },
      { tenant: "acme" },
    );
    expect(AdapterResult.isError(wrongKey)).toBe(true);

    const wrongColumn = await provider.lookupMany!(
      {
        table: "product_view_counts",
        key: "product_id",
        keys: ["p1"],
        select: ["missing"],
      },
      { tenant: "acme" },
    );
    expect(AdapterResult.isError(wrongColumn)).toBe(true);
  });
});
