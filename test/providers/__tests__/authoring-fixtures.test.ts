import { Result } from "better-result";
import { describe, expect, it } from "vite-plus/test";

import { createProviderConformanceCases } from "@tupl/provider-kit/testing";
import { unwrapProviderOperationResult } from "@tupl/provider-kit";
import { createExecutableSchema, createSchemaBuilder } from "@tupl/schema";

import {
  createSqlLikeConformanceOptions,
  createSqlLikeFixtureProvider,
} from "../../provider-fixtures/sql-like";
import { createUnusualScanLookupFixtureProvider } from "../../provider-fixtures/unusual-scan-lookup";

function unwrapResult<T>(result: Result<T, Error>): T {
  if (Result.isError(result)) {
    throw result.error;
  }

  return result.value;
}

describe("third-party authoring fixtures", () => {
  describe("ordinary SQL-like provider", () => {
    for (const conformanceCase of createProviderConformanceCases(
      createSqlLikeConformanceOptions(),
    )) {
      it(conformanceCase.name, async () => {
        await conformanceCase.run();
      });
    }

    it("can be used end to end through the public schema facade", async () => {
      const provider = createSqlLikeFixtureProvider();
      const builder = createSchemaBuilder<Record<string, never>>();

      builder.table("orders", provider.entities.orders, {
        columns: {
          id: "text",
          customer_id: "text",
          total_cents: "integer",
        },
      });

      const executableSchema = unwrapResult(createExecutableSchema(builder));
      const result = await executableSchema.query({
        context: {},
        sql: "SELECT id, total_cents FROM orders WHERE customer_id = 'c1' ORDER BY total_cents DESC LIMIT 1",
      });

      expect(Result.isOk(result)).toBe(true);
      if (Result.isError(result)) {
        throw result.error;
      }

      expect(result.value).toEqual([{ id: "o2", total_cents: 1800 }]);
    });
  });

  describe("unusual scan/lookup provider", () => {
    it("still works end to end through the public schema facade for scan-only pushdown", async () => {
      const provider = createUnusualScanLookupFixtureProvider();
      const builder = createSchemaBuilder<Record<string, never>>();

      builder.table("orders", provider.entities.orders, {
        columns: {
          id: "text",
          customer_id: "text",
          total_cents: "integer",
        },
      });

      const executableSchema = unwrapResult(createExecutableSchema(builder));
      const result = await executableSchema.query({
        context: {},
        sql: "SELECT id, total_cents FROM orders WHERE customer_id = 'c1' ORDER BY total_cents DESC LIMIT 2",
      });

      expect(Result.isOk(result)).toBe(true);
      if (Result.isError(result)) {
        throw result.error;
      }

      expect(result.value).toEqual([
        { id: "o2", total_cents: 1800 },
        { id: "o1", total_cents: 900 },
      ]);
    });

    it("exposes lookupMany without requiring relational pushdown support", async () => {
      const provider = createUnusualScanLookupFixtureProvider();
      const rows = unwrapProviderOperationResult(
        await provider.lookupMany(
          {
            table: "orders",
            key: "id",
            keys: ["o1", "o3"],
            select: ["id", "total_cents"],
          },
          {},
        ),
      );

      expect(rows).toEqual([
        { id: "o1", total_cents: 900 },
        { id: "o3", total_cents: 2600 },
      ]);
    });
  });
});
